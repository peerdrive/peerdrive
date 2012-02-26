%% PeerDrive
%% Copyright (C) 2011  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
%%
%% This program is free software: you can redistribute it and/or modify
%% it under the terms of the GNU General Public License as published by
%% the Free Software Foundation, either version 3 of the License, or
%% (at your option) any later version.
%%
%% This program is distributed in the hope that it will be useful,
%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
%% GNU General Public License for more details.
%%
%% You should have received a copy of the GNU General Public License
%% along with this program.  If not, see <http://www.gnu.org/licenses/>.

-module(peerdrive_sync_worker).
-behavior(gen_fsm).

-include("store.hrl").
-include("utils.hrl").

-export([start_link/3]).
-export([init/1, code_change/4, handle_event/3, handle_info/3,
	handle_sync_event/4, terminate/3]).
-export([working/2, waiting/2, paused/2, error/2]).

-record(state, {syncfun, from, to, fromsid, tosid, monitor, numdone,
	numremain, backlog}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Mode, Store, Peer) ->
	gen_fsm:start_link(?MODULE, {Mode, Store, Peer}, []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% High level store sync logic
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Mode, FromSId, ToSId}) ->
	SyncFun = case Mode of
		ff     -> fun sync_doc_ff/5;
		latest -> fun sync_doc_latest/5;
		merge  -> fun sync_doc_merge/5
	end,
	case peerdrive_volman:store(FromSId) of
		{ok, FromPid} ->
			case peerdrive_volman:store(ToSId) of
				{ok, ToPid} ->
					{ok, Monitor} = peerdrive_work:new({sync, FromSId, ToSId}),
					peerdrive_vol_monitor:register_proc(),
					process_flag(trap_exit, true),
					S = #state{
						syncfun   = SyncFun,
						from      = FromPid,
						fromsid   = FromSId,
						to        = ToPid,
						tosid     = ToSId,
						monitor   = Monitor,
						numdone   = 0,
						numremain = 0,
						backlog   = []
					},
					error_logger:info_report([{sync, start}, {from, FromSId},
						{to, ToSId}]),
					gen_fsm:send_event(self(), changed),
					{ok, waiting, S};

				error ->
					{stop, enxio}
			end;

		error ->
			{stop, enxio}
	end.


terminate(Reason, State, #state{from=FromStore, tosid=ToSId} = S) ->
	error_logger:info_report([{sync, stop}, {from, S#state.fromsid},
		{to, S#state.tosid}, {reason, Reason}]),
	peerdrive_work:delete(S#state.monitor),
	peerdrive_vol_monitor:deregister_proc(),
	(FromStore == undefined) or (State == waiting)
		orelse peerdrive_store:sync_finish(FromStore, ToSId).


handle_info({vol_event, rem_store, FromSId, _}, _, #state{fromsid=FromSId} = S) ->
	{stop, normal, S#state{from=undefined}};

handle_info({vol_event, rem_store, ToSId, _}, _, #state{tosid=ToSId} = S) ->
	{stop, normal, S};

handle_info({vol_event, mod_doc, FromSId, _Doc}, waiting, #state{fromsid=FromSId} = S) ->
	gen_fsm:send_event(self(), changed),
	{next_state, waiting, S};

handle_info({work_req, Req}, State, #state{monitor=Monitor} = S) ->
	case State of
		working ->
			case Req of
				pause ->
					peerdrive_work:pause(Monitor),
					{next_state, paused, S};
				_ ->
					{next_state, working, S, 0}
			end;

		waiting ->
			{next_state, waiting, S};

		Halted ->
			case Req of
				{resume, false} ->
					peerdrive_work:resume(Monitor),
					{next_state, working, S, 0};
				{resume, true} ->
					peerdrive_work:resume(Monitor),
					skip(S);
				_ ->
					{next_state, Halted, S}
			end
	end;

handle_info(_, working, S) ->
	{next_state, working, S, 0};

handle_info(_, State, S) ->
	{next_state, State, S}.


handle_event(_Event, working, StateData) ->
	{next_state, working, StateData, 0};

handle_event(_Event, StateName, StateData) ->
	{next_state, StateName, StateData}.


handle_sync_event(_Event, _From, working, StateData) ->
	{next_state, working, StateData, 0};

handle_sync_event(_Event, _From, StateName, StateData) ->
	{next_state, StateName, StateData}.


code_change(_OldVsn, StateName, StateData, _Extra) ->
	{ok, StateName, StateData}.


waiting(changed, S) ->
	#state{
		from=FromStore,
		tosid=ToSId,
		monitor=Monitor
	} = S,
	case peerdrive_store:sync_get_changes(FromStore, ToSId) of
		{ok, []} ->
			peerdrive_store:sync_finish(FromStore, ToSId),
			{next_state, waiting, S};
		{ok, Backlog} ->
			peerdrive_work:start(Monitor),
			S2 = S#state{backlog=Backlog, numdone=0, numremain=length(Backlog)},
			{next_state, working, S2, 0};
		{error, Reason} ->
			peerdrive_work:start(Monitor),
			peerdrive_work:error(Monitor, [{code, Reason}]),
			{next_state, error, S}
	end.


working(timeout, #state{backlog=[]} = S) ->
	#state{from=FromStore, tosid=ToSId, monitor=Monitor} = S,
	case peerdrive_store:sync_get_changes(FromStore, ToSId) of
		{ok, []} ->
			peerdrive_store:sync_finish(FromStore, ToSId),
			peerdrive_work:stop(Monitor),
			{next_state, waiting, S};

		{ok, NewBacklog} ->
			NewRemain = S#state.numremain + length(NewBacklog),
			peerdrive_work:progress(Monitor, S#state.numdone * 256 div NewRemain),
			S2 = S#state{backlog=NewBacklog, numremain=NewRemain},
			{next_state, working, S2, 0};

		{error, Reason} ->
			peerdrive_work:error(Monitor, [{code, Reason}]),
			{next_state, error, S}
	end;

working(timeout, #state{backlog=[Change|Backlog], monitor=Monitor} = S) ->
	try
		sync_step(Change, S),
		NewDone = S#state.numdone + 1,
		peerdrive_work:progress(Monitor, NewDone * 256 div S#state.numremain),
		S2 = S#state{backlog=Backlog, numdone=NewDone},
		{next_state, working, S2, 0}
	catch
		throw:ErrInfo ->
			peerdrive_work:error(Monitor, ErrInfo),
			{next_state, error, S}
	end;

working(changed, S) ->
	{next_state, working, S, 0}.


paused(changed, S) ->
	{next_state, paused, S}.


error(changed, S) ->
	{next_state, error, S}.


skip(#state{backlog=[]} = S) ->
	{next_state, working, S, 0};

skip(#state{backlog=[{_Doc, SeqNum} | Backlog], numdone=Done} = S) ->
	case peerdrive_store:sync_set_anchor(S#state.from, S#state.tosid, SeqNum) of
		ok ->
			S2 = S#state{backlog=Backlog, numdone=Done+1},
			{next_state, working, S2, 0};

		{error, Reason} ->
			peerdrive_work:error(S#state.monitor, [{code, Reason}]),
			{next_state, error, S}
	end.


sync_step({Doc, SeqNum}, S) ->
	#state{
		syncfun  = SyncFun,
		from     = FromStore,
		to       = ToStore,
		tosid    = ToSId
	} = S,
	peerdrive_sync_locks:lock(Doc),
	try
		sync_doc(Doc, FromStore, ToStore, SyncFun)
	after
		peerdrive_sync_locks:unlock(Doc)
	end,
	case peerdrive_store:sync_set_anchor(FromStore, ToSId, SeqNum) of
		ok ->
			ok;
		{error, Reason} ->
			throw([{code, Reason}])
	end.


sync_doc(Doc, From, To, SyncFun) ->
	case peerdrive_store:lookup(To, Doc) of
		{ok, ToRev, _PreRevs} ->
			case peerdrive_store:lookup(From, Doc) of
				{ok, ToRev, _} ->
					% alread the same
					ok;
				{ok, FromRev, _} ->
					SyncFun(Doc, From, FromRev, To, ToRev);
				error ->
					% deleted -> ignore
					ok
			end;
		error ->
			% doesn't exist on destination -> ignore
			ok
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions for fast-forward merge
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


sync_doc_ff(Doc, From, FromRev, To, ToRev) ->
	sync_doc_ff(Doc, From, FromRev, To, ToRev, 3).


sync_doc_ff(Doc, _From, NewRev, _To, _OldRev, 0) ->
	throw([{code, econflict}, {doc, Doc}, {rev, NewRev}]);

sync_doc_ff(Doc, From, NewRev, To, OldRev, Tries) ->
	case peerdrive_broker:forward_doc(To, Doc, OldRev, NewRev, From, []) of
		ok ->
			ok;
		{error, econflict} ->
			sync_doc_ff(Doc, From, NewRev, To, OldRev, Tries-1);
		{error, Reason} ->
			throw([{code, Reason}, {doc, Doc}, {rev, NewRev}])
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions for automatic merging
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

sync_doc_latest(Doc, From, FromRev, To, ToRev) ->
	sync_doc_merge(Doc, From, FromRev, To, ToRev, fun latest_strategy/6).


sync_doc_merge(Doc, From, FromRev, To, ToRev) ->
	sync_doc_merge(Doc, From, FromRev, To, ToRev, fun merge_strategy/6).


sync_doc_merge(Doc, From, FromRev, To, ToRev, Strategy) ->
	Graph = peerdrive_mergebase:new([FromRev, ToRev], [From, To]),
	try
		case peerdrive_mergebase:ff_head(Graph) of
			{ok, FromRev} ->
				% simple fast forward
				sync_doc_ff(Doc, From, FromRev, To, ToRev);

			{ok, ToRev} ->
				% just the other side was updated -> nothing for us
				ok;

			error ->
				case peerdrive_mergebase:merge_bases(Graph) of
					{ok, BaseRevs} ->
						% FIXME: This assumes that we found the optimal merge
						% base. Currently thats not necessarily the case...
						BaseRev = hd(BaseRevs),
						%% The strategy handler will create a merge commit in
						%% `From'.  The sync_worker will pick it up again and can
						%% simply forward it to the other store via fast-forward.
						Strategy(Doc, From, FromRev, To, ToRev, BaseRev);

					error ->
						% no common ancestor -> must fall back to "latest"
						latest_strategy(Doc, From, FromRev, To, ToRev, undefined)
				end
		end
	after
		peerdrive_mergebase:delete(Graph)
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 'latest' strategy
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

latest_strategy(Doc, From, FromRev, To, ToRev, _BaseRev) ->
	FromStat = check(peerdrive_broker:stat(FromRev, [From]), Doc, FromRev),
	ToStat = check(peerdrive_broker:stat(ToRev, [To]), Doc, ToRev),
	if
		FromStat#rev_stat.mtime >= ToStat#rev_stat.mtime ->
			% worker will pick up again the new merge rev
			Handle = check(peerdrive_broker:update(From, Doc, FromRev, undefined),
				Doc, FromRev),
			try
				check(peerdrive_broker:merge(Handle, To, ToRev, []), Doc, ToRev),
				check(peerdrive_broker:commit(Handle), Doc, FromRev)
			after
				peerdrive_broker:close(Handle)
			end;

		true ->
			% The other revision is newer. The other directions
			% sync_worker will pick it up.
			ok
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 'simple' strategy
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

merge_strategy(Doc, From, FromRev, To, ToRev, BaseRev) ->
	FromStat = check(peerdrive_broker:stat(FromRev, [From]), Doc, FromRev),
	ToStat = check(peerdrive_broker:stat(ToRev, [To]), Doc, ToRev),
	BaseStat = check(peerdrive_broker:stat(BaseRev, [From, To]), Doc, BaseRev),
	TypeSet = sets:from_list([
		BaseStat#rev_stat.type,
		FromStat#rev_stat.type,
		ToStat#rev_stat.type
	]),
	case get_handler_fun(TypeSet) of
		none ->
			% fall back to 'latest' strategy
			latest_strategy(Doc, From, FromRev, To, ToRev, BaseRev);

		HandlerFun ->
			HandlerFun(Doc, From, To, BaseRev, FromRev, ToRev)
	end.


% FIXME: hard coded at the moment
get_handler_fun(TypeSet) ->
	case sets:to_list(TypeSet) of
		[Folder] when Folder =:= <<"org.peerdrive.store">>;
		              Folder =:= <<"org.peerdrive.folder">> ->
			Handlers = orddict:from_list([
				{<<"META">>, fun merge_meta/4},
				{<<"PDSD">>, fun merge_folder/4}
			]),
			fun(Doc, From, To, BaseRev, FromRev, ToRev) ->
				merge(Doc, From, To, BaseRev, FromRev, ToRev, Handlers)
			end;

		_ ->
			none
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Generic merge algoritm
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%
%% TODO: Support addition and removal of whole parts
%%
merge(Doc, From, To, BaseRev, FromRev, ToRev, Handlers) ->
	#rev_stat{parts=FromParts} = check(peerdrive_broker:stat(FromRev, [From]),
		Doc, FromRev),
	#rev_stat{parts=ToParts} = check(peerdrive_broker:stat(ToRev, [To]), Doc,
		ToRev),
	#rev_stat{parts=BaseParts} = check(peerdrive_broker:stat(BaseRev, [From, To]),
		Doc, BaseRev),
	OtherParts = ToParts ++ FromParts,

	% Merge only changed parts, except META because we want to update the comment
	Parts = [ FourCC || {FourCC, _Size, Hash} <- BaseParts,
		(FourCC == <<"META">>) orelse
		lists:any(
			fun({F,_,H}) -> (F =:= FourCC) and (H =/= Hash) end,
			OtherParts) ],

	% Read all changed parts
	FromData = merge_read(Doc, FromRev, Parts, [From]),
	ToData   = merge_read(Doc, ToRev, Parts, [To]),
	BaseData = merge_read(Doc, BaseRev, Parts, [From, To]),

	{_Conflict, NewData} = merge_parts(Doc, BaseData, FromData, ToData, Handlers,
		false, []),

	% TODO: set a 'conflict' flag in the future?
	merge_write(Doc, From, FromRev, To, ToRev, NewData).


merge_read(Doc, Rev, _Parts, []) ->
	throw([{code, enoent}, {doc, Doc}, {rev, Rev}]);

merge_read(Doc, Rev, Parts, [Store | Rest]) ->
	case peerdrive_broker:peek(Store, Rev) of
		{ok, Reader} ->
			try
				[ {Part, merge_read_part(Doc, Rev, Reader, Part)} || Part <- Parts ]
			after
				peerdrive_broker:close(Reader)
			end;

		{error, enoent} ->
			merge_read(Doc, Rev, Parts, Rest);

		{error, Reason} ->
			throw([{code, Reason}, {doc, Doc}, {rev, Rev}])
	end.


merge_read_part(Doc, Rev, Reader, Part) ->
	merge_read_part(Doc, Rev, Reader, Part, 0, <<>>).


merge_read_part(Doc, Rev, Reader, Part, Offset, Acc) ->
	case check(peerdrive_broker:read(Reader, Part, Offset, 16#10000), Doc, Rev) of
		<<>> ->
			Acc;
		Data ->
			merge_read_part(Doc, Rev, Reader, Part, Offset+size(Data),
				<<Acc/binary, Data/binary>>)
	end.


merge_parts(_Doc, [], [], [], _Handlers, Conflicts, Acc) ->
	{Conflicts, Acc};

merge_parts(Doc,
		[{Part, Base} | BaseData],
		[{Part, From} | FromData],
		[{Part, To} | ToData],
		Handlers, Conflicts, Acc) ->
	Handler = orddict:fetch(Part, Handlers),
	{NewConflict, Data} = Handler(Doc, Base, From, To),
	merge_parts(Doc, BaseData, FromData, ToData, Handlers, Conflicts or NewConflict,
		[{Part, Data} | Acc]).


merge_write(Doc, From, FromRev, To, ToRev, NewData) ->
	Writer = check(peerdrive_broker:update(From, Doc, FromRev,
		<<"org.peerdrive.syncer">>), Doc, FromRev),
	try
		check(peerdrive_broker:merge(Writer, To, ToRev, []), Doc, ToRev),
		lists:foreach(
			fun({Part, Data}) ->
				check(peerdrive_broker:truncate(Writer, Part, 0), Doc, FromRev),
				check(peerdrive_broker:write(Writer, Part, 0, Data), Doc, FromRev)
			end,
			NewData),
		check(peerdrive_broker:commit(Writer), Doc, FromRev)
	after
		peerdrive_broker:close(Writer)
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Content handlers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

merge_meta(Doc, Base, From, To) ->
	case peerdrive_struct:merge(decode(Doc, Base), [decode(Doc, From), decode(Doc, To)]) of
		{ok, Data} ->
			{false, peerdrive_struct:encode(merge_update_meta(Data))};
		{econflict, Data} ->
			{true, peerdrive_struct:encode(merge_update_meta(Data))};
		error ->
			throw([{code, eio}, {doc, Doc}])
	end.


merge_folder(Doc, RawBase, RawFrom, RawTo) ->
	Base = folder_make_gb_tree(decode(Doc, RawBase)),
	From = folder_make_gb_tree(decode(Doc, RawFrom)),
	To   = folder_make_gb_tree(decode(Doc, RawTo)),
	{Conflict, New} = case peerdrive_struct:merge(Base, [From, To]) of
		{ok, Data} ->
			{false, Data};
		{econflict, Data} ->
			{true, Data};
		error ->
			throw([{code, eio}, {doc, Doc}])
	end,
	{Conflict, peerdrive_struct:encode(gb_trees:values(New))}.


folder_make_gb_tree(Folder) ->
	lists:foldl(
		fun(Entry, Acc) ->
			gb_trees:enter(gb_trees:get(<<>>, Entry), Entry, Acc)
		end,
		gb_trees:empty(),
		Folder).


decode(Doc, Data) ->
	try
		peerdrive_struct:decode(Data)
	catch
		error:_ ->
			throw([{code, eio}, {doc, Doc}])
	end.


%% update comment
merge_update_meta(Data) ->
	update_meta_field(
		[<<"org.peerdrive.annotation">>, <<"comment">>],
		<<"<<Synchronized by system>>">>,
		Data).


update_meta_field([Key], Value, Meta) when ?IS_GB_TREE(Meta) ->
	gb_trees:enter(Key, Value, Meta);

update_meta_field([Key | Path], Value, Meta) when ?IS_GB_TREE(Meta) ->
	NewValue = case gb_trees:lookup(Key, Meta) of
		{value, OldValue} -> update_meta_field(Path, Value, OldValue);
		none              -> update_meta_field(Path, Value, gb_trees:empty())
	end,
	gb_trees:enter(Key, NewValue, Meta);

update_meta_field(_Path, _Value, Meta) ->
	Meta. % Path conflicts with existing data


check(BrokerResult, Doc, Rev) ->
	case BrokerResult of
		{error, Reason} ->
			throw([{code, Reason}, {doc, Doc}, {rev, Rev}]);
		{ok, Result} ->
			Result;
		ok ->
			ok
	end.

