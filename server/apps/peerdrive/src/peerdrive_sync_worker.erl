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
	numremain, backlog, lastdone}).

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
					SeqNum = init_get_anchor(FromPid, FromSId, ToPid, ToSId),
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
						backlog   = [],
						lastdone  = {clean, SeqNum}
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


init_get_anchor(FromPid, FromSId, ToPid, ToSId) ->
	case peerdrive_store:sync_get_anchor(ToPid, FromSId, ToSId) of
		{ok, SN1} ->
			case peerdrive_store:sync_get_anchor(FromPid, FromSId, ToSId) of
				{ok, SN2} ->
					if SN1 < SN2 -> SN1; true -> SN2 end;
				_ ->
					% source store is not authorative -> ignore errors
					SN1
			end;

		{error, enoent} ->
			0;

		{error, Reason} ->
			throw(Reason)
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
		lastdone = {_, Anchor},
		from=FromStore,
		tosid=ToSId,
		monitor=Monitor
	} = S,
	case peerdrive_store:sync_get_changes(FromStore, ToSId, Anchor) of
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
	#state{
		lastdone = {_, Anchor},
		from=FromStore,
		tosid=ToSId,
		monitor=Monitor
	} = S,
	case sync_done(S) of
		{ok, S2} ->
			case peerdrive_store:sync_get_changes(FromStore, ToSId, Anchor) of
				{ok, []} ->
					peerdrive_store:sync_finish(FromStore, ToSId),
					peerdrive_work:stop(Monitor),
					{next_state, waiting, S2};

				{ok, NewBacklog} ->
					NewRemain = S2#state.numremain + length(NewBacklog),
					peerdrive_work:progress(Monitor, S2#state.numdone * 256 div NewRemain),
					S3 = S2#state{backlog=NewBacklog, numremain=NewRemain},
					{next_state, working, S3, 0};

				{error, Reason} ->
					peerdrive_work:error(Monitor, [{code, Reason}]),
					{next_state, error, S2}
			end;

		{error, ErrInfo} ->
			peerdrive_work:error(Monitor, ErrInfo),
			{next_state, error, S}
	end;

working(timeout, #state{backlog=[Change|Backlog], monitor=Monitor} = S) ->
	try
		S2 = sync_step(Change, S),
		NewDone = S2#state.numdone + 1,
		peerdrive_work:progress(Monitor, NewDone * 256 div S2#state.numremain),
		S3 = S2#state{backlog=Backlog, numdone=NewDone},
		{next_state, working, S3, 0}
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

skip(#state{backlog=[{_Doc, SeqNum} | Backlog]} = S) ->
	#state{
		numdone=Done,
		lastdone = {Changed, _PrevSeqNum}
	} = S,
	S2 = S#state{backlog=Backlog, numdone=Done+1, lastdone={Changed, SeqNum}},
	{next_state, working, S2, 0}.


sync_step({Doc, SeqNum}, S) ->
	#state{
		syncfun  = SyncFun,
		from     = FromStore,
		to       = ToStore,
		lastdone = {Changed, _PrevSeqNum}
	} = S,
	peerdrive_sync_locks:lock(Doc),
	try
		case sync_doc(Doc, FromStore, ToStore, SyncFun) of
			ok ->
				S#state{lastdone={dirty, SeqNum}};
			skip ->
				S#state{lastdone={Changed, SeqNum}}
		end
	after
		peerdrive_sync_locks:unlock(Doc)
	end.


sync_done(S) ->
	#state{
		lastdone = {State, SeqNum},
		from     = FromStore,
		fromsid  = FromSId,
		to       = ToStore,
		tosid    = ToSId
	} = S,
	try
		State == dirty andalso check_simple(peerdrive_store:sync(ToStore)),
		check_simple(peerdrive_store:sync_set_anchor(ToStore, FromSId, ToSId, SeqNum)),
		peerdrive_store:sync_set_anchor(FromStore, FromSId, ToSId, SeqNum),
		{ok, S#state{lastdone={clean, SeqNum}}}
	catch
		throw:ErrInfo -> {error, ErrInfo}
	end.


sync_doc(Doc, From, To, SyncFun) ->
	case peerdrive_store:lookup(To, Doc) of
		{ok, ToRev, _PreRevs} ->
			case peerdrive_store:lookup(From, Doc) of
				{ok, ToRev, _} ->
					% alread the same
					skip;
				{ok, FromRev, _} ->
					SyncFun(Doc, From, FromRev, To, ToRev);
				{error, enoent} ->
					% deleted -> ignore
					skip;
				{error, Reason} ->
					throw([{code, Reason}, {doc, Doc}])
			end;
		{error, enoent} ->
			% doesn't exist on destination -> ignore
			skip;
		{error, Reason} ->
			throw([{code, Reason}, {doc, Doc}])
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
				skip;

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
		FromStat#rev.mtime >= ToStat#rev.mtime ->
			% worker will pick up again the new merge rev
			Handle = check(peerdrive_broker:update(From, Doc, FromRev, undefined),
				Doc, FromRev),
			try
				check(peerdrive_broker:merge(Handle, To, ToRev, []), Doc, ToRev),
				check(peerdrive_broker:commit(Handle), Doc, FromRev)
			after
				peerdrive_broker:close(Handle)
			end,
			ok;

		true ->
			% The other revision is newer. The other directions
			% sync_worker will pick it up.
			skip
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 'simple' strategy
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

merge_strategy(Doc, From, FromRev, To, ToRev, BaseRev) ->
	FromStat = check(peerdrive_broker:stat(FromRev, [From]), Doc, FromRev),
	ToStat = check(peerdrive_broker:stat(ToRev, [To]), Doc, ToRev),
	BaseStat = check(peerdrive_broker:stat(BaseRev, [From, To]), Doc, BaseRev),
	TypeSet = sets:from_list([
		BaseStat#rev.type,
		FromStat#rev.type,
		ToStat#rev.type
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
				{data, fun merge_folder/4}
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
	#rev{attachments=FromParts} = check(peerdrive_broker:stat(FromRev, [From]),
		Doc, FromRev),
	#rev{attachments=ToParts} = check(peerdrive_broker:stat(ToRev, [To]), Doc,
		ToRev),
	#rev{attachments=BaseParts} = check(peerdrive_broker:stat(BaseRev, [From, To]),
		Doc, BaseRev),
	OtherParts = ToParts ++ FromParts,

	% Merge only changed parts
	Parts = [data] ++ [ Name || #rev_att{name=Name, hash=Hash} <- BaseParts,
		lists:any(
			fun(#rev_att{name=N,hash=H}) -> (N =:= Name) and (H =/= Hash) end,
			OtherParts) ],

	% Read all changed parts
	FromData = merge_read(Doc, FromRev, Parts, [From]),
	ToData   = merge_read(Doc, ToRev, Parts, [To]),
	BaseData = merge_read(Doc, BaseRev, Parts, [From, To]),

	{_Conflict, NewData} = merge_parts(Doc, BaseData, FromData, ToData, Handlers,
		false, []),

	% TODO: set a 'conflict' flag in the future?
	merge_write(Doc, From, FromRev, To, ToRev, NewData),
	ok.


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


merge_read_part(Doc, Rev, Reader, data) ->
	try
		peerdrive_struct:decode(check(peerdrive_broker:get_data(Reader, <<>>)
			, Doc, Rev))
	catch
		error:_ ->
			throw([{code, eio}, {doc, Doc}, {rev, Rev}])
	end;

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
			fun
				({data, Data}) ->
					check(peerdrive_broker:set_data(Writer, <<>>,
						peerdrive_struct:encode(Data)), Doc, FromRev);
				({Part, Data}) ->
					check(peerdrive_broker:truncate(Writer, Part, 0), Doc, FromRev),
					check(peerdrive_broker:write(Writer, Part, 0, Data), Doc, FromRev)
			end,
			NewData),
		check(peerdrive_broker:commit(Writer, <<"<<Synchronized by system>>">>),
			Doc, FromRev)
	after
		peerdrive_broker:close(Writer)
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Content handlers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

merge_folder(Doc, Base, From, To) ->
	% FIXME: The following code merges the data without checking for other
	% keys. This may lead to data loss!
	{Conflict1, NewMeta} = merge_folder_meta(Doc,
		get_default(<<"org.peerdrive.annotation">>, Base, gb_trees:empty()),
		get_default(<<"org.peerdrive.annotation">>, From, gb_trees:empty()),
		get_default(<<"org.peerdrive.annotation">>, To, gb_trees:empty())),
	{Conflict2, NewFolder} = merge_folder_content(Doc,
		get_default(<<"org.peerdrive.folder">>, Base, []),
		get_default(<<"org.peerdrive.folder">>, From, []),
		get_default(<<"org.peerdrive.folder">>, To, [])),
	New1 = update_default(<<"org.peerdrive.folder">>, NewFolder, Base, []),
	New2 = update_default(<<"org.peerdrive.annotation">>, NewMeta, New1, gb_trees:empty()),
	{Conflict1 or Conflict2, New2}.


merge_folder_meta(Doc, Base, From, To) ->
	case peerdrive_struct:merge(Base, [From, To]) of
		{ok, Data} ->
			{false, Data};
		{econflict, Data} ->
			{true, Data};
		error ->
			throw([{code, eio}, {doc, Doc}])
	end.


merge_folder_content(Doc, RawBase, RawFrom, RawTo) ->
	Base = folder_make_gb_tree(RawBase),
	From = folder_make_gb_tree(RawFrom),
	To   = folder_make_gb_tree(RawTo),
	{Conflict, New} = case peerdrive_struct:merge(Base, [From, To]) of
		{ok, Data} ->
			{false, Data};
		{econflict, Data} ->
			{true, Data};
		error ->
			throw([{code, eio}, {doc, Doc}])
	end,
	{Conflict, gb_trees:values(New)}.


folder_make_gb_tree(Folder) ->
	lists:foldl(
		fun(Entry, Acc) ->
			gb_trees:enter(gb_trees:get(<<>>, Entry), Entry, Acc)
		end,
		gb_trees:empty(),
		Folder).


check(BrokerResult, Doc, Rev) ->
	case BrokerResult of
		{error, Reason} ->
			throw([{code, Reason}, {doc, Doc}, {rev, Rev}]);
		{ok, Result} ->
			Result;
		ok ->
			ok
	end.


check_simple(Result) ->
	case Result of
		ok ->
			ok;
		{error, Reason} ->
			throw([{code, Reason}])
	end.


get_default(Key, Tree, Default) ->
	case gb_trees:lookup(Key, Tree) of
		{value, Value} -> Value;
		none -> Default
	end.

update_default(Key, Value, Tree, Default) ->
	case gb_trees:is_defined(Key, Tree) of
		true ->
			gb_trees:update(Key, Value, Tree);
		false when Value =/= Default ->
			gb_trees:enter(Key, Value, Tree);
		false ->
			Tree
	end.

