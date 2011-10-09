%% Hotchpotch
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

-module(hotchpotch_sync_worker).

-include("store.hrl").

-export([start_link/3]).
-export([init/4]).

-record(state, {syncfun, from, to, fromsid, tosid, monitor, numdone,
	numremain, parent}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Mode, Store, Peer) ->
	proc_lib:start_link(?MODULE, init, [self(), Mode, Store, Peer]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% High level store sync logic
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Parent, Mode, FromSId, ToSId) ->
	SyncFun = case Mode of
		ff     -> fun sync_doc_ff/5;
		latest -> fun sync_doc_latest/5;
		merge  -> fun sync_doc_merge/5
	end,
	case hotchpotch_volman:store(FromSId) of
		{ok, FromPid} ->
			case hotchpotch_volman:store(ToSId) of
				{ok, ToPid} ->
					Id = {FromSId, ToSId},
					{ok, Monitor} = hotchpotch_hysteresis:start({sync, FromSId, ToSId}),
					hotchpotch_vol_monitor:register_proc(Id),
					proc_lib:init_ack(Parent, {ok, self()}),
					process_flag(trap_exit, true),
					State = #state{
						syncfun   = SyncFun,
						from      = FromPid,
						fromsid   = FromSId,
						to        = ToPid,
						tosid     = ToSId,
						monitor   = Monitor,
						numdone   = 0,
						numremain = 0,
						parent    = Parent
					},
					error_logger:info_report([{sync, start}, {from, FromSId},
						{to, ToSId}]),
					Reason = try
						loop(State, [])
					catch
						throw:Term -> Term
					end,
					error_logger:info_report([{sync, stop}, {from, FromSId},
						{to, ToSId}, {reason, Reason}]),
					hotchpotch_hysteresis:stop(Monitor),
					hotchpotch_vol_monitor:deregister_proc(Id),
					exit(Reason);

				error ->
					proc_lib:init_ack(Parent, {error, enxio})
			end;

		error ->
			proc_lib:init_ack(Parent, {error, enxio})
	end.


loop(State, OldBacklog) ->
	#state{
		from      = FromStore,
		tosid     = ToSId,
		monitor   = Monitor,
		numdone   = OldDone,
		numremain = OldRemain
	} = State,
	case OldBacklog of
		[] ->
			NewDone = 1,
			Backlog = case hotchpotch_store:sync_get_changes(FromStore, ToSId) of
				{ok, Value} -> Value;
				Error       -> throw(Error)
			end,
			NewRemain = length(Backlog),
			case NewRemain of
				0 -> ok;
				_ -> hotchpotch_hysteresis:started(Monitor)
			end;

		_  ->
			Backlog    = OldBacklog,
			NewDone    = OldDone + 1,
			NewRemain  = OldRemain
	end,
	case Backlog of
		[Change | NewBacklog] ->
			sync_step(Change, State),
			Timeout = 0,
			case NewBacklog of
				[] -> hotchpotch_hysteresis:done(Monitor);
				_  -> hotchpotch_hysteresis:progress(Monitor, NewDone * 256 div NewRemain)
			end;

		[] ->
			NewBacklog = [],
			Timeout = infinity
	end,
	loop_check_msg(State#state{numdone=NewDone, numremain=NewRemain}, NewBacklog, Timeout).


loop_check_msg(State, Backlog, Timeout) ->
	#state{
		from    = FromStore,
		fromsid = FromSId,
		tosid   = ToSId,
		parent  = Parent
	} = State,
	receive
		{trigger_mod_doc, FromSId, _Doc} ->
			loop_check_msg(State, Backlog, 0);
		{trigger_rem_store, FromSId} ->
			normal;
		{trigger_rem_store, ToSId} ->
			hotchpotch_store:sync_finish(FromStore, ToSId),
			normal;
		{'EXIT', Parent, Reason} ->
			Reason;
		{'EXIT', _, normal} ->
			loop_check_msg(State, Backlog, Timeout);
		{'EXIT', _, Reason} ->
			Reason;

		% deliberately ignore all other messages
		_ -> loop_check_msg(State, Backlog, Timeout)
	after
		Timeout -> loop(State, Backlog)
	end.


sync_step({Doc, SeqNum}, S) ->
	#state{
		syncfun  = SyncFun,
		from     = FromStore,
		to       = ToStore,
		tosid    = ToSId
	} = S,
	sync_doc(Doc, FromStore, ToStore, SyncFun),
	case hotchpotch_store:sync_set_anchor(FromStore, ToSId, SeqNum) of
		ok -> ok;
		Error -> throw(Error)
	end.


sync_doc(Doc, From, To, SyncFun) ->
	hotchpotch_sync_locks:lock(Doc),
	try
		case hotchpotch_store:lookup(To, Doc) of
			{ok, ToRev, _PreRevs} ->
				case hotchpotch_store:lookup(From, Doc) of
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
		end
	catch
		throw:Term -> Term
	after
		hotchpotch_sync_locks:unlock(Doc)
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions for fast-forward merge
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


sync_doc_ff(Doc, From, FromRev, To, ToRev) ->
	sync_doc_ff(Doc, From, FromRev, To, ToRev, 3).


sync_doc_ff(_Doc, _From, _NewRev, _To, _OldRev, 0) ->
	{error, econflict};

sync_doc_ff(Doc, From, NewRev, To, OldRev, Tries) ->
	case hotchpotch_broker:forward_doc(To, Doc, OldRev, NewRev, From, 0) of
		ok ->
			ok;
		{error, econflict} ->
			sync_doc_ff(Doc, From, NewRev, To, OldRev, Tries-1);
		{error, _} = Error ->
			Error
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions for automatic merging
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

sync_doc_latest(Doc, From, FromRev, To, ToRev) ->
	sync_doc_merge(Doc, From, FromRev, To, ToRev, fun latest_strategy/6).


sync_doc_merge(Doc, From, FromRev, To, ToRev) ->
	sync_doc_merge(Doc, From, FromRev, To, ToRev, fun simple_strategy/6).


sync_doc_merge(Doc, From, FromRev, To, ToRev, Strategy) ->
	Graph = hotchpotch_mergebase:new([FromRev, ToRev], [From, To]),
	try
		case hotchpotch_mergebase:ff_head(Graph) of
			{ok, FromRev} ->
				% simple fast forward
				sync_doc_ff(Doc, From, FromRev, To, ToRev);

			{ok, ToRev} ->
				% just the other side was updated -> nothing for us
				ok;

			error ->
				case hotchpotch_mergebase:merge_bases(Graph) of
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
		hotchpotch_mergebase:delete(Graph)
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 'latest' strategy
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

latest_strategy(Doc, From, FromRev, To, ToRev, _BaseRev) ->
	FromStat = throws(hotchpotch_broker:stat(FromRev, [From])),
	ToStat = throws(hotchpotch_broker:stat(ToRev, [To])),
	if
		FromStat#rev_stat.mtime >= ToStat#rev_stat.mtime ->
			% worker will pick up again the new merge rev
			Handle = throws(hotchpotch_broker:update(From, Doc, FromRev, undefined)),
			try
				throws(hotchpotch_broker:merge(Handle, To, ToRev, 0)),
				throws(hotchpotch_broker:commit(Handle))
			after
				hotchpotch_broker:close(Handle)
			end;

		true ->
			% The other revision is newer. The other directions
			% sync_worker will pick it up.
			ok
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 'simple' strategy
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

simple_strategy(Doc, From, FromRev, To, ToRev, BaseRev) ->
	FromStat = throws(hotchpotch_broker:stat(FromRev, [From])),
	ToStat = throws(hotchpotch_broker:stat(ToRev, [To])),
	BaseStat = throws(hotchpotch_broker:stat(BaseRev, [From, To])),
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
			HandlerFun(Doc, From, To, BaseRev, FromRev, ToRev, TypeSet)
	end.


% FIXME: hard coded at the moment
get_handler_fun(TypeSet) ->
	case sets:to_list(TypeSet) of
		[Type] ->
			case Type of
				<<"org.hotchpotch.store">>  -> fun merge_hpsd/7;
				<<"org.hotchpotch.dict">>   -> fun merge_hpsd/7;
				<<"org.hotchpotch.set">>    -> fun merge_hpsd/7;
				_ -> none
			end;

		_ ->
			none
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Content handlers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%
%% Content handler for HPSD only documents. Will crash if any rev contains a
%% part *not* containing HPSD data.
%%

merge_hpsd(Doc, From, To, BaseRev, FromRev, ToRev, TypeSet) ->
	#rev_stat{parts=Parts} = throws(hotchpotch_broker:stat(FromRev, [From])),
	FCCs = [ FourCC || {FourCC, _Size, _Hash} <- Parts ],

	FromData = merge_hpsd_read(FromRev, FCCs, [From]),
	ToData   = merge_hpsd_read(ToRev, FCCs, [To]),
	BaseData = merge_hpsd_read(BaseRev, FCCs, [From, To]),

	NewData = merge_hpsd_parts(BaseData, FromData, ToData, []),
	[Type] = sets:to_list(TypeSet),
	merge_hpsd_write(Doc, From, FromRev, To, ToRev, Type, NewData).


merge_hpsd_read(_Rev, _FCCs, []) ->
	throw({error, enoent});

merge_hpsd_read(Rev, FCCs, [Store | Rest]) ->
	case hotchpotch_broker:peek(Store, Rev) of
		{ok, Reader} ->
			try
				merge_hpsd_read_parts(Reader, FCCs)
			after
				hotchpotch_broker:close(Reader)
			end;

		{error, enoent} ->
			merge_hpsd_read(Rev, FCCs, Rest);

		Error ->
			throw(Error)
	end.


merge_hpsd_read_parts(Reader, FCCs) ->
	merge_hpsd_read_parts(Reader, FCCs, []).


merge_hpsd_read_parts(_Reader, [], Acc) ->
	Acc;

merge_hpsd_read_parts(Reader, [Part | Remaining], Acc) ->
	Data = read_loop(Reader, Part, 0, <<>>),
	case catch hotchpotch_struct:decode(Data) of
		{'EXIT', _Reason} ->
			throw({error, econvert});

		Struct ->
			merge_hpsd_read_parts(Reader, Remaining, [{Part, Struct} | Acc])
	end.


read_loop(Reader, Part, Offset, Acc) ->
	Length = 16#10000,
	case throws(hotchpotch_broker:read(Reader, Part, Offset, Length)) of
		<<>> ->
			Acc;
		Data ->
			read_loop(Reader, Part, Offset+size(Data),
				<<Acc/binary, Data/binary>>)
	end.


merge_hpsd_parts([], [], [], Acc) ->
	Acc;

merge_hpsd_parts(
		[{Part, Base} | BaseData],
		[{Part, From} | FromData],
		[{Part, To} | ToData],
		Acc) ->
	case hotchpotch_struct:merge(Base, [From, To]) of
		{ok, Data} ->
			merge_hpsd_parts(BaseData, FromData, ToData, [{Part, Data} | Acc]);

		{econflict, Data} ->
			% ignore conflicts
			merge_hpsd_parts(BaseData, FromData, ToData, [{Part, Data} | Acc]);

		error ->
			throw({error, baddata})
	end.


merge_hpsd_write(Doc, From, FromRev, To, ToRev, Type, NewData) ->
	Writer = throws(hotchpotch_broker:update(From, Doc, FromRev,
		<<"org.hotchpotch.syncer">>)),
	try
		throws(hotchpotch_broker:merge(Writer, To, ToRev, 0)),
		throws(hotchpotch_broker:set_type(Writer, Type)),
		lists:foreach(
			fun({Part, Data}) ->
				FinalData = if
					Part == <<"META">> -> merge_hpsd_update_meta(Data);
					true               -> Data
				end,
				throws(hotchpotch_broker:truncate(Writer, Part, 0)),
				throws(hotchpotch_broker:write(Writer, Part, 0, hotchpotch_struct:encode(FinalData)))
			end,
			NewData),
		throws(hotchpotch_broker:commit(Writer))
	after
		hotchpotch_broker:close(Writer)
	end.


%% update comment
merge_hpsd_update_meta(Data) ->
	update_meta_field(
		[<<"org.hotchpotch.annotation">>, <<"comment">>],
		<<"<<Synchronized by system>>">>,
		Data).


update_meta_field([Key], Value, Meta) when is_record(Meta, dict, 9) ->
	dict:store(Key, Value, Meta);

update_meta_field([Key | Path], Value, Meta) when is_record(Meta, dict, 9) ->
	NewValue = case dict:find(Key, Meta) of
		{ok, OldValue} -> update_meta_field(Path, Value, OldValue);
		error          -> update_meta_field(Path, Value, dict:new())
	end,
	dict:store(Key, NewValue, Meta);

update_meta_field(_Path, _Value, Meta) ->
	Meta. % Path conflicts with existing data


throws(BrokerResult) ->
	case BrokerResult of
		{error, _Reason} = Error ->
			throw(Error);
		{ok, Result} ->
			Result;
		ok ->
			ok
	end.

