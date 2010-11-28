%% Hotchpotch
%% Copyright (C) 2010  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
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

-module(sync_worker).

-include("store.hrl").

-export([start_link/3]).
-export([init/4]).

-record(state, {syncfun, from, to, monitor, numdone, numremain}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Mode, Store, Peer) ->
	proc_lib:start_link(?MODULE, init, [self(), Mode, Store, Peer]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% High level store sync logic
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Parent, Mode, FromGuid, ToGuid) ->
	SyncFun = case Mode of
		ff     -> fun sync_doc_ff/3;
		latest -> fun sync_doc_latest/3;
		merge  -> fun sync_doc_merge/3
	end,
	case volman:store(FromGuid) of
		{ok, FromPid} ->
			case volman:store(ToGuid) of
				{ok, ToPid} ->
					Id = {FromGuid, ToGuid},
					{ok, Monitor} = hysteresis:start({sync, FromGuid, ToGuid}),
					vol_monitor:register_proc(Id),
					proc_lib:init_ack(Parent, {ok, self()}),
					State = #state{
						syncfun   = SyncFun,
						from      = {FromGuid, FromPid},
						to        = {ToGuid, ToPid},
						monitor   = Monitor,
						numdone   = 0,
						numremain = 0
					},
					try
						loop(State, [])
					catch
						throw:Term -> error_logger:warning_msg(
							"sync_worker: exit: ~p~n", [Term])
					end,
					hysteresis:stop(Monitor),
					vol_monitor:deregister_proc(Id);

				error ->
					proc_lib:init_ack(Parent, {error, {notfound, ToGuid}})
			end;

		error ->
			proc_lib:init_ack(Parent, {error, {notfound, FromGuid}})
	end,
	normal.


loop(State, OldBacklog) ->
	#state{
		from      = {_, FromPid},
		to        = {ToGuid, _},
		monitor   = Monitor,
		numdone   = OldDone,
		numremain = OldRemain
	} = State,
	case OldBacklog of
		[] ->
			NewDone = 1,
			Backlog = case store:sync_get_changes(FromPid, ToGuid) of
				{ok, Value} -> Value;
				Error       -> throw(Error)
			end,
			NewRemain = length(Backlog),
			case NewRemain of
				0 -> ok;
				_ -> hysteresis:started(Monitor)
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
				[] -> hysteresis:done(Monitor);
				_  -> hysteresis:progress(Monitor, NewDone * 256 div NewRemain)
			end;

		[] ->
			NewBacklog = [],
			Timeout = infinity
	end,
	loop_check_msg(State#state{numdone=NewDone, numremain=NewRemain}, NewBacklog, Timeout).


loop_check_msg(State, Backlog, Timeout) ->
	#state{
		from = {FromGuid, FromPid},
		to   = {ToGuid, _}
	} = State,
	receive
		{trigger_mod_doc, FromGuid, _Doc} ->
			loop_check_msg(State, Backlog, 0);
		{trigger_rem_store, FromGuid} ->
			ok;
		{trigger_rem_store, ToGuid} ->
			store:sync_finish(FromPid, ToGuid);

		% deliberately ignore all other messages
		_ -> loop_check_msg(State, Backlog, Timeout)
	after
		Timeout -> loop(State, Backlog)
	end.


sync_step({Doc, SeqNum}, S) ->
	#state{
		syncfun  = SyncFun,
		from     = {_, FromPid} = From,
		to       = {ToGuid, _}  = To
	} = S,
	sync_doc(Doc, From, To, SyncFun),
	case store:sync_set_anchor(FromPid, ToGuid, SeqNum) of
		ok -> ok;
		Error -> throw(Error)
	end.


sync_doc(Doc, From, To, SyncFun) ->
	sync_locks:lock(Doc),
	try
		SyncFun(Doc, From, To)
	catch
		throw:Term -> Term
	end,
	sync_locks:unlock(Doc).
	%case Result of
	%	ok ->
	%		ok;
	%	{error, conflict} ->
	%		ok;
	%	{error, _Reason} ->
	%		% TODO: log error? where? inform user?
	%		ok
	%end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions for fast-forward merge
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


sync_doc_ff(Doc, From, To) ->
	throws(broker:sync(Doc, 0, [From, To])).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions for automatic merging
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

sync_doc_latest(Doc, From, To) ->
	sync_doc_merge(Doc, From, To, fun latest_strategy/7).


sync_doc_merge(Doc, From, To) ->
	sync_doc_merge(Doc, From, To, fun simple_strategy/7).


sync_doc_merge(Doc, {FromGuid, _} = From, {ToGuid, _} = To, Strategy) ->
	case broker:lookup_doc(Doc, [From, To]) of
		{[{_Rev, _}], _PreRevs} ->
			ok;
		{[], []} ->
			ok;
		{Revs, _PreRevs} ->
			case broker:sync(Doc, 0, [From, To]) of
				{ok, _ErrInfo, _Rev} ->
					ok;

				{error, conflict, _ErrInfo} ->
					{FromRev, _} = lists:keyfind([FromGuid], 2, Revs),
					{ToRev, _} = lists:keyfind([ToGuid], 2, Revs),
					FromStat = throws(broker:stat(FromRev, [From])),
					ToStat = throws(broker:stat(ToRev, [To])),
					%% The strategy handler will create a merge commit in
					%% `From'.  The sync_worker will pick it up again and can
					%% simply forward it to the other store via fast-forward.
					Strategy(Doc, From, FromRev, FromStat, To, ToRev, ToStat);

				{error, Reason, _ErrInfo} ->
					{error, Reason}
			end
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 'latest' strategy
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

latest_strategy(Doc, From, FromRev, FromStat, _To, ToRev, ToStat) ->
	if
		FromStat#rev_stat.mtime >= ToStat#rev_stat.mtime ->
			% worker will pick up again the new merge rev
			Handle = throws(broker:update(Doc, FromRev, keep, [From])),
			try
				throws(broker:set_parents(Handle, [FromRev, ToRev])),
				throws(broker:commit(Handle))
			after
				broker:close(Handle)
			end;

		true ->
			% The other revision is newer. The other directions
			% sync_worker will pick it up.
			ok
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 'simple' strategy
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

simple_strategy(Doc, From, FromRev, FromStat, To, ToRev, ToStat) ->
	Stores = [From, To],
	case get_merge_base([FromRev, ToRev], Stores) of
		{ok, BaseRevs} ->
			% FIXME: This test assumes that get_merge_base/1 found the optimal
			% merge base. Currently thats not necessarily the case...
			BaseRev = hd(BaseRevs),
			BaseStat = throws(broker:stat(BaseRev, Stores)),
			TypeSet = sets:from_list([
				BaseStat#rev_stat.type,
				FromStat#rev_stat.type,
				ToStat#rev_stat.type
			]),
			case get_handler_fun(TypeSet) of
				none ->
					% fall back to 'latest' strategy
					latest_strategy(Doc, From, FromRev, FromStat, To, ToRev, ToStat);

				HandlerFun ->
					HandlerFun(Doc, From, To, BaseRev, FromRev, ToRev, TypeSet)
			end;

		error ->
			% no common ancestor
			latest_strategy(Doc, From, FromRev, FromStat, To, ToRev, ToStat)
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


%% This algorithm is brutally simple. It will return the first common ancestor
%% it finds. This may not be an optimal choice at all...
%%
get_merge_base(Revs, Stores) ->
	Paths = lists:map(fun(Rev) -> {[Rev], sets:new()} end, Revs),
	get_merge_base_loop(Paths, Stores).


get_merge_base_loop(Paths, Stores) ->
	{Heads, PathSets} = lists:unzip(Paths),
	% did we already find a common rev?
	Common = sets:intersection(PathSets),
	case sets:size(Common) of
		0 ->
			% traverse deeper into the history
			case lists:all(fun(H) -> H == [] end, Heads) of
				true ->
					% no heads anymore -> cannot traverse further
					error;

				false ->
					NewPaths = lists:map(
						fun({OldHeads, OldPath}) ->
							traverse(OldHeads, OldPath, Stores)
						end,
						Paths),
					get_merge_base_loop(NewPaths, Stores)
			end;

		% seems so :)
		_ ->
			{ok, sets:to_list(Common)}
	end.


traverse(Heads, Path, Stores) ->
	lists:foldl(
		fun(Head, {AccHeads, AccPath}) ->
			case broker:stat(Head, Stores) of
				{ok, _Errors, #rev_stat{parents=Parents}} ->
					NewHeads = Parents ++ AccHeads,
					NewPath = sets:add_element(Head, AccPath),
					{NewHeads, NewPath};

				{error, _, _} ->
					{AccHeads, AccPath}
			end
		end,
		{[], Path},
		Heads).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Content handlers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%
%% Content handler for HPSD only documents. Will crash if any rev contains a
%% part *not* containing HPSD data.
%%

merge_hpsd(Doc, From, To, BaseRev, FromRev, ToRev, TypeSet) ->
	[FromData, ToData, BaseData] = merge_hpsd_read([FromRev, ToRev, BaseRev],
		[From, To]),
	NewData = merge_hpsd_parts(BaseData, FromData, ToData, []),
	[Type] = sets:to_list(TypeSet),
	merge_hpsd_write(Doc, From, FromRev, ToRev, Type, NewData).


merge_hpsd_read(Revs, Stores) ->
	#rev_stat{parts=Parts} = throws(broker:stat(hd(Revs), Stores)),
	FCCs = lists:map(fun({FourCC, _Size, _Hash}) -> FourCC end, Parts),
	merge_hpsd_read_loop(Revs, Stores, FCCs, []).


merge_hpsd_read_loop([], _Stores, _FCCs, Acc) ->
	lists:reverse(Acc);

merge_hpsd_read_loop([Rev|Revs], Stores, FCCs, Acc) ->
	Reader = throws(broker:peek(Rev, Stores)),
	Data = try
		merge_hpsd_read_loop_part_loop(Reader, FCCs, [])
	after
		broker:close(Reader)
	end,
	merge_hpsd_read_loop(Revs, Stores, FCCs, [Data | Acc]).


merge_hpsd_read_loop_part_loop(_Reader, [], Acc) ->
	Acc;

merge_hpsd_read_loop_part_loop(Reader, [Part | Remaining], Acc) ->
	Data = read_loop(Reader, Part, 0, <<>>),
	case catch struct:decode(Data) of
		{'EXIT', _Reason} ->
			throw({error, econvert});

		Struct ->
			merge_hpsd_read_loop_part_loop(Reader, Remaining,
				[{Part, Struct} | Acc])
	end.


read_loop(Reader, Part, Offset, Acc) ->
	Length = 16#10000,
	case throws(broker:read(Reader, Part, Offset, Length)) of
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
	case struct:merge(Base, [From, To]) of
		{ok, Data} ->
			merge_hpsd_parts(BaseData, FromData, ToData, [{Part, Data} | Acc]);

		{conflict, Data} ->
			% ignore conflicts
			merge_hpsd_parts(BaseData, FromData, ToData, [{Part, Data} | Acc]);

		error ->
			throw({error, baddata})
	end.


merge_hpsd_write(Doc, From, FromRev, ToRev, Type, NewData) ->
	Writer = throws(broker:update(Doc, FromRev, <<"org.hotchpotch.syncer">>, [From])),
	try
		throws(broker:set_parents(Writer, [FromRev, ToRev])),
		throws(broker:set_type(Writer, Type)),
		lists:foreach(
			fun({Part, Data}) ->
				FinalData = if
					Part == <<"META">> -> merge_hpsd_update_meta(Data);
					true               -> Data
				end,
				throws(broker:truncate(Writer, Part, 0)),
				throws(broker:write(Writer, Part, 0, struct:encode(FinalData)))
			end,
			NewData),
		throws(broker:commit(Writer))
	after
		broker:close(Writer)
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
		{ok, _ErrInfo} ->
			ok;
		{ok, _ErrInfo, Result} ->
			Result;
		{error, Reason, _ErrInfo} ->
			throw({error, Reason})
	end.

