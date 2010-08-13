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

-export([start_link/3]).
-export([init/4]).

-record(state, {syncfun, fromguid, fromifc, toguid, toifc, monitor, numdone, numremain}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Mode, Store, Peer) ->
	proc_lib:start_link(?MODULE, init, [self(), Mode, Store, Peer]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% High level store sync logic
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Parent, Mode, StoreGuid, ToGuid) ->
	SyncFun = case Mode of
		ff ->
			fun sync_uuid_ff/5;

		automerge ->
			fun (P1, P2, P3, P4, P5) ->
				sync_uuid_merge(P1, P2, P3, P4, P5, true)
			end;

		savemerge ->
			fun (P1, P2, P3, P4, P5) ->
				sync_uuid_merge(P1, P2, P3, P4, P5, false)
			end
	end,
	case volman:store(StoreGuid) of
		{ok, StoreIfc} ->
			case volman:store(ToGuid) of
				{ok, ToIfc} ->
					Id = {StoreGuid, ToGuid},
					{ok, Monitor} = hysteresis:start({sync, StoreGuid, ToGuid}),
					vol_monitor:register_proc(Id),
					proc_lib:init_ack(Parent, {ok, self()}),
					State = #state{
						syncfun   = SyncFun,
						fromguid  = StoreGuid,
						fromifc   = StoreIfc,
						toguid    = ToGuid,
						toifc     = ToIfc,
						monitor   = Monitor,
						numdone   = 0,
						numremain = 0
					},
					loop(State, []),
					hysteresis:stop(Monitor),
					vol_monitor:deregister_proc(Id);

				error ->
					proc_lib:init_ack(Parent, {error, {notfound, ToGuid}})
			end;

		error ->
			proc_lib:init_ack(Parent, {error, {notfound, StoreGuid}})
	end,
	normal.


loop(#state{fromifc=FromIfc, toguid=ToGuid, monitor=Monitor, numdone=OldDone, numremain=OldRemain} = State, OldBacklog) ->
	case OldBacklog of
		[] ->
			NewDone = 1,
			Backlog = store:sync_get_changes(FromIfc, ToGuid),
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


loop_check_msg(#state{fromguid=FromGuid, toguid=ToGuid} = State, Backlog, Timeout) ->
	receive
		{trigger_mod_doc, FromGuid, _Uuid} -> loop_check_msg(State, Backlog, 0);
		{trigger_rem_store, FromGuid}      -> ok;
		{trigger_rem_store, ToGuid}        -> ok;

		% deliberately ignore all other messages
		_ -> loop_check_msg(State, Backlog, Timeout)
	after
		Timeout -> loop(State, Backlog)
	end.


sync_step(
		{Uuid, SeqNum},
		#state{syncfun=SyncFun, fromifc=FromIfc, toguid=ToGuid, toifc=ToIfc}) ->
	sync_uuid(Uuid, FromIfc, ToIfc, SyncFun),
	store:sync_set_anchor(FromIfc, ToGuid, SeqNum).


sync_uuid(Uuid, FromIfc, ToIfc, SyncFun) ->
	case store:lookup(FromIfc, Uuid) of
		{ok, FromRev} ->
			case store:lookup(ToIfc, Uuid) of
				{ok, FromRev} ->
					% points to same revision. done :)
					ok;

				{ok, ToRev} ->
					% need to do something
					case SyncFun(Uuid, FromIfc, FromRev, ToIfc, ToRev) of
						ok ->
							vol_monitor:trigger_mod_doc(local, Uuid);
						ignore ->
							ok;
						{error, _Reason} ->
							% TODO: log error?
							ok
					end;

				error ->
					% doesn't exist on destination store
					ok
			end;

		error ->
			ok
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions for fast-forward merge
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


sync_uuid_ff(Uuid, _FromIfc, FromRev, ToIfc, ToRev) ->
	case store:lookup(ToIfc, Uuid) of
		{ok, FromRev} ->
			ignore; % phantom change

		_Else ->
			case is_ff_head(FromRev, ToRev) of
				true ->
					case store:put_uuid(ToIfc, Uuid, ToRev, FromRev) of
						ok ->
							% We're done. The replicator will kick in
							% automatically at this stage...
							% FIXME: Really? The Rev is not yet on the destination store!
							ok;

						{error, _Reason} = Error ->
							% TODO: retry in case of `conflict'
							Error
					end;

				false ->
					% cannot be fast-forwarded
					ignore
			end
	end.


is_ff_head(FromRev, ToRev) ->
	case broker:stat(ToRev) of
		{ok, _Flags, _Parts, _Parents, Mtime, _Uti, _Volumes} ->
			is_ff_head_search([FromRev], ToRev, Mtime - 60*60*24);
		error ->
			false
	end.


is_ff_head_search([], _ToRev, _MinMtime) ->
	false;

is_ff_head_search([FromRev|OtherRevs], ToRev, MinMtime) ->
	case broker:stat(FromRev) of
		{ok, _Flags, _Parts, Parents, Mtime, _Uti, _Volumes} ->
			if
				FromRev == ToRev ->
					true;

				Mtime < MinMtime ->
					false;

				true ->
					case is_ff_head_search(Parents, ToRev, MinMtime) of
						true ->
							true;
						false ->
							is_ff_head_search(OtherRevs, ToRev, MinMtime)
					end
			end;

		error ->
			false
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions for automatic merging
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


sync_uuid_merge(Uuid, FromIfc, FromRev, ToIfc, ToRev, Force) ->
	case get_merge_base([FromRev, ToRev]) of
		{ok, BaseRevs} ->
			% FIXME: This test assumes that get_merge_base/1 found the optimal
			% merge base. Currently thats not necessarily the case...
			case lists:member(ToRev, BaseRevs) of
				true ->
					% can be handled by fast-forward
					sync_uuid_ff(Uuid, FromIfc, FromRev, ToIfc, ToRev);

				false ->
					% seems to be a real merge
					BaseRev = hd(BaseRevs),
					UtiSet = get_utis([BaseRev, FromRev, ToRev]),
					case get_sync_fun(UtiSet) of
						none ->
							% fall back to fast-forward
							% FIXME: don't we already known it's no FF?
							sync_uuid_ff(Uuid, FromIfc, FromRev, ToIfc, ToRev);

						SyncFun ->
							SyncFun(FromIfc, Uuid, BaseRev, FromRev,
								ToRev, UtiSet, Force)
					end
			end;

		error ->
			% no common ancestor
			ignore
	end.


get_utis(Revs) ->
	lists:foldl(
		fun(Rev, Acc) ->
			case broker:stat(Rev) of
				{ok, _Flags, _Parts, _Parents, _Mtime, Uti, _Volumes} ->
					sets:add_element(Uti, Acc);
				error ->
					Acc
			end
		end,
		sets:new(),
		Revs).


% FIXME: hard coded at the moment
get_sync_fun(UtiSet) ->
	case sets:to_list(UtiSet) of
		[Uti] ->
			case Uti of
				<<"org.hotchpotch.volume">> -> fun merge_hpsd/7;
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
get_merge_base(Revs) ->
	Paths = lists:map(fun(Rev) -> {[Rev], sets:new()} end, Revs),
	get_merge_base_loop(Paths).


get_merge_base_loop(Paths) ->
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
						fun({OldHeads, OldPath}) -> traverse(OldHeads, OldPath) end,
						Paths),
					get_merge_base_loop(NewPaths)
			end;

		% seems so :)
		_ ->
			{ok, sets:to_list(Common)}
	end.


traverse(Heads, Path) ->
	lists:foldl(
		fun(Head, {AccHeads, AccPath}) ->
			case broker:stat(Head) of
				{ok, _Flags, _Parts, Parents, _Mtime, _Uti, _Volumes} ->
					NewHeads = Parents ++ AccHeads,
					NewPath = sets:add_element(Head, AccPath),
					{NewHeads, NewPath};

				error ->
					{AccHeads, AccPath}
			end
		end,
		{[], Path},
		Heads).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Merge strategies
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%%
%% Merge algo for HPSD only documents. Just creates the merged document in
%% `Store'. The sync_worker will pick it up again and can simply forward it to
%% the other store via fast-forward.
%%

merge_hpsd(Store, Uuid, BaseRev, FromRev, ToRev, UtiSet, Force) ->
	case merge_hpsd_read([FromRev, ToRev, BaseRev]) of
		{ok, [FromData, ToData, BaseData]} ->
			case merge_hpsd_parts(BaseData, FromData, ToData, [], Force) of
				{ok, NewData} ->
					[Uti] = sets:to_list(UtiSet),
					merge_hpsd_write(Store, Uuid, FromRev, ToRev, Uti, NewData);

				{error, _} = Error ->
					Error
			end;

		{error, _} = Error ->
			Error
	end.


merge_hpsd_read(Revs) ->
	case broker:stat(hd(Revs)) of
		{ok, _Flags, Parts, _Parents, _Mtime, _Uti, _Volumes} ->
			FCCs = lists:map(fun({FourCC, _Size, _Hash}) -> FourCC end, Parts),
			merge_hpsd_read_loop(Revs, FCCs, []);

		error ->
			{error, enoent}
	end.


merge_hpsd_read_loop([], _FCCs, Acc) ->
	{ok, lists:reverse(Acc)};

merge_hpsd_read_loop([Rev|Revs], FCCs, Acc) ->
	case broker:peek(Rev, []) of
		{ok, Reader} ->
			case merge_hpsd_read_loop_part_loop(Reader, FCCs, []) of
				{ok, Data} ->
					broker:abort(Reader),
					merge_hpsd_read_loop(Revs, FCCs, [Data | Acc]);

				{error, _} = Error ->
					broker:abort(Reader),
					Error
			end;

		{error, _} = Error->
			Error
	end.


merge_hpsd_read_loop_part_loop(_Reader, [], Acc) ->
	{ok, Acc};

merge_hpsd_read_loop_part_loop(Reader, [Part | Remaining], Acc) ->
	case read_loop(Reader, Part, 0, <<>>) of
		{ok, Data} ->
			case catch struct:decode(Data) of
				{'EXIT', _Reason} ->
					{error, econvert};

				Struct ->
					merge_hpsd_read_loop_part_loop(Reader, Remaining,
						[{Part, Struct} | Acc])
			end;

		eof ->
			{error, enoent};

		{error, _} = Error->
			Error
	end.


read_loop(Reader, Part, Offset, Acc) ->
	Length = 16#10000,
	case broker:read(Reader, Part, Offset, Length) of
		{ok, Data} ->
			read_loop(Reader, Part, Offset+Length, <<Acc/binary, Data/binary>>);
		eof ->
			{ok, Acc};
		{error, _Reason} = Error ->
			Error
	end.


merge_hpsd_parts([], [], [], Acc, _Force) ->
	{ok, Acc};

merge_hpsd_parts(
		[{Part, Base} | BaseData],
		[{Part, From} | FromData],
		[{Part, To} | ToData],
		Acc,
		Force) ->
	case struct:merge(Base, [From, To]) of
		{ok, Data} ->
			merge_hpsd_parts(BaseData, FromData, ToData, [{Part, Data} | Acc], Force);

		{conflict, Data} ->
			if
				(Part == <<"META">>) or Force ->
					% ignore conflict in this case
					merge_hpsd_parts(BaseData, FromData, ToData,
						[{Part, Data} | Acc], Force);
				true ->
					{error, conflict}
			end;

		error ->
			{error, baddata}
	end.


merge_hpsd_write(Store, Uuid, FromRev, ToRev, Uti, NewData) ->
	case store:update(Store, Uuid, FromRev, Uti) of
		{ok, Writer} ->
			Written = lists:foldl(
				fun({Part, Data}, Result) ->
					FinalData = if
						Part == <<"META">> -> merge_hpsd_update_meta(Data);
						true               -> merge_hpsd_update_dlinks(Data)
					end,
					store:truncate(Writer, Part, 0),
					case store:write(Writer, Part, 0, struct:encode(FinalData)) of
						ok                 -> Result;
						{error, _} = Error -> Error
					end
				end,
				ok,
				NewData),
			case Written of
				ok ->
					case store:commit(Writer, util:get_time(), [ToRev]) of
						{ok, _Rev} -> ok;
						{error, _} = Error -> Error
					end;

				{error, _} = Error ->
					store:abort(Writer),
					Error
			end;

		{error, _} = Error ->
			Error
	end.


%% update comment
merge_hpsd_update_meta(Data) ->
	update_meta_field(
		[<<"org.hotchpotch.annotation">>, <<"comment">>],
		<<"<<Synchronized by system>>">>,
		merge_hpsd_update_dlinks(Data)). % update revs in dlinks


%% update Revs in dlink's as they are seen now
merge_hpsd_update_dlinks(Data) when is_record(Data, dict, 9) ->
	dict:map(fun(_Key, Value) -> merge_hpsd_update_dlinks(Value) end, Data);

merge_hpsd_update_dlinks(Data) when is_list(Data) ->
	lists:map(fun(Value) -> merge_hpsd_update_dlinks(Value) end, Data);

merge_hpsd_update_dlinks({dlink, Uuid, _Revs}) ->
	Revs = lists:map(fun({Rev, _StoreList}) -> Rev end, broker:lookup(Uuid)),
	{dlink, Uuid, Revs};

merge_hpsd_update_dlinks(Data) ->
	Data.


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

