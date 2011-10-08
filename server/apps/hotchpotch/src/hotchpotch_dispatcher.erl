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

-module(hotchpotch_dispatcher).
-behaviour(gen_fsm).

-export([start_link/0]).
-export([init/1, handle_info/3, handle_event/3, handle_sync_event/4,
	terminate/3, code_change/4]).

-record(state, {store, sys, doc, rules=sets:new(), workers=sets:new()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	gen_fsm:start_link(?MODULE, [], []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	{ok, SysStore, SysDoc} = find_sys_store(),
	hotchpotch_vol_monitor:register_proc(?MODULE),
	case find_sync_rules(SysStore, SysDoc) of
		{ok, Doc} ->
			case read_sync_rules(SysStore, Doc) of
				{ok, SyncRules} ->
					S = #state{store=SysStore, sys=SysDoc, doc=Doc, rules=SyncRules},
					S2 = check_workers(S),
					{ok, monitor, S2};

				error ->
					S = #state{store=SysStore, sys=SysDoc, doc=Doc},
					{ok, try_read, S}
			end;

		error ->
			S = #state{store=SysStore, sys=SysDoc},
			{ok, wait, S}
	end.


handle_info({trigger_add_store, _Store}, monitor, S) ->
	S2 = check_workers(S),
	{next_state, monitor, S2};

handle_info({trigger_rem_store, Store}, monitor, S) ->
	S2 = remove_store(Store, S),
	{next_state, monitor, S2};

handle_info({trigger_mod_doc, Store, Doc}, State, #state{sys=Store, doc=Doc} = S)
	when
		(State == monitor) or
		(State == try_read) ->
	case read_sync_rules(S#state.store, Doc) of
		{ok, SyncRules} ->
			S2 = check_workers(S#state{rules=SyncRules}),
			{next_state, monitor, S2};

		error ->
			{next_state, try_read, S}
	end;

handle_info({trigger_mod_doc, SysDoc, SysDoc}, wait, #state{sys=SysDoc} = S) ->
	case find_sync_rules(S#state.store, SysDoc) of
		{ok, Doc} ->
			case read_sync_rules(S#state.store, Doc) of
				{ok, SyncRules} ->
					S2 = S#state{doc=Doc, rules=SyncRules},
					S3 = check_workers(S2),
					{next_state, monitor, S3};

				error ->
					S2 = S#state{doc=Doc},
					{next_state, try_read, S2}
			end;

		error ->
			{next_state, wait, S}
	end;

handle_info(_, State, S) ->
	{next_state, State, S}.


handle_event(_Event, StateName, StateData) ->
	{next_state, StateName, StateData}.


handle_sync_event(_Event, _From, StateName, StateData) ->
	{reply, badarg, StateName, StateData}.


terminate(_Reason, _StateName, _StateData) ->
	hotchpotch_vol_monitor:deregister_proc(?MODULE).


code_change(_OldVsn, StateName, StateData, _Extra) ->
	{ok, StateName, StateData}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local stuff
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

find_sys_store() ->
	find_sys_store(hotchpotch_volman:enum()).


find_sys_store([]) ->
	error;
find_sys_store([{_Id, _Descr, Guid, Tags} | Remaining]) ->
	case proplists:is_defined(system, Tags) of
		true ->
			{ok, Store} = hotchpotch_volman:store(Guid),
			{ok, Store, Guid};
		false ->
			find_sys_store(Remaining)
	end.


find_sync_rules(Store, Doc) ->
	case read_doc(Store, Doc) of
		{ok, Dir} ->
			find_sync_rules_loop(Store, Dir);
		error ->
			error
	end.


find_sync_rules_loop(_Store, []) ->
	error;

find_sync_rules_loop(Store, [{dlink, Doc} | Rest]) ->
	try read_file_name(Store, Doc) of
		<<"syncrules">> ->
			{ok, Doc};
		_ ->
			find_sync_rules_loop(Store, Rest)
	catch
		throw:error ->
			find_sync_rules_loop(Store, Rest)
	end;

find_sync_rules_loop(Store, [_ | Rest]) ->
	find_sync_rules_loop(Store, Rest).


read_doc(StorePid, Doc) ->
	case hotchpotch_store:lookup(StorePid, Doc) of
		{ok, Rev, _} -> hotchpotch_util:read_rev_struct(StorePid, Rev, <<"HPSD">>);
		error        -> {error, enoent}
	end.


read_file_name(Store, Doc) ->
	Rev = case hotchpotch_store:lookup(Store, Doc) of
		{ok, R, _} -> R;
		erro -> throw(error)
	end,
	Meta = case hotchpotch_util:read_rev_struct(Store, Rev, <<"META">>) of
		{ok, Value1} when is_record(Value1, dict, 9) ->
			Value1;
		{ok, _} ->
			throw(error);
		{error, _} ->
			throw(error)
	end,
	case meta_read_entry(Meta, [<<"org.hotchpotch.annotation">>, <<"title">>]) of
		{ok, Title} when is_binary(Title) ->
			Title;
		{ok, _} ->
			throw(error);
		error ->
			throw(error)
	end.


meta_read_entry(Meta, []) ->
	{ok, Meta};
meta_read_entry(Meta, [Step|Path]) when is_record(Meta, dict, 9) ->
	case dict:find(Step, Meta) of
		{ok, Value} -> meta_read_entry(Value, Path);
		error       -> error
	end;
meta_read_entry(_Meta, _Path) ->
	error.


read_sync_rules(Store, Doc) ->
	case read_doc(Store, Doc) of
		{ok, Rules} when is_list(Rules) ->
			{ok, lists:foldl(fun parse_rule/2, sets:new(), Rules)};
		{ok, _} ->
			error;
		error ->
			error
	end.


parse_rule(Rule, Acc) when is_record(Rule, dict, 9) ->
	case dict:find(<<"from">>, Rule) of
		{ok, StoreHex} ->
			Store = hotchpotch_util:hexstr_to_bin(binary_to_list(StoreHex)),
			case dict:find(<<"to">>, Rule) of
				{ok, PeerHex} ->
					Peer = hotchpotch_util:hexstr_to_bin(binary_to_list(PeerHex)),
					case dict:find(<<"mode">>, Rule) of
						{ok, <<"ff">>}     ->
							sets:add_element({ff, Store, Peer}, Acc);
						{ok, <<"latest">>} ->
							sets:add_element({latest, Store, Peer}, Acc);
						{ok, <<"merge">>} ->
							sets:add_element({merge, Store, Peer}, Acc);
						{ok, _} ->
							Acc;
						error ->
							Acc
					end;

				error ->
					Acc
			end;

		error ->
			Acc
	end;

parse_rule(_, Acc) ->
	Acc.


check_workers(#state{rules=Rules, workers=Workers} = S) ->
	% find all sync rules which have been removed
	Stopped = sets:subtract(Workers, Rules),
	lists:foreach(
		fun({_Mode, Store, Peer}) ->
			Ret = hotchpotch_sync_sup:stop_sync(Store, Peer),
			Ret == ok orelse error_logger:warning_report([{module, ?MODULE},
				{warning, 'stop sync failed'}, {reason, Ret}])
		end,
		sets:to_list(Stopped)),
	% try to start all new sync rules which could be started
	Started = sets:filter(
		fun({Mode, Store, Peer}) ->
			case hotchpotch_volman:store(Store) of
				{ok, _} ->
					case hotchpotch_volman:store(Peer) of
						{ok, _} ->
							case hotchpotch_sync_sup:start_sync(Mode, Store, Peer) of
								ok ->
									true;
								Error ->
									error_logger:warning_report([{module, ?MODULE},
										{warning, 'start sync failed'},
										{reason, Error}]),
									false
							end;
						error ->
							false
					end;
				error ->
					false
			end
		end,
		sets:subtract(Rules, Workers)),
	S#state{workers=sets:union(sets:subtract(Workers, Stopped), Started)}.


remove_store(Store, #state{workers=Workers} = S) ->
	% The affected sync workers terminate themselves. We just have to remove
	% them from the workers list.
	NewWorkers = sets:filter(
		fun({_Mode, FromStore, ToStore}) ->
			(FromStore =/= Store) and (ToStore =/= Store)
		end,
		Workers),
	S#state{workers=NewWorkers}.

