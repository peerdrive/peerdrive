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

-module(peerdrive_sync_manager).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2,
	terminate/2]).

-include("utils.hrl").
-include("volman.hrl").

-record(state, {store, sys, doc, rules=sets:new(), workers=sets:new()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	gen_server:start_link(?MODULE, [], []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	#peerdrive_store{sid=SysDoc, pid=SysStore} = peerdrive_volman:sys_store(),
	peerdrive_vol_monitor:register_proc(),
	case peerdrive_util:walk(SysStore, <<"syncrules">>) of
		{ok, Doc} ->
			case read_sync_rules(SysStore, Doc) of
				{ok, SyncRules} ->
					S = #state{store=SysStore, sys=SysDoc, doc=Doc, rules=SyncRules},
					S2 = check_workers(S),
					{ok, S2};

				{error, Reason} ->
					error_logger:error_report([{module, ?MODULE},
						{error, 'cannot read syncrules'}, {reason, Reason}]),
					S = #state{store=SysStore, sys=SysDoc, doc=Doc},
					{ok, S}
			end;

		{error, enoent} ->
			case create_sync_rules(SysStore, SysDoc) of
				{ok, Doc} ->
					S = #state{store=SysStore, sys=SysDoc, doc=Doc},
					{ok, S};
				{error, Reason} ->
					error_logger:error_report([{module, ?MODULE},
						{error, 'cannot create syncrules'}, {reason, Reason}]),
					S = #state{store=SysStore, sys=SysDoc},
					{ok, S}
			end;

		{error, Reason} ->
			{stop, Reason}
	end.


handle_info({vol_event, add_store, _Store, _}, S) ->
	S2 = check_workers(S),
	{noreply, S2};

handle_info({vol_event, rem_store, Store, _}, S) ->
	S2 = remove_store(Store, S),
	{noreply, S2};

handle_info({vol_event, mod_doc, Store, Doc}, #state{sys=Store, doc=Doc} = S) ->
	case read_sync_rules(S#state.store, Doc) of
		{ok, SyncRules} ->
			S2 = check_workers(S#state{rules=SyncRules}),
			{noreply, S2};

		{error, Reason} ->
			{stop, Reason, S}
	end;

handle_info(_, S) ->
	{noreply, S}.


terminate(_Reason, _StateData) ->
	peerdrive_vol_monitor:deregister_proc().


handle_call(_Request, _From, S) -> {reply, badarg, S}.
handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local stuff
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

read_sync_rules(Store, Doc) ->
	case peerdrive_util:read_doc_struct(Store, Doc, <<"/org.peerdrive.syncrules">>) of
		{ok, Rules} when is_list(Rules) ->
			{ok, lists:foldl(fun parse_rule/2, sets:new(), Rules)};
		{ok, _} ->
			{error, eio};
		{error, _} = Error ->
			Error
	end.


parse_rule(Rule, Acc) when ?IS_GB_TREE(Rule) ->
	case gb_trees:lookup(<<"from">>, Rule) of
		{value, StoreHex} ->
			Store = peerdrive_util:hexstr_to_bin(binary_to_list(StoreHex)),
			case gb_trees:lookup(<<"to">>, Rule) of
				{value, PeerHex} ->
					Peer = peerdrive_util:hexstr_to_bin(binary_to_list(PeerHex)),
					case gb_trees:lookup(<<"mode">>, Rule) of
						_ when Store == Peer ->
							Acc; % reject sync on itself
						{value, <<"ff">>}     ->
							sets:add_element({ff, Store, Peer}, Acc);
						{value, <<"latest">>} ->
							sets:add_element({latest, Store, Peer}, Acc);
						{value, <<"merge">>} ->
							sets:add_element({merge, Store, Peer}, Acc);
						{value, _} ->
							Acc;
						none ->
							Acc
					end;

				none ->
					Acc
			end;

		none ->
			Acc
	end;

parse_rule(_, Acc) ->
	Acc.


check_workers(#state{rules=Rules, workers=Workers} = S) ->
	% find all sync rules which have been removed
	Stopped = sets:subtract(Workers, Rules),
	lists:foreach(
		fun({_Mode, Store, Peer}) ->
			Ret = peerdrive_sync_sup:stop_sync(Store, Peer),
			Ret == ok orelse error_logger:warning_report([{module, ?MODULE},
				{warning, 'stop sync failed'}, {reason, Ret}])
		end,
		sets:to_list(Stopped)),
	% try to start all new sync rules which could be started
	Started = sets:filter(
		fun({Mode, Store, Peer}) ->
			case peerdrive_volman:store(Store) of
				{ok, _} ->
					case peerdrive_volman:store(Peer) of
						{ok, _} ->
							case peerdrive_sync_sup:start_sync(Mode, Store, Peer) of
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


create_sync_rules(Store, Root) ->
	try
		{ok, Doc, Handle} = check(peerdrive_broker:create(Store,
			<<"org.peerdrive.syncrules">>, <<"">>)),
		try
			Meta = gb_trees:enter(<<"title">>, <<"syncrules">>, gb_trees:empty()),
			Data = gb_trees:enter(<<"org.peerdrive.syncrules">>, [],
				gb_trees:enter(<<"org.peerdrive.annotation">>, Meta, gb_trees:empty())),
			ok = check(peerdrive_broker:set_data(Handle, <<"">>,
				peerdrive_struct:encode(Data))),
			{ok, _Rev} = check(peerdrive_broker:commit(Handle)),
			ok = check(peerdrive_util:folder_link(Store, Root, Doc)),
			{ok, Doc}
		after
			peerdrive_broker:close(Handle)
		end
	catch
		throw:Error -> Error
	end.


check({error, _} = Error) ->
	throw(Error);
check(Term) ->
	Term.

