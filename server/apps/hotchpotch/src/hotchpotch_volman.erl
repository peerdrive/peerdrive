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

-module(hotchpotch_volman).
-behaviour(gen_server).

-export([start_link/0]).
-export([reg_store/2, enum/0, stores/0, store/1, mount/1, unmount/1]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

% specs:  [{Id, Descr, Disposition, Module, Args}]
%           Id = atom()
%           Descr = string()
%           Disposition = [system | removable | net]
%           Module = atom()
%           Args = [term()]
% stores: [{Pid, Id, Guid}]
%           Pid = pid()
%           Id = atom()
%           Guid = guid()
-record(state, {specs, stores}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Register a store.
%%
%% Must be called by each store when it was initialized. The volman will link to
%% the process to trap its exit. No explicit unregistration is needed.
%%
%% @spec reg_store(Id, Guid, Interface) -> none()
reg_store(Id, Guid) ->
	gen_server:cast(?MODULE, {reg, {self(), Id, Guid}}).

%% @doc Enumerate all known stores
%%
%% Returns information about all known stores in the system, even unmounted
%% ones.
%%
%% @spec enum() -> Result
%%       Result = [{Id, Descr, Guid, [Tag]}]
%%       Id = atom()
%%       Descr = string()
%%       Guid = guid() | unknown
%%       Tag = mounted | removable | system | net
enum() ->
	gen_server:call(?MODULE, enum, infinity).

%% @doc Get Guids/pid's of all currently mounted stores
%% @spec stores() -> [{guid(), pid()}]
stores() ->
	gen_server:call(?MODULE, stores, infinity).

%% @doc Get pid of a specific store
%% @spec store(Guid) -> {ok, pid()} | error
%%       Guid = guid()
store(Guid) ->
	gen_server:call(?MODULE, {store, Guid}, infinity).

%% @doc Mount store by ID
%% @spec mount(StoreId) -> {ok, Guid} | {error, Reason}
%%       StoreId = atom()
%%       Guid = guid()
%%       Reason = ecode()
mount(StoreId) ->
	gen_server:call(?MODULE, {mount, StoreId}, infinity).

%% @doc Unmount store by ID
%% @spec unmount(StoreId) -> ok | {error, Reason}
%%       StoreId = atom()
unmount(StoreGuid) ->
	gen_server:call(?MODULE, {unmount, StoreGuid}, infinity).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks implementation...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	process_flag(trap_exit, true),
	Specs = get_store_specs(),
	case start_permanent_stores(Specs) of
		ok ->
			case have_system_store(Specs) of
				true ->
					{ok, #state{specs=Specs, stores=[]}};
				false ->
					{stop, nosysstore}
			end;

		{error, Reason} ->
			{stop, Reason}
	end.

handle_cast({reg, Info}, #state{stores=Stores, specs=Specs} = S) ->
	{Pid, _Id, Guid} = Info,
	link(Pid),
	hotchpotch_vol_monitor:trigger_add_store(Guid),
	NewStores = lists:sort(
		fun({_, IdA, _}, {_, IdB, _}) ->
			rank(IdA, Specs) =< rank(IdB, Specs)
		end,
		[Info|Stores]),
	{noreply, S#state{stores=NewStores}}.


% returns [{Id, Descr, Guid, [Tag]}]
handle_call(enum, _From, #state{specs=Specs, stores=Stores} = S) ->
	Reply = lists:map(
		fun({Id, Descr, Disposition, _Module, _Args}) ->
			case lists:keysearch(Id, 2, Stores) of
				{value, {_Pid, _Id, Guid}} ->
					{Id, Descr, Guid, [mounted|Disposition]};
				false ->
					{Id, Descr, <<0:128>>, Disposition}
			end
		end,
		Specs),
	{reply, Reply, S};

handle_call(stores, _From, #state{stores=Stores} = S) ->
	Reply = lists:map(
		fun({Pid, _Id, Guid}) -> {Guid, Pid} end,
		Stores),
	{reply, Reply, S};

handle_call({store, Guid}, _From, #state{stores=Stores} = S) ->
	Reply = case lists:keysearch(Guid, 3, Stores) of
		{value, {Pid, _Id, _Guid}} -> {ok, Pid};
		false -> error
	end,
	{reply, Reply, S};

handle_call({mount, StoreId}, _From, #state{specs=Specs} = S) ->
	Reply = case lists:keysearch(StoreId, 1, Specs) of
		{value, {Id, _Descr, Disposition, Module, Args}} ->
			case hotchpotch_store_sup:spawn_store(Id, Disposition, Module, Args) of
				{ok, Pid} ->
					Guid = hotchpotch_store:guid(Pid),
					{ok, Guid};
				Else ->
					Else
			end;
		false ->
			{error, enoent}
	end,
	{reply, Reply, S};
	
handle_call({unmount, StoreId}, _From, #state{stores=Stores} = S) ->
	Reply = case lists:keysearch(StoreId, 2, Stores) of
		{value, {_Pid, Id, _Guid}} ->
			hotchpotch_store_sup:stop_store(Id);
		false ->
			{error, enoent}
	end,
	{reply, Reply, S}.


handle_info({'EXIT', Pid, _Reason}, #state{stores=Stores1} = S) ->
	case lists:keytake(Pid, 1, Stores1) of
		{value, {_Pid, _Id, Guid}, Stores2} ->
			hotchpotch_vol_monitor:trigger_rem_store(Guid),
			{noreply, S#state{stores=Stores2}};
		false ->
			{noreply, S}
	end.


terminate(_Reason, _State)          -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% local functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

rank(Id, Specs) ->
	rank_loop(Id, Specs, 0).

rank_loop(Id, [{SpecId, _, _, _, _} | Remaining], Rank) ->
	if
		Id == SpecId -> Rank;
		true         -> rank_loop(Id, Remaining, Rank+1)
	end.

get_store_specs() ->
	case application:get_env(hotchpotch, stores) of
		{ok, SysSpecs} ->
			StoreSpecs = case application:get_env(hotchpotch, platform_stores) of
				{ok, PlatformSpecs} -> SysSpecs ++ PlatformSpecs;
				undefined           -> SysSpecs
			end,
			lists:filter(
				fun(Spec) ->
					case check_store_spec(Spec) of
						true ->
							true;
						false ->
							error_logger:warning_msg("Dropping invalid store spec: ~p~n", [Spec]),
							false
					end
				end,
				StoreSpecs);

		undefined ->
			error_logger:warning_msg("No store specs found!~n"),
			[]
	end.


start_permanent_stores([]) ->
	ok;
start_permanent_stores([{Id, _Descr, Disposition, Module, Args} | Specs]) ->
	case proplists:is_defined(removable, Disposition) of
		false ->
			case hotchpotch_store_sup:spawn_store(Id, Disposition, Module, Args) of
				{ok, _Pid} ->
					start_permanent_stores(Specs);

				{error, Error} ->
					{error, {{Id, Disposition, Module, Args}, Error}}
			end;

		true ->
			start_permanent_stores(Specs)
	end.


have_system_store(Specs) ->
	lists:foldl(
		fun({_Id, _Descr, Disposition, _Module, _Args}, Acc) ->
			Found = proplists:is_defined(system, Disposition) andalso
			not proplists:is_defined(removable, Disposition),
			if Found -> Acc+1; true -> Acc end
		end,
		0,
		Specs) == 1.


check_store_spec({Id, Descr, Disposition, Module, _Args}) when
		is_atom(Id) and
		is_list(Disposition) and
		is_list(Descr) ->
	lists:member(Module, [hotchpotch_file_store, hotchpotch_net_store]) and
	lists:foldl(
		fun(Tag, Acc) ->
			Acc and case Tag of
				system    -> true;
				removable -> true;
				net       -> true;
				_Else     -> false
			end
		end,
		true,
		Disposition);

check_store_spec(_) ->
	false.

