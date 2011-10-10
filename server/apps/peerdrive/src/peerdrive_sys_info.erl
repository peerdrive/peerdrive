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

-module(peerdrive_sys_info).

-export([start_link/0]).
-export([publish/2, lookup/1]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2,
	terminate/2]).

-record(state, {table}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

publish(Param, Value) ->
	gen_server:call(?MODULE, {publish, Param, Value}).

lookup(Param) ->
	gen_server:call(?MODULE, {lookup, Param}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	process_flag(trap_exit, true),
	Tid = ets:new(blackboard, [private]),
	{ok, #state{table=Tid}}.


handle_call({publish, Param, Value}, From, State) ->
	do_publish(Param, Value, From, State);

handle_call({lookup, Param}, _From, State) ->
	do_lookup(Param, State).


handle_info({'EXIT', Pid, _Reason}, State) ->
	NewState = do_trap_exit(Pid, State),
	{noreply, NewState};

handle_info(_, State) ->
	{noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_cast(_, State) -> {noreply, State}.
terminate(_, _) -> ok.
code_change(_, State, _) -> {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Service implementation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_publish(Param, Value, {Owner, _}, S) ->
	ets:insert(S#state.table, {Param, Value, Owner}),
	link(Owner),
	{reply, ok, S}.


do_lookup(Param, S) ->
	case ets:lookup(S#state.table, Param) of
		[{Param, Value, _Owner}] ->
			{reply, {ok, Value}, S};
		[] ->
			{reply, {error, enoent}, S}
	end.


do_trap_exit(Owner, S) ->
	Spec = [{{'_','_','$1'},[{'==','$1',Owner}],[true]}],
	ets:match_delete(S#state.table, Spec),
	S.

