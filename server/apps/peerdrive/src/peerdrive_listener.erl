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

-module(peerdrive_listener).
-behaviour(gen_server).

-export([start_link/4]).
-export([servlet_occupied/1]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).


-define(START_SERVLETS, 3).
-record(state, {socket, serversup, servletsup, mod, modstate}).

start_link(ServerSup, Module, Port, Options) ->
	gen_server:start_link(?MODULE, {ServerSup, Module, Port, Options}, []).


servlet_occupied(Pid) ->
	gen_server:cast(Pid, occupied).


init({ServerSup, Module, Port, Options}) ->
	process_flag(trap_exit, true),
	ListenOpt1 = case proplists:get_value(ip, Options) of
		RawAddr when is_list(RawAddr) ->
			case inet_parse:address(RawAddr) of
				{ok, Addr} ->
					[{ip, Addr}];
				{error, _} ->
					error_logger:warning_msg("Invalid listening IP address: ~s~n",
						[RawAddr]),
					[]
			end;
		_ ->
			[]
	end,
	ListenOpt2 = [binary, {active, false}, {packet, 2} | ListenOpt1],
	case gen_tcp:listen(Port, ListenOpt2) of
		{ok, ListenSock} ->
			case Module:init_listen(ListenSock, Options) of
				{ok, ModState} ->
					start_servlets(?START_SERVLETS),
					S = #state{socket=ListenSock, serversup=ServerSup,
						mod=Module, modstate=ModState},
					{ok, S};
				{error, Reason} ->
					gen_tcp:close(ListenSock),
					{stop, Reason}
			end;

		{error, Reason} ->
			{stop, Reason}
	end.


handle_cast(occupied, #state{servletsup=ServletSup} = S) ->
	S2 = case ServletSup of
		undefined ->
			Pid = peerdrive_server_sup:get_servlet_sup_pid(S#state.serversup),
			S#state{servletsup=Pid};
		_ ->
			S
	end,
	start_servlet(S2),
	{noreply, S2}.


start_servlets(0) ->
	ok;

start_servlets(Num) ->
	gen_server:cast(self(), occupied),
	start_servlets(Num-1).


start_servlet(#state{servletsup=ServletSup, socket=ListenSock, modstate=State}) ->
	peerdrive_servlet_sup:spawn_servlet(ServletSup, ListenSock, State).


terminate(_Reason, #state{socket=Socket, mod=Mod, modstate=State}) ->
	Mod:terminate_listen(State),
	gen_tcp:close(Socket).


handle_call(_, _, State)            -> {reply, error, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
handle_info(_Info, State)           -> {noreply, State}.

