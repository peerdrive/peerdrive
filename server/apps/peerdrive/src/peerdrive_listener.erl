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
-record(state, {socket, servletsup}).

start_link(ListenerId, ServletSup, Port, Options) ->
	gen_server:start_link({local, ListenerId}, ?MODULE,
		{ServletSup, Port, Options}, []).


servlet_occupied(Pid) ->
	gen_server:cast(Pid, occupied).


init({ServletSup, Port, Options}) ->
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
	ListenOpt2 = [binary, {active, false}, {packet, 2}, {keepalive, true} |
		ListenOpt1],
	case gen_tcp:listen(Port, ListenOpt2) of
		{ok, ListenSock} ->
			start_servlets(ServletSup, ?START_SERVLETS, ListenSock),
			{ok, #state{socket=ListenSock, servletsup=ServletSup}};

		{error, Reason} ->
			{stop, Reason}
	end.


handle_cast(occupied, S) ->
	start_servlet(S#state.servletsup, S#state.socket),
	{noreply, S}.


start_servlets(_ServletSup, 0, _) ->
	ok;

start_servlets(ServletSup, Num, ListenSock) ->
	start_servlet(ServletSup, ListenSock),
	start_servlets(ServletSup, Num-1, ListenSock).


start_servlet(ServletSup, ListenSock) ->
	peerdrive_servlet_sup:spawn_servlet(ServletSup, ListenSock).


terminate(_Reason, State) ->
	gen_tcp:close(State#state.socket).


handle_call(_, _, State)            -> {reply, error, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
handle_info(_Info, State)           -> {noreply, State}.

