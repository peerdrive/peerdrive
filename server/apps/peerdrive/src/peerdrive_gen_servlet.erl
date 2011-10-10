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

-module(peerdrive_gen_servlet).
-export([start_link/4]).
-export([init/5]).


start_link(Module, Args, Listener, ListenSock) ->
	proc_lib:start_link(?MODULE, init, [self(), Module, Listener, ListenSock, Args]).


init(Parent, Module, Listener, ListenSocket, Args) ->
	proc_lib:init_ack(Parent, {ok, self()}),
	server(Module, Listener, ListenSocket, Args).


server(Module, Listener, ListenSocket, Args) ->
	case gen_tcp:accept(ListenSocket) of
		{ok, Socket} ->
			peerdrive_listener:servlet_occupied(Listener),
			State = Module:init(Socket, Args),
			inet:setopts(Socket, [{nodelay, true}]),
			loop(Module, Socket, State);

		_Other ->
			%io:format("[~w] accept returned ~w - goodbye!~n", [self(), Other]),
			shutdown
	end.


loop(Module, Socket, State) ->
	Result = try
		inet:setopts(Socket, [{active, once}]),
		receive
			{tcp, Socket, Packet} ->
				Module:handle_packet(Packet, State);

			{tcp_closed, _Socket} ->
				Module:terminate(State),
				%io:format("[~w] Socket ~w closed~n", [self(), Socket]),
				closed;

			Info ->
				Module:handle_info(Info, State)
		end
	catch
		throw:Term   -> {error, {throw, Term}};
		exit:Reason  -> {error, {exit, Reason}};
		error:Reason -> {error, {error, {Reason, erlang:get_stacktrace()}}}
	end,
	case Result of
		{ok, NewState} ->
			loop(Module, Socket, NewState);
		{stop, NewState} ->
			gen_tcp:close(Socket),
			Module:terminate(NewState),
			normal;
		closed ->
			normal;
		{error, Error} ->
			error_logger:error_report([{module, Module}, {state, State},
				{error, Error}]),
			gen_tcp:close(Socket),
			Module:terminate(State),
			{error, Error}
	end.

