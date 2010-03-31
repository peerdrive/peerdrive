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

-module(gen_servlet).
-export([start_link/3]).
-export([init/4]).


start_link(Module, Listener, ListenSock) ->
	proc_lib:start_link(?MODULE, init, [self(), Module, Listener, ListenSock]).


init(Parent, Module, Listener, ListenSocket) ->
	proc_lib:init_ack(Parent, {ok, self()}),
	server(Module, Listener, ListenSocket).


server(Module, Listener, ListenSocket) ->
	case gen_tcp:accept(ListenSocket) of
		{ok, Socket} ->
			try
				listener:servlet_occupied(Listener),
				State = Module:init(Socket),
				loop(Module, Socket, State)
			after
				listener:servlet_idle(Listener),
				Module:terminate()
			end,
			server(Module, Listener, ListenSocket);

		_Other ->
			%io:format("[~w] accept returned ~w - goodbye!~n", [self(), Other]),
			shutdown
	end.


loop(Module, Socket, State) ->
	inet:setopts(Socket, [{active, once}]),
	receive
		{tcp, Socket, Packet} ->
			NewState = Module:handle_packet(Packet, State),
			loop(Module, Socket, NewState);

		{tcp_closed, _Socket} ->
			%io:format("[~w] Socket ~w closed~n", [self(), Socket]),
			ok;

		Info ->
			NewState = Module:handle_info(Info, State),
			loop(Module, Socket, NewState)
	end.


