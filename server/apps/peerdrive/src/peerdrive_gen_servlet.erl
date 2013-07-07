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
-export([start_link/5]).
-export([init/6]).


start_link(Module, Args, Listener, ListenSock, ListenState) ->
	proc_lib:start_link(?MODULE, init, [self(), Module, Listener, ListenSock,
		ListenState, Args]).


init(Parent, Module, Listener, ListenSocket, ListenState, Args) ->
	proc_lib:init_ack(Parent, {ok, self()}),
	Result = server(Module, Listener, ListenSocket, ListenState, Args),
	exit(Result).


server(Module, Listener, ListenSocket, ListenState, Args) ->
	case gen_tcp:accept(ListenSocket) of
		{ok, Socket} ->
			peerdrive_listener:servlet_occupied(Listener),
			State = Module:init(Args, ListenState),
			inet:setopts(Socket, [{nodelay, true}, {keepalive, true}]),
			{Result, NewState} = loop(Module, gen_tcp, Socket, State),
			Module:terminate(NewState),
			Result;

		_Other ->
			shutdown
	end.


loop(Module, Transport, Socket, State) ->
	Result = try
		case Transport of
			gen_tcp -> inet:setopts(Socket, [{active, once}]);
			ssl     -> ssl:setopts(Socket, [{active, once}])
		end,
		receive
			{tcp, _Socket, Packet} ->
				Module:handle_packet(Packet, State);
			{ssl, _Socket, Packet} ->
				Module:handle_packet(Packet, State);
			{tcp_closed, _Socket} ->
				'$closed';
			{ssl_closed, _Socket} ->
				'$closed';
			{tcp_error, Socket, TcpError} ->
				{error, TcpError};
			{ssl_error, Socket, SslError} ->
				{error, SslError};

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
			loop(Module, Transport, Socket, NewState);
		{reply, Data, NewState}->
			case Transport:send(Socket, Data) of
				ok ->
					loop(Module, Transport, Socket, NewState);
				{error, Error} ->
					Transport:close(Socket),
					{Error, NewState}
			end;
		{ssl, Data, SslOpt, NewState} when Transport == gen_tcp ->
			ssl_upgrade(Module, Socket, Data, SslOpt, NewState);
		{stop, NewState} ->
			Transport:close(Socket),
			{normal, NewState};
		{stop, Data, NewState} ->
			Transport:send(Socket, Data),
			Transport:close(Socket),
			{normal, NewState};
		'$closed' ->
			{normal, State};
		{error, Error} ->
			{Error, State}
	end.


ssl_upgrade(Module, Socket, Data, SslOpt, State) ->
	inet:setopts(Socket, [{active, false}]),
	case gen_tcp:send(Socket, Data) of
		ok ->
			case ssl:ssl_accept(Socket, SslOpt, 5000) of
				{ok, SslSocket} ->
					loop(Module, ssl, SslSocket, State);
				{error, Error} ->
					gen_tcp:close(Socket),
					{Error, State}
			end;

		{error, Error} ->
			gen_tcp:close(Socket),
			{Error, State}
	end.

