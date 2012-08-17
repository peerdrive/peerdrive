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

-module(peerdrive_net_store_importer).
-behaviour(gen_server).

-export([start_link/4]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-record(state, {store, handle, mps}).

-include("store.hrl").
-include("netstore.hrl").
-include("peerdrive_netstore_pb.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Store, Handle, MaxPacketSize, User) ->
	State = #state{store=Store, handle=Handle, mps=MaxPacketSize},
	gen_server:start_link(?MODULE, {State, User}, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({State, User}) ->
	process_flag(trap_exit, true),
	link(User),
	{ok, State}.


handle_call({put_part, Part, Data}, _From, S) ->
	do_put_part(Part, Data, S);

handle_call(commit, _From, S) ->
	{reply, do_commit(S), S};

handle_call(close, _From, S) ->
	do_close(S),
	{stop, normal, ok, S}.


handle_info({'EXIT', From, Reason}, #state{store=Store} = S) ->
	case From of
		Store ->
			{stop, {orphaned, Reason}, S};
		_User ->
			do_close(S),
			{stop, normal, S}
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


handle_cast(_, State)    -> {stop, enotsup, State}.
code_change(_, State, _) -> {ok, State}.
terminate(_, _)          -> ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_put_part(Part, Data, S) ->
	case do_put_part_loop(Part, Data, S, 0, make_ref()) of
		ok ->
			{reply, ok, S};
		{error, _} = Error ->
			{reply, Error, S};
		{stop, Reason} ->
			{stop, Reason, {error, enxio}, S}
	end.


do_put_part_loop(_Part, <<>>, _S, 0, _Ref) ->
	ok;

do_put_part_loop(Part, Data, S, Pending, Ref) when (size(Data) > 0) and
                                                   (Pending < 2) ->
	#state{store=Store, handle=Handle, mps=MaxPS} = S,
	if
		size(Data) > MaxPS ->
			<<SendData:MaxPS/binary, Rest/binary>> = Data;
		true ->
			SendData = Data,
			Rest = <<>>
	end,
	Req = peerdrive_netstore_pb:encode_putrevpartreq(
		#putrevpartreq{handle=Handle, part=Part, data=SendData}),
	Self = self(),
	Finish = fun
		({ok, <<>>}) ->
			Self ! {put_part, Ref, ok};
		({error, _} = Error) ->
			Self ! {put_part, Ref, Error}
	end,
	case peerdrive_net_store:io_request_async(Store, ?PUT_REV_PART_MSG, Req, Finish) of
		ok ->
			do_put_part_loop(Part, Rest, S, Pending+1, Ref);
		{error, _} = Error ->
			Error
	end;

do_put_part_loop(Part, Data, S, Pending, Ref) ->
	receive
		{put_part, Ref, ok} ->
			do_put_part_loop(Part, Data, S, Pending-1, Ref);

		{put_part, Ref, Error} ->
			Error;

		{'EXIT', Store, Reason} when S#state.store == Store ->
			{stop, Reason};

		{'EXIT', _User, _Reason} ->
			do_close(S),
			{stop, normal}
	end.


do_commit(#state{handle=Handle} = S) ->
	Req = peerdrive_netstore_pb:encode_putrevcommitreq(
		#putrevcommitreq{handle=Handle}),
	relay_request(?PUT_REV_COMMIT_MSG, Req, S).


do_close(#state{handle=Handle} = S) ->
	Req = peerdrive_netstore_pb:encode_putrevclosereq(
		#putrevclosereq{handle=Handle}),
	relay_request(?PUT_REV_CLOSE_MSG, Req, S).


relay_request(Request, Body, #state{store=Store}) ->
	case peerdrive_net_store:io_request(Store, Request, Body) of
		{ok, <<>>} -> ok;
		Error      -> Error
	end.

