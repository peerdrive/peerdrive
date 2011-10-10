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
	Reply = do_put_part(Part, Data, S),
	{reply, Reply, S};

handle_call(commit, _From, S) ->
	Reply = do_commit(S),
	{stop, normal, Reply, S};

handle_call(abort, _From, S) ->
	do_abort(S),
	{stop, normal, ok, S}.


handle_info({'EXIT', From, Reason}, #state{store=Store} = S) ->
	case From of
		Store ->
			{stop, {orphaned, Reason}, S};
		_User ->
			do_abort(S),
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

do_put_part(Part, Data, #state{handle=Handle, mps=MaxPS} = S) when size(Data) =< MaxPS ->
	Req = peerdrive_netstore_pb:encode_putrevpartreq(
		#putrevpartreq{handle=Handle, part=Part, data=Data}),
	relay_request(?PUT_REV_PART_MSG, Req, S);

do_put_part(Part, Data, #state{mps=MaxPS} = S) ->
	<<Chunk1:MaxPS/binary, Chunk2/binary>> = Data,
	case do_put_part(Part, Chunk1, S) of
		ok ->
			do_put_part(Part, Chunk2, S);
		Error ->
			Error
	end.


do_commit(#state{handle=Handle} = S) ->
	Req = peerdrive_netstore_pb:encode_putrevcommitreq(
		#putrevcommitreq{handle=Handle}),
	relay_request(?PUT_REV_COMMIT_MSG, Req, S).


do_abort(#state{handle=Handle} = S) ->
	Req = peerdrive_netstore_pb:encode_putrevabortreq(
		#putrevabortreq{handle=Handle}),
	relay_request(?PUT_REV_ABORT_MSG, Req, S).


relay_request(Request, Body, #state{store=Store}) ->
	case peerdrive_net_store:io_request(Store, Request, Body) of
		{ok, <<>>} -> ok;
		Error      -> Error
	end.

