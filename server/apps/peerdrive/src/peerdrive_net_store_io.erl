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

-module(peerdrive_net_store_io).
-behaviour(gen_server).

-export([start_link/4]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-record(state, {store, handle, mps}).

-include("store.hrl").
-include("netstore.hrl").
-include("peerdrive_netstore_pb.hrl").
-include("utils.hrl").

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


handle_call({read, Part, Offset, Length}, _From, S) ->
	do_read(Part, Offset, Length, S);

handle_call({write, Part, Offset, Data}, _From, S) ->
	{reply, do_write(Part, Offset, Data, S), S};

handle_call(close, _From, S) ->
	do_close(S),
	{stop, normal, ok, S};

handle_call(get_type, _From, S) ->
	{reply, do_get_type(S), S};

handle_call(get_parents, _From, S) ->
	{reply, do_get_parents(S), S};

handle_call(get_links, _From, S) ->
	{reply, do_get_links(S), S};

handle_call(get_flags, _From, S) ->
	{reply, do_get_flags(S), S};

handle_call({truncate, Part, Offset}, _From, S) ->
	{reply, do_truncate(Part, Offset, S), S};

handle_call({commit, Comment}, _From, S) ->
	{reply, do_commit(Comment, S), S};

handle_call({suspend, Comment}, _From, S) ->
	{reply, do_suspend(Comment, S), S};

handle_call({set_type, Type}, _From, S) ->
	{reply, do_set_type(Type, S), S};

handle_call({set_links, DocLinks, RevLinks}, _From, S) ->
	{reply, do_set_links(DocLinks, RevLinks, S), S};

handle_call({set_parents, Parents}, _From, S) ->
	{reply, do_set_parents(Parents, S), S};

handle_call({set_flags, Flags}, _From, S) ->
	{reply, do_set_flags(Flags, S), S}.


handle_info({'EXIT', From, Reason}, #state{store=Store} = S) ->
	case From of
		Store ->
			{stop, {orphaned, Reason}, S};
		_User ->
			do_close(S),
			{stop, normal, S}
	end;

handle_info({read, _Ref, _Result}, S) ->
	{noreply, S}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

terminate(_Reason, _State) -> ok.
handle_cast(_, State)    -> {noreply, State}.
code_change(_, State, _) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_read(Part, Offset, Length, S) ->
	try
		Data = do_read_loop(Part, Offset, Length, S, 0, [], make_ref()),
		{reply, {ok, iolist_to_binary(lists:reverse(Data))}, S}
	catch
		throw:{error, _} = Error -> {reply, Error, S};
		throw:{stop, Reason} -> {stop, Reason, {error, enxio}, S}
	end.


do_read_loop(_Part, _Offset, 0, _S, 0, Acc, _Ref) ->
	Acc;

do_read_loop(Part, Offset, Length, S, Pending, Acc, Ref) when (Length > 0) and
                                                              (Pending < 2) ->
	#state{mps=MaxPS, handle=Handle, store=Store} = S,
	Actual = if Length > MaxPS -> MaxPS; true -> Length end,
	Req = peerdrive_netstore_pb:encode_readreq(#readreq{
		handle=Handle, part=Part, offset=Offset, length=Actual}),
	Self = self(),
	Finish = fun
		({ok, Cnf}) ->
			#readcnf{data=Data} = peerdrive_netstore_pb:decode_readcnf(Cnf),
			Self ! {read, Ref, Data};
		({error, _} = Error) ->
			Self ! {read, Ref, Error}
	end,
	case peerdrive_net_store:io_request_async(Store, ?READ_MSG, Req, Finish) of
		ok ->
			do_read_loop(Part, Offset+Actual, Length-Actual, S, Pending+1, Acc, Ref);
		{error, _} = Error ->
			throw(Error)
	end;

do_read_loop(Part, Offset, Length, S, Pending, Acc, Ref) ->
	receive
		{read, Ref, <<>>} ->
			Acc;

		{read, Ref, Data} when is_binary(Data) ->
			do_read_loop(Part, Offset, Length, S, Pending-1, [Data | Acc], Ref);

		{read, Ref, Error} ->
			throw(Error);

		{'EXIT', Store, Reason} when S#state.store == Store ->
			throw({stop, Reason});

		{'EXIT', _User, _Reason} ->
			do_close(S),
			throw({stop, normal})
	end.


do_write(Part, Offset, Data, #state{mps=MaxPS} = S) when size(Data) =< MaxPS ->
	#state{handle=Handle, store=Store} = S,
	Req = peerdrive_netstore_pb:encode_writecommitreq(#writecommitreq{
		handle=Handle, part=Part, offset=Offset, data=Data}),
	case peerdrive_net_store:io_request(Store, ?WRITE_COMMIT_MSG, Req) of
		{ok, <<>>} -> ok;
		Error      -> Error
	end;

do_write(Part, Offset, BigData, S) ->
	#state{handle=Handle, store=Store, mps=MaxPS} = S,
	<<Data:MaxPS/binary, Rest/binary>> = BigData,
	Req = peerdrive_netstore_pb:encode_writebufferreq(#writebufferreq{
		handle=Handle, part=Part, data=Data}),
	case peerdrive_net_store:io_request(Store, ?WRITE_BUFFER_MSG, Req) of
		{ok, <<>>} -> do_write(Part, Offset, Rest, S);
		Error      -> Error
	end.


do_close(#state{handle=Handle} = S) ->
	Req = peerdrive_netstore_pb:encode_closereq(#closereq{handle=Handle}),
	simple_request(?CLOSE_MSG, Req, S).


do_get_type(#state{handle=Handle, store=Store}) ->
	Req = peerdrive_netstore_pb:encode_gettypereq(#gettypereq{handle=Handle}),
	case peerdrive_net_store:io_request(Store, ?GET_TYPE_MSG, Req) of
		{ok, Cnf} ->
			#gettypecnf{type_code=Type} =
				peerdrive_netstore_pb:decode_gettypecnf(Cnf),
			{ok, Type};

		Error ->
			Error
	end.


do_get_parents(#state{handle=Handle, store=Store}) ->
	Req = peerdrive_netstore_pb:encode_getparentsreq(#getparentsreq{handle=Handle}),
	case peerdrive_net_store:io_request(Store, ?GET_PARENTS_MSG, Req) of
		{ok, Cnf} ->
			try
				#getparentscnf{parents=Parents} =
					peerdrive_netstore_pb:decode_getparentscnf(Cnf),
				{ok, Parents}
			catch
				throw:Error -> Error
			end;

		Error ->
			Error
	end.


do_get_links(#state{handle=Handle, store=Store}) ->
	Req = peerdrive_netstore_pb:encode_getlinksreq(#getlinksreq{handle=Handle}),
	case peerdrive_net_store:io_request(Store, ?GET_LINKS_MSG, Req) of
		{ok, Cnf} ->
			#getlinkscnf{doc_links=DocLinks, rev_links=RevLinks} =
				peerdrive_netstore_pb:decode_getlinkscnf(Cnf),
			{ok, {DocLinks, RevLinks}};

		Error ->
			Error
	end.


do_get_flags(#state{handle=Handle, store=Store}) ->
	Req = peerdrive_netstore_pb:encode_getflagsreq(#getflagsreq{handle=Handle}),
	case peerdrive_net_store:io_request(Store, ?GET_FLAGS_MSG, Req) of
		{ok, Cnf} ->
			#getflagscnf{flags=Flags} =
				peerdrive_netstore_pb:decode_getflagscnf(Cnf),
			{ok, Flags};

		Error ->
			Error
	end.


do_truncate(Part, Offset, #state{handle=Handle} = S) ->
	Req = peerdrive_netstore_pb:encode_truncreq(#truncreq{handle=Handle,
		part=Part, offset=Offset}),
	simple_request(?TRUNC_MSG, Req, S).


do_commit(Comment, #state{store=Store, handle=Handle}) ->
	Req = peerdrive_netstore_pb:encode_commitreq(#commitreq{handle=Handle,
		comment=Comment}),
	case peerdrive_net_store:io_request(Store, ?COMMIT_MSG, Req) of
		{ok, Cnf} ->
			#commitcnf{rev=Rev} = peerdrive_netstore_pb:decode_commitcnf(Cnf),
			{ok, Rev};

		Error ->
			Error
	end.


do_suspend(Comment, #state{store=Store, handle=Handle}) ->
	Req = peerdrive_netstore_pb:encode_suspendreq(#suspendreq{handle=Handle,
		comment=Comment}),
	case peerdrive_net_store:io_request(Store, ?SUSPEND_MSG, Req) of
		{ok, Cnf} ->
			#suspendcnf{rev=Rev} = peerdrive_netstore_pb:decode_suspendcnf(Cnf),
			{ok, Rev};

		Error ->
			Error
	end.


do_set_type(Type, #state{handle=Handle} = S) ->
	Req = peerdrive_netstore_pb:encode_settypereq(#settypereq{handle=Handle,
		type_code=Type}),
	simple_request(?SET_TYPE_MSG, Req, S).


do_set_links(DocLinks, RevLinks, #state{handle=Handle} = S) ->
	Req = peerdrive_netstore_pb:encode_setlinksreq(#setlinksreq{handle=Handle,
		doc_links=DocLinks, rev_links=RevLinks}),
	simple_request(?SET_LINKS_MSG, Req, S).


do_set_parents(Parents, #state{handle=Handle} = S) ->
	Req = peerdrive_netstore_pb:encode_setparentsreq(#setparentsreq{
		handle=Handle, parents=Parents}),
	simple_request(?SET_PARENTS_MSG, Req, S).


do_set_flags(Flags, #state{handle=Handle} = S) ->
	Req = peerdrive_netstore_pb:encode_setflagsreq(#setflagsreq{handle=Handle,
		flags=Flags}),
	simple_request(?SET_FLAGS_MSG, Req, S).


simple_request(Request, Body, #state{store=Store}) ->
	case peerdrive_net_store:io_request(Store, Request, Body) of
		{ok, <<>>} -> ok;
		Error      -> Error
	end.

