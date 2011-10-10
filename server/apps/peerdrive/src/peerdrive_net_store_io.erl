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
-import(peerdrive_netencode, [encode_list/1, encode_list_32/1, encode_string/1,
	parse_string/1, parse_uuid/1, parse_uuid_list/1, parse_list_32/2]).

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


handle_call({read, Part, Offset, Length}, _From, S) ->
	{reply, do_read(Part, Offset, Length, S), S};

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

handle_call(commit, _From, S) ->
	{reply, do_commit(S), S};

handle_call(suspend, _From, S) ->
	{reply, do_suspend(S), S};

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
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

terminate(_Reason, _State) -> ok.
handle_cast(_, State)    -> {noreply, State}.
code_change(_, State, _) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_read(Part, Offset, Length, #state{mps=MaxPS} = S) when Length =< MaxPS ->
	#state{handle=Handle, store=Store} = S,
	Req = peerdrive_netstore_pb:encode_readreq(#readreq{
		handle=Handle, part=Part, offset=Offset, length=Length}),
	case peerdrive_net_store:io_request(Store, ?READ_MSG, Req) of
		{ok, Cnf} ->
			#readcnf{data=Data} = peerdrive_netstore_pb:decode_readcnf(Cnf),
			{ok, Data};
		{error, _} = Error ->
			Error
	end;

do_read(Part, Offset, Length, S) ->
	do_read_loop(Part, Offset, Length, S, <<>>).


do_read_loop(_Part, _Offset, 0, _S, Acc) ->
	{ok, Acc};

do_read_loop(Part, Offset, Length, #state{mps=MaxPS} = S, Acc) ->
	Size = if
		Length > MaxPS -> MaxPS;
		true           -> Length
	end,
	case do_read(Part, Offset, Size, S) of
		{ok, Data} ->
			NewAcc = <<Acc/binary, Data/binary>>,
			if
				size(Data) < Size ->
					{ok, NewAcc};
				true ->
					do_read_loop(Part, Offset+Size, Length-Size, S, NewAcc)
			end;

		Error ->
			Error
	end.


do_write(Part, Offset, Data, #state{mps=MaxPS} = S) when size(Data) =< MaxPS ->
	#state{handle=Handle, store=Store} = S,
	Req = peerdrive_netstore_pb:encode_writereq(#writereq{
		handle=Handle, part=Part, offset=Offset, data=Data}),
	case peerdrive_net_store:io_request(Store, ?WRITE_MSG, Req) of
		{ok, <<>>} -> ok;
		Error      -> Error
	end;

do_write(Part, Offset, Data, #state{mps=MaxPS} = S) ->
	<<Chunk1:MaxPS/binary, Chunk2/binary>> = Data,
	case do_write(Part, Offset, Chunk1, S) of
		ok ->
			do_write(Part, Offset+MaxPS, Chunk2, S);
		Error ->
			Error
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
			{ok, unicode:characters_to_binary(Type)};

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
				?ASSERT_GUID_LIST(Parents),
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


do_commit(#state{store=Store, handle=Handle}) ->
	Req = peerdrive_netstore_pb:encode_commitreq(#commitreq{handle=Handle}),
	case peerdrive_net_store:io_request(Store, ?COMMIT_MSG, Req) of
		{ok, Cnf} ->
			#commitcnf{rev=Rev} = peerdrive_netstore_pb:decode_commitcnf(Cnf),
			{ok, Rev};

		Error ->
			Error
	end.


do_suspend(#state{store=Store, handle=Handle}) ->
	Req = peerdrive_netstore_pb:encode_suspendreq(#suspendreq{handle=Handle}),
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

