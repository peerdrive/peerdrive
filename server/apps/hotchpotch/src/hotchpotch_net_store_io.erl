%% Hotchpotch
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

-module(hotchpotch_net_store_io).
-behaviour(gen_server).

-export([start_link/4]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).
-import(hotchpotch_netencode, [encode_list/1, encode_list_32/1, encode_string/1,
	parse_string/1, parse_uuid/1, parse_uuid_list/1, parse_list_32/2]).

-record(state, {store, handle, mps}).

-include("store.hrl").
-include("netstore.hrl").

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

handle_call({truncate, Part, Offset}, _From, S) ->
	{reply, do_truncate(Part, Offset, S), S};

handle_call({commit, Mtime}, _From, S) ->
	{reply, do_commit(Mtime, S), S};

handle_call({suspend, Mtime}, _From, S) ->
	{reply, do_suspend(Mtime, S), S};

handle_call({set_type, Type}, _From, S) ->
	{reply, do_set_type(Type, S), S};

handle_call({set_links, DocLinks, RevLinks}, _From, S) ->
	{reply, do_set_links(DocLinks, RevLinks, S), S};

handle_call({set_parents, Parents}, _From, S) ->
	{reply, do_set_parents(Parents, S), S}.


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
	ReqBody = <<Handle:32, Part/binary, Offset:64, Length:32>>,
	hotchpotch_net_store:io_request(Store, ?READ_REQ, ReqBody);

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
	ReqBody = <<Handle:32, Part/binary, Offset:64, Data/binary>>,
	case hotchpotch_net_store:io_request(Store, ?WRITE_REQ, ReqBody) of
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


do_close(S) ->
	relay_request_noresult(?CLOSE_REQ, <<>>, S).


do_get_type(S) ->
	case relay_request(?GET_TYPE_REQ, <<>>, S) of
		{ok, Body} ->
			{Type, <<>>} = parse_string(Body),
			{ok, Type};

		Error ->
			Error
	end.


do_get_parents(S) ->
	case relay_request(?GET_PARENTS_REQ, <<>>, S) of
		{ok, Body} ->
			{Parents, <<>>} = parse_uuid_list(Body),
			{ok, Parents};

		Error ->
			Error
	end.


do_get_links(S) ->
	case relay_request(?GET_LINKS_REQ, <<>>, S) of
		{ok, Body} ->
			{DocLinks, Body1} = parse_list_32(fun(B) -> parse_uuid(B) end, Body),
			{RevLinks, <<>>} = parse_list_32(fun(B) -> parse_uuid(B) end, Body1),
			{ok, {DocLinks, RevLinks}};

		Error ->
			Error
	end.


do_truncate(Part, Offset, S) ->
	relay_request_noresult(?TRUNC_REQ, <<Part/binary, Offset:64>>, S).


do_commit(Mtime, S) ->
	case relay_request(?COMMIT_REQ, <<Mtime:64>>, S) of
		{ok, Body} ->
			{Rev, <<>>} = parse_uuid(Body),
			{ok, Rev};

		Error ->
			Error
	end.


do_suspend(Mtime, S) ->
	case relay_request(?SUSPEND_REQ, <<Mtime:64>>, S) of
		{ok, Body} ->
			{Rev, <<>>} = parse_uuid(Body),
			{ok, Rev};

		Error ->
			Error
	end.


do_set_type(Type, S) ->
	relay_request_noresult(?SET_TYPE_REQ, encode_string(Type), S).


do_set_links(DocLinks, RevLinks, S) ->
	Body = <<
		(encode_list_32(DocLinks))/binary,
		(encode_list_32(RevLinks))/binary
	>>,
	relay_request_noresult(?SET_LINKS_REQ, Body, S).


do_set_parents(Parents, S) ->
	relay_request_noresult(?SET_PARENTS_REQ, encode_list(Parents), S).


relay_request(Request, Body, #state{handle=Handle, store=Store}) ->
	hotchpotch_net_store:io_request(Store, Request, <<Handle:32, Body/binary>>).


relay_request_noresult(Request, Body, S) ->
	case relay_request(Request, Body, S) of
		{ok, <<>>} -> ok;
		Error      -> Error
	end.

