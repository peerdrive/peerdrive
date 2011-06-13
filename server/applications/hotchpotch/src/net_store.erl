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

-module(net_store).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).
-export([io_request/3]).

-import(netencode, [encode_linkmap/1, encode_list/1, encode_list/2,
	encode_string/1, parse_direct_result/1, parse_linkmap/1, parse_list/2,
	parse_list_32/2, parse_string/1, parse_uuid/1, parse_uuid_list/1]).

-include("store.hrl").
-include("netstore.hrl").

-record(state, {socket, id, requests, guid, mps, synclocks}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Id, {Address, Port, Name}) ->
	RegId = list_to_atom(atom_to_list(Id) ++ "_store"),
	gen_server:start_link({local, RegId}, ?MODULE, {Id, Address, Port, Name}, []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions used by helper processes...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

io_request(NetStore, Request, Body) ->
	gen_server:call(NetStore, {io_request, Request, Body}, infinity).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Gen_server callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Id, Address, Port, Name}) ->
	Options = [binary, {packet, 2}, {active, false}, {nodelay, true},
		{keepalive, true}],
	case gen_tcp:connect(Address, Port, Options) of
		{ok, Socket} ->
			process_flag(trap_exit, true),
			S = #state{
				socket    = Socket,
				id        = Id,
				requests  = gb_trees:empty(),
				synclocks = dict:new()
			},
			do_init(Name, S);

		{error, Reason} ->
			{stop, Reason}
	end.


terminate(_Reason, #state{socket=Socket, requests=Requests}) ->
	lists:foreach(
		fun
			({From, _OkHandler, ErrHandler}) ->
				gen_server:reply(From, ErrHandler({error, eio}));
			(_) ->
				ok
		end,
		gb_trees:values(Requests)),
	case Socket of
		undefined -> ok;
		_Else     -> gen_tcp:close(Socket)
	end.


handle_info({tcp, _Socket, Packet}, S) ->
	handle_packet(Packet, S);

handle_info({tcp_closed, _Socket}, #state{} = S) ->
	{stop, normal, S#state{socket=undefined}};

handle_info({'EXIT', From, Reason}, S) ->
	case sync_trap_exit(From, S) of
		error ->
			% must be an associated worker process
			case Reason of
				normal   -> {noreply, S};
				shutdown -> {noreply, S};
				_ ->        {stop, {eunexpected, Reason}, S}
			end;

		Else ->
			% a sync process went away
			Else
	end.


handle_call({io_request, Request, Body}, From, S) ->
	req_io_op(From, Request, Body, S);

handle_call(guid, _From, S) ->
	{reply, S#state.guid, S};

handle_call(statfs, From, S) ->
	send_request(From, ?STATFS_REQ, <<>>, fun cnf_statfs/1, S);

handle_call({lookup, Doc}, From, S) ->
	send_request(From, ?LOOKUP_REQ, Doc, fun cnf_lookup/1, fun(_) -> error end, S);

handle_call({contains, Rev}, From, S) ->
	send_request(From, ?CONTAINS_REQ, Rev, fun cnf_contains/1, fun cnf_contains/1, S);

handle_call({stat, Rev}, From, S) ->
	send_request(From, ?STAT_REQ, Rev, fun cnf_stat/1, S);

handle_call({peek, Rev}, From, S) ->
	req_start_io(?PEEK_REQ, Rev, From, S);

handle_call({create, Doc, Type, Creator}, From, S) ->
	Body = <<Doc/binary, (encode_string(Type))/binary,
		(encode_string(Creator))/binary>>,
	req_start_io(?CREATE_REQ, Body, From, S);

handle_call({fork, Doc, StartRev, Creator}, From, S) ->
	Body = <<Doc/binary, StartRev/binary, (encode_string(Creator))/binary>>,
	req_start_io(?FORK_REQ, Body, From, S);

handle_call({update, Doc, StartRev, Creator}, From, S) ->
	BinCreator = case Creator of
		keep -> <<>>;
		_    -> Creator
	end,
	Body = <<Doc/binary, StartRev/binary, (encode_string(BinCreator))/binary>>,
	req_start_io(?UPDATE_REQ, Body, From, S);

handle_call({resume, Doc, PreRev, Creator}, From, S) ->
	BinCreator = case Creator of
		keep -> <<>>;
		_    -> Creator
	end,
	Body = <<Doc/binary, PreRev/binary, (encode_string(BinCreator))/binary>>,
	req_start_io(?RESUME_REQ, Body, From, S);

handle_call({forget, Doc, PreRev}, From, S) ->
	send_request(From, ?FORGET_REQ, <<Doc/binary, PreRev/binary>>, S);

handle_call({delete_rev, Rev}, From, S) ->
	send_request(From, ?DELETE_REV_REQ, Rev, S);

handle_call({delete_doc, Doc, Rev}, From, S) ->
	send_request(From, ?DELETE_DOC_REQ, <<Doc/binary, Rev/binary>>, S);

handle_call({put_doc, Doc, Rev}, From, S) ->
	Body = <<Doc/binary, Rev/binary>>,
	send_request(From, ?PUT_DOC_REQ, Body, S);

handle_call({forward_doc, Doc, RevPath}, From, S) ->
	req_forward_doc(Doc, RevPath, From, S);

handle_call({put_rev, Rev, Revision}, From, S) ->
	req_put_rev(Rev, Revision, From, S);

handle_call({sync_get_changes, PeerGuid}, From, S) ->
	req_sync_get_changes(PeerGuid, From, S);

handle_call({sync_set_anchor, PeerGuid, SeqNum}, From, S) ->
	Body = <<PeerGuid/binary, SeqNum:64>>,
	send_request(From, ?SYNC_SET_ANCHOR_REQ, Body, S);

handle_call({sync_finish, PeerGuid}, From, S) ->
	req_sync_finish(PeerGuid, From, S).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

code_change(_, State, _) -> {ok, State}.
handle_cast(_Request, State) -> {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Request handlers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_init(Name, #state{id=Id, socket=Socket} = S) ->
	InitReq = <<0:32, ?INIT_REQ:16, 0:32, (encode_string(Name))/binary>>,
	try
		InitCnf = case gen_tcp:send(Socket, InitReq) of
			ok ->
				case gen_tcp:recv(Socket, 0, 5000) of
					{ok, Packet} -> Packet;
					Error1       -> throw(Error1)
				end;
			Error2 ->
				throw(Error2)
		end,
		InitCnfBody = case InitCnf of
			<<0:32, ?INIT_CNF:16, Body/binary>> -> Body;
			_ -> throw({error, einval})
		end,
		S2 = case parse_direct_result(InitCnfBody) of
			{ok, <<Ver:32, MaxPacketSize:32, Guid:16/binary>>} ->
				case Ver of
					0 ->
						volman:reg_store(Id, Guid),
						S#state{guid=Guid, mps=MaxPacketSize};
					_ ->
						throw({error, erpcmismatch})
				end;

			Error3 ->
				throw(Error3)
		end,
		inet:setopts(Socket, [{active, true}]),
		{ok, S2}
	catch
		throw:Error -> gen_tcp:close(Socket), {stop, Error}
	end.


cnf_statfs(Body) ->
	<<BSize:32, Blocks:64, BFree:64, BAvail:64>> = Body,
	Stat = #fs_stat{
		bsize  = BSize,
		blocks = Blocks,
		bfree  = BFree,
		bavail = BAvail
	},
	{ok, Stat}.


cnf_lookup(Body) ->
	{Rev, Body1} = parse_uuid(Body),
	{PreRevs, <<>>} = parse_uuid_list(Body1),
	{ok, Rev, PreRevs}.


cnf_contains(<<>>) ->
	true;

cnf_contains({error, enoent}) ->
	false.


cnf_stat(Body) ->
	<<Flags:32, Body1/binary>> = Body,
	{Parts, Body2} = parse_list(
		fun(<<FourCC:4/binary, Size:64, Hash:16/binary, Rest/binary>>) ->
			{{FourCC, Size, Hash}, Rest}
		end,
		Body1),
	{Parents, Body3} = parse_uuid_list(Body2),
	<<Mtime:64, Body4/binary>> = Body3,
	{Type, Body5} = parse_string(Body4),
	{Creator, Body6} = parse_string(Body5),
	{Links, <<>>} = parse_linkmap(Body6),
	Stat = #rev_stat{
		flags   = Flags,
		parts   = Parts,
		parents = Parents,
		mtime   = Mtime,
		type    = Type,
		creator = Creator,
		links   = Links
	},
	{ok, Stat}.


req_start_io(Request, Body, From, #state{mps=MPS} = S) ->
	{User, _Tag} = From,
	Handler = fun(B) -> cnf_start_io(B, MPS, User) end,
	send_request(From, Request, Body, Handler, S).


cnf_start_io(<<Handle:32>>, MaxPacketSize, User) ->
	net_store_io:start_link(self(), Handle, MaxPacketSize, User).


req_forward_doc(Doc, RevPath, From, S) ->
	{User, _Tag} = From,
	Body = <<Doc/binary, (encode_list(RevPath))/binary>>,
	Handler = fun(B) -> cnf_forward_doc(B, User) end,
	send_request(From, ?FF_DOC_START_REQ, Body, Handler, S).


cnf_forward_doc(Body, User) ->
	<<Handle:32, Body1/binary>> = Body,
	{Missing, <<>>} = parse_uuid_list(Body1),
	case Missing of
		[] ->
			ok;
		_ ->
			{ok, Importer} = net_store_forwarder:start_link(self(), Handle, User),
			{ok, Missing, Importer}
	end.


req_put_rev(Rev, Revision, From, #state{mps=MPS} = S) ->
	{User, _Tag} = From,
	#revision{
		flags   = Flags,
		parts   = Parts,
		parents = Parents,
		mtime   = Mtime,
		type    = TypeCode,
		creator = CreatorCode,
		links   = Links
	} = Revision,
	ReqParts = encode_list(
		fun ({FourCC, Hash}) -> <<FourCC/binary, Hash/binary>> end,
		Parts),
	Body = <<
		Rev/binary,
		Flags:32,
		ReqParts/binary,
		(encode_list(Parents))/binary,
		Mtime:64,
		(encode_string(TypeCode))/binary,
		(encode_string(CreatorCode))/binary,
		(encode_linkmap(Links))/binary
	>>,
	Handler = fun(B) -> cnf_put_rev(B, MPS, User) end,
	send_request(From, ?PUT_REV_START_REQ, Body, Handler, S).


cnf_put_rev(Body, MaxPacketSize, User) ->
	<<Handle:32, Body1/binary>> = Body,
	{Missing, <<>>} = parse_list(
		fun(<<FCC:4/binary, Rest/binary>>) -> {FCC, Rest} end,
		Body1),
	case Missing of
		[] ->
			ok;
		_ ->
			{ok, Importer} = net_store_importer:start_link(self(), Handle,
				MaxPacketSize, User),
			{ok, Missing, Importer}
	end.


req_sync_get_changes(PeerGuid, {Caller, _} = From, S) ->
	case sync_lock(PeerGuid, Caller, S) of
		{ok, S2} ->
			send_request(From, ?SYNC_GET_CHANGES_REQ, PeerGuid,
				fun cnf_sync_get_changes/1, S2);
		error ->
			{reply, {error, ebusy}, S}
	end.


cnf_sync_get_changes(Body) ->
	{Backlog, <<>>} = parse_list_32(
		fun(<<Doc:16/binary, SeqNum:64, Rest/binary>>) ->
			{{Doc, SeqNum}, Rest}
		end,
		Body),
	{ok, Backlog}.


req_sync_finish(PeerGuid, {Caller, _} = From, #state{synclocks=SLocks} = S) ->
	case dict:find(PeerGuid, SLocks) of
		{ok, Caller} ->
			unlink(Caller),
			S2 = S#state{synclocks=dict:erase(PeerGuid, SLocks)},
			send_request(From, ?SYNC_FINISH_REQ, PeerGuid, S2);

		{ok, _Other} ->
			{reply, {error, eacces}, S};
		error ->
			{reply, {error, einval}, S}
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

req_io_op(From, Request, Body, S) ->
	send_request(From, Request, Body, fun cnf_io_op/1, S).


cnf_io_op(Body) ->
	{ok, Body}.


send_request(From, Req, Body, S) ->
	send_request_internal(Req, Body, {From, fun(<<>>) -> ok end, fun(E) -> E end}, S).


send_request(From, Req, Body, Handler, S) ->
	send_request_internal(Req, Body, {From, Handler, fun(E) -> E end}, S).


send_request(From, Req, Body, OkHandler, ErrHandler, S) ->
	send_request_internal(Req, Body, {From, OkHandler, ErrHandler}, S).


send_request_internal(Req, Body, Continuation, S) ->
	#state{socket=Socket, requests=Requests} = S,
	Ref = get_next_ref(S),
	case gen_tcp:send(Socket, <<Ref:32, Req:16, Body/binary>>) of
		ok ->
			S2 = S#state{requests=gb_trees:enter(Ref, Continuation, Requests)},
			{noreply, S2};

		{error, Reason} ->
			error_logger:warning_report([{module, ?MODULE}, {send_error, Reason}]),
			{stop, normal, {error, eio}, S}
	end.


get_next_ref(#state{requests=Requests}) ->
	case gb_trees:is_empty(Requests) of
		true  -> 0;
		false -> {Key, _Val} = gb_trees:largest(Requests), Key+1
	end.


handle_packet(<<Ref:32, Opcode:16, Body/binary>>, S) ->
	case Opcode band 3 of
		1 -> handle_confirm(Ref, Opcode, Body, S);
		2 -> handle_indication(Opcode, Body, S)
	end.


handle_confirm(Ref, _Cnf, Body, #state{requests=Requests} = S) ->
	S2 = S#state{requests=gb_trees:delete(Ref, Requests)},
	case gb_trees:get(Ref, Requests) of
		{From, OkHandler, ErrHandler} ->
			Reply = case parse_direct_result(Body) of
				{ok, Rest} -> OkHandler(Rest);
				Error      -> ErrHandler(Error)
			end,
			gen_server:reply(From, Reply),
			{noreply, S2};

		ignore ->
			{noreply, S2};

		Handler ->
			Handler(Body, S2)
	end.


handle_indication(Ind, Body, #state{guid=Guid} = S) ->
	{Uuid, <<>>} = parse_uuid(Body),
	case Ind of
		?ADD_REV_IND -> vol_monitor:trigger_add_rev(Guid, Uuid);
		?REM_REV_IND -> vol_monitor:trigger_rm_rev(Guid, Uuid);
		?ADD_DOC_IND -> vol_monitor:trigger_add_doc(Guid, Uuid);
		?REM_DOC_IND -> vol_monitor:trigger_rm_doc(Guid, Uuid);
		?MOD_DOC_IND -> vol_monitor:trigger_mod_doc(Guid, Uuid)
	end,
	{noreply, S}.


sync_lock(PeerGuid, Caller, #state{synclocks=SLocks} = S) ->
	case dict:find(PeerGuid, SLocks) of
		{ok, Caller} ->
			{ok, S};
		{ok, _Other} ->
			error;
		error ->
			link(Caller),
			{ok, S#state{synclocks=dict:store(PeerGuid, Caller, SLocks)}}
	end.


sync_trap_exit(From, #state{synclocks=SLocks} = S) ->
	Found = dict:fold(
		fun(Guid, Pid, Acc) ->
			case Pid of
				From -> [Guid | Acc];
				_    -> Acc
			end
		end,
		[],
		SLocks),
	case Found of
		[] ->
			error;
		_ ->
			Cleanup = fun(Guid, State) ->
				case send_request_internal(?SYNC_FINISH_REQ, Guid, ignore, State) of
					{noreply, NewState} ->
						NewState;
					{stop, Reason, _Error, NewState} ->
						throw({stop, Reason, NewState})
				end
			end,
			S2 = S#state{synclocks=dict:filter(fun(_, Pid) -> Pid =/= From end, SLocks)},
			try
				{noreply, lists:foldl(Cleanup, S2, Found)}
			catch
				throw:Error -> Error
			end
	end.

