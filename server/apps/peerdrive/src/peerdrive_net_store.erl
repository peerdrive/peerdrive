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

-module(peerdrive_net_store).
-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).
-export([io_request/3, io_request_async/4]).

-include("store.hrl").
-include("netstore.hrl").
-include("peerdrive_netstore_pb.hrl").
-include("utils.hrl").

-record(state, {socket, requests, guid, mps, synclocks, transport=gen_tcp}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Address, Options, _Credentials) ->
	gen_server:start_link(?MODULE, {Address, Options}, []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions used by helper processes...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

io_request(NetStore, Request, Body) ->
	try
		gen_server:call(NetStore, {io_request, Request, Body}, infinity)
	catch
		exit:_ -> {error, enxio}
	end.


io_request_async(NetStore, Request, Body, Finish) ->
	try
		gen_server:call(NetStore, {io_request_async, Request, Body, Finish}, infinity)
	catch
		exit:_ -> {error, enxio}
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Gen_server callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Address, Options}) ->
	try
		{IpAddress, Port, Name} = parse_address(Address),
		Tls = parse_tls(Options),
		TcpOptions = [binary, {packet, 2}, {active, false}, {nodelay, true},
			{keepalive, true}],
		case gen_tcp:connect(IpAddress, Port, TcpOptions) of
			{ok, Socket} ->
				process_flag(trap_exit, true),
				S = #state{
					socket    = Socket,
					transport = gen_tcp,
					requests  = gb_trees:empty(),
					synclocks = dict:new()
				},
				S2 = do_init(Tls, S),
				S3 = do_mount(Name, proplists:get_bool(<<"noverify">>, Options), S2),
				case S3#state.transport of
					gen_tcp ->
						inet:setopts(S3#state.socket, [{active, true}]);
					ssl ->
						ssl:setopts(S3#state.socket, [{active, true}])
				end,
				{ok, S3};

			{error, TcpErr} ->
				{stop, TcpErr}
		end
	catch
		throw:Reason ->
			{stop, Reason}
	end.


terminate(_Reason, #state{socket=Socket, requests=Requests, transport=Trsp}) ->
	lists:foreach(
		fun
			({From, _OkHandler, ErrHandler}) ->
				gen_server:reply(From, ErrHandler({error, enxio}));
			(_) ->
				ok
		end,
		gb_trees:values(Requests)),
	case Socket of
		undefined -> ok;
		_Else     -> Trsp:close(Socket)
	end.


handle_info({tcp, _Socket, Packet}, S) ->
	handle_packet(Packet, S);

handle_info({ssl, _Socket, Packet}, S) ->
	handle_packet(Packet, S);

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
	end;

handle_info({tcp_closed, _Socket}, #state{} = S) ->
	{stop, normal, S#state{socket=undefined}};

handle_info({ssl_closed, _Socket}, #state{} = S) ->
	{stop, normal, S#state{socket=undefined}};

handle_info({tcp_error, _Socket, Reason}, S) ->
	{stop, {tcp_error, Reason}, S#state{socket=undefined}};

handle_info({ssl_error, _Socket, Reason}, S) ->
	{stop, {ssl_error, Reason}, S#state{socket=undefined}}.


handle_call({io_request, Request, Body}, From, S) ->
	req_io_op(From, Request, Body, S);

handle_call({io_request_async, Request, Body, Finish}, _From, S) ->
	req_io_op_async(Request, Body, Finish, S);

handle_call(guid, _From, S) ->
	{reply, S#state.guid, S};

handle_call(statfs, From, S) ->
	send_request(From, ?STATFS_MSG, <<>>, fun cnf_statfs/1, S);

handle_call({lookup, Doc}, From, S) ->
	Req = peerdrive_netstore_pb:encode_lookupreq(#lookupreq{doc=Doc}),
	send_request(From, ?LOOKUP_MSG, Req, fun cnf_lookup/1, S);

handle_call({contains, Rev}, From, S) ->
	Req = peerdrive_netstore_pb:encode_containsreq(#containsreq{rev=Rev}),
	send_request(From, ?CONTAINS_MSG, Req, fun cnf_contains/1, fun cnf_contains/1, S);

handle_call({stat, Rev}, From, S) ->
	Req = peerdrive_netstore_pb:encode_statreq(#statreq{rev=Rev}),
	send_request(From, ?STAT_MSG, Req, fun cnf_stat/1, S);

handle_call({get_links, Rev}, From, S) ->
	Req = peerdrive_netstore_pb:encode_getlinksreq(#getlinksreq{rev=Rev}),
	send_request(From, ?GET_LINKS_MSG, Req, fun cnf_get_links/1, S);

handle_call({peek, Rev}, {User, _Tag} = From, #state{mps=MPS} = S) ->
	Req = peerdrive_netstore_pb:encode_peekreq(#peekreq{rev=Rev}),
	Handler = fun(B) -> cnf_peek(B, MPS, User) end,
	send_request(From, ?PEEK_MSG, Req, Handler, S);

handle_call({create, Type, Creator}, {User, _Tag} = From, #state{mps=MPS} = S) ->
	Req = peerdrive_netstore_pb:encode_createreq(#createreq{type_code=Type,
		creator_code=Creator}),
	Handler = fun(B) -> cnf_create(B, MPS, User) end,
	send_request(From, ?CREATE_MSG, Req, Handler, S);

handle_call({fork, StartRev, Creator}, {User, _Tag} = From, #state{mps=MPS} = S) ->
	Req = peerdrive_netstore_pb:encode_forkreq(#forkreq{rev=StartRev,
		creator_code=Creator}),
	Handler = fun(B) -> cnf_fork(B, MPS, User) end,
	send_request(From, ?FORK_MSG, Req, Handler, S);

handle_call({update, Doc, StartRev, Creator}, {User, _Tag} = From, #state{mps=MPS} = S) ->
	Req = peerdrive_netstore_pb:encode_updatereq(#updatereq{doc=Doc,
		rev=StartRev, creator_code=Creator}),
	Handler = fun(B) -> cnf_update(B, MPS, User) end,
	send_request(From, ?UPDATE_MSG, Req, Handler, S);

handle_call({resume, Doc, PreRev, Creator}, {User, _Tag} = From, #state{mps=MPS} = S) ->
	Req = peerdrive_netstore_pb:encode_resumereq(#resumereq{doc=Doc,
		rev=PreRev, creator_code=Creator}),
	Handler = fun(B) -> cnf_resume(B, MPS, User) end,
	send_request(From, ?RESUME_MSG, Req, Handler, S);

handle_call({forget, Doc, PreRev}, From, S) ->
	Req = peerdrive_netstore_pb:encode_forgetreq(#forgetreq{doc=Doc,
		rev=PreRev}),
	send_request(From, ?FORGET_MSG, Req, S);

handle_call({delete_rev, Rev}, From, S) ->
	Req = peerdrive_netstore_pb:encode_deleterevreq(#deleterevreq{rev=Rev}),
	send_request(From, ?DELETE_REV_MSG, Req, S);

handle_call({delete_doc, Doc, Rev}, From, S) ->
	Req = peerdrive_netstore_pb:encode_deletedocreq(#deletedocreq{doc=Doc,
		rev=Rev}),
	send_request(From, ?DELETE_DOC_MSG, Req, S);

handle_call({put_doc, Doc, Rev}, From, S) ->
	req_put_doc(Doc, Rev, From, S);

handle_call({forward_doc, Doc, RevPath, OldPreRev}, From, S) ->
	req_forward_doc(Doc, RevPath, OldPreRev, From, S);

handle_call({put_rev, Rev, Revision, Data, DocLinks, RevLinks}, From, S) ->
	req_put_rev(Rev, Revision, Data, DocLinks, RevLinks, From, S);

handle_call({remember_rev, DId, PreRId, OldPreRId}, From, S) ->
	req_remember_rev(DId, PreRId, OldPreRId, From, S);

handle_call({sync_get_changes, PeerGuid, Anchor}, From, S) ->
	req_sync_get_changes(PeerGuid, Anchor, From, S);

handle_call({sync_get_anchor, FromSId, ToSId}, From, S) ->
	req_sync_get_anchor(FromSId, ToSId, From, S);

handle_call({sync_set_anchor, FromSId, ToSId, SeqNum}, From, S) ->
	Req = peerdrive_netstore_pb:encode_syncsetanchorreq(#syncsetanchorreq{
		from_sid=FromSId, to_sid=ToSId, seq_num=SeqNum}),
	send_request(From, ?SYNC_SET_ANCHOR_MSG, Req, S);

handle_call({sync_finish, PeerGuid}, From, S) ->
	req_sync_finish(PeerGuid, From, S);

handle_call(sync, From, S) ->
	send_request(From, ?SYNC_MSG, <<>>, S).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

code_change(_, State, _) -> {ok, State}.
handle_cast(_Request, State) -> {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Request handlers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_init(Tls, #state{socket=Socket} = S) ->
	case Tls of
		deny -> TlsReq = deny, SslOpts = [];
		{TlsReq, SslOpts} -> ok
	end,
	Req = peerdrive_netstore_pb:encode_initreq(#initreq{major=3, minor=0,
		starttls=TlsReq}),
	InitReq = [<<0:32, ?INIT_MSG:12, ?FLAG_REQ:4>>, Req],
	try
		InitCnfMsg = case gen_tcp:send(Socket, InitReq) of
			ok ->
				case gen_tcp:recv(Socket, 0, 5000) of
					{ok, Packet} -> Packet;
					Error1       -> throw(Error1)
				end;
			Error2 ->
				throw(Error2)
		end,
		#initcnf{starttls=StartTls} = InitCnf = case InitCnfMsg of
			<<0:32, ?INIT_MSG:12, ?FLAG_CNF:4, Body/binary>> ->
				peerdrive_netstore_pb:decode_initcnf(Body);

			<<0:32, ?ERROR_MSG:12, ?FLAG_CNF:4, Body/binary>> ->
				#errorcnf{error=Error3} =
					peerdrive_netstore_pb:decode_errorcnf(Body),
				throw({error, Error3});

			_ ->
				throw({error, einval})
		end,
		S2 = case InitCnf of
			#initcnf{major=3, minor=0, max_packet_size=MaxPacketSize} ->
				S#state{mps=MaxPacketSize};
			#initcnf{} ->
				throw({error, erpcmismatch})
		end,
		if
			StartTls and (TlsReq =/= deny) ->
				case ssl:connect(Socket, SslOpts, 5000) of
					{ok, SslSocket} ->
						S2#state{transport=ssl, socket=SslSocket};
					Error4 ->
						throw(Error4)
				end;
			not StartTls and (TlsReq =/= required) ->
				S2;
			true ->
				throw({error, ebade})
		end
	catch
		throw:{error, Error} ->
			gen_tcp:close(Socket), throw(fixup_sock_err(Error))
	end.


do_mount(Name, NoVerify, #state{transport=Trsp, socket=Socket} = S) ->
	Req = peerdrive_netstore_pb:encode_mountreq(#mountreq{
		store=Name, no_verify=NoVerify}),
	MountReq = [<<0:32, ?MOUNT_MSG:12, ?FLAG_REQ:4>>, Req],
	try
		MountCnfMsg = case Trsp:send(Socket, MountReq) of
			ok ->
				case Trsp:recv(Socket, 0, 5000) of
					{ok, Packet} -> Packet;
					Error1       -> throw(Error1)
				end;
			Error2 ->
				throw(Error2)
		end,
		#mountcnf{sid=SId} = case MountCnfMsg of
			<<0:32, ?MOUNT_MSG:12, ?FLAG_CNF:4, Body/binary>> ->
				peerdrive_netstore_pb:decode_mountcnf(Body);

			<<0:32, ?ERROR_MSG:12, ?FLAG_CNF:4, Body/binary>> ->
				#errorcnf{error=Error3} =
					peerdrive_netstore_pb:decode_errorcnf(Body),
				throw({error, Error3});

			_ ->
				throw({error, einval})
		end,
		S#state{guid=SId}
	catch
		throw:{error, Error} ->
			Trsp:close(Socket), throw(fixup_sock_err(Error))
	end.


cnf_statfs(Body) ->
	#statfscnf{bsize=BSize, blocks=Blocks, bfree=BFree, bavail=BAvail} =
		peerdrive_netstore_pb:decode_statfscnf(Body),
	Stat = #fs_stat{
		bsize  = BSize,
		blocks = Blocks,
		bfree  = BFree,
		bavail = BAvail
	},
	{ok, Stat}.


cnf_lookup(Body) ->
	#lookupcnf{rev=Rev, pre_revs=PreRevs} =
		peerdrive_netstore_pb:decode_lookupcnf(Body),
	{ok, Rev, PreRevs}.


cnf_contains(Body) when is_binary(Body) ->
	#containscnf{found=Found} = peerdrive_netstore_pb:decode_containscnf(Body),
	Found;

cnf_contains({error, enoent}) ->
	false.


cnf_stat(Body) ->
	#statcnf{
		flags = Flags,
		data = #statcnf_data{size=DataSize, hash=DataHash},
		attachments = Attachments,
		parents = Parents,
		crtime = CrTime,
		mtime = Mtime,
		type_code = TypeCode,
		creator_code = CreatorCode,
		comment = Comment
	} = peerdrive_netstore_pb:decode_statcnf(Body),
	Stat = #rev{
		flags       = Flags,
		data        = #rev_dat{size=DataSize, hash=DataHash},
		attachments = [ #rev_att{name=Name, size=Size, hash=Hash, crtime=CrT,
				mtime=MT}
			|| #statcnf_attachment{name=Name, size=Size, hash=Hash, crtime=CrT,
				mtime=MT}
			<- Attachments ],
		parents   = Parents,
		crtime    = CrTime,
		mtime     = Mtime,
		type      = TypeCode,
		creator   = CreatorCode,
		comment   = Comment
	},
	{ok, Stat}.


cnf_get_links(Body) ->
	#getlinkscnf{doc_links=DocLinks, rev_links=RevLinks} =
		peerdrive_netstore_pb:decode_getlinkscnf(Body),
	{ok, {DocLinks, RevLinks}}.


cnf_peek(Cnf, MaxPacketSize, User) ->
	#peekcnf{handle=Handle} =
		peerdrive_netstore_pb:decode_peekcnf(Cnf),
	peerdrive_net_store_io:start_link(self(), Handle, MaxPacketSize, User).


cnf_create(Cnf, MaxPacketSize, User) ->
	#createcnf{handle=Handle, doc=Doc} =
		peerdrive_netstore_pb:decode_createcnf(Cnf),
	{ok, IoPid} = peerdrive_net_store_io:start_link(self(), Handle,
		MaxPacketSize, User),
	{ok, Doc, IoPid}.


cnf_fork(Cnf, MaxPacketSize, User) ->
	#forkcnf{handle=Handle, doc=Doc} =
		peerdrive_netstore_pb:decode_forkcnf(Cnf),
	{ok, IoPid} = peerdrive_net_store_io:start_link(self(), Handle,
		MaxPacketSize, User),
	{ok, Doc, IoPid}.


cnf_update(Cnf, MaxPacketSize, User) ->
	#updatecnf{handle=Handle} =
		peerdrive_netstore_pb:decode_updatecnf(Cnf),
	peerdrive_net_store_io:start_link(self(), Handle, MaxPacketSize, User).


cnf_resume(Cnf, MaxPacketSize, User) ->
	#resumecnf{handle=Handle} =
		peerdrive_netstore_pb:decode_resumecnf(Cnf),
	peerdrive_net_store_io:start_link(self(), Handle, MaxPacketSize, User).


req_put_doc(Doc, Rev, From, S) ->
	{User, _Tag} = From,
	Req = peerdrive_netstore_pb:encode_putdocreq(#putdocreq{
		doc=Doc, rev=Rev}),
	Handler = fun(B) -> cnf_put_doc(B, User) end,
	send_request(From, ?PUT_DOC_MSG, Req, Handler, S).


cnf_put_doc(Body, User) ->
	#putdoccnf{handle=Handle} =
		peerdrive_netstore_pb:decode_putdoccnf(Body),
	peerdrive_net_store_io:start_link(self(), Handle, 1024, User).


req_forward_doc(Doc, RevPath, OldPreRev, From, S) ->
	{User, _Tag} = From,
	Req = peerdrive_netstore_pb:encode_forwarddocreq(#forwarddocreq{
		doc=Doc, rev_path=RevPath, old_pre_rev=OldPreRev}),
	Handler = fun(B) -> cnf_forward_doc(B, User) end,
	send_request(From, ?FF_DOC_MSG, Req, Handler, S).


cnf_forward_doc(Body, User) ->
	#forwarddoccnf{handle=Handle, missing_revs=Missing} =
		peerdrive_netstore_pb:decode_forwarddoccnf(Body),
	case Handle of
		undefined ->
			ok;
		_ ->
			{ok, Importer} = peerdrive_net_store_io:start_link(self(),
				Handle, 1024, User),
			{ok, Missing, Importer}
	end.


req_put_rev(Rev, Revision, Data, DocLinks, RevLinks, From, #state{mps=MPS} = S) ->
	{User, _Tag} = From,
	#rev{
		flags       = Flags,
		data        = #rev_dat{size=DataSize, hash=DataHash},
		attachments = Attachments,
		parents     = Parents,
		crtime      = CrTime,
		mtime       = Mtime,
		type        = TypeCode,
		creator     = CreatorCode,
		comment     = Comment
	} = Revision,
	Req = peerdrive_netstore_pb:encode_putrevreq(#putrevreq{
		rid = Rev,
		revision = #putrevreq_revision{
			flags = Flags,
			data = #putrevreq_revision_data{size=DataSize, hash=DataHash},
			attachments = [ #putrevreq_revision_attachment{name=Name, size=Size,
					hash=Hash, crtime=CrT, mtime=MT}
				|| #rev_att{name=Name, size=Size, hash=Hash, crtime=CrT, mtime=MT}
				<- Attachments ],
			parents = Parents,
			crtime = CrTime,
			mtime = Mtime,
			type_code = TypeCode,
			creator_code = CreatorCode,
			comment = Comment
		},
		data = Data,
		doc_links = DocLinks,
		rev_links = RevLinks
	}),
	Handler = fun(B) -> cnf_put_rev(B, MPS, User) end,
	send_request(From, ?PUT_REV_MSG, Req, Handler, S).


cnf_put_rev(Body, MaxPacketSize, User) ->
	#putrevcnf{handle=Handle, missing_attachments=Missing} =
		peerdrive_netstore_pb:decode_putrevcnf(Body),
	{ok, Importer} = peerdrive_net_store_io:start_link(self(), Handle,
		MaxPacketSize, User),
	{ok, Missing, Importer}.


req_remember_rev(DId, PreRId, OldPreRId, From, S) ->
	{User, _Tag} = From,
	Req = peerdrive_netstore_pb:encode_rememberrevreq(#rememberrevreq{
		doc=DId, pre_rev=PreRId, old_pre_rev=OldPreRId}),
	Handler = fun(B) -> cnf_remember_rev(B, User) end,
	send_request(From, ?RMBR_REV_MSG, Req, Handler, S).


cnf_remember_rev(Body, User) ->
	#rememberrevcnf{handle=Handle} =
		peerdrive_netstore_pb:decode_rememberrevcnf(Body),
	case Handle of
		undefined ->
			ok;
		_ ->
			{ok, _} = peerdrive_net_store_io:start_link(self(), Handle, 1024, User)
	end.


req_sync_get_changes(PeerGuid, Anchor, {Caller, _} = From, S) ->
	case sync_lock(PeerGuid, Caller, S) of
		{ok, S2} ->
			Req = peerdrive_netstore_pb:encode_syncgetchangesreq(
				#syncgetchangesreq{peer_sid=PeerGuid, anchor=Anchor}),
			send_request(From, ?SYNC_GET_CHANGES_MSG, Req,
				fun cnf_sync_get_changes/1, S2);
		error ->
			{reply, {error, ebusy}, S}
	end.


cnf_sync_get_changes(Body) ->
	#syncgetchangescnf{backlog=Backlog} =
		peerdrive_netstore_pb:decode_syncgetchangescnf(Body),
	{ok, [ {Doc, SeqNum} || #syncgetchangescnf_item{doc=Doc, seq_num=SeqNum} <-
		Backlog ]}.


req_sync_get_anchor(FromSId, ToSId, From, S) ->
	Req = peerdrive_netstore_pb:encode_syncgetanchorreq(
		#syncgetanchorreq{from_sid=FromSId, to_sid=ToSId}),
	send_request(From, ?SYNC_GET_ANCHOR_MSG, Req,
		fun cnf_sync_get_anchor/1, S).


cnf_sync_get_anchor(Body) ->
	#syncgetanchorcnf{anchor=Anchor} =
		peerdrive_netstore_pb:decode_syncgetanchorcnf(Body),
	{ok, Anchor}.


req_sync_finish(PeerGuid, {Caller, _} = From, #state{synclocks=SLocks} = S) ->
	case dict:find(PeerGuid, SLocks) of
		{ok, Caller} ->
			unlink(Caller),
			S2 = S#state{synclocks=dict:erase(PeerGuid, SLocks)},
			Req = peerdrive_netstore_pb:encode_syncfinishreq(#syncfinishreq{
				peer_sid=PeerGuid}),
			send_request(From, ?SYNC_FINISH_MSG, Req, S2);

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


req_io_op_async(Request, Body, Finish, S) ->
	send_request_internal(Request, Body, {Request, Finish}, true, S).


send_request(From, Req, Body, S) ->
	send_request_internal(Req, Body, {From, Req, fun(<<>>) -> ok end, fun(E) -> E end}, S).


send_request(From, Req, Body, Handler, S) ->
	send_request_internal(Req, Body, {From, Req, Handler, fun(E) -> E end}, S).


send_request(From, Req, Body, OkHandler, ErrHandler, S) ->
	send_request_internal(Req, Body, {From, Req, OkHandler, ErrHandler}, S).


send_request_internal(Req, Body, Continuation, S) ->
	send_request_internal(Req, Body, Continuation, false, S).

send_request_internal(Req, Body, Continuation, Asyc, S) ->
	#state{transport=Transport, socket=Socket, requests=Requests} = S,
	Ref = get_next_ref(S),
	case Transport:send(Socket, [<<Ref:32, Req:12, ?FLAG_REQ:4>>, Body]) of
		ok ->
			S2 = S#state{requests=gb_trees:enter(Ref, Continuation, Requests)},
			case Asyc of
				false ->
					{noreply, S2};
				true ->
					{reply, ok, S2}
			end;

		{error, Reason} ->
			error_logger:warning_report([{module, ?MODULE},
				{transport, Transport}, {send_error, Reason}]),
			{stop, normal, {error, eio}, S}
	end.


get_next_ref(#state{requests=Requests}) ->
	case gb_trees:is_empty(Requests) of
		true  -> 0;
		false -> {Key, _Val} = gb_trees:largest(Requests), Key+1
	end.


handle_packet(<<Ref:32, Opcode:12, Type:4, Body/binary>>, S) ->
	try
		case Type of
			?FLAG_CNF -> handle_confirm(Ref, Opcode, Body, S);
			?FLAG_IND -> handle_indication(Opcode, Body, S)
		end
	catch
		throw:_ -> ok
	end.


handle_confirm(Ref, Cnf, Body, #state{requests=Requests} = S) ->
	S2 = S#state{requests=gb_trees:delete(Ref, Requests)},
	case gb_trees:get(Ref, Requests) of
		{From, Req, OkHandler, ErrHandler} ->
			Reply = try
				case Cnf of
					Req ->
						OkHandler(Body);
					?ERROR_MSG ->
						#errorcnf{error=Error} =
							peerdrive_netstore_pb:decode_errorcnf(Body),
						ErrHandler({error, Error})
				end
			catch
				throw:Err -> Err
			end,
			gen_server:reply(From, Reply),
			{noreply, S2};

		{Req, Handler} ->
			case Cnf of
				Req ->
					Handler({ok, Body});
				?ERROR_MSG ->
					#errorcnf{error=Error} =
						peerdrive_netstore_pb:decode_errorcnf(Body),
					Handler({error, Error})
			end,
			{noreply, S2};

		ignore ->
			{noreply, S2}
	end.


handle_indication(?TRIGGER_MSG, Body, #state{guid=Guid} = S) ->
	#triggerind{event=Event, element=Element} =
		peerdrive_netstore_pb:decode_triggerind(Body),
	case Event of
		add_rev -> peerdrive_vol_monitor:trigger_add_rev(Guid, Element);
		rem_rev -> peerdrive_vol_monitor:trigger_rm_rev(Guid, Element);
		add_doc -> peerdrive_vol_monitor:trigger_add_doc(Guid, Element);
		rem_doc -> peerdrive_vol_monitor:trigger_rm_doc(Guid, Element);
		mod_doc -> peerdrive_vol_monitor:trigger_mod_doc(Guid, Element)
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
				Req = peerdrive_netstore_pb:encode_syncfinishreq(
					#syncfinishreq{peer_sid=Guid}),
				case send_request_internal(?SYNC_FINISH_MSG, Req, ignore, State) of
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


fixup_sock_err({error, Error}) -> {error, fixup_sock_err(Error)};
fixup_sock_err(closed) -> econnaborted;
fixup_sock_err(timeout) -> etimedout;
fixup_sock_err(ecacertfile) -> einval;
fixup_sock_err(ecertfile) -> einval;
fixup_sock_err(ekeyfile) -> einval;
fixup_sock_err(esslaccept) -> erpcmismatch;
fixup_sock_err(esslconnect) -> erpcmismatch;
fixup_sock_err({eoptions, _}) -> einval;
fixup_sock_err(Posix) -> Posix.


% {IpAddress, Port, Name} = "name@xxx.xxx.xxx.xxx:port"
parse_address(Address) ->
	Res = re:run(Address, "^(.+)@([-.[:alnum:]]+)(:[0-9]+)?$",
		[{capture, all_but_first, list}]),
	case Res of
		{match, [Name, Ip]} ->
			{Ip, 4568, Name};
		{match, [Name, Ip, [_|PortStr]]} ->
			Port = try
				list_to_integer(PortStr)
			catch
				error:badarg -> throw(einval)
			end,
			{Ip, Port, Name};
		_ ->
			throw(einval)
	end.


% deny | {optional, SslOpts} | {required, SslOpts}
parse_tls(Options) ->
	case proplists:get_value(<<"tls">>, Options, <<"deny">>) of
		<<"deny">> ->
			deny;
		<<"optional">> ->
			{optional, parse_ssl_opts(Options)};
		<<"required">> ->
			{required, parse_ssl_opts(Options)};
		_ ->
			throw(einval)
	end.


parse_ssl_opts(Options) ->
	Verify = case proplists:get_value(<<"verify_peer">>, Options, <<"true">>) of
		<<"true">> ->
			[{verify, verify_peer}, {fail_if_no_peer_cert, true}];
		<<"false">> ->
			[{verify, verify_none}];
		_ ->
			throw(einval)
	end,
	lists:foldl(
		fun
			({<<"cacertfile">>, CaCert}, Acc) ->
				[{cacertfile, unicode:characters_to_list(CaCert)} | Acc];
			({<<"certfile">>, Cert}, Acc) ->
				[{certfile, unicode:characters_to_list(Cert)} | Acc];
			({<<"keyfile">>, Key}, Acc) ->
				[{keyfile, unicode:characters_to_list(Key)} | Acc];
			(_, Acc) ->
				Acc
		end,
		Verify,
		Options).

