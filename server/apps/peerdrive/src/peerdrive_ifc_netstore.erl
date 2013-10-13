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

-module(peerdrive_ifc_netstore).
-export([init/2, handle_packet/2, handle_info/2, terminate/1]).
-export([init_listen/2, terminate_listen/1]).

-include("store.hrl").
-include("netstore.hrl").
-include("peerdrive_netstore_pb.hrl").
-include("utils.hrl").
-include("volman.hrl").

-record(state, {init, handles, next, stores, store_pid, store_uuid, tls}).
-record(retpath, {servlet, req, ref}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Servlet callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_listen(_Socket, _Options) ->
	{ok, []}.


terminate_listen([]) ->
	ok.


init(Options, []) ->
	process_flag(trap_exit, true),
	peerdrive_vol_monitor:register_proc(),
	Stores = proplists:get_value(stores, Options, []),
	Tls = proplists:get_value(tls, Options, deny),
	#state{init=false, handles=dict:new(), next=0, stores=Stores, tls=Tls}.


terminate(State) ->
	peerdrive_vol_monitor:deregister_proc(),
	dict:fold(
		fun(_Handle, Worker, _Acc) -> Worker ! closed end,
		ok,
		State#state.handles).


handle_info({send, Data}, S) ->
	{reply, Data, S};

handle_info({done, Handle}, S) ->
	{ok, S#state{handles=dict:erase(Handle, S#state.handles)}};

handle_info({'EXIT', _From, normal}, S) ->
	{ok, S};

handle_info({vol_event, Event, Store, Element}, #state{store_uuid=Store}=S) ->
	case Event of
		rem_store ->
			{stop, S};
		add_store ->
			{ok, S};
		_ ->
			Ind = peerdrive_netstore_pb:encode_triggerind(
				#triggerind{event=Event, element=Element}),
			send_indication(?TRIGGER_MSG, Ind, S)
	end;

handle_info({vol_event, _Event, _Store, _Element}, S) ->
	{ok, S};

handle_info({'EXIT', From, Reason}, S) ->
	case Reason of
		normal ->
			{ok, S};
		shutdown ->
			{ok, S};
		_ ->
			error_logger:error_report([{module, ?MODULE},
				{error, 'neighbour crashed'}, {from, From}, {reason, Reason}]),
			{stop, S}
	end;

handle_info({gen_event_EXIT, _Handler, _Reason}, S) ->
	{ok, S}.


handle_packet(Packet, #state{store_pid=Store} = S) when is_pid(Store) ->
	<<Ref:32, Request:12, ?FLAG_REQ:4, Body/binary>> = Packet,
	RetPath = #retpath{servlet=self(), req=Request, ref=Ref},
	case Request of
		?STATFS_MSG ->
			handle(Body, RetPath, Store, fun do_statfs/2, S);

		?LOOKUP_MSG ->
			handle(Body, RetPath, Store, fun do_loopup/2, S);

		?CONTAINS_MSG ->
			handle(Body, RetPath, Store, fun do_contains/2, S);

		?STAT_MSG ->
			handle(Body, RetPath, Store, fun do_stat/2, S);

		?GET_LINKS_MSG ->
			handle(Body, RetPath, Store, fun do_get_links/2, S);

		?PEEK_MSG ->
			start_worker(S, Body, RetPath, fun do_peek/3, fun io_handler/3);

		?CREATE_MSG ->
			start_worker(S, Body, RetPath, fun do_create/3, fun io_handler/3);

		?FORK_MSG ->
			start_worker(S, Body, RetPath, fun do_fork/3, fun io_handler/3);

		?UPDATE_MSG ->
			start_worker(S, Body, RetPath, fun do_update/3, fun io_handler/3);

		?RESUME_MSG ->
			start_worker(S, Body, RetPath, fun do_resume/3, fun io_handler/3);

		?FORGET_MSG ->
			handle(Body, RetPath, Store, fun do_forget/2, S);

		?DELETE_DOC_MSG ->
			handle(Body, RetPath, Store, fun do_delete_doc/2, S);

		?DELETE_REV_MSG ->
			handle(Body, RetPath, Store, fun do_delete_rev/2, S);

		?PUT_DOC_MSG ->
			start_worker(S, Body, RetPath, fun do_put_doc/3, fun io_handler/3);

		?FF_DOC_MSG ->
			start_worker(S, Body, RetPath, fun do_forward_doc/3, fun io_handler/3);

		?PUT_REV_MSG ->
			start_worker(S, Body, RetPath, fun do_put_rev/3, fun io_handler/3);

		?SYNC_GET_CHANGES_MSG ->
			handle(Body, RetPath, Store, fun do_sync_get_changes/2, S);

		?SYNC_GET_ANCHOR_MSG ->
			handle(Body, RetPath, Store, fun do_sync_get_anchor/2, S);

		?SYNC_SET_ANCHOR_MSG ->
			handle(Body, RetPath, Store, fun do_sync_set_anchor/2, S);

		?SYNC_FINISH_MSG ->
			handle(Body, RetPath, Store, fun do_sync_finish/2, S);

		?SYNC_MSG ->
			handle(Body, RetPath, Store, fun do_sync/2, S);

		?RMBR_REV_MSG ->
			start_worker(S, Body, RetPath, fun do_remember_rev/3, fun io_handler/3);

		?READ_MSG ->
			ReqData = #readreq{handle=Handle} =
				peerdrive_netstore_pb:decode_readreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?WRITE_BUFFER_MSG ->
			ReqData = #writebufferreq{handle=Handle} =
				peerdrive_netstore_pb:decode_writebufferreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?WRITE_COMMIT_MSG ->
			ReqData = #writecommitreq{handle=Handle} =
				peerdrive_netstore_pb:decode_writecommitreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?TRUNC_MSG ->
			ReqData = #truncreq{handle=Handle} =
				peerdrive_netstore_pb:decode_truncreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?CLOSE_MSG ->
			ReqData = #closereq{handle=Handle} =
				peerdrive_netstore_pb:decode_closereq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?COMMIT_MSG ->
			ReqData = #commitreq{handle=Handle} =
				peerdrive_netstore_pb:decode_commitreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?SUSPEND_MSG ->
			ReqData = #suspendreq{handle=Handle} =
				peerdrive_netstore_pb:decode_suspendreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?SET_DATA_MSG ->
			ReqData = #setdatareq{handle=Handle} =
				peerdrive_netstore_pb:decode_setdatareq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?GET_DATA_MSG ->
			ReqData = #getdatareq{handle=Handle} =
				peerdrive_netstore_pb:decode_getdatareq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?SET_PARENTS_MSG ->
			ReqData = #setparentsreq{handle=Handle} =
				peerdrive_netstore_pb:decode_setparentsreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?SET_FLAGS_MSG ->
			ReqData = #setflagsreq{handle=Handle} =
				peerdrive_netstore_pb:decode_setflagsreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?SET_MTIME_MSG ->
			ReqData = #setmtimereq{handle=Handle} =
				peerdrive_netstore_pb:decode_setmtimereq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?SET_TYPE_MSG ->
			ReqData = #settypereq{handle=Handle} =
				peerdrive_netstore_pb:decode_settypereq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?FSTAT_MSG ->
			ReqData = #fstatreq{handle=Handle} =
				peerdrive_netstore_pb:decode_fstatreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?PUT_REV_PART_MSG ->
			ReqData = #putrevpartreq{handle=Handle} =
				peerdrive_netstore_pb:decode_putrevpartreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S)
	end;

handle_packet(<<Ref:32, Request:12, ?FLAG_REQ:4, Body/binary>>, S) ->
	RetPath = #retpath{servlet=self(), req=Request, ref=Ref},
	case Request of
		?INIT_MSG when not S#state.init ->
			do_init(Body, RetPath, S);

		?MOUNT_MSG when S#state.init ->
			do_mount(Body, RetPath, S);
		_ ->
			{stop, send_error(RetPath, {error, ebadrpc}), S}
	end.


handle_packet_forward(Request, Handle, ReqData, RetPath, S) ->
	Worker = dict:fetch(Handle, S#state.handles),
	Worker ! {Request, ReqData, RetPath},
	{ok, S}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Request handling functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_init(Body, RetPath, #state{tls=Tls} = S) ->
	case Tls of
		deny -> TlsReq = deny, SslOpts = [];
		{TlsReq, SslOpts} -> ok
	end,
	try
		#initreq{
			major = Major,
			starttls = StartTls
		} = peerdrive_netstore_pb:decode_initreq(Body),
		case Major of
			3 -> ok;
			_ -> throw({error, erpcmismatch})
		end,
		S2 = S#state{init=true},
		if
			((StartTls == deny) and (TlsReq == deny)) or
			((StartTls == deny) and (TlsReq == optional)) or
			((StartTls == optional) and (TlsReq == deny)) ->
				Cnf = peerdrive_netstore_pb:encode_initcnf(
					#initcnf{major=3, minor=0, starttls=false}),
				{reply, send_reply(RetPath, Cnf), S2};

			((StartTls == optional) and (TlsReq == optional)) or
			((StartTls == optional) and (TlsReq == required)) or
			((StartTls == required) and (TlsReq == optional)) or
			((StartTls == required) and (TlsReq == required)) ->
				Cnf = peerdrive_netstore_pb:encode_initcnf(
					#initcnf{major=3, minor=0, starttls=true}),
				{ssl, send_reply(RetPath, Cnf), SslOpts, S2};

			true ->
				throw({error, ebade})
		end
	catch
		throw:Error ->
			{stop, send_error(RetPath, Error), S}
	end.


do_mount(Body, RetPath, #state{stores=Stores} = S) ->
	try
		#mountreq{store=Store, no_verify=NoVerify} =
			peerdrive_netstore_pb:decode_mountreq(Body),
		#peerdrive_store{label=Id, sid=SId, options=Options} = get_store_by_id(Store),
		lists:member(Id, Stores) orelse throw({error, eacces}),
		(not NoVerify or proplists:get_bool(<<"noverify">>, Options)) orelse
			throw({error, einval}),
		{ok, Pid} = check(peerdrive_volman:store(SId)),
		S2 = S#state{store_pid=Pid, store_uuid=SId},
		Cnf = peerdrive_netstore_pb:encode_mountcnf(#mountcnf{sid=SId}),
		{reply, send_reply(RetPath, Cnf), S2}
	catch
		throw:Error ->
			{stop, send_error(RetPath, Error), S}
	end.


do_statfs(_Body, Store) ->
	{ok, Stat} = check(peerdrive_store:statfs(Store)),
	#fs_stat{
		bsize  = BSize,
		blocks = Blocks,
		bfree  = BFree,
		bavail = BAvail
	} = Stat,
	peerdrive_netstore_pb:encode_statfscnf(#statfscnf{
		bsize  = BSize,
		blocks = Blocks,
		bfree  = BFree,
		bavail = BAvail
	}).


do_loopup(Body, Store) ->
	#lookupreq{doc=Doc} = peerdrive_netstore_pb:decode_lookupreq(Body),
	{ok, Rev, PreRevs} = check(peerdrive_store:lookup(Store, Doc)),
	peerdrive_netstore_pb:encode_lookupcnf(#lookupcnf{rev=Rev,
		pre_revs=PreRevs}).


do_contains(Body, Store) ->
	#containsreq{rev=Rev} = peerdrive_netstore_pb:decode_containsreq(Body),
	Cnf = #containscnf{found=peerdrive_store:contains(Store, Rev)},
	peerdrive_netstore_pb:encode_containscnf(Cnf).


do_stat(Body, Store) ->
	#statreq{rev=Rev} = peerdrive_netstore_pb:decode_statreq(Body),
	{ok, Stat} = check(peerdrive_store:stat(Store, Rev)),
	peerdrive_netstore_pb:encode_statcnf(rev_to_statcnf(Stat)).


do_get_links(Body, Store) ->
	#getlinksreq{rev=Rev} = peerdrive_netstore_pb:decode_getlinksreq(Body),
	{ok, {DocLinks, RevLinks}} = check(peerdrive_store:get_links(Store, Rev)),
	Reply = #getlinkscnf{doc_links=DocLinks, rev_links=RevLinks},
	peerdrive_netstore_pb:encode_getlinkscnf(Reply).


do_sync(<<>>, Store) ->
	ok = check(peerdrive_store:sync(Store)),
	<<>>.


do_peek(Store, NetHandle, ReqData) ->
	#peekreq{rev=Rev} = peerdrive_netstore_pb:decode_peekreq(ReqData),
	{ok, StoreHandle} = check(peerdrive_store:peek(Store, Rev)),
	Cnf = #peekcnf{handle=NetHandle},
	{start, {StoreHandle, []}, peerdrive_netstore_pb:encode_peekcnf(Cnf)}.


do_create(Store, NetHandle, ReqData) ->
	#createreq{type_code=Type, creator_code=Creator} =
		peerdrive_netstore_pb:decode_createreq(ReqData),
	{ok, Doc, StoreHandle} = check(peerdrive_store:create(Store, Type, Creator)),
	Cnf = #createcnf{handle=NetHandle, doc=Doc},
	{start, {StoreHandle, []}, peerdrive_netstore_pb:encode_createcnf(Cnf)}.


do_fork(Store, NetHandle, ReqData) ->
	#forkreq{rev=Rev, creator_code=Creator} =
		peerdrive_netstore_pb:decode_forkreq(ReqData),
	{ok, Doc, StoreHandle} = check(peerdrive_store:fork(Store, Rev, Creator)),
	Cnf = #forkcnf{handle=NetHandle, doc=Doc},
	{start, {StoreHandle, []}, peerdrive_netstore_pb:encode_forkcnf(Cnf)}.


do_update(Store, NetHandle, ReqData) ->
	#updatereq{doc=Doc, rev=Rev, creator_code=Creator} =
		peerdrive_netstore_pb:decode_updatereq(ReqData),
	{ok, StoreHandle} = check(peerdrive_store:update(Store, Doc, Rev, Creator)),
	Cnf = #updatecnf{handle=NetHandle},
	{start, {StoreHandle, []}, peerdrive_netstore_pb:encode_updatecnf(Cnf)}.


do_resume(Store, NetHandle, ReqData) ->
	#resumereq{doc=Doc, rev=Rev, creator_code=Creator} =
		peerdrive_netstore_pb:decode_resumereq(ReqData),
	{ok, StoreHandle} = check(peerdrive_store:resume(Store, Doc, Rev, Creator)),
	Cnf = #resumecnf{handle=NetHandle},
	{start, {StoreHandle, []}, peerdrive_netstore_pb:encode_resumecnf(Cnf)}.


do_forget(Body, Store) ->
	#forgetreq{doc=Doc, rev=Rev} =
		peerdrive_netstore_pb:decode_forgetreq(Body),
	ok = check(peerdrive_store:forget(Store, Doc, Rev)),
	<<>>.


do_delete_doc(Body, Store) ->
	#deletedocreq{doc=Doc, rev=Rev} =
		peerdrive_netstore_pb:decode_deletedocreq(Body),
	ok = check(peerdrive_store:delete_doc(Store, Doc, Rev)),
	<<>>.


do_delete_rev(Body, Store) ->
	#deleterevreq{rev=Rev} = peerdrive_netstore_pb:decode_deleterevreq(Body),
	ok = check(peerdrive_store:delete_rev(Store, Rev)),
	<<>>.


do_put_doc(Store, NetHandle, ReqData) ->
	#putdocreq{doc=Doc, rev=Rev} =
		peerdrive_netstore_pb:decode_putdocreq(ReqData),
	{ok, StoreHandle} = check(peerdrive_store:put_doc(Store, Doc, Rev)),
	Cnf = #putdoccnf{handle=NetHandle},
	{start, {StoreHandle, []}, peerdrive_netstore_pb:encode_putdoccnf(Cnf)}.


do_forward_doc(Store, NetHandle, ReqData) ->
	#forwarddocreq{doc=Doc, rev_path=RevPath, old_pre_rev=OldPreRev} =
		peerdrive_netstore_pb:decode_forwarddocreq(ReqData),
	case check(peerdrive_store:forward_doc(Store, Doc, RevPath, OldPreRev)) of
		ok ->
			{stop, <<>>};

		{ok, Missing, StoreHandle} ->
			Cnf = #forwarddoccnf{handle=NetHandle, missing_revs=Missing},
			{start, {StoreHandle, []}, peerdrive_netstore_pb:encode_forwarddoccnf(Cnf)}
	end.


do_remember_rev(Store, NetHandle, ReqData) ->
	#rememberrevreq{doc=Doc, pre_rev=PreRev, old_pre_rev=OldPreRev} =
		peerdrive_netstore_pb:decode_rememberrevreq(ReqData),
	case check(peerdrive_store:remember_rev(Store, Doc, PreRev, OldPreRev)) of
		ok ->
			{stop, <<>>};

		{ok, StoreHandle} ->
			Cnf = #rememberrevcnf{handle=NetHandle},
			{start, {StoreHandle, []}, peerdrive_netstore_pb:encode_rememberrevcnf(Cnf)}
	end.


do_put_rev(Store, NetHandle, ReqData) ->
	#putrevreq{rid=RId, revision=PbRev, data=Data, doc_links=DocLinks,
		rev_links=RevLinks} = peerdrive_netstore_pb:decode_putrevreq(ReqData),
	Rev = #rev{
		flags = PbRev#putrevreq_revision.flags,
		data = #rev_dat{
			size = (PbRev#putrevreq_revision.data)#putrevreq_revision_data.size,
			hash = (PbRev#putrevreq_revision.data)#putrevreq_revision_data.hash
		},
		attachments = [ #rev_att{name=Name, size=Size, hash=Hash, crtime=CrTime,
				mtime=MTime}
			|| #putrevreq_revision_attachment{name=Name, size=Size, hash=Hash,
				crtime=CrTime, mtime=MTime}
			<- PbRev#putrevreq_revision.attachments ],
		parents = PbRev#putrevreq_revision.parents,
		crtime = PbRev#putrevreq_revision.crtime,
		mtime = PbRev#putrevreq_revision.mtime,
		type = PbRev#putrevreq_revision.type_code,
		creator = PbRev#putrevreq_revision.creator_code,
		comment = PbRev#putrevreq_revision.comment
	},
	{ok, Missing, StoreHandle} = check(peerdrive_store:put_rev(Store,
		RId, Rev, Data, DocLinks, RevLinks)),
	Cnf = #putrevcnf{handle=NetHandle, missing_attachments=Missing},
	{start, {StoreHandle, []}, peerdrive_netstore_pb:encode_putrevcnf(Cnf)}.


do_sync_get_changes(Body, Store) ->
	#syncgetchangesreq{peer_sid=Peer, anchor=Anchor} =
		peerdrive_netstore_pb:decode_syncgetchangesreq(Body),
	{ok, Backlog} = check(peerdrive_store:sync_get_changes(Store, Peer, Anchor)),
	CnfBacklog = [ #syncgetchangescnf_item{doc=Doc, seq_num=SeqNum} ||
		{Doc, SeqNum} <- Backlog ],
	Cnf = #syncgetchangescnf{backlog=CnfBacklog},
	peerdrive_netstore_pb:encode_syncgetchangescnf(Cnf).


do_sync_get_anchor(Body, Store) ->
	#syncgetanchorreq{from_sid=FromSId, to_sid=ToSId} =
		peerdrive_netstore_pb:decode_syncgetanchorreq(Body),
	{ok, Anchor} = check(peerdrive_store:sync_get_anchor(Store, FromSId, ToSId)),
	Cnf = #syncgetanchorcnf{anchor=Anchor},
	peerdrive_netstore_pb:encode_syncgetanchorcnf(Cnf).


do_sync_set_anchor(Body, Store) ->
	#syncsetanchorreq{from_sid=FromSId, to_sid=ToSId, seq_num=SeqNum} =
		peerdrive_netstore_pb:decode_syncsetanchorreq(Body),
	ok = check(peerdrive_store:sync_set_anchor(Store, FromSId, ToSId, SeqNum)),
	<<>>.


do_sync_finish(Body, Store) ->
	#syncfinishreq{peer_sid=Peer} =
		peerdrive_netstore_pb:decode_syncfinishreq(Body),
	ok = check(peerdrive_store:sync_finish(Store, Peer)),
	<<>>.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% IO handler loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_worker(S, Body, RetPath, InitFun, ReqFun) ->
	Handle = S#state.next,
	Store = S#state.store_pid,
	Server = self(),
	Worker = spawn_link(
		fun() ->
			try
				case InitFun(Store, Handle, Body) of
					{start, State, Reply} ->
						send_reply(RetPath, Reply),
						worker_loop(ReqFun, State);
					{stop, Reply} ->
						send_reply(RetPath, Reply)
				end,
				Server ! {done, Handle}
			catch
				throw:Error -> send_error(RetPath, Error)
			end
		end),
	{ok, S#state{
		handles = dict:store(Handle, Worker, S#state.handles),
		next    = Handle + 1}}.


worker_loop(ReqFun, State) ->
	receive
		{Req, ReqData, RetPath} ->
			try ReqFun(State, Req, ReqData) of
				{stop, Reply} ->
					send_reply(RetPath, Reply);
				{abort, Error} ->
					send_error(RetPath, Error);
				{error, Error, NewState} ->
					send_error(RetPath, Error),
					worker_loop(ReqFun, NewState);
				{Reply, NewState} ->
					send_reply(RetPath, Reply),
					worker_loop(ReqFun, NewState);
				Reply ->
					send_reply(RetPath, Reply),
					worker_loop(ReqFun, State)
			catch
				throw:Error ->
					send_error(RetPath, Error),
					worker_loop(ReqFun, State)
			end;

		closed ->
			ReqFun(State, closed, <<>>)
	end.


io_handler({Handle, WriteBuffer}, Request, ReqData) ->
	case Request of
		?READ_MSG ->
			#readreq{part=Part, offset=Offset, length=Length} = ReqData,
			{ok, Data} = check(peerdrive_store:read(Handle, Part, Offset, Length)),
			peerdrive_netstore_pb:encode_readcnf(#readcnf{data=Data});

		?WRITE_BUFFER_MSG ->
			#writebufferreq{part=Part, data=Data} = ReqData,
			NewWrBuf = orddict:update(Part, fun(Old) -> [Data | Old] end,
				[Data], WriteBuffer),
			{<<>>, {Handle, NewWrBuf}};

		?WRITE_COMMIT_MSG ->
			#writecommitreq{part=Part, offset=Offset, data=Data} = ReqData,
			case orddict:find(Part, WriteBuffer) of
				error ->
					ok = check(peerdrive_store:write(Handle, Part, Offset, Data)),
					<<>>;

				{ok, BufData} ->
					AllData = iolist_to_binary(lists:reverse([Data | BufData])),
					case peerdrive_store:write(Handle, Part, Offset, AllData) of
						ok ->
							{<<>>, {Handle, orddict:erase(Part, WriteBuffer)}};
						{error, _} = Error ->
							{error, Error, {Handle, orddict:erase(Part, WriteBuffer)}}
					end
			end;

		?TRUNC_MSG ->
			#truncreq{part=Part, offset=Offset} = ReqData,
			ok = check(peerdrive_store:truncate(Handle, Part, Offset)),
			<<>>;

		?CLOSE_MSG ->
			ok = peerdrive_store:close(Handle),
			{stop, <<>>};

		?COMMIT_MSG ->
			#commitreq{comment=Comment} = ReqData,
			{ok, Rev} = case Comment of
				undefined -> check(peerdrive_store:commit(Handle));
				_ -> check(peerdrive_store:commit(Handle,
					unicode:characters_to_binary(Comment)))
			end,
			peerdrive_netstore_pb:encode_commitcnf(#commitcnf{rev=Rev});

		?SUSPEND_MSG ->
			#suspendreq{comment=CommentStr} = ReqData,
			Comment = if
				CommentStr == undefined -> undefined;
				true -> unicode:characters_to_binary(CommentStr)
			end,
			{ok, Rev} = check(peerdrive_store:suspend(Handle, Comment)),
			peerdrive_netstore_pb:encode_suspendcnf(#suspendcnf{rev=Rev});

		?SET_DATA_MSG ->
			#setdatareq{selector=Selector, data=Data} = ReqData,
			ok = check(peerdrive_store:set_data(Handle, Selector, Data)),
			<<>>;

		?GET_DATA_MSG ->
			#getdatareq{selector=Selector} = ReqData,
			{ok, Data} = check(peerdrive_store:get_data(Handle, Selector)),
			Cnf = #getdatacnf{data=Data},
			peerdrive_netstore_pb:encode_getdatacnf(Cnf);

		?SET_PARENTS_MSG ->
			#setparentsreq{parents=Parents} = ReqData,
			ok = check(peerdrive_store:set_parents(Handle, Parents)),
			<<>>;

		?SET_FLAGS_MSG ->
			#setflagsreq{flags=Flags} = ReqData,
			ok = check(peerdrive_store:set_flags(Handle, Flags)),
			<<>>;

		?SET_MTIME_MSG ->
			#setmtimereq{attachment=Attachment, mtime=MTime} = ReqData,
			ok = check(peerdrive_store:set_mtime(Handle, Attachment, MTime)),
			<<>>;

		?SET_TYPE_MSG ->
			#settypereq{type_code=Type} = ReqData,
			ok = check(peerdrive_store:set_type(Handle, Type)),
			<<>>;

		?FSTAT_MSG ->
			{ok, Rev} = check(peerdrive_store:fstat(Handle)),
			peerdrive_netstore_pb:encode_statcnf(rev_to_statcnf(Rev));

		?PUT_REV_PART_MSG ->
			#putrevpartreq{attachment=Attachment, data=Data} = ReqData,
			ok = check(peerdrive_store:put_rev_part(Handle, Attachment, Data)),
			<<>>;

		closed ->
			peerdrive_store:close(Handle)
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

check({error, _} = Error) ->
	throw(Error);

check(error) ->
	throw({error, enoent});

check(Result) ->
	Result.


handle(Body, RetPath, Store, Fun, S) ->
	Reply = try
		send_reply(RetPath, Fun(Body, Store))
	catch
		throw:Error -> send_error(RetPath, Error)
	end,
	{reply, Reply, S}.


send_error(RetPath, {error, Error}) ->
	Data = peerdrive_netstore_pb:encode_errorcnf(#errorcnf{error=Error}),
	send_cnf(RetPath, (?ERROR_MSG bsl 4) bor ?FLAG_CNF, Data).


send_reply(#retpath{req=Req} = RetPath, Data) ->
	send_cnf(RetPath, (Req bsl 4) bor ?FLAG_CNF, Data).


send_cnf(#retpath{ref=Ref, servlet=Servlet}, Cnf, Data) ->
	Raw = [<<Ref:32, Cnf:16>>, Data],
	case self() of
		Servlet -> Raw;
		_       -> Servlet ! {send, Raw}, Raw
	end.


send_indication(Ind, Data, S) ->
	Indication = (Ind bsl 4) bor ?FLAG_IND,
	Raw = [<<16#FFFFFFFF:32, Indication:16>>, Data],
	{reply, Raw, S}.


get_store_by_id(Store) ->
	try
		case lists:keysearch(Store, #peerdrive_store.label, peerdrive_volman:enum()) of
			{value, StoreSpec} ->
				StoreSpec;
			false ->
				throw({error, enoent})
		end
	catch
		error:badarg -> throw({error, enoent})
	end.


rev_to_statcnf(Rev) ->
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
	} = Rev,
	 #statcnf{
		flags        = Flags,
		data         = #statcnf_data{size=DataSize, hash=DataHash},
		attachments  = [ #statcnf_attachment{name=N, size=S, hash=H, crtime=CrT,
			mtime=MT} || #rev_att{name=N, size=S, hash=H, crtime=CrT, mtime=MT}
			<- Attachments ],
		parents      = Parents,
		crtime       = CrTime,
		mtime        = Mtime,
		type_code    = TypeCode,
		creator_code = CreatorCode,
		comment      = Comment
	}.

