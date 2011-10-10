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

-include("store.hrl").
-include("netstore.hrl").
-include("peerdrive_netstore_pb.hrl").

-record(state, {socket, handles, next, stores, store_pid, store_uuid}).
-record(retpath, {socket, req, ref}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Servlet callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Socket, Options) ->
	process_flag(trap_exit, true),
	peerdrive_vol_monitor:register_proc(self()),
	Stores = proplists:get_value(stores, Options, []),
	#state{socket=Socket, handles=dict:new(), next=0, stores=Stores}.


terminate(State) ->
	peerdrive_vol_monitor:deregister_proc(self()),
	dict:fold(
		fun(_Handle, Worker, _Acc) -> Worker ! closed end,
		ok,
		State#state.handles).


handle_info({done, Handle}, S) ->
	{ok, S#state{handles=dict:erase(Handle, S#state.handles)}};

handle_info({'EXIT', _From, normal}, S) ->
	{ok, S};

handle_info({Event, Store, Element}, S) when (Event == trigger_add_rev) or
                                             (Event == trigger_rm_rev) or
                                             (Event == trigger_add_doc) or
                                             (Event == trigger_rm_doc) or
                                             (Event == trigger_mod_doc) ->
	do_trigger(Event, Store, Element, S),
	{ok, S};

handle_info({trigger_rem_store, StoreGuid}, #state{store_uuid=Uuid}=S) ->
	case StoreGuid of
		Uuid  -> {stop, S};
		_Else -> {ok, S}
	end;

handle_info({trigger_add_store, _StoreGuid}, S) ->
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


handle_packet(Packet, #state{socket=Socket, store_pid=Store} = S) ->
	<<Ref:32, Request:12, ?FLAG_REQ:4, Body/binary>> = Packet,
	RetPath = #retpath{socket=Socket, req=Request, ref=Ref},
	%io:format("[~w] Ref:~w Request:~w Body:~w~n", [self(), Ref, Request, Body]),
	case Request of
		?INIT_MSG ->
			do_init(Body, RetPath, S);

		?STATFS_MSG ->
			handle(Body, RetPath, Store, fun do_statfs/2),
			{ok, S};

		?LOOKUP_MSG ->
			handle(Body, RetPath, Store, fun do_loopup/2),
			{ok, S};

		?CONTAINS_MSG ->
			handle(Body, RetPath, Store, fun do_contains/2),
			{ok, S};

		?STAT_MSG ->
			handle(Body, RetPath, Store, fun do_stat/2),
			{ok, S};

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
			handle(Body, RetPath, Store, fun do_forget/2),
			{ok, S};

		?DELETE_DOC_MSG ->
			handle(Body, RetPath, Store, fun do_delete_doc/2),
			{ok, S};

		?DELETE_REV_MSG ->
			handle(Body, RetPath, Store, fun do_delete_rev/2),
			{ok, S};

		?PUT_DOC_START_MSG ->
			start_worker(S, Body, RetPath, fun do_put_doc_start/3, fun put_doc_handler/3);

		?FF_DOC_START_MSG ->
			start_worker(S, Body, RetPath, fun do_forward_doc_start/3, fun forward_handler/3);

		?PUT_REV_START_MSG ->
			start_worker(S, Body, RetPath, fun do_put_rev_start/3, fun put_rev_handler/3);

		?SYNC_GET_CHANGES_MSG ->
			handle(Body, RetPath, Store, fun do_sync_get_changes/2),
			{ok, S};

		?SYNC_SET_ANCHOR_MSG ->
			handle(Body, RetPath, Store, fun do_sync_set_anchor/2),
			{ok, S};

		?SYNC_FINISH_MSG ->
			handle(Body, RetPath, Store, fun do_sync_finish/2),
			{ok, S};

		_ ->
			{{1, Handle}, _} = protobuffs:decode(Body, uint32),
			Worker = dict:fetch(Handle, S#state.handles),
			Worker ! {Request, Body, RetPath},
			{ok, S}
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Request handling functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_trigger(Event, SId, Element, #state{socket=Socket, store_uuid=SId}) ->
	Ind = peerdrive_netstore_pb:encode_triggerind(
		#triggerind{event=Event, element=Element}),
	send_indication(Socket, ?TRIGGER_MSG, Ind);

do_trigger(_Event, _SId, _Element, _S) ->
	ok.


do_init(Body, RetPath, #state{store_pid=undefined, stores=Stores} = S) ->
	try
		#initreq{
			major = Major,
			store = Store
		} = peerdrive_netstore_pb:decode_initreq(Body),
		case Major of
			0 -> ok;
			_ -> throw({error, erpcmismatch})
		end,
		{Id, _Descr, SId, _Tags} = get_store_by_id(Store),
		lists:member(Id, Stores) orelse throw({error, eacces}),
		{ok, Pid} = check(peerdrive_volman:store(SId)),
		S2 = S#state{store_pid=Pid, store_uuid=SId},
		Cnf = peerdrive_netstore_pb:encode_initcnf(
			#initcnf{major=0, minor=0, sid=SId}),
		send_reply(RetPath, Cnf),
		{ok, S2}
	catch
		throw:Error ->
			send_error(RetPath, Error),
			{stop, S}
	end;

do_init(_Body, RetPath, S) ->
	send_error(RetPath, {error, ebadrpc}),
	{stop, S}.


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
	?ASSERT_GUID(Doc),
	{ok, Rev, PreRevs} = check(peerdrive_store:lookup(Store, Doc)),
	peerdrive_netstore_pb:encode_lookupcnf(#lookupcnf{rev=Rev,
		pre_revs=PreRevs}).


do_contains(Body, Store) ->
	#containsreq{rev=Rev} = peerdrive_netstore_pb:decode_containsreq(Body),
	?ASSERT_GUID(Rev),
	Cnf = #containscnf{found=peerdrive_store:contains(Store, Rev)},
	peerdrive_netstore_pb:encode_containscnf(Cnf).


do_stat(Body, Store) ->
	#statreq{rev=Rev} = peerdrive_netstore_pb:decode_statreq(Body),
	?ASSERT_GUID(Rev),
	{ok, Stat} = check(peerdrive_store:stat(Store, Rev)),
	#rev_stat{
		flags     = Flags,
		parts     = Parts,
		parents   = Parents,
		mtime     = Mtime,
		type      = TypeCode,
		creator   = CreatorCode,
		doc_links = DocLinks,
		rev_links = RevLinks
	} = Stat,
	Reply = #statcnf{
		flags        = Flags,
		parts        = [ #statcnf_part{fourcc=F, size=S, pid=P}
						 || {F, S, P} <- Parts ],
		parents      = Parents,
		mtime        = Mtime,
		type_code    = TypeCode,
		creator_code = CreatorCode,
		doc_links    = DocLinks,
		rev_links    = RevLinks
	},
	peerdrive_netstore_pb:encode_statcnf(Reply).


do_peek(Store, NetHandle, ReqData) ->
	#peekreq{rev=Rev} = peerdrive_netstore_pb:decode_peekreq(ReqData),
	?ASSERT_GUID(Rev),
	{ok, StoreHandle} = check(peerdrive_store:peek(Store, Rev)),
	Cnf = #peekcnf{handle=NetHandle},
	{start, StoreHandle, peerdrive_netstore_pb:encode_peekcnf(Cnf)}.


do_create(Store, NetHandle, ReqData) ->
	#createreq{type_code=Type, creator_code=Creator} =
		peerdrive_netstore_pb:decode_createreq(ReqData),
	{ok, Doc, StoreHandle} = check(peerdrive_store:create(Store,
		unicode:characters_to_binary(Type),
		unicode:characters_to_binary(Creator))),
	Cnf = #createcnf{handle=NetHandle, doc=Doc},
	{start, StoreHandle, peerdrive_netstore_pb:encode_createcnf(Cnf)}.


do_fork(Store, NetHandle, ReqData) ->
	#forkreq{rev=Rev, creator_code=Creator} =
		peerdrive_netstore_pb:decode_forkreq(ReqData),
	?ASSERT_GUID(Rev),
	{ok, Doc, StoreHandle} = check(peerdrive_store:fork(Store, Rev,
		unicode:characters_to_binary(Creator))),
	Cnf = #forkcnf{handle=NetHandle, doc=Doc},
	{start, StoreHandle, peerdrive_netstore_pb:encode_forkcnf(Cnf)}.


do_update(Store, NetHandle, ReqData) ->
	#updatereq{doc=Doc, rev=Rev, creator_code=CreatorStr} =
		peerdrive_netstore_pb:decode_updatereq(ReqData),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(Rev),
	Creator = if
		CreatorStr == undefined -> undefined;
		true -> unicode:characters_to_binary(CreatorStr)
	end,
	{ok, StoreHandle} = check(peerdrive_store:update(Store, Doc, Rev, Creator)),
	Cnf = #updatecnf{handle=NetHandle},
	{start, StoreHandle, peerdrive_netstore_pb:encode_updatecnf(Cnf)}.


do_resume(Store, NetHandle, ReqData) ->
	#resumereq{doc=Doc, rev=Rev, creator_code=CreatorStr} =
		peerdrive_netstore_pb:decode_resumereq(ReqData),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(Rev),
	Creator = if
		CreatorStr == undefined -> undefined;
		true -> unicode:characters_to_binary(CreatorStr)
	end,
	{ok, StoreHandle} = check(peerdrive_store:resume(Store, Doc, Rev, Creator)),
	Cnf = #resumecnf{handle=NetHandle},
	{start, StoreHandle, peerdrive_netstore_pb:encode_resumecnf(Cnf)}.


do_forget(Body, Store) ->
	#forgetreq{doc=Doc, rev=Rev} =
		peerdrive_netstore_pb:decode_forgetreq(Body),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(Rev),
	ok = check(peerdrive_store:forget(Store, Doc, Rev)),
	<<>>.


do_delete_doc(Body, Store) ->
	#deletedocreq{doc=Doc, rev=Rev} =
		peerdrive_netstore_pb:decode_deletedocreq(Body),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(Rev),
	ok = check(peerdrive_store:delete_doc(Store, Doc, Rev)),
	<<>>.


do_delete_rev(Body, Store) ->
	#deleterevreq{rev=Rev} = peerdrive_netstore_pb:decode_deleterevreq(Body),
	?ASSERT_GUID(Rev),
	ok = check(peerdrive_store:delete_rev(Store, Rev)),
	<<>>.


do_put_doc_start(Store, NetHandle, ReqData) ->
	#putdocstartreq{doc=Doc, rev=Rev} =
		peerdrive_netstore_pb:decode_putdocstartreq(ReqData),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(Rev),
	{ok, StoreHandle} = check(peerdrive_store:put_doc(Store, Doc, Rev)),
	Cnf = #putdocstartcnf{handle=NetHandle},
	{start, StoreHandle, peerdrive_netstore_pb:encode_putdocstartcnf(Cnf)}.


do_forward_doc_start(Store, NetHandle, ReqData) ->
	#forwarddocstartreq{doc=Doc, rev_path=RevPath} =
		peerdrive_netstore_pb:decode_forwarddocstartreq(ReqData),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID_LIST(RevPath),
	case check(peerdrive_store:forward_doc_start(Store, Doc, RevPath)) of
		ok ->
			{stop, <<>>};

		{ok, Missing, StoreHandle} ->
			Cnf = #forwarddocstartcnf{handle=NetHandle, missing_revs=Missing},
			{start, StoreHandle, peerdrive_netstore_pb:encode_forwarddocstartcnf(Cnf)}
	end.


do_put_rev_start(Store, NetHandle, ReqData) ->
	#putrevstartreq{rid=RId, revision=PbRev} =
		peerdrive_netstore_pb:decode_putrevstartreq(ReqData),
	?ASSERT_GUID(RId),
	Rev = #revision{
		flags = PbRev#putrevstartreq_revision.flags,
		parts = [ {FCC, PId} || #putrevstartreq_revision_part{fourcc=FCC, pid=PId}
			<- PbRev#putrevstartreq_revision.parts, ?ASSERT_PART(FCC),
			?ASSERT_GUID(PId) ],
		parents = PbRev#putrevstartreq_revision.parents,
		mtime = PbRev#putrevstartreq_revision.mtime,
		type = unicode:characters_to_binary(PbRev#putrevstartreq_revision.type_code),
		creator = unicode:characters_to_binary(PbRev#putrevstartreq_revision.creator_code),
		doc_links = PbRev#putrevstartreq_revision.doc_links,
		rev_links = PbRev#putrevstartreq_revision.rev_links
	},
	?ASSERT_GUID_LIST(Rev#revision.parents),
	?ASSERT_GUID_LIST(Rev#revision.doc_links),
	?ASSERT_GUID_LIST(Rev#revision.rev_links),
	case check(peerdrive_store:put_rev_start(Store, RId, Rev)) of
		ok ->
			{stop, <<>>};

		{ok, Missing, StoreHandle} ->
			Cnf = #putrevstartcnf{handle=NetHandle, missing_parts=Missing},
			{start, StoreHandle, peerdrive_netstore_pb:encode_putrevstartcnf(Cnf)}
	end.


do_sync_get_changes(Body, Store) ->
	#syncgetchangesreq{peer_sid=Peer} =
		peerdrive_netstore_pb:decode_syncgetchangesreq(Body),
	?ASSERT_GUID(Peer),
	{ok, Backlog} = check(peerdrive_store:sync_get_changes(Store, Peer)),
	CnfBacklog = [ #syncgetchangescnf_item{doc=Doc, seq_num=SeqNum} ||
		{Doc, SeqNum} <- Backlog ],
	Cnf = #syncgetchangescnf{backlog=CnfBacklog},
	peerdrive_netstore_pb:encode_syncgetchangescnf(Cnf).


do_sync_set_anchor(Body, Store) ->
	#syncsetanchorreq{peer_sid=Peer, seq_num=SeqNum} =
		peerdrive_netstore_pb:decode_syncsetanchorreq(Body),
	?ASSERT_GUID(Peer),
	ok = check(peerdrive_store:sync_set_anchor(Store, Peer, SeqNum)),
	<<>>.


do_sync_finish(Body, Store) ->
	#syncfinishreq{peer_sid=Peer} =
		peerdrive_netstore_pb:decode_syncfinishreq(Body),
	?ASSERT_GUID(Peer),
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
		{Req, Body, RetPath} ->
			try ReqFun(State, Req, Body) of
				Reply when is_binary(Reply) ->
					send_reply(RetPath, Reply),
					worker_loop(ReqFun, State);
				{stop, Reply} ->
					send_reply(RetPath, Reply);
				{abort, Error} ->
					send_error(RetPath, Error)
			catch
				throw:Error ->
					send_error(RetPath, Error),
					worker_loop(ReqFun, State)
			end;

		closed ->
			ReqFun(State, closed, <<>>)
	end.


io_handler(Handle, Request, ReqData) ->
	case Request of
		?READ_MSG ->
			#readreq{part=Part, offset=Offset, length=Length} =
				peerdrive_netstore_pb:decode_readreq(ReqData),
			?ASSERT_PART(Part),
			{ok, Data} = check(peerdrive_store:read(Handle, Part, Offset, Length)),
			peerdrive_netstore_pb:encode_readcnf(#readcnf{data=Data});

		?WRITE_MSG ->
			#writereq{part=Part, offset=Offset, data=Data} =
				peerdrive_netstore_pb:decode_writereq(ReqData),
			?ASSERT_PART(Part),
			ok = check(peerdrive_store:write(Handle, Part, Offset, Data)),
			<<>>;

		?TRUNC_MSG ->
			#truncreq{part=Part, offset=Offset} =
				peerdrive_netstore_pb:decode_truncreq(ReqData),
			?ASSERT_PART(Part),
			ok = check(peerdrive_store:truncate(Handle, Part, Offset)),
			<<>>;

		?CLOSE_MSG ->
			ok = peerdrive_store:close(Handle),
			{stop, <<>>};

		?COMMIT_MSG ->
			{ok, Rev} = check(peerdrive_store:commit(Handle)),
			peerdrive_netstore_pb:encode_commitcnf(#commitcnf{rev=Rev});

		?SUSPEND_MSG ->
			{ok, Rev} = check(peerdrive_store:suspend(Handle)),
			peerdrive_netstore_pb:encode_suspendcnf(#suspendcnf{rev=Rev});

		?SET_LINKS_MSG ->
			#setlinksreq{doc_links=DocLinks, rev_links=RevLinks} =
				peerdrive_netstore_pb:decode_setlinksreq(ReqData),
			?ASSERT_GUID_LIST(DocLinks),
			?ASSERT_GUID_LIST(RevLinks),
			ok = check(peerdrive_store:set_links(Handle, DocLinks, RevLinks)),
			<<>>;

		?GET_LINKS_MSG ->
			{ok, {DocLinks, RevLinks}} = check(peerdrive_store:get_links(Handle)),
			Cnf = #getlinkscnf{doc_links=DocLinks, rev_links=RevLinks},
			peerdrive_netstore_pb:encode_getlinkscnf(Cnf);

		?SET_PARENTS_MSG ->
			#setparentsreq{parents=Parents} =
				peerdrive_netstore_pb:decode_setparentsreq(ReqData),
			?ASSERT_GUID_LIST(Parents),
			ok = check(peerdrive_store:set_parents(Handle, Parents)),
			<<>>;

		?GET_PARENTS_MSG ->
			{ok, Parents} = check(peerdrive_store:get_parents(Handle)),
			Cnf = #getparentscnf{parents=Parents},
			peerdrive_netstore_pb:encode_getparentscnf(Cnf);

		?SET_FLAGS_MSG ->
			#setflagsreq{flags=Flags} =
				peerdrive_netstore_pb:decode_setflagsreq(ReqData),
			ok = check(peerdrive_store:set_flags(Handle, Flags)),
			<<>>;

		?GET_FLAGS_MSG ->
			{ok, Flags} = check(peerdrive_store:get_flags(Handle)),
			Cnf = #getflagscnf{flags=Flags},
			peerdrive_netstore_pb:encode_getflagscnf(Cnf);

		?SET_TYPE_MSG ->
			#settypereq{type_code=Type} =
				peerdrive_netstore_pb:decode_settypereq(ReqData),
			ok = check(peerdrive_store:set_type(Handle,
				unicode:characters_to_binary(Type))),
			<<>>;

		?GET_TYPE_MSG ->
			{ok, Type} = check(peerdrive_store:get_type(Handle)),
			Cnf = #gettypecnf{type_code=Type},
			peerdrive_netstore_pb:encode_gettypecnf(Cnf);

		closed ->
			peerdrive_store:close(Handle)
	end.


forward_handler(Handle, Request, _ReqData) ->
	case Request of
		?FF_DOC_COMMIT_MSG ->
			case peerdrive_store:forward_doc_commit(Handle) of
				ok ->
					{stop, <<>>};
				{error, _} = Error ->
					{abort, Error}
			end;

		?FF_DOC_ABORT_MSG ->
			ok = peerdrive_store:forward_doc_abort(Handle),
			{stop, <<>>};

		closed ->
			ok = peerdrive_store:forward_doc_abort(Handle)
	end.


put_doc_handler(Handle, Request, _ReqData) ->
	case Request of
		?PUT_DOC_COMMIT_MSG ->
			case peerdrive_store:put_doc_commit(Handle) of
				ok ->
					{stop, <<>>};
				{error, _} = Error ->
					{abort, Error}
			end;

		?PUT_DOC_ABORT_MSG ->
			ok = peerdrive_store:put_doc_abort(Handle),
			{stop, <<>>};

		closed ->
			ok = peerdrive_store:put_doc_abort(Handle)
	end.


put_rev_handler(Handle, Request, ReqData) ->
	case Request of
		?PUT_REV_PART_MSG ->
			#putrevpartreq{part=Part, data=Data} =
				peerdrive_netstore_pb:decode_putrevpartreq(ReqData),
			?ASSERT_PART(Part),
			ok = check(peerdrive_store:put_rev_part(Handle, Part, Data)),
			<<>>;

		?PUT_REV_COMMIT_MSG ->
			case peerdrive_store:put_rev_commit(Handle) of
				ok ->
					{stop, <<>>};
				{error, _} = Error ->
					{abort, Error}
			end;

		?PUT_REV_ABORT_MSG ->
			ok = peerdrive_store:put_rev_abort(Handle),
			{stop, <<>>};

		closed ->
			ok = peerdrive_store:put_rev_abort(Handle)
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


handle(Body, RetPath, Store, Fun) ->
	try
		send_reply(RetPath, Fun(Body, Store))
	catch
		throw:Error -> send_error(RetPath, Error)
	end.


send_error(RetPath, {error, Error}) ->
	Data = peerdrive_netstore_pb:encode_errorcnf(#errorcnf{error=Error}),
	send_cnf(RetPath, (?ERROR_MSG bsl 4) bor ?FLAG_CNF, Data).


send_reply(#retpath{req=Req} = RetPath, Data) ->
	send_cnf(RetPath, (Req bsl 4) bor ?FLAG_CNF, Data).


send_cnf(#retpath{ref=Ref, socket=Socket}, Cnf, Data) ->
	Raw = <<Ref:32, Cnf:16, Data/binary>>,
	%io:format("[~w] Confirm: ~w~n", [self(), Raw]),
	case gen_tcp:send(Socket, Raw) of
		ok ->
			ok;
		{error, Reason} ->
			error_logger:warning_msg(
				"[~w] Failed to send confirm: ~w~n",
				[self(), Reason])
	end.


send_indication(Socket, Ind, Data) ->
	Indication = (Ind bsl 4) bor ?FLAG_IND,
	Raw = <<16#FFFFFFFF:32, Indication:16, Data/binary>>,
	%io:format("[~w] Indication: ~w~n", [self(), Raw]),
	case gen_tcp:send(Socket, Raw) of
		ok ->
			ok;
		{error, Reason} ->
			error_logger:warning_msg(
				"[~w] Failed to send indication: ~w~n",
				[self(), Reason])
	end.


get_store_by_id(Store) ->
	try
		Id = list_to_existing_atom(Store),
		case lists:keysearch(Id, 1, peerdrive_volman:enum()) of
			{value, StoreSpec} ->
				StoreSpec;
			false ->
				throw({error, enoent})
		end
	catch
		error:badarg -> throw({error, enoent})
	end.
