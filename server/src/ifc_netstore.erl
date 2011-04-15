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

-module(ifc_netstore).
-export([init/2, handle_packet/2, handle_info/2, terminate/1]).
-import(netencode, [encode_direct_result/1, encode_list/1, encode_list/2,
	encode_list_32/1, encode_list_32/2, encode_string/1, parse_list/2,
	parse_list_32/2, parse_store/1, parse_string/1, parse_uuid/1,
	parse_uuid_list/1, encode_linkmap/1, parse_linkmap/1]).
-include("store.hrl").
-include("netstore.hrl").

-record(state, {socket, handles, next, stores, store_pid, store_uuid}).
-record(retpath, {socket, ref}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Servlet callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Socket, Stores) ->
	process_flag(trap_exit, true),
	vol_monitor:register_proc(self()),
	#state{socket=Socket, handles=dict:new(), next=0, stores=Stores}.


terminate(State) ->
	vol_monitor:deregister_proc(self()),
	dict:fold(
		fun(_Handle, Worker, _Acc) -> Worker ! closed end,
		ok,
		State#state.handles).


handle_info({done, Handle}, S) ->
	{ok, S#state{handles=dict:erase(Handle, S#state.handles)}};

handle_info({'EXIT', _From, normal}, S) ->
	{ok, S};

handle_info({trigger_add_rev, StoreGuid, Rev}, S) ->
	#state{socket=Socket, store_uuid=Uuid} = S,
	case StoreGuid of
		Uuid  -> send_indication(Socket, ?ADD_REV_IND, Rev);
		_Else -> ok
	end,
	{ok, S};

handle_info({trigger_rm_rev, StoreGuid, Rev}, S) ->
	#state{socket=Socket, store_uuid=Uuid} = S,
	case StoreGuid of
		Uuid  -> send_indication(Socket, ?REM_REV_IND, Rev);
		_Else -> ok
	end,
	{ok, S};

handle_info({trigger_add_doc, StoreGuid, Doc}, S) ->
	#state{socket=Socket, store_uuid=Uuid} = S,
	case StoreGuid of
		Uuid  -> send_indication(Socket, ?ADD_DOC_IND, Doc);
		_Else -> ok
	end,
	{ok, S};

handle_info({trigger_rm_doc, StoreGuid, Doc}, S) ->
	#state{socket=Socket, store_uuid=Uuid} = S,
	case StoreGuid of
		Uuid  -> send_indication(Socket, ?REM_DOC_IND, Doc);
		_Else -> ok
	end,
	{ok, S};

handle_info({trigger_mod_doc, StoreGuid, Doc}, S) ->
	#state{socket=Socket, store_uuid=Uuid} = S,
	case StoreGuid of
		Uuid  -> send_indication(Socket, ?MOD_DOC_IND, Doc);
		_Else -> ok
	end,
	{ok, S};

handle_info({trigger_rem_store, StoreGuid}, #state{store_uuid=Uuid}=S) ->
	case StoreGuid of
		Uuid  -> {stop, S};
		_Else -> {ok, S}
	end;

handle_info({trigger_add_store, _StoreGuid}, S) ->
	{ok, S};

handle_info({'EXIT', From, Reason}, S) ->
	error_logger:error_report(["Netstore servlet neighbour crashed", {from, From},
		{reason, Reason}]),
	{stop, S};

handle_info({gen_event_EXIT, _Handler, _Reason}, S) ->
	{ok, S}.


handle_packet(Packet, #state{socket=Socket, store_pid=Store} = S) ->
	<<Ref:32, Request:16, Body/binary>> = Packet,
	RetPath = #retpath{socket=Socket, ref=Ref},
	%io:format("[~w] Ref:~w Request:~w Body:~w~n", [self(), Ref, Request, Body]),
	case Request of
		?INIT_REQ ->
			do_init(Body, RetPath, S);

		?STATFS_REQ ->
			do_statfs(RetPath, Store),
			{ok, S};

		?LOOKUP_REQ ->
			do_loopup(Body, RetPath, Store),
			{ok, S};

		?CONTAINS_REQ ->
			do_contains(Body, RetPath, Store),
			{ok, S};

		?STAT_REQ ->
			do_stat(Body, RetPath, Store),
			{ok, S};

		?PEEK_REQ ->
			start_worker(S, fun(Handle) -> do_peek(Handle, RetPath, Body, Store) end);

		?CREATE_REQ ->
			start_worker(S, fun(Handle) -> do_create(Handle, RetPath, Body, Store) end);

		?FORK_REQ ->
			start_worker(S, fun(Handle) -> do_fork(Handle, RetPath, Body, Store) end);

		?UPDATE_REQ ->
			start_worker(S, fun(Handle) -> do_update(Handle, RetPath, Body, Store) end);

		?RESUME_REQ ->
			start_worker(S, fun(Handle) -> do_resume(Handle, RetPath, Body, Store) end);

		?FORGET_REQ ->
			do_forget(Body, RetPath, Store),
			{ok, S};

		?DELETE_DOC_REQ ->
			do_delete_doc(Body, RetPath, Store),
			{ok, S};

		?DELETE_REV_REQ ->
			do_delete_rev(Body, RetPath, Store),
			{ok, S};

		?PUT_DOC_REQ ->
			do_put_doc(Body, RetPath, Store),
			{ok, S};

		?FF_DOC_START_REQ ->
			do_forward_doc_start(RetPath, Body, S);

		?PUT_REV_START_REQ ->
			do_put_rev_start(RetPath, Body, S);

		?SYNC_GET_CHANGES_REQ ->
			do_sync_get_changed(Body, RetPath, Store),
			{ok, S};

		?SYNC_SET_ANCHOR_REQ ->
			do_sync_set_anchor(Body, RetPath, Store),
			{ok, S};

		?SYNC_FINISH_REQ ->
			do_sync_finish(Body, RetPath, Store),
			{ok, S};

		_ ->
			<<Handle:32, Data/binary>> = Body,
			Worker = dict:fetch(Handle, S#state.handles),
			Worker ! {Request, Data, RetPath},
			{ok, S}
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Request handling functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_init(Body, RetPath, #state{store_pid=undefined, stores=Stores} = S) ->
	try
		<<0:16, Major:8, _Minor:8, Body1/binary>> = Body,
		case Major of
			0 -> ok;
			_ -> throw({error, erpcmismatch})
		end,
		Guid = case parse_store(Body1) of
			{ok, {Id, _Descr, Guid_, _Tags}} ->
				case lists:member(Id, Stores) of
					true -> Guid_;
					false -> throw({error, eacces})
				end;

			{error, _Reason} = Error ->
				throw(Error)
		end,
		Pid = case volman:store(Guid) of
			{ok, Pid_} -> Pid_;
			error -> throw({error, enoent})
		end,
		S2 = S#state{store_pid=Pid, store_uuid=Guid},
		send_reply(RetPath, ?INIT_CNF, <<(encode_direct_result(ok))/binary,
			0:32, 16#1000:32, Guid/binary>>),
		{ok, S2}
	catch
		throw:Err ->
			send_reply(RetPath, ?INIT_CNF, encode_direct_result(Err)),
			{stop, S}
	end;

do_init(_Body, RetPath, S) ->
	send_reply(RetPath, ?INIT_CNF, encode_direct_result({error, ebadrpc})),
	{stop, S}.


do_statfs(RetPath, Store) ->
	Reply = case store:statfs(Store) of
		{ok, Stat} ->
			#fs_stat{
				bsize  = BSize,
				blocks = Blocks,
				bfree  = BFree,
				bavail = BAvail
			} = Stat,
			<<(encode_direct_result(ok))/binary, BSize:32, Blocks:64, BFree:64,
				BAvail:64>>;

		{error, _Reason} = Error ->
			encode_direct_result(Error)
	end,
	send_reply(RetPath, ?STATFS_CNF, Reply).


do_loopup(Body, RetPath, Store) ->
	{Doc, <<>>} = parse_uuid(Body),
	Reply = case store:lookup(Store, Doc) of
		{ok, Rev, PreRevs} ->
			<<(encode_direct_result(ok))/binary, Rev/binary,
				(encode_list(PreRevs))/binary>>;
		error ->
			encode_direct_result({error, enoent})
	end,
	send_reply(RetPath, ?LOOKUP_CNF, Reply).


do_contains(Body, RetPath, Store) ->
	{Rev, <<>>} = parse_uuid(Body),
	Reply = case store:contains(Store, Rev) of
		true  -> encode_direct_result(ok);
		false -> encode_direct_result({error, enoent})
	end,
	send_reply(RetPath, ?CONTAINS_CNF, Reply).


do_stat(Body, RetPath, Store) ->
	{Rev, <<>>} = parse_uuid(Body),
	Reply = case store:stat(Store, Rev) of
		{ok, Stat} ->
			#rev_stat{
				flags   = Flags,
				parts   = Parts,
				parents = Parents,
				mtime   = Mtime,
				type    = TypeCode,
				creator = CreatorCode,
				links   = Links
			} = Stat,
			ReplyParts = encode_list(
				fun ({FourCC, Size, Hash}) ->
					<<FourCC/binary, Size:64, Hash/binary>>
				end,
				Parts),
			<<
				(encode_direct_result(ok))/binary,
				Flags:32,
				ReplyParts/binary,
				(encode_list(Parents))/binary,
				Mtime:64,
				(encode_string(TypeCode))/binary,
				(encode_string(CreatorCode))/binary,
				(encode_linkmap(Links))/binary
			>>;

		Error ->
			encode_direct_result(Error)
	end,
	send_reply(RetPath, ?STAT_CNF, Reply).


do_peek(NetHandle, RetPath, Body, Store) ->
	{Rev, <<>>} = parse_uuid(Body),
	case store:peek(Store, Rev) of
		{ok, StoreHandle} ->
			Reply = <<(encode_direct_result(ok))/binary, NetHandle:32>>,
			send_reply(RetPath, ?PEEK_CNF, Reply),
			io_loop(StoreHandle);

		Error ->
			send_reply(RetPath, ?PEEK_CNF, encode_direct_result(Error))
	end.


do_create(NetHandle, RetPath, Body, Store) ->
	{Doc, Body1} = parse_uuid(Body),
	{Type, Body2} = parse_string(Body1),
	{Creator, <<>>} = parse_string(Body2),
	case store:create(Store, Doc, Type, Creator) of
		{ok, StoreHandle} ->
			Reply = <<(encode_direct_result(ok))/binary, NetHandle:32>>,
			send_reply(RetPath, ?CREATE_CNF, Reply),
			io_loop(StoreHandle);

		Error ->
			send_reply(RetPath, ?CREATE_CNF, encode_direct_result(Error))
	end.


do_fork(NetHandle, RetPath, Body, Store) ->
	{Doc, Body1} = parse_uuid(Body),
	{Rev, Body2} = parse_uuid(Body1),
	{Creator, <<>>} = parse_string(Body2),
	case store:fork(Store, Doc, Rev, Creator) of
		{ok, StoreHandle} ->
			Reply = <<(encode_direct_result(ok))/binary, NetHandle:32>>,
			send_reply(RetPath, ?FORK_CNF, Reply),
			io_loop(StoreHandle);

		Error ->
			send_reply(RetPath, ?FORK_CNF, encode_direct_result(Error))
	end.


do_update(NetHandle, RetPath, Body, Store) ->
	{Doc, Body1} = parse_uuid(Body),
	{Rev, Body2} = parse_uuid(Body1),
	{Creator, <<>>} = parse_string(Body2),
	RealCreator = case Creator of
		<<>> -> keep;
		_    -> Creator
	end,
	case store:update(Store, Doc, Rev, RealCreator) of
		{ok, StoreHandle} ->
			Reply = <<(encode_direct_result(ok))/binary, NetHandle:32>>,
			send_reply(RetPath, ?UPDATE_CNF, Reply),
			io_loop(StoreHandle);

		Error ->
			send_reply(RetPath, ?UPDATE_CNF, encode_direct_result(Error))
	end.


do_resume(NetHandle, RetPath, Body, Store) ->
	{Doc, Body1} = parse_uuid(Body),
	{Rev, Body2} = parse_uuid(Body1),
	{Creator, <<>>} = parse_string(Body2),
	RealCreator = case Creator of
		<<>> -> keep;
		_    -> Creator
	end,
	case store:resume(Store, Doc, Rev, RealCreator) of
		{ok, StoreHandle} ->
			Reply = <<(encode_direct_result(ok))/binary, NetHandle:32>>,
			send_reply(RetPath, ?RESUME_CNF, Reply),
			io_loop(StoreHandle);

		Error ->
			send_reply(RetPath, ?RESUME_CNF, encode_direct_result(Error))
	end.


do_forget(Body, RetPath, Store) ->
	{Doc, Body1} = parse_uuid(Body),
	{Rev, <<>>} = parse_uuid(Body1),
	Reply = store:forget(Store, Doc, Rev),
	send_reply(RetPath, ?FORGET_CNF, encode_direct_result(Reply)).


do_delete_doc(Body, RetPath, Store) ->
	{Doc, Body1} = parse_uuid(Body),
	{Rev, <<>>} = parse_uuid(Body1),
	Reply = store:delete_doc(Store, Doc, Rev),
	send_reply(RetPath, ?DELETE_DOC_CNF, encode_direct_result(Reply)).


do_delete_rev(Body, RetPath, Store) ->
	{Rev, <<>>} = parse_uuid(Body),
	Reply = store:delete_rev(Store, Rev),
	send_reply(RetPath, ?DELETE_REV_CNF, encode_direct_result(Reply)).


do_put_doc(Body, RetPath, Store) ->
	{Doc, Body1} = parse_uuid(Body),
	{Rev, <<>>} = parse_uuid(Body1),
	Reply = store:put_doc(Store, Doc, Rev),
	send_reply(RetPath, ?PUT_DOC_CNF, encode_direct_result(Reply)).


do_forward_doc_start(RetPath, Body, #state{store_pid=Store} = S) ->
	{Doc, Body1} = parse_uuid(Body),
	{RevPath, <<>>} = parse_uuid_list(Body1),
	case store:forward_doc_start(Store, Doc, RevPath) of
		ok ->
			Reply = <<(encode_direct_result(ok))/binary, 0:32,
				(encode_list([]))/binary>>,
			send_reply(RetPath, ?FF_DOC_START_CNF, Reply),
			{ok, S};

		{ok, Missing, StoreHandle} ->
			Fun = fun(NetHandle) ->
				Reply = <<
					(encode_direct_result(ok))/binary,
					NetHandle:32,
					(encode_list(Missing))/binary
				>>,
				send_reply(RetPath, ?FF_DOC_START_CNF, Reply),
				forward_loop(StoreHandle)
			end,
			start_worker(S, Fun);

		Error ->
			send_reply(RetPath, ?FF_DOC_START_CNF, encode_direct_result(Error)),
			{ok, S}
	end.


do_put_rev_start(RetPath, Body, #state{store_pid=Store} = S) ->
	{Rev, Body1} = parse_uuid(Body),
	{Revision, <<>>} = parse_revision(Body1),
	case store:put_rev_start(Store, Rev, Revision) of
		ok ->
			Reply = <<(encode_direct_result(ok))/binary, 0:32,
				(encode_list([]))/binary>>,
			send_reply(RetPath, ?PUT_REV_START_CNF, Reply),
			{ok, S};

		{ok, Missing, StoreHandle} ->
			Fun = fun(NetHandle) ->
				Reply = <<
					(encode_direct_result(ok))/binary,
					NetHandle:32,
					(encode_list(Missing))/binary
				>>,
				send_reply(RetPath, ?PUT_REV_START_CNF, Reply),
				put_loop(StoreHandle)
			end,
			start_worker(S, Fun);

		Error ->
			send_reply(RetPath, ?PUT_REV_START_CNF, encode_direct_result(Error)),
			{ok, S}
	end.


do_sync_get_changed(Body, RetPath, Store) ->
	{Peer, <<>>} = parse_uuid(Body),
	Reply = case store:sync_get_changes(Store, Peer) of
		{ok, Backlog} ->
			List = encode_list_32(
				fun({Doc, SeqNum}) -> <<Doc/binary, SeqNum:64>> end,
				Backlog),
			<<(encode_direct_result(ok))/binary, List/binary>>;

		Error ->
			encode_direct_result(Error)
	end,
	send_reply(RetPath, ?SYNC_GET_CHANGES_CNF, Reply).


do_sync_set_anchor(Body, RetPath, Store) ->
	<<Peer:16/binary, SeqNum:64>> = Body,
	Reply = store:sync_set_anchor(Store, Peer, SeqNum),
	send_reply(RetPath, ?SYNC_SET_ANCHOR_CNF, encode_direct_result(Reply)).


do_sync_finish(Body, RetPath, Store) ->
	{Peer, <<>>} = parse_uuid(Body),
	Reply = store:sync_finish(Store, Peer),
	send_reply(RetPath, ?SYNC_FINISH_CNF, encode_direct_result(Reply)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% IO handler loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

io_loop(Handle) ->
	receive
		{?READ_REQ, ReqData, RetPath} ->
			<<Part:4/binary, Offset:64, Length:32>> = ReqData,
			Reply = case store:read(Handle, Part, Offset, Length) of
				{ok, Data} ->
					<<(encode_direct_result(ok))/binary, Data/binary>>;
				Error ->
					encode_direct_result(Error)
			end,
			send_reply(RetPath, ?READ_CNF, Reply),
			io_loop(Handle);

		{?WRITE_REQ, ReqData, RetPath} ->
			<<Part:4/binary, Offset:64, Data/binary>> = ReqData,
			Reply = store:write(Handle, Part, Offset, Data),
			send_reply(RetPath, ?WRITE_CNF, encode_direct_result(Reply)),
			io_loop(Handle);

		{?TRUNC_REQ, ReqData, RetPath} ->
			<<Part:4/binary, Offset:64>> = ReqData,
			Reply = store:truncate(Handle, Part, Offset),
			send_reply(RetPath, ?TRUNC_CNF, encode_direct_result(Reply)),
			io_loop(Handle);

		{?CLOSE_REQ, <<>>, RetPath} ->
			Reply = store:close(Handle),
			send_reply(RetPath, ?CLOSE_CNF, encode_direct_result(Reply));

		{?COMMIT_REQ, ReqData, RetPath} ->
			<<Mtime:64>> = ReqData,
			Reply = case store:commit(Handle, Mtime) of
				{ok, Rev} ->
					<<(encode_direct_result(ok))/binary, Rev/binary>>;
				Error ->
					encode_direct_result(Error)
			end,
			send_reply(RetPath, ?COMMIT_CNF, Reply),
			io_loop(Handle);

		{?SUSPEND_REQ, ReqData, RetPath} ->
			<<Mtime:64>> = ReqData,
			Reply = case store:suspend(Handle, Mtime) of
				{ok, Rev} ->
					<<(encode_direct_result(ok))/binary, Rev/binary>>;
				Error ->
					encode_direct_result(Error)
			end,
			send_reply(RetPath, ?SUSPEND_CNF, Reply),
			io_loop(Handle);

		{?SET_LINKS_REQ, Body, RetPath} ->
			{Links, <<>>} = parse_linkmap(Body),
			Reply = store:set_links(Handle, Links),
			send_reply(RetPath, ?SET_LINKS_CNF, encode_direct_result(Reply)),
			io_loop(Handle);

		{?GET_LINKS_REQ, <<>>, RetPath} ->
			Reply = case store:get_links(Handle) of
				{ok, Links} ->
					<<(encode_direct_result(ok))/binary,
						(encode_linkmap(Links))/binary>>;
				Error ->
					encode_direct_result(Error)
			end,
			send_reply(RetPath, ?GET_LINKS_CNF, Reply),
			io_loop(Handle);

		{?SET_PARENTS_REQ, Body, RetPath} ->
			{Parents, <<>>} = parse_uuid_list(Body),
			Reply = store:set_parents(Handle, Parents),
			send_reply(RetPath, ?SET_PARENTS_CNF, encode_direct_result(Reply)),
			io_loop(Handle);

		{?GET_PARENTS_REQ, <<>>, RetPath} ->
			Reply = case store:get_parents(Handle) of
				{ok, Parents} ->
					<<(encode_direct_result(ok))/binary,
						(encode_list(Parents))/binary>>;
				Error ->
					encode_direct_result(Error)
			end,
			send_reply(RetPath, ?GET_PARENTS_CNF, Reply),
			io_loop(Handle);

		{?SET_TYPE_REQ, Body, RetPath} ->
			{Type, <<>>} = parse_string(Body),
			Reply = store:set_type(Handle, Type),
			send_reply(RetPath, ?SET_TYPE_CNF, encode_direct_result(Reply)),
			io_loop(Handle);

		{?GET_TYPE_REQ, <<>>, RetPath} ->
			Reply = case store:get_type(Handle) of
				{ok, Type} ->
					<<(encode_direct_result(ok))/binary,
						(encode_string(Type))/binary>>;
				Error ->
					encode_direct_result(Error)
			end,
			send_reply(RetPath, ?GET_TYPE_CNF, Reply),
			io_loop(Handle);

		closed ->
			store:close(Handle)
	end.


forward_loop(Handle) ->
	receive
		{?FF_DOC_COMMIT_REQ, <<>>, RetPath} ->
			Reply = store:forward_doc_commit(Handle),
			send_reply(RetPath, ?FF_DOC_COMMIT_CNF, encode_direct_result(Reply));

		{?FF_DOC_ABORT_REQ, <<>>, RetPath} ->
			Reply = store:forward_doc_abort(Handle),
			send_reply(RetPath, ?FF_DOC_ABORT_CNF, encode_direct_result(Reply));

		closed ->
			store:forward_doc_abort(Handle)
	end.


put_loop(Handle) ->
	receive
		{?PUT_REV_PART_REQ, ReqData, RetPath} ->
			<<Part:4/binary, Data/binary>> = ReqData,
			Reply = store:put_rev_part(Handle, Part, Data),
			send_reply(RetPath, ?PUT_REV_PART_CNF, encode_direct_result(Reply)),
			put_loop(Handle);

		{?PUT_REV_COMMIT_REQ, <<>>, RetPath} ->
			Reply = store:put_rev_commit(Handle),
			send_reply(RetPath, ?PUT_REV_COMMIT_CNF, encode_direct_result(Reply));

		{?PUT_REV_ABORT_REQ, <<>>, RetPath} ->
			Reply = store:put_rev_abort(Handle),
			send_reply(RetPath, ?PUT_REV_ABORT_CNF, encode_direct_result(Reply));

		closed ->
			store:put_rev_abort(Handle)
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_worker(S, Fun) ->
	Handle = S#state.next,
	Server = self(),
	Worker = spawn_link(fun() -> Fun(Handle), Server ! {done, Handle} end),
	{ok, S#state{
		handles = dict:store(Handle, Worker, S#state.handles),
		next    = Handle + 1}}.


send_reply(RetPath, Reply, Data) ->
	Raw = <<(RetPath#retpath.ref):32, Reply:16, Data/binary>>,
	%io:format("[~w] Reply: ~w~n", [self(), Raw]),
	case gen_tcp:send(RetPath#retpath.socket, Raw) of
		ok ->
			ok;
		{error, Reason} ->
			error_logger:warning_msg(
				"[~w] Failed to send reply: ~w~n",
				[self(), Reason])
	end.


send_indication(Socket, Indication, Data) ->
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


parse_revision(Body) ->
	<<Flags:32, Body1/binary>> = Body,
	{Parts, Body2} = parse_list(
		fun(<<FourCC:4/binary, Hash:16/binary, Rest/binary>>) ->
			{{FourCC, Hash}, Rest}
		end,
		Body1),
	{Parents, Body3} = parse_uuid_list(Body2),
	<<Mtime:64, Body4/binary>> = Body3,
	{Type, Body5} = parse_string(Body4),
	{Creator, Body6} = parse_string(Body5),
	{Links, Body7} = parse_linkmap(Body6),
	Result = #revision{
		flags   = Flags,
		parts   = Parts,
		parents = Parents,
		mtime   = Mtime,
		type    = Type,
		creator = Creator,
		links   = Links
	},
	{Result, Body7}.

