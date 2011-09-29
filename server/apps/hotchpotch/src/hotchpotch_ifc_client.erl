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

-module(hotchpotch_ifc_client).
-export([init/2, handle_packet/2, handle_info/2, terminate/1]).

-include("store.hrl").
-include("hotchpotch_client_pb.hrl").

-record(state, {socket, handles, next, progreg=false}).
-record(retpath, {socket, req, ref}).

-define(FLAG_REQ, 0).
-define(FLAG_CNF, 1).
-define(FLAG_IND, 2).
-define(FLAG_RSP, 3).

-define(ERROR_MSG,           16#000).
-define(INIT_MSG,            16#001).
-define(ENUM_MSG,            16#002).
-define(LOOKUP_DOC_MSG,      16#003).
-define(LOOKUP_REV_MSG,      16#004).
-define(STAT_MSG,            16#005).
-define(PEEK_MSG,            16#006).
-define(CREATE_MSG,          16#007).
-define(FORK_MSG,            16#008).
-define(UPDATE_MSG,          16#009).
-define(RESUME_MSG,          16#00a).
-define(READ_MSG,            16#00b).
-define(TRUNC_MSG,           16#00c).
-define(WRITE_MSG,           16#00d).
-define(GET_FLAGS_MSG,       16#00e).
-define(SET_FLAGS_MSG,       16#00f).
-define(GET_TYPE_MSG,        16#010).
-define(SET_TYPE_MSG,        16#011).
-define(GET_PARENTS_MSG,     16#012).
-define(MERGE_MSG,           16#013).
-define(REBASE_MSG,          16#014).
-define(COMMIT_MSG,          16#015).
-define(SUSPEND_MSG,         16#016).
-define(CLOSE_MSG,           16#017).
-define(WATCH_ADD_MSG,       16#018).
-define(WATCH_REM_MSG,       16#019).
-define(WATCH_PROGRESS_MSG,  16#01a).
-define(FORGET_MSG,          16#01b).
-define(DELETE_DOC_MSG,      16#01c).
-define(DELETE_REV_MSG,      16#01d).
-define(FORWARD_DOC_MSG,     16#01e).
-define(REPLICATE_DOC_MSG,   16#01f).
-define(REPLICATE_REV_MSG,   16#020).
-define(MOUNT_MSG,           16#021).
-define(UNMOUNT_MSG,         16#022).
-define(SYS_INFO_MSG,        16#023).
-define(WATCH_MSG,           16#024).
-define(PROGRESS_START_MSG,  16#025).
-define(PROGRESS_MSG,        16#026).
-define(PROGRESS_END_MSG,    16#027).

-define(ASSERT_GUID(G), ((is_binary(G) and (size(G) == 16)) orelse
                         throw({error, einval}))).
-define(ASSERT_PART(G), ((is_binary(G) and (size(G) == 4)) orelse
                         throw({error, einval}))).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Servlet callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Socket, _Options) ->
	process_flag(trap_exit, true),
	#state{socket=Socket, handles=dict:new(), next=0}.


terminate(#state{progreg=ProgReg} = State) ->
	if
		ProgReg -> hotchpotch_work_monitor:deregister_proc(self());
		true    -> ok
	end,
	hotchpotch_change_monitor:remove(),
	dict:fold(
		fun(_Cookie, Worker, _Acc) -> Worker ! closed end,
		ok,
		State#state.handles).


handle_info({work_event, Event, Tag, Info}, S) ->
	case Event of
		progress ->
			Ind = #progressind{tag=Tag, progress=Info},
			send_indication(S#state.socket, ?PROGRESS_MSG,
				hotchpotch_client_pb:encode_progressind(Ind));

		started ->
			% Type = sync | rep_doc | rep_rev
			{Type, Source, Dest} = Info,
			Ind = #progressstartind{tag=Tag, type=Type, source=Source, dest=Dest},
			send_indication(S#state.socket, ?PROGRESS_START_MSG,
				hotchpotch_client_pb:encode_progressstartind(Ind));

		done ->
			Ind = #progressendind{tag=Tag},
			send_indication(S#state.socket, ?PROGRESS_END_MSG,
				hotchpotch_client_pb:encode_progressendind(Ind))
	end,
	{ok, S};

handle_info({done, Cookie}, S) ->
	{ok, S#state{handles=dict:erase(Cookie, S#state.handles)}};

handle_info({watch, Cause, Type, Uuid}, S) ->
	Ind = #watchind{event=Cause, type=Type, element=Uuid},
	send_indication(S#state.socket, ?WATCH_MSG,
		hotchpotch_client_pb:encode_watchind(Ind)),
	{ok, S};

handle_info({'EXIT', _From, normal}, S) ->
	{ok, S};

handle_info({'EXIT', From, Reason}, S) ->
	error_logger:error_report(["Client servlet neighbour crashed", {from, From},
		{reason, Reason}]),
	{stop, S}.


handle_packet(<<Ref:32, Request:12, ?FLAG_REQ:4, Body/binary>>, S) ->
	RetPath = #retpath{socket=S#state.socket, req=Request, ref=Ref},
	%io:format("[~w] Ref:~w Request:~w Body:~w~n", [self(), Ref, Request, Body]),
	case Request of
		?INIT_MSG ->
			do_init(Body, RetPath),
			{ok, S};

		?ENUM_MSG ->
			do_enum(RetPath),
			{ok, S};

		?LOOKUP_DOC_MSG ->
			fork(Body, RetPath, fun do_loopup_doc/1),
			{ok, S};

		?LOOKUP_REV_MSG ->
			fork(Body, RetPath, fun do_loopup_rev/1),
			{ok, S};

		?STAT_MSG ->
			fork(Body, RetPath, fun do_stat/1),
			{ok, S};

		?PEEK_MSG ->
			start_worker(S, fun do_peek/2, RetPath, Body);

		?CREATE_MSG ->
			start_worker(S, fun do_create/2, RetPath, Body);

		?FORK_MSG ->
			start_worker(S, fun do_fork/2, RetPath, Body);

		?UPDATE_MSG ->
			start_worker(S, fun do_update/2, RetPath, Body);

		?RESUME_MSG ->
			start_worker(S, fun do_resume/2, RetPath, Body);

		?WATCH_ADD_MSG ->
			do_watch_add_req(RetPath, Body),
			{ok, S};

		?WATCH_REM_MSG ->
			do_watch_rem_req(RetPath, Body),
			{ok, S};

		?FORGET_MSG ->
			fork(Body, RetPath, fun do_forget/1),
			{ok, S};

		?DELETE_DOC_MSG ->
			fork(Body, RetPath, fun do_delete_doc/1),
			{ok, S};

		?DELETE_REV_MSG ->
			fork(Body, RetPath, fun do_delete_rev/1),
			{ok, S};

		?FORWARD_DOC_MSG ->
			fork(Body, RetPath, fun do_forward/1),
			{ok, S};

		?REPLICATE_DOC_MSG ->
			fork(Body, RetPath, fun do_replicate_doc/1),
			{ok, S};

		?REPLICATE_REV_MSG ->
			fork(Body, RetPath, fun do_replicate_rev/1),
			{ok, S};

		?MOUNT_MSG ->
			fork(Body, RetPath, fun do_mount/1),
			{ok, S};

		?UNMOUNT_MSG ->
			fork(Body, RetPath, fun do_unmount/1),
			{ok, S};

		?WATCH_PROGRESS_MSG ->
			S2 = do_watch_progress_req(Body, RetPath, S),
			{ok, S2};

		?SYS_INFO_MSG ->
			fork(Body, RetPath, fun do_sys_info_req/1),
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

do_init(Body, RetPath) ->
	#initreq{ major=Major } = hotchpotch_client_pb:decode_initreq(Body),
	case Major of
		0 ->
			Reply = #initcnf{major = 0, minor = 0, max_packet_size = 16#1000},
			send_reply(RetPath, hotchpotch_client_pb:encode_initcnf(Reply));
		_ ->
			send_error(RetPath, {error, erpcmismatch})
	end.


do_enum(RetPath) ->
	Stores = [
		#enumcnf_store{
			guid = Store,
			id = atom_to_binary(Id, utf8),
			name = Descr,
			is_mounted = proplists:get_bool(mounted, Properties),
			is_removable = proplists:get_bool(removable, Properties),
			is_system_store = proplists:get_bool(system, Properties),
			is_network_store = proplists:get_bool(net, Properties)
		} || {Id, Descr, Store, Properties} <- hotchpotch_volman:enum() ],
	Reply = #enumcnf{stores=Stores},
	send_reply(RetPath, hotchpotch_client_pb:encode_enumcnf(Reply)).


do_loopup_doc(Body) ->
	#lookupdocreq{doc=Doc, stores=Stores} =
		hotchpotch_client_pb:decode_lookupdocreq(Body),
	?ASSERT_GUID(Doc),
	{Revs, PreRevs} = hotchpotch_broker:lookup_doc(Doc, get_stores(Stores)),
	Reply = #lookupdoccnf{
		revs = [ #lookupdoccnf_revmap{rid=RId, stores=RS} || {RId, RS} <- Revs ],
		pre_revs = [ #lookupdoccnf_revmap{rid=RId, stores=RS} || {RId, RS} <- PreRevs ]
	},
	hotchpotch_client_pb:encode_lookupdoccnf(Reply).


do_loopup_rev(Body) ->
	#lookuprevreq{rev=Rev, stores=Stores} =
		hotchpotch_client_pb:decode_lookuprevreq(Body),
	?ASSERT_GUID(Rev),
	Found = hotchpotch_broker:lookup_rev(Rev, get_stores(Stores)),
	Reply = #lookuprevcnf{stores=Found},
	hotchpotch_client_pb:encode_lookuprevcnf(Reply).


do_stat(Body) ->
	#statreq{rev=Rev, stores=Stores} =
		hotchpotch_client_pb:decode_statreq(Body),
	?ASSERT_GUID(Rev),
	{ok, Stat} = check(hotchpotch_broker:stat(Rev, get_stores(Stores))),
	#rev_stat{
		flags     = Flags,
		parts     = Parts,
		parents   = Parents,
		mtime     = Mtime,
		type      = TypeCode,
		creator   = CreatorCode
	} = Stat,
	Reply = #statcnf{
		flags        = Flags,
		parts        = [ #statcnf_part{fourcc=F, size=S, pid=P}
						 || {F, S, P} <- Parts ],
		parents      = Parents,
		mtime        = Mtime,
		type_code    = TypeCode,
		creator_code = CreatorCode
	},
	hotchpotch_client_pb:encode_statcnf(Reply).


do_peek(Cookie, Body) ->
	#peekreq{store=Store, rev=Rev} =
		hotchpotch_client_pb:decode_peekreq(Body),
	?ASSERT_GUID(Rev),
	{ok, Handle} = check(hotchpotch_broker:peek(get_store(Store), Rev)),
	Reply = #peekcnf{handle=Cookie},
	{Handle, hotchpotch_client_pb:encode_peekcnf(Reply)}.


do_create(Cookie, Body) ->
	#createreq{
		store = Store,
		type_code = Type,
		creator_code = Creator
	} = hotchpotch_client_pb:decode_createreq(Body),
	{ok, Doc, Handle} = check(hotchpotch_broker:create(get_store(Store),
		unicode:characters_to_binary(Type), unicode:characters_to_binary(Creator))),
	Reply = #createcnf{handle=Cookie, doc=Doc},
	{Handle, hotchpotch_client_pb:encode_createcnf(Reply)}.


do_fork(Cookie, Body) ->
	#forkreq{
		store = Store,
		rev = Rev,
		creator_code = Creator
	} = hotchpotch_client_pb:decode_forkreq(Body),
	?ASSERT_GUID(Rev),
	{ok, Doc, Handle} = check(hotchpotch_broker:fork(get_store(Store), Rev,
		unicode:characters_to_binary(Creator))),
	Reply = #forkcnf{handle=Cookie, doc=Doc},
	{Handle, hotchpotch_client_pb:encode_forkcnf(Reply)}.


do_update(Cookie, Body) ->
	#updatereq{
		store = Store,
		doc = Doc,
		rev = Rev,
		creator_code = CreatorStr
	} = hotchpotch_client_pb:decode_updatereq(Body),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(Rev),
	Creator = if
		CreatorStr == undefined -> undefined;
		true -> unicode:characters_to_binary(CreatorStr)
	end,
	{ok, Handle} = check(hotchpotch_broker:update(get_store(Store), Doc, Rev, Creator)),
	Reply = #updatecnf{handle=Cookie},
	{Handle, hotchpotch_client_pb:encode_updatecnf(Reply)}.


do_resume(Cookie, Body) ->
	#resumereq{
		store = Store,
		doc = Doc,
		rev = Rev,
		creator_code = CreatorStr
	} = hotchpotch_client_pb:decode_resumereq(Body),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(Rev),
	Creator = if
		CreatorStr == undefined -> undefined;
		true -> unicode:characters_to_binary(CreatorStr)
	end,
	{ok, Handle} = check(hotchpotch_broker:resume(get_store(Store), Doc, Rev, Creator)),
	Reply = #resumecnf{handle=Cookie},
	{Handle, hotchpotch_client_pb:encode_resumecnf(Reply)}.


do_forget(Body) ->
	#forgetreq{
		store = Store,
		doc = Doc,
		rev = Rev
	} = hotchpotch_client_pb:decode_forgetreq(Body),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(Rev),
	ok = check(hotchpotch_broker:forget(get_store(Store), Doc, Rev)),
	<<>>.


do_delete_doc(Body) ->
	#deletedocreq{
		store = Store,
		doc = Doc,
		rev = Rev
	} = hotchpotch_client_pb:decode_deletedocreq(Body),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(Rev),
	ok = check(hotchpotch_broker:delete_doc(get_store(Store), Doc, Rev)),
	<<>>.


do_delete_rev(Body) ->
	#deleterevreq{
		store = Store,
		rev = Rev
	} = hotchpotch_client_pb:decode_deleterevreq(Body),
	?ASSERT_GUID(Rev),
	ok = check(hotchpotch_broker:delete_rev(get_store(Store), Rev)),
	<<>>.


do_forward(Body) ->
	#forwarddocreq{
		store = Store,
		doc = Doc,
		from_rev = FromRev,
		to_rev = ToRev,
		src_store = SrcStore,
		depth = Depth
	} = hotchpotch_client_pb:decode_forwarddocreq(Body),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(FromRev),
	?ASSERT_GUID(ToRev),
	ok = check(hotchpotch_broker:forward_doc(get_store(Store), Doc, FromRev,
		ToRev, get_store(SrcStore), get_depth(Depth))),
	<<>>.


do_replicate_doc(Body) ->
	#replicatedocreq{
		src_store = SrcStore,
		doc = Doc,
		dst_store = DstStore,
		depth = Depth
	} = hotchpotch_client_pb:decode_replicatedocreq(Body),
	?ASSERT_GUID(Doc),
	ok = check(hotchpotch_broker:replicate_doc(get_store(SrcStore), Doc,
		get_store(DstStore), get_depth(Depth))),
	<<>>.


do_replicate_rev(Body) ->
	#replicaterevreq{
		src_store = SrcStore,
		rev = Rev,
		dst_store = DstStore,
		depth = Depth
	} = hotchpotch_client_pb:decode_replicaterevreq(Body),
	?ASSERT_GUID(Rev),
	ok = check(hotchpotch_broker:replicate_rev(get_store(SrcStore), Rev,
		get_store(DstStore), get_depth(Depth))),
	<<>>.


do_mount(Body) ->
	#mountreq{id=IdStr} = hotchpotch_client_pb:decode_mountreq(Body),
	{Id, _Descr, _Guid, Tags} = get_store_by_id(IdStr),
	case proplists:is_defined(removable, Tags) of
		true  -> {ok, _} = check(hotchpotch_volman:mount(Id));
		false -> throw({error, einval})
	end,
	<<>>.


do_unmount(Body) ->
	#unmountreq{id=IdStr} = hotchpotch_client_pb:decode_unmountreq(Body),
	{Id, _Descr, _Guid, Tags} = get_store_by_id(IdStr),
	case proplists:is_defined(removable, Tags) of
		true  -> ok = check(hotchpotch_volman:unmount(Id));
		false -> throw({error, einval})
	end,
	<<>>.


do_watch_add_req(RetPath, Body) ->
	#watchaddreq{type=Type, element=Obj} =
		hotchpotch_client_pb:decode_watchaddreq(Body),
	try
		?ASSERT_GUID(Obj),
		ok = check(hotchpotch_change_monitor:watch(Type, Obj)),
		send_reply(RetPath, <<>>)
	catch
		throw:Error -> send_error(RetPath, Error)
	end.


do_watch_rem_req(RetPath, Body) ->
	#watchremreq{type=Type, element=Obj} =
		hotchpotch_client_pb:decode_watchremreq(Body),
	try
		?ASSERT_GUID(Obj),
		ok = check(hotchpotch_change_monitor:unwatch(Type, Obj)),
		send_reply(RetPath, <<>>)
	catch
		throw:Error -> send_error(RetPath, Error)
	end.


do_watch_progress_req(Body, RetPath, #state{progreg=ProgReg} = S) ->
	#watchprogressreq{enable=Enable} =
		hotchpotch_client_pb:decode_watchprogressreq(Body),
	S2 = case Enable of
		ProgReg ->
			S;
		true ->
			hotchpotch_work_monitor:register_proc(self()),
			S#state{progreg=true};
		false ->
			hotchpotch_work_monitor:deregister_proc(self()),
			S#state{progreg=false}
	end,
	send_reply(RetPath, <<>>),
	S2.


do_sys_info_req(Body) ->
	#sysinforeq{param=Param} = hotchpotch_client_pb:decode_sysinforeq(Body),
	{ok, Result} = check(hotchpotch_sys_info:lookup(
		unicode:characters_to_binary(Param))),
	Reply = case Result of
		Str when is_binary(Str)  -> #sysinfocnf{as_string=Str};
		Int when is_integer(Int) -> #sysinfocnf{as_int=Int}
	end,
	hotchpotch_client_pb:encode_sysinfocnf(Reply).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% IO handler loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_worker(S, Fun, RetPath, Body) ->
	Cookie = S#state.next,
	Server = self(),
	Worker = spawn_link(
		fun() ->
			try
				{Handle, Reply} = Fun(Cookie, Body),
				send_reply(RetPath, Reply),
				io_loop(Handle),
				Server ! {done, Cookie}
			catch
				throw:Error -> send_error(RetPath, Error)
			end
		end),
	{ok, S#state{
		handles = dict:store(Cookie, Worker, S#state.handles),
		next    = Cookie + 1}}.


io_loop(Handle) ->
	receive
		{Req, Body, RetPath} ->
			try io_loop_process(Handle, Req, Body) of
				Reply when is_binary(Reply) ->
					send_reply(RetPath, Reply),
					io_loop(Handle);
				{stop, Reply} ->
					send_reply(RetPath, Reply)
			catch
				throw:Error ->
					send_error(RetPath, Error),
					io_loop(Handle)
			end;

		closed ->
			hotchpotch_broker:close(Handle)
	end.


io_loop_process(Handle, Request, ReqData) ->
	case Request of
		?READ_MSG ->
			#readreq{part=Part, offset=Offset, length=Length} =
				hotchpotch_client_pb:decode_readreq(ReqData),
			?ASSERT_PART(Part),
			{ok, Data} = check(hotchpotch_broker:read(Handle, Part, Offset, Length)),
			hotchpotch_client_pb:encode_readcnf(#readcnf{data=Data});

		?WRITE_MSG ->
			#writereq{part=Part, offset=Offset, data=Data} =
				hotchpotch_client_pb:decode_writereq(ReqData),
			?ASSERT_PART(Part),
			ok = check(hotchpotch_broker:write(Handle, Part, Offset, Data)),
			<<>>;

		?TRUNC_MSG ->
			#truncreq{part=Part, offset=Offset} =
				hotchpotch_client_pb:decode_truncreq(ReqData),
			?ASSERT_PART(Part),
			ok = check(hotchpotch_broker:truncate(Handle, Part, Offset)),
			<<>>;

		?CLOSE_MSG ->
			ok = hotchpotch_broker:close(Handle),
			{stop, <<>>};

		?COMMIT_MSG ->
			{ok, Rev} = check(hotchpotch_broker:commit(Handle)),
			hotchpotch_client_pb:encode_commitcnf(#commitcnf{rev=Rev});

		?SUSPEND_MSG ->
			{ok, Rev} = check(hotchpotch_broker:suspend(Handle)),
			hotchpotch_client_pb:encode_commitcnf(#commitcnf{rev=Rev});

		?MERGE_MSG ->
			#mergereq{store=Store, rev=Rev, depth=Depth} =
				hotchpotch_client_pb:decode_mergereq(ReqData),
			?ASSERT_GUID(Rev),
			ok = check(hotchpotch_broker:merge(Handle, get_store(Store), Rev,
				get_depth(Depth))),
			<<>>;

		?REBASE_MSG ->
			#rebasereq{rev=Rev} = hotchpotch_client_pb:decode_rebasereq(ReqData),
			?ASSERT_GUID(Rev),
			ok = check(hotchpotch_broker:rebase(Handle, Rev)),
			<<>>;

		?GET_PARENTS_MSG ->
			{ok, Parents} = check(hotchpotch_broker:get_parents(Handle)),
			hotchpotch_client_pb:encode_getparentscnf(
				#getparentscnf{parents=Parents});

		?SET_TYPE_MSG ->
			#settypereq{type_code=Type} =
				hotchpotch_client_pb:decode_settypereq(ReqData),
			ok = check(hotchpotch_broker:set_type(Handle,
				unicode:characters_to_binary(Type))),
			<<>>;

		?GET_TYPE_MSG ->
			{ok, Type} = check(hotchpotch_broker:get_type(Handle)),
			hotchpotch_client_pb:encode_gettypecnf(#gettypecnf{type_code=Type});

		?SET_FLAGS_MSG ->
			#setflagsreq{flags=Flags} =
				hotchpotch_client_pb:decode_setflagsreq(ReqData),
			ok = check(hotchpotch_broker:set_flags(Handle, Flags)),
			<<>>;

		?GET_FLAGS_MSG ->
			{ok, Flags} = check(hotchpotch_broker:get_flags(Handle)),
			hotchpotch_client_pb:encode_getflagscnf(#getflagscnf{flags=Flags})
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_store(SId) ->
	?ASSERT_GUID(SId),
	case hotchpotch_volman:store(SId) of
		{ok, Store} -> Store;
		error       -> throw({error, enxio})
	end.


get_stores(StoreList) ->
	case StoreList of
		undefined ->
			[Store || {_SId, Store} <- hotchpotch_volman:stores()];

		_ ->
			lists:foldr(
				fun(SId, Acc) ->
					?ASSERT_GUID(SId),
					case hotchpotch_volman:store(SId) of
						{ok, Store} -> [Store | Acc];
						error       -> Acc
					end
				end,
				[],
				StoreList)
	end.


get_store_by_id(Store) ->
	try
		Id = list_to_existing_atom(Store),
		case lists:keysearch(Id, 1, hotchpotch_volman:enum()) of
			{value, StoreSpec} ->
				StoreSpec;
			false ->
				throw({error, enoent})
		end
	catch
		error:badarg -> throw({error, enoent})
	end.


check({error, _} = Error) ->
	throw(Error);

check(Result) ->
	Result.


get_depth(Depth) when is_integer(Depth) ->
	Depth;

get_depth(undefined) ->
	0.

fork(Body, RetPath, Fun) ->
	spawn_link(
		fun() ->
			try
				send_reply(RetPath, Fun(Body))
			catch
				throw:Error -> send_error(RetPath, Error)
			end
		end).


send_error(RetPath, {error, Error}) ->
	Data = hotchpotch_client_pb:encode_errorcnf(#errorcnf{error=Error}),
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

