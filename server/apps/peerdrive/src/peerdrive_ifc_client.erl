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

-module(peerdrive_ifc_client).
-export([init/2, handle_packet/2, handle_info/2, terminate/1]).

-include("store.hrl").
-include("peerdrive_client_pb.hrl").
-include("utils.hrl").

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
-define(PROGRESS_QUERY_MSG,  16#028).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Servlet callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Socket, _Options) ->
	process_flag(trap_exit, true),
	#state{socket=Socket, handles=dict:new(), next=0}.


terminate(#state{progreg=ProgReg} = State) ->
	if
		ProgReg -> peerdrive_work_monitor:deregister_proc();
		true    -> ok
	end,
	peerdrive_change_monitor:remove(),
	dict:fold(
		fun(_Cookie, Worker, _Acc) -> Worker ! closed end,
		ok,
		State#state.handles).


handle_info({work_event, Event, Tag, Info}, S) ->
	case Event of
		progress ->
			{State, Progress, ErrInfo} = Info,
			Ind = #progressind{
				tag      = Tag,
				progress = Progress,
				state    = State,
				err_code = proplists:get_value(code, ErrInfo, undefined),
				err_doc  = proplists:get_value(doc, ErrInfo, undefined),
				err_rev  = proplists:get_value(rev, ErrInfo, undefined)
			},
			send_indication(S#state.socket, ?PROGRESS_MSG,
				peerdrive_client_pb:encode_progressind(Ind));

		started ->
			{_Proc, StartInfo} = Info,
			Ind = encode_progress_start(Tag, StartInfo),
			send_indication(S#state.socket, ?PROGRESS_START_MSG,
				peerdrive_client_pb:encode_progressstartind(Ind));

		done ->
			Ind = #progressendind{tag=Tag},
			send_indication(S#state.socket, ?PROGRESS_END_MSG,
				peerdrive_client_pb:encode_progressendind(Ind))
	end,
	{ok, S};

handle_info({done, Cookie}, S) ->
	{ok, S#state{handles=dict:erase(Cookie, S#state.handles)}};

handle_info({watch, Cause, Type, Uuid}, S) ->
	Ind = #watchind{event=Cause, type=Type, element=Uuid},
	send_indication(S#state.socket, ?WATCH_MSG,
		peerdrive_client_pb:encode_watchind(Ind)),
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

		?PROGRESS_START_MSG ->
			do_progress_start(Body, RetPath),
			{ok, S};

		?PROGRESS_END_MSG ->
			do_progress_end(Body, RetPath),
			{ok, S};

		?PROGRESS_QUERY_MSG ->
			do_progress_query(RetPath),
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
	#initreq{ major=Major } = peerdrive_client_pb:decode_initreq(Body),
	case Major of
		0 ->
			Reply = #initcnf{major = 0, minor = 0, max_packet_size = 16#1000},
			send_reply(RetPath, peerdrive_client_pb:encode_initcnf(Reply));
		_ ->
			send_error(RetPath, {error, erpcmismatch})
	end.


do_enum(RetPath) ->
	Stores = [
		{proplists:get_bool(system, Properties), #enumcnf_store{
			guid = Store,
			id = atom_to_binary(Id, utf8),
			name = Descr,
			is_mounted = proplists:get_bool(mounted, Properties),
			is_removable = proplists:get_bool(removable, Properties),
			is_network_store = proplists:get_bool(net, Properties)
		}} || {Id, Descr, Store, Properties} <- peerdrive_volman:enum() ],
	{value, {true, SysStore}, AllStores} = lists:keytake(true, 1, Stores),
	Reply = #enumcnf{sys_store=SysStore, stores=[S || {false,S} <- AllStores]},
	send_reply(RetPath, peerdrive_client_pb:encode_enumcnf(Reply)).


do_loopup_doc(Body) ->
	#lookupdocreq{doc=Doc, stores=Stores} =
		peerdrive_client_pb:decode_lookupdocreq(Body),
	?ASSERT_GUID(Doc),
	{Revs, PreRevs} = peerdrive_broker:lookup_doc(Doc, get_stores(Stores)),
	Reply = #lookupdoccnf{
		revs = [ #lookupdoccnf_revmap{rid=RId, stores=RS} || {RId, RS} <- Revs ],
		pre_revs = [ #lookupdoccnf_revmap{rid=RId, stores=RS} || {RId, RS} <- PreRevs ]
	},
	peerdrive_client_pb:encode_lookupdoccnf(Reply).


do_loopup_rev(Body) ->
	#lookuprevreq{rev=Rev, stores=Stores} =
		peerdrive_client_pb:decode_lookuprevreq(Body),
	?ASSERT_GUID(Rev),
	Found = peerdrive_broker:lookup_rev(Rev, get_stores(Stores)),
	Reply = #lookuprevcnf{stores=Found},
	peerdrive_client_pb:encode_lookuprevcnf(Reply).


do_stat(Body) ->
	#statreq{rev=Rev, stores=Stores} =
		peerdrive_client_pb:decode_statreq(Body),
	?ASSERT_GUID(Rev),
	{ok, Stat} = check(peerdrive_broker:stat(Rev, get_stores(Stores))),
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
	peerdrive_client_pb:encode_statcnf(Reply).


do_peek(Cookie, Body) ->
	#peekreq{store=Store, rev=Rev} =
		peerdrive_client_pb:decode_peekreq(Body),
	?ASSERT_GUID(Rev),
	{ok, Handle} = check(peerdrive_broker:peek(get_store(Store), Rev)),
	Reply = #peekcnf{handle=Cookie},
	{Handle, peerdrive_client_pb:encode_peekcnf(Reply)}.


do_create(Cookie, Body) ->
	#createreq{
		store = Store,
		type_code = Type,
		creator_code = Creator
	} = peerdrive_client_pb:decode_createreq(Body),
	{ok, Doc, Handle} = check(peerdrive_broker:create(get_store(Store),
		unicode:characters_to_binary(Type), unicode:characters_to_binary(Creator))),
	Reply = #createcnf{handle=Cookie, doc=Doc},
	{Handle, peerdrive_client_pb:encode_createcnf(Reply)}.


do_fork(Cookie, Body) ->
	#forkreq{
		store = Store,
		rev = Rev,
		creator_code = Creator
	} = peerdrive_client_pb:decode_forkreq(Body),
	?ASSERT_GUID(Rev),
	{ok, Doc, Handle} = check(peerdrive_broker:fork(get_store(Store), Rev,
		unicode:characters_to_binary(Creator))),
	Reply = #forkcnf{handle=Cookie, doc=Doc},
	{Handle, peerdrive_client_pb:encode_forkcnf(Reply)}.


do_update(Cookie, Body) ->
	#updatereq{
		store = Store,
		doc = Doc,
		rev = Rev,
		creator_code = CreatorStr
	} = peerdrive_client_pb:decode_updatereq(Body),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(Rev),
	Creator = if
		CreatorStr == undefined -> undefined;
		true -> unicode:characters_to_binary(CreatorStr)
	end,
	{ok, Handle} = check(peerdrive_broker:update(get_store(Store), Doc, Rev, Creator)),
	Reply = #updatecnf{handle=Cookie},
	{Handle, peerdrive_client_pb:encode_updatecnf(Reply)}.


do_resume(Cookie, Body) ->
	#resumereq{
		store = Store,
		doc = Doc,
		rev = Rev,
		creator_code = CreatorStr
	} = peerdrive_client_pb:decode_resumereq(Body),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(Rev),
	Creator = if
		CreatorStr == undefined -> undefined;
		true -> unicode:characters_to_binary(CreatorStr)
	end,
	{ok, Handle} = check(peerdrive_broker:resume(get_store(Store), Doc, Rev, Creator)),
	Reply = #resumecnf{handle=Cookie},
	{Handle, peerdrive_client_pb:encode_resumecnf(Reply)}.


do_forget(Body) ->
	#forgetreq{
		store = Store,
		doc = Doc,
		rev = Rev
	} = peerdrive_client_pb:decode_forgetreq(Body),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(Rev),
	ok = check(peerdrive_broker:forget(get_store(Store), Doc, Rev)),
	<<>>.


do_delete_doc(Body) ->
	#deletedocreq{
		store = Store,
		doc = Doc,
		rev = Rev
	} = peerdrive_client_pb:decode_deletedocreq(Body),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(Rev),
	ok = check(peerdrive_broker:delete_doc(get_store(Store), Doc, Rev)),
	<<>>.


do_delete_rev(Body) ->
	#deleterevreq{
		store = Store,
		rev = Rev
	} = peerdrive_client_pb:decode_deleterevreq(Body),
	?ASSERT_GUID(Rev),
	ok = check(peerdrive_broker:delete_rev(get_store(Store), Rev)),
	<<>>.


do_forward(Body) ->
	#forwarddocreq{
		store = Store,
		doc = Doc,
		from_rev = FromRev,
		to_rev = ToRev,
		src_store = SrcStore,
		depth = Depth,
		verbose = Verbose
	} = peerdrive_client_pb:decode_forwarddocreq(Body),
	?ASSERT_GUID(Doc),
	?ASSERT_GUID(FromRev),
	?ASSERT_GUID(ToRev),
	Opt1 = case Depth of undefined -> []; _ -> [{depth, Depth}] end,
	Opt2 = case Verbose of false -> Opt1; true -> [verbose | Opt1] end,
	ok = check(peerdrive_broker:forward_doc(get_store(Store), Doc, FromRev,
		ToRev, get_store(SrcStore), Opt2)),
	<<>>.


do_replicate_doc(Body) ->
	#replicatedocreq{
		src_store = SrcStore,
		doc = Doc,
		dst_store = DstStore,
		depth = Depth,
		verbose = Verbose
	} = peerdrive_client_pb:decode_replicatedocreq(Body),
	?ASSERT_GUID(Doc),
	Opt1 = case Depth of undefined -> []; _ -> [{depth, Depth}] end,
	Opt2 = case Verbose of false -> Opt1; true -> [verbose | Opt1] end,
	ok = check(peerdrive_broker:replicate_doc(get_store(SrcStore), Doc,
		get_store(DstStore), [pauseonerror | Opt2])),
	<<>>.


do_replicate_rev(Body) ->
	#replicaterevreq{
		src_store = SrcStore,
		rev = Rev,
		dst_store = DstStore,
		depth = Depth,
		verbose = Verbose
	} = peerdrive_client_pb:decode_replicaterevreq(Body),
	?ASSERT_GUID(Rev),
	Opt1 = case Depth of undefined -> []; _ -> [{depth, Depth}] end,
	Opt2 = case Verbose of false -> Opt1; true -> [verbose | Opt1] end,
	ok = check(peerdrive_broker:replicate_rev(get_store(SrcStore), Rev,
		get_store(DstStore), [pauseonerror | Opt2])),
	<<>>.


do_mount(Body) ->
	#mountreq{id=IdStr} = peerdrive_client_pb:decode_mountreq(Body),
	{Id, _Descr, _Guid, Tags} = get_store_by_id(IdStr),
	case proplists:is_defined(removable, Tags) of
		true  -> {ok, _} = check(peerdrive_volman:mount(Id));
		false -> throw({error, einval})
	end,
	<<>>.


do_unmount(Body) ->
	#unmountreq{id=IdStr} = peerdrive_client_pb:decode_unmountreq(Body),
	{Id, _Descr, _Guid, Tags} = get_store_by_id(IdStr),
	case proplists:is_defined(removable, Tags) of
		true  -> ok = check(peerdrive_volman:unmount(Id));
		false -> throw({error, einval})
	end,
	<<>>.


do_watch_add_req(RetPath, Body) ->
	#watchaddreq{type=Type, element=Obj} =
		peerdrive_client_pb:decode_watchaddreq(Body),
	try
		?ASSERT_GUID(Obj),
		ok = check(peerdrive_change_monitor:watch(Type, Obj)),
		send_reply(RetPath, <<>>)
	catch
		throw:Error -> send_error(RetPath, Error)
	end.


do_watch_rem_req(RetPath, Body) ->
	#watchremreq{type=Type, element=Obj} =
		peerdrive_client_pb:decode_watchremreq(Body),
	try
		?ASSERT_GUID(Obj),
		ok = check(peerdrive_change_monitor:unwatch(Type, Obj)),
		send_reply(RetPath, <<>>)
	catch
		throw:Error -> send_error(RetPath, Error)
	end.


do_watch_progress_req(Body, RetPath, #state{progreg=ProgReg} = S) ->
	#watchprogressreq{enable=Enable} =
		peerdrive_client_pb:decode_watchprogressreq(Body),
	S2 = case Enable of
		ProgReg ->
			S;
		true ->
			peerdrive_work_monitor:register_proc(),
			S#state{progreg=true};
		false ->
			peerdrive_work_monitor:deregister_proc(),
			S#state{progreg=false}
	end,
	send_reply(RetPath, <<>>),
	S2.


do_sys_info_req(Body) ->
	#sysinforeq{param=Param} = peerdrive_client_pb:decode_sysinforeq(Body),
	{ok, Result} = check(peerdrive_sys_info:lookup(
		unicode:characters_to_binary(Param))),
	Reply = case Result of
		Str when is_binary(Str)  -> #sysinfocnf{as_string=Str};
		Int when is_integer(Int) -> #sysinfocnf{as_int=Int}
	end,
	peerdrive_client_pb:encode_sysinfocnf(Reply).


do_progress_start(Body, RetPath) ->
	#progressstartreq{tag=Tag, skip=Skip} =
		peerdrive_client_pb:decode_progressstartreq(Body),
	peerdrive_work_roster:resume(Tag, Skip),
	send_reply(RetPath, <<>>).


do_progress_end(Body, RetPath) ->
	#progressendreq{tag=Tag, pause=Pause} =
		peerdrive_client_pb:decode_progressendreq(Body),
	case Pause of
		true  -> peerdrive_work_roster:pause(Tag);
		false -> peerdrive_work_roster:stop(Tag)
	end,
	send_reply(RetPath, <<>>).


do_progress_query(RetPath) ->
	Items = [
		#progressquerycnf_item{
			item  = encode_progress_start(Tag, Info),
			state = #progressind{
				tag      = Tag,
				progress = Progress,
				state    = State,
				err_code = proplists:get_value(code, ErrInfo, undefined),
				err_doc  = proplists:get_value(doc, ErrInfo, undefined),
				err_rev  = proplists:get_value(rev, ErrInfo, undefined)
			}
		} || {Tag, Info, State, Progress, ErrInfo} <- peerdrive_work_roster:all() ],
	Reply = peerdrive_client_pb:encode_progressquerycnf(
		#progressquerycnf{items=Items}),
	send_reply(RetPath, Reply).



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
			peerdrive_broker:close(Handle)
	end.


io_loop_process(Handle, Request, ReqData) ->
	case Request of
		?READ_MSG ->
			#readreq{part=Part, offset=Offset, length=Length} =
				peerdrive_client_pb:decode_readreq(ReqData),
			?ASSERT_PART(Part),
			{ok, Data} = check(peerdrive_broker:read(Handle, Part, Offset, Length)),
			peerdrive_client_pb:encode_readcnf(#readcnf{data=Data});

		?WRITE_MSG ->
			#writereq{part=Part, offset=Offset, data=Data} =
				peerdrive_client_pb:decode_writereq(ReqData),
			?ASSERT_PART(Part),
			ok = check(peerdrive_broker:write(Handle, Part, Offset, Data)),
			<<>>;

		?TRUNC_MSG ->
			#truncreq{part=Part, offset=Offset} =
				peerdrive_client_pb:decode_truncreq(ReqData),
			?ASSERT_PART(Part),
			ok = check(peerdrive_broker:truncate(Handle, Part, Offset)),
			<<>>;

		?CLOSE_MSG ->
			ok = peerdrive_broker:close(Handle),
			{stop, <<>>};

		?COMMIT_MSG ->
			{ok, Rev} = check(peerdrive_broker:commit(Handle)),
			peerdrive_client_pb:encode_commitcnf(#commitcnf{rev=Rev});

		?SUSPEND_MSG ->
			{ok, Rev} = check(peerdrive_broker:suspend(Handle)),
			peerdrive_client_pb:encode_suspendcnf(#suspendcnf{rev=Rev});

		?MERGE_MSG ->
			#mergereq{store=Store, rev=Rev, depth=Depth, verbose=Verbose} =
				peerdrive_client_pb:decode_mergereq(ReqData),
			?ASSERT_GUID(Rev),
			Opt1 = case Depth of undefined -> []; _ -> [{depth, Depth}] end,
			Opt2 = case Verbose of false -> Opt1; true -> [verbose | Opt1] end,
			ok = check(peerdrive_broker:merge(Handle, get_store(Store), Rev,
				Opt2)),
			<<>>;

		?REBASE_MSG ->
			#rebasereq{rev=Rev} = peerdrive_client_pb:decode_rebasereq(ReqData),
			?ASSERT_GUID(Rev),
			ok = check(peerdrive_broker:rebase(Handle, Rev)),
			<<>>;

		?GET_PARENTS_MSG ->
			{ok, Parents} = check(peerdrive_broker:get_parents(Handle)),
			peerdrive_client_pb:encode_getparentscnf(
				#getparentscnf{parents=Parents});

		?SET_TYPE_MSG ->
			#settypereq{type_code=Type} =
				peerdrive_client_pb:decode_settypereq(ReqData),
			ok = check(peerdrive_broker:set_type(Handle,
				unicode:characters_to_binary(Type))),
			<<>>;

		?GET_TYPE_MSG ->
			{ok, Type} = check(peerdrive_broker:get_type(Handle)),
			peerdrive_client_pb:encode_gettypecnf(#gettypecnf{type_code=Type});

		?SET_FLAGS_MSG ->
			#setflagsreq{flags=Flags} =
				peerdrive_client_pb:decode_setflagsreq(ReqData),
			ok = check(peerdrive_broker:set_flags(Handle, Flags)),
			<<>>;

		?GET_FLAGS_MSG ->
			{ok, Flags} = check(peerdrive_broker:get_flags(Handle)),
			peerdrive_client_pb:encode_getflagscnf(#getflagscnf{flags=Flags})
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_store(SId) ->
	?ASSERT_GUID(SId),
	case peerdrive_volman:store(SId) of
		{ok, Store} -> Store;
		error       -> throw({error, enxio})
	end.


get_stores(StoreList) ->
	case StoreList of
		[] ->
			[Store || {_SId, Store} <- peerdrive_volman:stores()];

		_ ->
			lists:foldr(
				fun(SId, Acc) ->
					?ASSERT_GUID(SId),
					case peerdrive_volman:store(SId) of
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
		case lists:keysearch(Id, 1, peerdrive_volman:enum()) of
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
	Data = peerdrive_client_pb:encode_errorcnf(#errorcnf{error=Error}),
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


encode_progress_start(Tag, Info) ->
	case Info of
		{sync, Source, Dest} ->
			#progressstartind{tag=Tag, type=sync, source=Source,
				dest=Dest};
		{Type, Source, Item, Dest} when Type == rep_doc;
										Type == rep_rev ->
			#progressstartind{tag=Tag, type=Type, source=Source,
				item=Item, dest=Dest}
	end.

