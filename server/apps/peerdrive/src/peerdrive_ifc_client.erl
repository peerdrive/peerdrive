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
-export([init_listen/2, terminate_listen/1]).

-include("store.hrl").
-include("peerdrive_client_pb.hrl").
-include("utils.hrl").
-include("volman.hrl").

-record(listenstate, {pubfile, cookie}).
-record(state, {handles, next, progreg=false, cookie, auth}).
-record(retpath, {servlet, req, ref}).

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
-define(WRITE_BUFFER_MSG,    16#00d).
-define(WRITE_COMMIT_MSG,    16#00e).
-define(FSTAT,               16#00f).
-define(SET_FLAGS_MSG,       16#010).
-define(SET_TYPE_MSG,        16#011).
-define(SET_MTIME_MSG,       16#012).
% unused 16#013
-define(MERGE_MSG,           16#014).
-define(REBASE_MSG,          16#015).
-define(COMMIT_MSG,          16#016).
-define(SUSPEND_MSG,         16#017).
-define(CLOSE_MSG,           16#018).
-define(WATCH_ADD_MSG,       16#019).
-define(WATCH_REM_MSG,       16#01a).
-define(WATCH_PROGRESS_MSG,  16#01b).
-define(FORGET_MSG,          16#01c).
-define(DELETE_DOC_MSG,      16#01d).
-define(DELETE_REV_MSG,      16#01e).
-define(FORWARD_DOC_MSG,     16#01f).
-define(REPLICATE_DOC_MSG,   16#020).
-define(REPLICATE_REV_MSG,   16#021).
-define(MOUNT_MSG,           16#022).
-define(UNMOUNT_MSG,         16#023).
-define(GET_PATH_MSG,        16#024).
-define(WATCH_MSG,           16#025).
-define(PROGRESS_START_MSG,  16#026).
-define(PROGRESS_MSG,        16#027).
-define(PROGRESS_END_MSG,    16#028).
-define(PROGRESS_QUERY_MSG,  16#029).
-define(WALK_PATH_MSG,       16#02a).
-define(GET_DATA_MSG,        16#02b).
-define(SET_DATA_MSG,        16#02c).
-define(GET_LINKS_MSG,       16#02d).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Servlet callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_listen(Socket, Options) ->
	case proplists:get_bool(nopublish, Options) of
		false ->
			Cookie = crypto:rand_bytes(16),
			PubFile = filename:join(peerdrive_util:cfg_run_dir(), "server.info"),
			case publish(Socket, Cookie, PubFile) of
				ok ->
					{ok, #listenstate{pubfile=PubFile, cookie=Cookie}};
				{error, _Reason} = Error ->
					Error
			end;

		true ->
			{ok, #listenstate{cookie = <<>>}}
	end.


terminate_listen(#listenstate{pubfile=PubFile}) ->
	PubFile == undefined orelse file:delete(PubFile).


init(_Options, #listenstate{cookie=Cookie}) ->
	process_flag(trap_exit, true),
	#state{handles=dict:new(), next=0, cookie=Cookie, auth=false}.


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


handle_info({send, Data}, S) ->
	{reply, Data, S};

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
			send_indication(?PROGRESS_MSG,
				peerdrive_client_pb:encode_progressind(Ind), S);

		started ->
			{_Proc, StartInfo} = Info,
			Ind = encode_progress_start(Tag, StartInfo),
			send_indication(?PROGRESS_START_MSG,
				peerdrive_client_pb:encode_progressstartind(Ind), S);

		done ->
			Ind = #progressendind{tag=Tag},
			send_indication(?PROGRESS_END_MSG,
				peerdrive_client_pb:encode_progressendind(Ind), S)
	end;

handle_info({done, Cookie}, S) ->
	{ok, S#state{handles=dict:erase(Cookie, S#state.handles)}};

handle_info({watch, Cause, Type, Store, Uuid}, S) ->
	Ind = #watchind{event=Cause, type=Type, store=Store, element=Uuid},
	send_indication(?WATCH_MSG, peerdrive_client_pb:encode_watchind(Ind), S);

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


handle_packet(<<Ref:32, Request:12, ?FLAG_REQ:4, Body/binary>>, #state{auth=true}=S) ->
	RetPath = #retpath{servlet=self(), req=Request, ref=Ref},
	%io:format("[~w] Ref:~w Request:~w Body:~w~n", [self(), Ref, Request, Body]),
	case Request of
		?ENUM_MSG ->
			{reply, do_enum(RetPath), S};

		?LOOKUP_DOC_MSG ->
			fork(Body, RetPath, fun do_loopup_doc/1),
			{ok, S};

		?LOOKUP_REV_MSG ->
			fork(Body, RetPath, fun do_loopup_rev/1),
			{ok, S};

		?STAT_MSG ->
			fork(Body, RetPath, fun do_stat/1),
			{ok, S};

		?GET_LINKS_MSG ->
			fork(Body, RetPath, fun do_get_links/1),
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
			{reply, do_watch_add_req(RetPath, Body), S};

		?WATCH_REM_MSG ->
			{reply, do_watch_rem_req(RetPath, Body), S};

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
			start_worker(S, fun do_replicate_doc/2, RetPath, Body);

		?REPLICATE_REV_MSG ->
			start_worker(S, fun do_replicate_rev/2, RetPath, Body);

		?MOUNT_MSG ->
			fork(Body, RetPath, fun do_mount/1),
			{ok, S};

		?UNMOUNT_MSG ->
			fork(Body, RetPath, fun do_unmount/1),
			{ok, S};

		?WATCH_PROGRESS_MSG ->
			do_watch_progress_req(Body, RetPath, S);

		?GET_PATH_MSG ->
			fork(Body, RetPath, fun do_get_path_req/1),
			{ok, S};

		?PROGRESS_START_MSG ->
			{reply, do_progress_start(Body, RetPath), S};

		?PROGRESS_END_MSG ->
			{reply, do_progress_end(Body, RetPath), S};

		?PROGRESS_QUERY_MSG ->
			{reply, do_progress_query(RetPath), S};

		?WALK_PATH_MSG ->
			fork(Body, RetPath, fun do_walk_path/1),
			{ok, S};

		?GET_DATA_MSG ->
			ReqData = #getdatareq{handle=Handle} =
				peerdrive_client_pb:decode_getdatareq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?SET_DATA_MSG ->
			ReqData = #setdatareq{handle=Handle} =
				peerdrive_client_pb:decode_setdatareq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?READ_MSG ->
			ReqData = #readreq{handle=Handle} =
				peerdrive_client_pb:decode_readreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?WRITE_BUFFER_MSG ->
			ReqData = #writebufferreq{handle=Handle} =
				peerdrive_client_pb:decode_writebufferreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?WRITE_COMMIT_MSG ->
			ReqData = #writecommitreq{handle=Handle} =
				peerdrive_client_pb:decode_writecommitreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?TRUNC_MSG ->
			ReqData = #truncreq{handle=Handle} =
				peerdrive_client_pb:decode_truncreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?CLOSE_MSG ->
			ReqData = #closereq{handle=Handle} =
				peerdrive_client_pb:decode_closereq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?COMMIT_MSG ->
			ReqData = #commitreq{handle=Handle} =
				peerdrive_client_pb:decode_commitreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?SUSPEND_MSG ->
			ReqData = #suspendreq{handle=Handle} =
				peerdrive_client_pb:decode_suspendreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?MERGE_MSG ->
			ReqData = #mergereq{handle=Handle} =
				peerdrive_client_pb:decode_mergereq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?REBASE_MSG ->
			ReqData = #rebasereq{handle=Handle} =
				peerdrive_client_pb:decode_rebasereq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?FSTAT ->
			ReqData = #fstatreq{handle=Handle} =
				peerdrive_client_pb:decode_fstatreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?SET_TYPE_MSG ->
			ReqData = #settypereq{handle=Handle} =
				peerdrive_client_pb:decode_settypereq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?SET_FLAGS_MSG ->
			ReqData = #setflagsreq{handle=Handle} =
				peerdrive_client_pb:decode_setflagsreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S);

		?SET_MTIME_MSG ->
			ReqData = #setmtimereq{handle=Handle} =
				peerdrive_client_pb:decode_getflagsreq(Body),
			handle_packet_forward(Request, Handle, ReqData, RetPath, S)
	end;

handle_packet(<<Ref:32, ?INIT_MSG:12, ?FLAG_REQ:4, Body/binary>>, S) ->
	RetPath = #retpath{servlet=self(), req=?INIT_MSG, ref=Ref},
	case do_init(Body, S) of
		{ok, Cnf} ->
			Reply = send_reply(RetPath, peerdrive_client_pb:encode_initcnf(Cnf)),
			{reply, Reply, S#state{auth=true}};
		{error, _Reason} = Error ->
			Reply = send_error(RetPath, Error),
			{stop, Reply, S}
	end.


handle_packet_forward(Request, Handle, ReqData, RetPath, S) ->
	Worker = dict:fetch(Handle, S#state.handles),
	Worker ! {Request, ReqData, RetPath},
	{ok, S}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Request handling functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_init(Body, #state{cookie=Cookie}) ->
	#initreq{
		major  = Major,
		cookie = ClientCookie
	} = peerdrive_client_pb:decode_initreq(Body),
	case Major of
		2 ->
			case ClientCookie of
				Cookie ->
					{ok, #initcnf{major = 2, minor = 0, max_packet_size = 16#1000}};
				_ ->
					{error, eperm}
			end;
		_ ->
			{error, erpcmismatch}
	end.


do_enum(RetPath) ->
	Stores = [ do_enum_convert(Store) || Store <- peerdrive_volman:enum() ],
	SysStore = do_enum_convert(peerdrive_volman:sys_store()),
	Reply = #enumcnf{sys_store=SysStore, stores=Stores},
	send_reply(RetPath, peerdrive_client_pb:encode_enumcnf(Reply)).


do_enum_convert(Store) ->
	#peerdrive_store{sid=SId, src=Src, type=Type, label=Label, options=Opts} = Store,
	OptsString = string:join(
		[ case Opt of
			{Key, true} ->
				unicode:characters_to_list(Key);
			{Key, Value} ->
				unicode:characters_to_list(Key) ++ "=" ++
					unicode:characters_to_list(Value)
		  end || Opt <- Opts ],
		","),
	#enumcnf_store{sid=SId, src=Src, type=Type, label=Label, options=OptsString}.


do_loopup_doc(Body) ->
	#lookupdocreq{doc=Doc, stores=Stores} =
		peerdrive_client_pb:decode_lookupdocreq(Body),
	{Revs, PreRevs} = peerdrive_broker:lookup_doc(Doc, get_stores(Stores)),
	Reply = #lookupdoccnf{
		revs = [ #lookupdoccnf_revmap{rid=RId, stores=RS} || {RId, RS} <- Revs ],
		pre_revs = [ #lookupdoccnf_revmap{rid=RId, stores=RS} || {RId, RS} <- PreRevs ]
	},
	peerdrive_client_pb:encode_lookupdoccnf(Reply).


do_loopup_rev(Body) ->
	#lookuprevreq{rev=Rev, stores=Stores} =
		peerdrive_client_pb:decode_lookuprevreq(Body),
	Found = peerdrive_broker:lookup_rev(Rev, get_stores(Stores)),
	Reply = #lookuprevcnf{stores=Found},
	peerdrive_client_pb:encode_lookuprevcnf(Reply).


do_stat(Body) ->
	#statreq{rev=Rev, stores=Stores} =
		peerdrive_client_pb:decode_statreq(Body),
	{ok, Stat} = check(peerdrive_broker:stat(Rev, get_stores(Stores))),
	peerdrive_client_pb:encode_statcnf(rev_to_statcnf(Stat)).


do_get_links(Body) ->
	#getlinksreq{rev=Rev, stores=Stores} =
		peerdrive_client_pb:decode_getlinksreq(Body),
	{ok, {DocLinks, RevLinks}} = check(peerdrive_broker:get_links(Rev,
		get_stores(Stores))),
	Reply = #getlinkscnf{doc_links=DocLinks, rev_links=RevLinks},
	peerdrive_client_pb:encode_getlinkscnf(Reply).


do_peek(Cookie, Body) ->
	#peekreq{store=Store, rev=Rev} =
		peerdrive_client_pb:decode_peekreq(Body),
	{ok, Handle} = check(peerdrive_broker:peek(get_store(Store), Rev)),
	Reply = #peekcnf{handle=Cookie},
	{Handle, [], peerdrive_client_pb:encode_peekcnf(Reply)}.


do_create(Cookie, Body) ->
	#createreq{
		store = Store,
		type_code = Type,
		creator_code = Creator
	} = peerdrive_client_pb:decode_createreq(Body),
	{ok, Doc, Handle} = check(peerdrive_broker:create(get_store(Store),
		unicode:characters_to_binary(Type), unicode:characters_to_binary(Creator))),
	Reply = #createcnf{handle=Cookie, doc=Doc},
	{Handle, [], peerdrive_client_pb:encode_createcnf(Reply)}.


do_fork(Cookie, Body) ->
	#forkreq{
		store = Store,
		rev = Rev,
		creator_code = Creator
	} = peerdrive_client_pb:decode_forkreq(Body),
	{ok, Doc, Handle} = check(peerdrive_broker:fork(get_store(Store), Rev,
		unicode:characters_to_binary(Creator))),
	Reply = #forkcnf{handle=Cookie, doc=Doc},
	{Handle, [], peerdrive_client_pb:encode_forkcnf(Reply)}.


do_update(Cookie, Body) ->
	#updatereq{
		store = Store,
		doc = Doc,
		rev = Rev,
		creator_code = CreatorStr
	} = peerdrive_client_pb:decode_updatereq(Body),
	Creator = if
		CreatorStr == undefined -> undefined;
		true -> unicode:characters_to_binary(CreatorStr)
	end,
	{ok, Handle} = check(peerdrive_broker:update(get_store(Store), Doc, Rev, Creator)),
	Reply = #updatecnf{handle=Cookie},
	{Handle, [], peerdrive_client_pb:encode_updatecnf(Reply)}.


do_resume(Cookie, Body) ->
	#resumereq{
		store = Store,
		doc = Doc,
		rev = Rev,
		creator_code = CreatorStr
	} = peerdrive_client_pb:decode_resumereq(Body),
	Creator = if
		CreatorStr == undefined -> undefined;
		true -> unicode:characters_to_binary(CreatorStr)
	end,
	{ok, Handle} = check(peerdrive_broker:resume(get_store(Store), Doc, Rev, Creator)),
	Reply = #resumecnf{handle=Cookie},
	{Handle, [], peerdrive_client_pb:encode_resumecnf(Reply)}.


do_forget(Body) ->
	#forgetreq{
		store = Store,
		doc = Doc,
		rev = Rev
	} = peerdrive_client_pb:decode_forgetreq(Body),
	ok = check(peerdrive_broker:forget(get_store(Store), Doc, Rev)),
	<<>>.


do_delete_doc(Body) ->
	#deletedocreq{
		store = Store,
		doc = Doc,
		rev = Rev
	} = peerdrive_client_pb:decode_deletedocreq(Body),
	ok = check(peerdrive_broker:delete_doc(get_store(Store), Doc, Rev)),
	<<>>.


do_delete_rev(Body) ->
	#deleterevreq{
		store = Store,
		rev = Rev
	} = peerdrive_client_pb:decode_deleterevreq(Body),
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
	Opt1 = case Depth of undefined -> []; _ -> [{depth, Depth}] end,
	Opt2 = case Verbose of false -> Opt1; true -> [verbose | Opt1] end,
	ok = check(peerdrive_broker:forward_doc(get_store(Store), Doc, FromRev,
		ToRev, get_store(SrcStore), Opt2)),
	<<>>.


do_replicate_doc(Cookie, Body) ->
	#replicatedocreq{
		src_store = SrcStore,
		doc = Doc,
		dst_store = DstStore,
		depth = Depth,
		verbose = Verbose
	} = peerdrive_client_pb:decode_replicatedocreq(Body),
	Opt1 = case Depth of undefined -> []; _ -> [{depth, Depth}] end,
	Opt2 = case Verbose of false -> Opt1; true -> [verbose | Opt1] end,
	{ok, Handle} = check(peerdrive_broker:replicate_doc(get_store(SrcStore),
		Doc, get_store(DstStore), [pauseonerror | Opt2])),
	Reply = #replicatedoccnf{handle=Cookie},
	{Handle, [], peerdrive_client_pb:encode_replicatedoccnf(Reply)}.


do_replicate_rev(Cookie, Body) ->
	#replicaterevreq{
		src_store = SrcStore,
		rev = Rev,
		dst_store = DstStore,
		depth = Depth,
		verbose = Verbose
	} = peerdrive_client_pb:decode_replicaterevreq(Body),
	Opt1 = case Depth of undefined -> []; _ -> [{depth, Depth}] end,
	Opt2 = case Verbose of false -> Opt1; true -> [verbose | Opt1] end,
	{ok, Handle} = check(peerdrive_broker:replicate_rev(get_store(SrcStore),
		Rev, get_store(DstStore), [pauseonerror | Opt2])),
	Reply = #replicaterevcnf{handle=Cookie},
	{Handle, [], peerdrive_client_pb:encode_replicaterevcnf(Reply)}.


do_mount(Body) ->
	#mountreq{src=Src, type=Type, label=Label, options=Options,
		credentials=Creds} = peerdrive_client_pb:decode_mountreq(Body),
	{ok, SId} = check(peerdrive_volman:mount(Src, Options, Creds, Type, Label)),
	peerdrive_client_pb:encode_mountcnf(#mountcnf{sid=SId}).


do_unmount(Body) ->
	#unmountreq{sid=SId} = peerdrive_client_pb:decode_unmountreq(Body),
	ok = check(peerdrive_volman:unmount(SId)),
	<<>>.


do_watch_add_req(RetPath, Body) ->
	#watchaddreq{type=Type, element=Obj} =
		peerdrive_client_pb:decode_watchaddreq(Body),
	try
		ok = check(peerdrive_change_monitor:watch(Type, Obj)),
		send_reply(RetPath, <<>>)
	catch
		throw:Error -> send_error(RetPath, Error)
	end.


do_watch_rem_req(RetPath, Body) ->
	#watchremreq{type=Type, element=Obj} =
		peerdrive_client_pb:decode_watchremreq(Body),
	try
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
	{reply, send_reply(RetPath, <<>>), S2}.


do_get_path_req(Body) ->
	#getpathreq{store=Store, object=DId, is_rev=IsRev} =
		peerdrive_client_pb:decode_getpathreq(Body),
	not IsRev orelse throw({error, enoent}), % only documents so far
	{ok, BasePath} = check(peerdrive_sys_info:lookup(<<"vfs.mountpath">>)),
	{ok, Title} = check(peerdrive_util:read_doc_file_name(get_store(Store), DId)),
	StoreName = case lists:keyfind(Store, #peerdrive_store.sid, peerdrive_volman:enum_all()) of
		#peerdrive_store{label=Found} -> Found;
		false -> throw({error, enoent})
	end,
	Path0 = filename:join([BasePath, StoreName, <<".docs">>,
		peerdrive_util:bin_to_hexstr(DId), Title]),
	Path = filename:nativename(unicode:characters_to_list(Path0)),
	peerdrive_client_pb:encode_getpathcnf(#getpathcnf{path=Path}).


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


do_walk_path(Body) ->
	#walkpathreq{path=Path} = peerdrive_client_pb:decode_walkpathreq(Body),
	Enum = peerdrive_volman:enum_all(),
	Result = case re:split(Path, ":", [{return, list}]) of
		[Store, []] ->
			case lists:keyfind(Store, #peerdrive_store.label, Enum) of
				#peerdrive_store{sid=SId} ->
					[ #walkpathcnf_item{store=SId, doc=SId} ];
				false -> throw({error, enoent})
			end;

		[Store, Remainder] ->
			case lists:keyfind(Store, #peerdrive_store.label, Enum) of
				#peerdrive_store{pid=Pid, sid=SId} ->
					case peerdrive_util:walk(Pid, Remainder) of
						{ok, Doc} -> [ #walkpathcnf_item{store=SId, doc=Doc} ];
						{error, _} = Error -> throw(Error)
					end;
				false -> throw({error, enoent})
			end;

		_ ->
			throw({error, einval})
	end,
	peerdrive_client_pb:encode_walkpathcnf(#walkpathcnf{items=Result}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% IO handler loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_worker(S, Fun, RetPath, Body) ->
	Cookie = S#state.next,
	Server = self(),
	Worker = spawn_link(
		fun() ->
			try
				{Handle, State, Reply} = Fun(Cookie, Body),
				send_reply(RetPath, Reply),
				io_loop(Handle, State),
				Server ! {done, Cookie}
			catch
				throw:Error -> send_error(RetPath, Error)
			end
		end),
	{ok, S#state{
		handles = dict:store(Cookie, Worker, S#state.handles),
		next    = Cookie + 1}}.


io_loop(Handle, State) ->
	receive
		{Req, ReqData, RetPath} ->
			try io_loop_process(Handle, State, Req, ReqData) of
				{stop, Reply} ->
					send_reply(RetPath, Reply);
				{error, Error, NewState} ->
					send_error(RetPath, Error),
					io_loop(Handle, NewState);
				{Reply, NewState} ->
					send_reply(RetPath, Reply),
					io_loop(Handle, NewState);
				Reply ->
					send_reply(RetPath, Reply),
					io_loop(Handle, State)
			catch
				throw:Error ->
					send_error(RetPath, Error),
					io_loop(Handle, State)
			end;

		closed ->
			peerdrive_broker:close(Handle)
	end.


io_loop_process(Handle, WriteBuffer, Request, ReqData) ->
	case Request of
		?GET_DATA_MSG ->
			#getdatareq{selector=Selector} = ReqData,
			{ok, Data} = check(peerdrive_broker:get_data(Handle, Selector)),
			peerdrive_client_pb:encode_getdatacnf(#getdatacnf{data=Data});

		?SET_DATA_MSG ->
			#setdatareq{selector=Selector, data=Data} = ReqData,
			ok = check(peerdrive_broker:set_data(Handle, Selector, Data)),
			<<>>;

		?READ_MSG ->
			#readreq{part=Part, offset=Offset, length=Length} = ReqData,
			{ok, Data} = check(peerdrive_broker:read(Handle, Part, Offset, Length)),
			peerdrive_client_pb:encode_readcnf(#readcnf{data=Data});

		?WRITE_BUFFER_MSG ->
			#writebufferreq{part=Part, data=Data} = ReqData,
			NewWrBuf = orddict:update(Part, fun(Old) -> [Data | Old] end,
				[Data], WriteBuffer),
			{<<>>, NewWrBuf};

		?WRITE_COMMIT_MSG ->
			#writecommitreq{part=Part, offset=Offset, data=Data} = ReqData,
			case orddict:find(Part, WriteBuffer) of
				error ->
					ok = check(peerdrive_broker:write(Handle, Part, Offset, Data)),
					<<>>;

				{ok, BufData} ->
					AllData = iolist_to_binary(lists:reverse([Data | BufData])),
					case peerdrive_broker:write(Handle, Part, Offset, AllData) of
						ok ->
							{<<>>, orddict:erase(Part, WriteBuffer)};
						{error, _} = Error ->
							{error, Error, orddict:erase(Part, WriteBuffer)}
					end
			end;

		?TRUNC_MSG ->
			#truncreq{part=Part, offset=Offset} = ReqData,
			ok = check(peerdrive_broker:truncate(Handle, Part, Offset)),
			<<>>;

		?CLOSE_MSG ->
			ok = peerdrive_broker:close(Handle),
			{stop, <<>>};

		?COMMIT_MSG ->
			#commitreq{comment=CommentStr} = ReqData,
			Comment = if
				CommentStr == undefined -> undefined;
				true -> unicode:characters_to_binary(CommentStr)
			end,
			{ok, Rev} = check(peerdrive_broker:commit(Handle, Comment)),
			peerdrive_client_pb:encode_commitcnf(#commitcnf{rev=Rev});

		?SUSPEND_MSG ->
			#suspendreq{comment=CommentStr} = ReqData,
			Comment = if
				CommentStr == undefined -> undefined;
				true -> unicode:characters_to_binary(CommentStr)
			end,
			{ok, Rev} = check(peerdrive_broker:suspend(Handle, Comment)),
			peerdrive_client_pb:encode_suspendcnf(#suspendcnf{rev=Rev});

		?MERGE_MSG ->
			#mergereq{store=Store, rev=Rev, depth=Depth, verbose=Verbose} = ReqData,
			Opt1 = case Depth of undefined -> []; _ -> [{depth, Depth}] end,
			Opt2 = case Verbose of false -> Opt1; true -> [verbose | Opt1] end,
			ok = check(peerdrive_broker:merge(Handle, get_store(Store), Rev,
				Opt2)),
			<<>>;

		?REBASE_MSG ->
			#rebasereq{rev=Rev} = ReqData,
			ok = check(peerdrive_broker:rebase(Handle, Rev)),
			<<>>;

		?FSTAT ->
			{ok, Rev} = check(peerdrive_broker:fstat(Handle)),
			peerdrive_client_pb:encode_statcnf(rev_to_statcnf(Rev));

		?SET_TYPE_MSG ->
			#settypereq{type_code=Type} = ReqData,
			ok = check(peerdrive_broker:set_type(Handle,
				unicode:characters_to_binary(Type))),
			<<>>;

		?SET_FLAGS_MSG ->
			#setflagsreq{flags=Flags} = ReqData,
			ok = check(peerdrive_broker:set_flags(Handle, Flags)),
			<<>>;

		?SET_MTIME_MSG ->
			#setmtimereq{attachment=Attachment, mtime=MTime} = ReqData,
			ok = check(peerdrive_broker:set_mtime(Handle, Attachment, MTime)),
			<<>>
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_store(SId) ->
	case peerdrive_volman:store(SId) of
		{ok, Store} -> Store;
		error       -> throw({error, enxio})
	end.


get_stores(StoreList) ->
	case StoreList of
		[] ->
			[Store || #peerdrive_store{pid=Store} <- peerdrive_volman:enum_all()];

		_ ->
			lists:foldr(
				fun(SId, Acc) ->
					case peerdrive_volman:store(SId) of
						{ok, Store} -> [Store | Acc];
						error       -> Acc
					end
				end,
				[],
				StoreList)
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


send_cnf(#retpath{ref=Ref, servlet=Servlet}, Cnf, Data) ->
	Raw = [<<Ref:32, Cnf:16>>, Data],
	%io:format("[~w] Confirm: ~w~n", [self(), iolist_to_binary(Raw)]),
	case self() of
		Servlet -> Raw;
		_       -> Servlet ! {send, Raw}, Raw
	end.


send_indication(Ind, Data, S) ->
	Indication = (Ind bsl 4) bor ?FLAG_IND,
	Raw = [<<16#FFFFFFFF:32, Indication:16>>, Data],
	%io:format("[~w] Indication: ~w~n", [self(), iolist_to_binary(Raw)]),
	{reply, Raw, S}.


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


publish(Socket, Cookie, Path) ->
	case do_publish(Socket, Cookie, Path) of
		ok ->
			ok;
		{error, eexist} ->
			%% Probably stale server.info file, but may also be someone else
			%% who tries to eavesdrop the cookie. Try to get rid of the file...
			case file:delete(Path) of
				ok -> do_publish(Socket, Cookie, Path);
				Error -> Error
			end;
		{error, _Reason} = Error ->
			Error
	end.


do_publish(Socket, Cookie, Path) ->
	{ok, {Address, Port}} = inet:sockname(Socket),
	case file:open(Path, [write, exclusive]) of
		{ok, IoDevice} ->
			file:change_mode(Path, 8#00640) == ok andalso
			io:format(IoDevice, "tcp://~s:~w/~s~n", [inet_parse:ntoa(Address),
				Port, peerdrive_util:bin_to_hexstr(Cookie)]) == ok andalso
			file:close(IoDevice);
		{error, _Reason} = Error ->
			Error
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
		data        = #statcnf_data{size=DataSize, hash=DataHash},
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

