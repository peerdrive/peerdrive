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
-import(hotchpotch_netencode, [parse_uuid/1, parse_string/1, parse_uuid_list/1,
	parse_store/1, encode_list/1, encode_list/2, encode_list_32/1,
	encode_list_32/2, encode_string/1, encode_result/1,
	encode_error_code/1, err2int/1]).

-include("store.hrl").

-record(state, {socket, cookies, next, progreg=false}).
-record(retpath, {socket, req, ref}).

-define(FLAG_REQ, 0).
-define(FLAG_CNF, 1).
-define(FLAG_IND, 2).
-define(FLAG_RSP, 3).

-define(INIT_REQ,           16#0000).
-define(ENUM_REQ,           16#0010).
-define(LOOKUP_DOC_REQ,     16#0020).
-define(LOOKUP_REV_REQ,     16#0030).
-define(STAT_REQ,           16#0040).
-define(PEEK_REQ,           16#0050).
-define(CREATE_REQ,         16#0060).
-define(FORK_REQ,           16#0070).
-define(UPDATE_REQ,         16#0080).
-define(RESUME_REQ,         16#0090).
-define(READ_REQ,           16#00A0).
-define(TRUNC_REQ,          16#00B0).
-define(WRITE_REQ,          16#00C0).
-define(GET_FLAGS_REQ,      16#0210).
-define(GET_FLAGS_CNF,      16#0211).
-define(SET_FLAGS_REQ,      16#0220).
-define(SET_FLAGS_CNF,      16#0221).
-define(GET_TYPE_REQ,       16#00D0).
-define(SET_TYPE_REQ,       16#00E0).
-define(GET_PARENTS_REQ,    16#00F0).
-define(MERGE_REQ,          16#0100).
-define(REBASE_REQ,         16#0110).
-define(COMMIT_REQ,         16#0120).
-define(SUSPEND_REQ,        16#0130).
-define(CLOSE_REQ,          16#0140).
-define(WATCH_ADD_REQ,      16#0150).
-define(WATCH_REM_REQ,      16#0160).
-define(WATCH_PROGRESS_REQ, 16#0170).
-define(FORGET_REQ,         16#0180).
-define(DELETE_DOC_REQ,     16#0190).
-define(DELETE_REV_REQ,     16#01A0).
-define(FORWARD_DOC_REQ,    16#01B0).
-define(REPLICATE_DOC_REQ,  16#01C0).
-define(REPLICATE_REV_REQ,  16#01D0).
-define(MOUNT_REQ,          16#01E0).
-define(UNMOUNT_REQ,        16#01F0).
-define(SYS_INFO_REQ,       16#0200).
-define(WATCH_IND,          16#0002).
-define(PROGRESS_START_IND, 16#0012).
-define(PROGRESS_IND,       16#0022).
-define(PROGRESS_END_IND,   16#0032).

-define(WATCH_CAUSE_MOD, 0). % Doc has been modified
-define(WATCH_CAUSE_ADD, 1). % Doc/Rev appeared
-define(WATCH_CAUSE_REP, 2). % Doc/Rev was spread to another store
-define(WATCH_CAUSE_DIM, 3). % Doc/Rev removed from a store
-define(WATCH_CAUSE_REM, 4). % Doc/Rev has disappeared

-define(PROGRESS_TYPE_SYNC,    0).
-define(PROGRESS_TYPE_REP_DOC, 1).
-define(PROGRESS_TYPE_REP_REV, 2).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Servlet callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Socket, _Options) ->
	process_flag(trap_exit, true),
	#state{socket=Socket, cookies=dict:new(), next=0}.


terminate(#state{progreg=ProgReg} = State) ->
	if
		ProgReg -> hotchpotch_work_monitor:deregister_proc(self());
		true    -> ok
	end,
	hotchpotch_change_monitor:remove(),
	dict:fold(
		fun(_Cookie, Worker, _Acc) -> Worker ! closed end,
		ok,
		State#state.cookies).


handle_info({work_event, Event, Tag, Info}, S) ->
	case Event of
		progress ->
			send_indication(S#state.socket, ?PROGRESS_IND, <<Tag:16, Info:8>>);

		started ->
			Data = case Info of
				{sync, FromGuid, ToGuid} ->
					<<?PROGRESS_TYPE_SYNC:8, FromGuid/binary, ToGuid/binary>>;

				{rep_doc, Doc, Store} ->
					<<?PROGRESS_TYPE_REP_DOC:8, Doc/binary, Store/binary>>;

				{rep_rev, Rev, Store} ->
					<<?PROGRESS_TYPE_REP_REV:8, Rev/binary, Store/binary>>
			end,
			send_indication(S#state.socket, ?PROGRESS_START_IND,
				<<Tag:16, Data/binary>>);

		done ->
			send_indication(S#state.socket, ?PROGRESS_END_IND, <<Tag:16>>)
	end,
	{ok, S};

handle_info({done, Cookie}, S) ->
	{ok, S#state{cookies=dict:erase(Cookie, S#state.cookies)}};

handle_info({watch, Cause, Type, Uuid}, S) ->
	EncType = case Type of
		doc -> 0;
		rev  -> 1
	end,
	Data = case Cause of
		modified    -> <<?WATCH_CAUSE_MOD:8, EncType:8, Uuid/binary>>;
		appeared    -> <<?WATCH_CAUSE_ADD:8, EncType:8, Uuid/binary>>;
		replicated  -> <<?WATCH_CAUSE_REP:8, EncType:8, Uuid/binary>>;
		diminished  -> <<?WATCH_CAUSE_DIM:8, EncType:8, Uuid/binary>>;
		disappeared -> <<?WATCH_CAUSE_REM:8, EncType:8, Uuid/binary>>
	end,
	send_indication(S#state.socket, ?WATCH_IND, <<Data/binary>>),
	{ok, S};

handle_info({'EXIT', _From, normal}, S) ->
	{ok, S};

handle_info({'EXIT', From, Reason}, S) ->
	error_logger:error_report(["Client servlet neighbour crashed", {from, From},
		{reason, Reason}]),
	{stop, S}.


handle_packet(Packet, S) ->
	<<Ref:32, Request:16, Body/binary>> = Packet,
	RetPath = #retpath{socket=S#state.socket, req=Request, ref=Ref},
	%io:format("[~w] Ref:~w Request:~w Body:~w~n", [self(), Ref, Request, Body]),
	case Request of
		?INIT_REQ ->
			do_init(Body, RetPath),
			{ok, S};

		?ENUM_REQ ->
			do_enum(RetPath),
			{ok, S};

		?LOOKUP_DOC_REQ ->
			spawn_link(fun () -> do_loopup_doc(Body, RetPath) end),
			{ok, S};

		?LOOKUP_REV_REQ ->
			spawn_link(fun () -> do_loopup_rev(Body, RetPath) end),
			{ok, S};

		?STAT_REQ ->
			spawn_link(fun () -> do_stat(Body, RetPath) end),
			{ok, S};

		?PEEK_REQ ->
			start_worker(S, fun(Cookie) -> do_peek(Cookie, RetPath, Body) end);

		?CREATE_REQ ->
			start_worker(S, fun(Cookie) -> do_create(Cookie, RetPath, Body) end);

		?FORK_REQ ->
			start_worker(S, fun(Cookie) -> do_fork(Cookie, RetPath, Body) end);

		?UPDATE_REQ ->
			start_worker(S, fun(Cookie) -> do_update(Cookie, RetPath, Body) end);

		?RESUME_REQ ->
			start_worker(S, fun(Cookie) -> do_resume(Cookie, RetPath, Body) end);

		?WATCH_ADD_REQ ->
			<<EncType:8, Hash:16/binary>> = Body,
			Type = case EncType of
				0 -> doc;
				1 -> rev
			end,
			hotchpotch_change_monitor:watch(Type, Hash),
			send_reply(RetPath, encode_error_code(ok)),
			{ok, S};

		?WATCH_REM_REQ ->
			<<EncType:8, Hash:16/binary>> = Body,
			Type = case EncType of
				0 -> doc;
				1 -> rev
			end,
			hotchpotch_change_monitor:unwatch(Type, Hash),
			send_reply(RetPath, encode_error_code(ok)),
			{ok, S};

		?FORGET_REQ ->
			spawn_link(fun () -> do_forget(Body, RetPath) end),
			{ok, S};

		?DELETE_DOC_REQ ->
			spawn_link(fun () -> do_delete_doc(Body, RetPath) end),
			{ok, S};

		?DELETE_REV_REQ ->
			spawn_link(fun () -> do_delete_rev(Body, RetPath) end),
			{ok, S};

		?FORWARD_DOC_REQ ->
			spawn_link(fun () -> do_forward(Body, RetPath) end),
			{ok, S};

		?REPLICATE_DOC_REQ ->
			spawn_link(fun () -> do_replicate_doc(Body, RetPath) end),
			{ok, S};

		?REPLICATE_REV_REQ ->
			spawn_link(fun () -> do_replicate_rev(Body, RetPath) end),
			{ok, S};

		?MOUNT_REQ ->
			spawn_link(fun () -> do_mount(Body, RetPath) end),
			{ok, S};

		?UNMOUNT_REQ ->
			spawn_link(fun () -> do_unmount(Body, RetPath) end),
			{ok, S};

		?WATCH_PROGRESS_REQ ->
			S2 = do_watch_progress_req(Body, RetPath, S),
			{ok, S2};

		?SYS_INFO_REQ ->
			do_sys_info_req(Body, RetPath),
			{ok, S};

		_ ->
			<<Cookie:32, Data/binary>> = Body,
			Worker = dict:fetch(Cookie, S#state.cookies),
			Worker ! {Request, Data, RetPath},
			{ok, S}
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Request handling functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_init(Body, RetPath) ->
	<<0:16, Major:8, _Minor:8>> = Body,
	case Major of
		0 -> send_reply(RetPath, <<(err2int(ok)):32, 0:32, 16#1000:32>>);
		_ -> send_reply(RetPath, <<(err2int(erpcmismatch)):32, 0:32, 16#1000:32>>)
	end.


do_enum(RetPath) ->
	Stores = hotchpotch_volman:enum(),
	Reply = encode_list(
		fun({Id, Descr, Store, Properties}) ->
			Flags = lists:foldl(
				fun(Flag, Bitset) ->
					case Flag of
						mounted   -> Bitset bor 1;
						removable -> Bitset bor 2;
						system    -> Bitset bor 4;
						net       -> Bitset bor 8;
						_         -> Bitset
					end
				end,
				0,
				Properties),
			<<Store/binary, Flags:32, (encode_string(Id))/binary,
				(encode_string(Descr))/binary>>
		end,
		Stores),
	send_reply(RetPath, Reply).


do_loopup_doc(Body, RetPath) ->
	<<Doc:16/binary, Body1/binary>> = Body,
	{Stores, <<>>} = parse_uuid_list(Body1),
	{Revs, PreRevs} = hotchpotch_broker:lookup_doc(Doc, get_stores(Stores)),
	RevsBin = do_lookup_encode(Revs),
	PreRevsBin = do_lookup_encode(PreRevs),
	send_reply(RetPath, <<RevsBin/binary, PreRevsBin/binary>>).


do_lookup_encode(Revs) ->
	encode_list(
		fun({Rev, Stores}) ->
			StoresBin = encode_list(Stores),
			<<Rev/binary, StoresBin/binary>>
		end,
		Revs).


do_loopup_rev(Body, RetPath) ->
	<<Rev:16/binary, Body1/binary>> = Body,
	{Stores, <<>>} = parse_uuid_list(Body1),
	Found = hotchpotch_broker:lookup_rev(Rev, get_stores(Stores)),
	send_reply(RetPath, encode_list(Found)).


do_stat(Body, RetPath) ->
	<<Rev:16/binary, Body1/binary>> = Body,
	{Stores, <<>>} = parse_uuid_list(Body1),
	Reply = case hotchpotch_broker:stat(Rev, get_stores(Stores)) of
		{ok, Stat} ->
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
			ReplyParts = encode_list(
				fun ({FourCC, Size, Hash}) ->
					<<FourCC/binary, Size:64, Hash/binary>>
				end,
				Parts),
			<<
				(encode_result(ok))/binary,
				Flags:32,
				ReplyParts/binary,
				(encode_list(Parents))/binary,
				Mtime:64,
				(encode_string(TypeCode))/binary,
				(encode_string(CreatorCode))/binary,
				(encode_list_32(DocLinks))/binary,
				(encode_list_32(RevLinks))/binary
			>>;

		Error ->
			encode_result(Error)
	end,
	send_reply(RetPath, Reply).


do_peek(Cookie, RetPath, Body) ->
	try
		{Store, Body1} = get_store(Body),
		{Rev, <<>>} = parse_uuid(Body1),
		{ok, Handle} = check(hotchpotch_broker:peek(Store, Rev)),
		Reply = <<(encode_result(ok))/binary, Cookie:32>>,
		send_reply(RetPath, Reply),
		io_loop(Handle)
	catch
		throw:Error ->
			send_reply(RetPath, encode_result(Error))
	end.


do_create(Cookie, RetPath, Body) ->
	try
		{Store, Body1} = get_store(Body),
		{Type, Body2} = parse_string(Body1),
		{Creator, <<>>} = parse_string(Body2),
		{ok, Doc, Handle} = check(hotchpotch_broker:create(Store, Type, Creator)),
		Reply = <<(encode_result(ok))/binary, Cookie:32, Doc/binary>>,
		send_reply(RetPath, Reply),
		io_loop(Handle)
	catch
		throw:Error ->
			send_reply(RetPath, encode_result(Error))
	end.


do_fork(Cookie, RetPath, Body) ->
	try
		{Store, Body1} = get_store(Body),
		{Rev, Body2} = parse_uuid(Body1),
		{Creator, <<>>} = parse_string(Body2),
		{ok, Doc, Handle} = check(hotchpotch_broker:fork(Store, Rev, Creator)),
		Reply = <<(encode_result(ok))/binary, Cookie:32, Doc/binary>>,
		send_reply(RetPath, Reply),
		io_loop(Handle)
	catch
		throw:Error ->
			send_reply(RetPath, encode_result(Error))
	end.


do_update(Cookie, RetPath, Body) ->
	try
		{Store, Body1} = get_store(Body),
		{Doc, Body2} = parse_uuid(Body1),
		{Rev, Body3} = parse_uuid(Body2),
		{Creator, <<>>} = parse_string(Body3),
		RealCreator = case Creator of
			<<>> -> keep;
			_    -> Creator
		end,
		{ok, Handle} = check(hotchpotch_broker:update(Store, Doc, Rev, RealCreator)),
		Reply = <<(encode_result(ok))/binary, Cookie:32>>,
		send_reply(RetPath, Reply),
		io_loop(Handle)
	catch
		throw:Error ->
			send_reply(RetPath, encode_result(Error))
	end.


do_resume(Cookie, RetPath, Body) ->
	try
		{Store, Body1} = get_store(Body),
		{Doc, Body2} = parse_uuid(Body1),
		{Rev, Body3} = parse_uuid(Body2),
		{Creator, <<>>} = parse_string(Body3),
		RealCreator = case Creator of
			<<>> -> keep;
			_    -> Creator
		end,
		{ok, Handle} = check(hotchpotch_broker:resume(Store, Doc, Rev, RealCreator)),
		Reply = <<(encode_result(ok))/binary, Cookie:32>>,
		send_reply(RetPath, Reply),
		io_loop(Handle)
	catch
		throw:Error ->
			send_reply(RetPath, encode_result(Error))
	end.


do_forget(Body, RetPath) ->
	Reply = try
		{Store, Body1} = get_store(Body),
		{Doc, Body2} = parse_uuid(Body1),
		{Rev, <<>>} = parse_uuid(Body2),
		hotchpotch_broker:forget(Store, Doc, Rev)
	catch
		throw:Error -> Error
	end,
	send_reply(RetPath, encode_result(Reply)).


do_delete_doc(Body, RetPath) ->
	Reply = try
		{Store, Body1} = get_store(Body),
		{Doc, Body2} = parse_uuid(Body1),
		{Rev, <<>>} = parse_uuid(Body2),
		hotchpotch_broker:delete_doc(Store, Doc, Rev)
	catch
		throw:Error -> Error
	end,
	send_reply(RetPath, encode_result(Reply)).


do_delete_rev(Body, RetPath) ->
	Reply = try
		{Store, Body1} = get_store(Body),
		{Rev, <<>>} = parse_uuid(Body1),
		hotchpotch_broker:delete_rev(Store, Rev)
	catch
		throw:Error -> Error
	end,
	send_reply(RetPath, encode_result(Reply)).


do_forward(Body, RetPath) ->
	Reply = try
		{Store, Body1} = get_store(Body),
		<<Doc:16/binary, FromRev:16/binary, ToRev:16/binary, Body2/binary>> = Body1,
		{SrcStore, <<Depth:64>>} = get_store(Body2),
		hotchpotch_broker:forward_doc(Store, Doc, FromRev, ToRev, SrcStore, Depth)
	catch
		throw:Error -> Error
	end,
	send_reply(RetPath, encode_result(Reply)).


do_replicate_doc(Body, RetPath) ->
	Reply = try
		{SrcStore, Body1} = get_store(Body),
		<<Doc:16/binary, Body2/binary>> = Body1,
		{DstStore, <<Depth:64>>} = get_store(Body2),
		hotchpotch_broker:replicate_doc(SrcStore, Doc, DstStore, Depth)
	catch
		throw:Error -> Error
	end,
	send_reply(RetPath, encode_result(Reply)).


do_replicate_rev(Body, RetPath) ->
	Reply = try
		{SrcStore, Body1} = get_store(Body),
		<<Rev:16/binary, Body2/binary>> = Body1,
		{DstStore, <<Depth:64>>} = get_store(Body2),
		hotchpotch_broker:replicate_rev(SrcStore, Rev, DstStore, Depth)
	catch
		throw:Error -> Error
	end,
	send_reply(RetPath, encode_result(Reply)).


do_mount(Body, RetPath) ->
	Reply = case parse_store(Body) of
		{ok, {Id, _Descr, _Guid, Tags}} ->
			case proplists:is_defined(removable, Tags) of
				true  -> hotchpotch_volman:mount(Id);
				false -> {error, einval}
			end;

		{error, _Reason} = Error ->
			Error
	end,
	send_reply(RetPath, encode_result(Reply)).


do_unmount(Body, RetPath) ->
	Reply = case parse_store(Body) of
		{ok, {Id, _Descr, _Guid, Tags}} ->
			case proplists:is_defined(removable, Tags) of
				true  -> hotchpotch_volman:unmount(Id);
				false -> {error, einval}
			end;

		{error, _Reason} = Error ->
			Error
	end,
	send_reply(RetPath, encode_result(Reply)).


do_watch_progress_req(Body, RetPath, #state{progreg=ProgReg} = S) ->
	<<EnableInt:8>> = Body,
	Enable = EnableInt =/= 0,
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
	send_reply(RetPath, encode_result(ok)),
	S2.

do_sys_info_req(Body, RetPath) ->
	{Param, <<>>} = parse_string(Body),
	Reply = case hotchpotch_sys_info:lookup(Param) of
		{ok, Result} ->
			<<(encode_result(ok))/binary, (encode_string(Result))/binary>>;
		{error, _} = Error ->
			encode_result(Error)
	end,
	send_reply(RetPath, Reply).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% IO handler loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

io_loop(Handle) ->
	receive
		{Req, Body, RetPath} ->
			case io_loop_process(Handle, Req, Body) of
				{reply, Reply} ->
					send_reply(RetPath, Reply),
					io_loop(Handle);
				{stop, Reply} ->
					send_reply(RetPath, Reply)
			end;

		closed ->
			hotchpotch_broker:close(Handle)
	end.


io_loop_process(Handle, Request, ReqData) ->
	case Request of
		?READ_REQ ->
			<<Part:4/binary, Offset:64, Length:32>> = ReqData,
			Reply = case hotchpotch_broker:read(Handle, Part, Offset, Length) of
				{ok, Data} ->
					<<(encode_result(ok))/binary, Data/binary>>;
				Error ->
					encode_result(Error)
			end,
			{reply, Reply};

		?WRITE_REQ ->
			<<Part:4/binary, Offset:64, Data/binary>> = ReqData,
			Reply = hotchpotch_broker:write(Handle, Part, Offset, Data),
			{reply, encode_result(Reply)};

		?TRUNC_REQ ->
			<<Part:4/binary, Offset:64>> = ReqData,
			Reply = hotchpotch_broker:truncate(Handle, Part, Offset),
			{reply, encode_result(Reply)};

		?CLOSE_REQ ->
			Reply = hotchpotch_broker:close(Handle),
			{stop, encode_result(Reply)};

		?COMMIT_REQ ->
			<<>> = ReqData,
			case hotchpotch_broker:commit(Handle) of
				{ok, Rev} ->
					{reply, <<(encode_result(ok))/binary, Rev/binary>>};
				Error ->
					{reply, encode_result(Error)}
			end;

		?SUSPEND_REQ ->
			<<>> = ReqData,
			case hotchpotch_broker:suspend(Handle) of
				{ok, Rev} ->
					{reply, <<(encode_result(ok))/binary, Rev/binary>>};
				Error ->
					{reply, encode_result(Error)}
			end;

		?MERGE_REQ ->
			Reply = try
				{Store, ReqData1} = get_store(ReqData),
				{Rev, <<Depth:64>>} = parse_uuid(ReqData1),
				hotchpotch_broker:merge(Handle, Store, Rev, Depth)
			catch
				throw:Error -> Error
			end,
			{reply, encode_result(Reply)};

		?REBASE_REQ ->
			{Parent, <<>>} = parse_uuid(ReqData),
			Reply = hotchpotch_broker:rebase(Handle, Parent),
			{reply, encode_result(Reply)};

		?GET_PARENTS_REQ ->
			<<>> = ReqData,
			Reply = case hotchpotch_broker:get_parents(Handle) of
				{ok, Parents} ->
					<<(encode_result(ok))/binary, (encode_list(Parents))/binary>>;
				Error ->
					encode_result(Error)
			end,
			{reply, Reply};

		?SET_TYPE_REQ ->
			{Type, <<>>} = parse_string(ReqData),
			Reply = hotchpotch_broker:set_type(Handle, Type),
			{reply, encode_result(Reply)};

		?GET_TYPE_REQ ->
			<<>> = ReqData,
			Reply = case hotchpotch_broker:get_type(Handle) of
				{ok, Type} ->
					<<(encode_result(ok))/binary, (encode_string(Type))/binary>>;
				Error ->
					encode_result(Error)
			end,
			{reply, Reply};

		?SET_FLAGS_REQ ->
			<<Flags:32>> = ReqData,
			Reply = hotchpotch_broker:set_flags(Handle, Flags),
			{reply, encode_result(Reply)};

		?GET_FLAGS_REQ ->
			<<>> = ReqData,
			Reply = case hotchpotch_broker:get_flags(Handle) of
				{ok, Flags} ->
					<<(encode_result(ok))/binary, Flags:32>>;
				Error ->
					encode_result(Error)
			end,
			{reply, Reply}
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_store(Body) ->
	{SId, Body1} = parse_uuid(Body),
	case hotchpotch_volman:store(SId) of
		{ok, Store} -> {Store, Body1};
		error       -> throw({error, enxio})
	end.


get_stores(StoreList) ->
	case StoreList of
		[] ->
			[Store || {_SId, Store} <- hotchpotch_volman:stores()];

		_ ->
			lists:foldr(
				fun(SId, Acc) ->
					case hotchpotch_volman:store(SId) of
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


start_worker(S, Fun) ->
	Cookie = S#state.next,
	Server = self(),
	Worker = spawn_link(fun() -> Fun(Cookie), Server ! {done, Cookie} end),
	{ok, S#state{
		cookies = dict:store(Cookie, Worker, S#state.cookies),
		next    = Cookie + 1}}.


send_reply(#retpath{req=Req, ref=Ref, socket=Socket}, Data) ->
	Reply = Req bor ?FLAG_CNF,
	Raw = <<Ref:32, Reply:16, Data/binary>>,
	%io:format("[~w] Reply: ~w~n", [self(), Raw]),
	case gen_tcp:send(Socket, Raw) of
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

