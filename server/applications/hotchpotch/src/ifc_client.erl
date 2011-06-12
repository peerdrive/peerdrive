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

-module(ifc_client).
-export([init/2, handle_packet/2, handle_info/2, terminate/1]).
-import(netencode, [parse_uuid/1, parse_string/1, parse_uuid_list/1,
	parse_store/1, encode_list/1, encode_list/2, encode_list_32/1,
	encode_list_32/2, encode_string/1, encode_direct_result/1,
	encode_error_code/1, err2int/1]).

-include("store.hrl").

-record(state, {socket, cookies, next, progreg=false}).
-record(retpath, {socket, ref}).

-define(INIT_REQ,           16#0000).
-define(INIT_CNF,           16#0001).
-define(ENUM_REQ,           16#0010).
-define(ENUM_CNF,           16#0011).
-define(LOOKUP_DOC_REQ,     16#0020).
-define(LOOKUP_DOC_CNF,     16#0021).
-define(LOOKUP_REV_REQ,     16#0030).
-define(LOOKUP_REV_CNF,     16#0031).
-define(STAT_REQ,           16#0040).
-define(STAT_CNF,           16#0041).
-define(PEEK_REQ,           16#0050).
-define(PEEK_CNF,           16#0051).
-define(CREATE_REQ,         16#0060).
-define(CREATE_CNF,         16#0061).
-define(FORK_REQ,           16#0070).
-define(FORK_CNF,           16#0071).
-define(UPDATE_REQ,         16#0080).
-define(UPDATE_CNF,         16#0081).
-define(RESUME_REQ,         16#0090).
-define(RESUME_CNF,         16#0091).
-define(READ_REQ,           16#00A0).
-define(READ_CNF,           16#00A1).
-define(TRUNC_REQ,          16#00B0).
-define(TRUNC_CNF,          16#00B1).
-define(WRITE_REQ,          16#00C0).
-define(WRITE_CNF,          16#00C1).
-define(GET_TYPE_REQ,       16#00D0).
-define(GET_TYPE_CNF,       16#00D1).
-define(SET_TYPE_REQ,       16#00E0).
-define(SET_TYPE_CNF,       16#00E1).
-define(GET_PARENTS_REQ,    16#00F0).
-define(GET_PARENTS_CNF,    16#00F1).
-define(SET_PARENTS_REQ,    16#0100).
-define(SET_PARENTS_CNF,    16#0101).
-define(COMMIT_REQ,         16#0110).
-define(COMMIT_CNF,         16#0111).
-define(SUSPEND_REQ,        16#0120).
-define(SUSPEND_CNF,        16#0121).
-define(CLOSE_REQ,          16#0130).
-define(CLOSE_CNF,          16#0131).
-define(WATCH_ADD_REQ,      16#0140).
-define(WATCH_ADD_CNF,      16#0141).
-define(WATCH_REM_REQ,      16#0150).
-define(WATCH_REM_CNF,      16#0151).
-define(WATCH_PROGRESS_REQ, 16#0160).
-define(WATCH_PROGRESS_CNF, 16#0161).
-define(FORGET_REQ,         16#0170).
-define(FORGET_CNF,         16#0171).
-define(DELETE_DOC_REQ,     16#0180).
-define(DELETE_DOC_CNF,     16#0181).
-define(DELETE_REV_REQ,     16#0190).
-define(DELETE_REV_CNF,     16#0191).
-define(SYNC_DOC_REQ,       16#01A0).
-define(SYNC_DOC_CNF,       16#01A1).
-define(REPLICATE_DOC_REQ,  16#01B0).
-define(REPLICATE_DOC_CNF,  16#01B1).
-define(REPLICATE_REV_REQ,  16#01C0).
-define(REPLICATE_REV_CNF,  16#01C1).
-define(MOUNT_REQ,          16#01D0).
-define(MOUNT_CNF,          16#01D1).
-define(UNMOUNT_REQ,        16#01E0).
-define(UNMOUNT_CNF,        16#01E1).
-define(SYS_INFO_REQ,       16#01F0).
-define(SYS_INFO_CNF,       16#01F1).
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
		ProgReg -> work_monitor:deregister_proc(self());
		true    -> ok
	end,
	change_monitor:remove(),
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

				{rep_doc, Doc, Stores} ->
					BinStores = lists:foldl(
						fun(Store, Acc) -> <<Acc/binary, Store/binary>> end,
						<<>>,
						Stores),
					<<?PROGRESS_TYPE_REP_DOC:8, Doc/binary, BinStores/binary>>;

				{rep_rev, Rev, Stores} ->
					BinStores = lists:foldl(
						fun(Store, Acc) -> <<Acc/binary, Store/binary>> end,
						<<>>,
						Stores),
					<<?PROGRESS_TYPE_REP_REV:8, Rev/binary, BinStores/binary>>
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
	RetPath = #retpath{socket=S#state.socket, ref=Ref},
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
			change_monitor:watch(Type, Hash),
			send_reply(RetPath, ?WATCH_ADD_CNF, encode_error_code(ok)),
			{ok, S};

		?WATCH_REM_REQ ->
			<<EncType:8, Hash:16/binary>> = Body,
			Type = case EncType of
				0 -> doc;
				1 -> rev
			end,
			change_monitor:unwatch(Type, Hash),
			send_reply(RetPath, ?WATCH_REM_CNF, encode_error_code(ok)),
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

		?SYNC_DOC_REQ ->
			spawn_link(fun () -> do_sync(Body, RetPath) end),
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
		0 -> send_reply(RetPath, ?INIT_CNF, <<(err2int(ok)):32, 0:32,
			16#1000:32>>);
		_ -> send_reply(RetPath, ?INIT_CNF, <<(err2int(erpcmismatch)):32,
			0:32, 16#1000:32>>)
	end.


do_enum(RetPath) ->
	Stores = volman:enum(),
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
	send_reply(RetPath, ?ENUM_CNF, Reply).


do_loopup_doc(Body, RetPath) ->
	% parse request
	<<Doc:16/binary, Body1/binary>> = Body,
	{Stores, <<>>} = parse_uuid_list(Body1),

	% execute
	{Revs, PreRevs} = broker:lookup_doc(Doc, broker:get_stores(Stores)),
	RevsBin = do_lookup_encode(Revs),
	PreRevsBin = do_lookup_encode(PreRevs),
	send_reply(RetPath, ?LOOKUP_DOC_CNF, <<RevsBin/binary, PreRevsBin/binary>>).


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
	Found = broker:lookup_rev(Rev, broker:get_stores(Stores)),
	FoundBin = encode_list(Found),
	send_reply(RetPath, ?LOOKUP_REV_CNF, FoundBin).


do_stat(Body, RetPath) ->
	% parse request
	<<Rev:16/binary, Body1/binary>> = Body,
	{Stores, <<>>} = parse_uuid_list(Body1),

	% execute
	Reply = case broker:stat(Rev, broker:get_stores(Stores)) of
		{ok, _Errors, Stat} = Result ->
			#rev_stat{
				flags   = Flags,
				parts   = Parts,
				parents = Parents,
				mtime   = Mtime,
				type    = TypeCode,
				creator = CreatorCode,
				links   = {SDL, WDL, SRL, WRL, DocMap}
			} = Stat,
			ReplyParts = encode_list(
				fun ({FourCC, Size, Hash}) ->
					<<FourCC/binary, Size:64, Hash/binary>>
				end,
				Parts),
			BinDocMap = encode_list_32(
				fun ({Doc, Revs}) ->
					<<Doc/binary, (encode_list(Revs))/binary>>
				end,
				DocMap),
			ReplyLinks = <<
				(encode_list_32(SDL))/binary, (encode_list_32(WDL))/binary,
				(encode_list_32(SRL))/binary, (encode_list_32(WRL))/binary,
				BinDocMap/binary>>,
			ReplyParents = encode_list(Parents),
			<<
				(encode_broker_result(Result))/binary,
				Flags:32,
				ReplyParts/binary,
				ReplyParents/binary,
				Mtime:64,
				(encode_string(TypeCode))/binary,
				(encode_string(CreatorCode))/binary,
				ReplyLinks/binary
			>>;

		Error ->
			encode_broker_result(Error)
	end,
	send_reply(RetPath, ?STAT_CNF, Reply).


do_peek(Cookie, RetPath, Body) ->
	{Rev, Body1} = parse_uuid(Body),
	{Stores, <<>>} = parse_uuid_list(Body1),
	case broker:peek(Rev, broker:get_stores(Stores)) of
		{ok, _Errors, Handle} = Result ->
			Reply = <<(encode_broker_result(Result))/binary, Cookie:32>>,
			send_reply(RetPath, ?PEEK_CNF, Reply),
			io_loop(Handle);

		Error ->
			send_reply(RetPath, ?PEEK_CNF, encode_broker_result(Error))
	end.


do_create(Cookie, RetPath, Body) ->
	{Type, Body1} = parse_string(Body),
	{Creator, Body2} = parse_string(Body1),
	{Stores, <<>>} = parse_uuid_list(Body2),
	case broker:create(Type, Creator, broker:get_stores(Stores)) of
		{ok, _Errors, {Doc, Handle}} = Result ->
			Reply = <<(encode_broker_result(Result))/binary, Cookie:32, Doc/binary>>,
			send_reply(RetPath, ?CREATE_CNF, Reply),
			io_loop(Handle);

		Error ->
			send_reply(RetPath, ?CREATE_CNF, encode_broker_result(Error))
	end.


do_fork(Cookie, RetPath, Body) ->
	{Rev, Body1} = parse_uuid(Body),
	{Creator, Body2} = parse_string(Body1),
	{Stores, <<>>} = parse_uuid_list(Body2),
	case broker:fork(Rev, Creator, broker:get_stores(Stores)) of
		{ok, _Errors, {Doc, Handle}} = Result ->
			Reply = <<(encode_broker_result(Result))/binary, Cookie:32, Doc/binary>>,
			send_reply(RetPath, ?FORK_CNF, Reply),
			io_loop(Handle);

		Error ->
			send_reply(RetPath, ?FORK_CNF, encode_broker_result(Error))
	end.


do_update(Cookie, RetPath, Body) ->
	{Doc, Body1} = parse_uuid(Body),
	{Rev, Body2} = parse_uuid(Body1),
	{Creator, Body3} = parse_string(Body2),
	{Stores, <<>>} = parse_uuid_list(Body3),
	RealCreator = case Creator of
		<<>> -> keep;
		_    -> Creator
	end,
	case broker:update(Doc, Rev, RealCreator, broker:get_stores(Stores)) of
		{ok, _Errors, Handle} = Result ->
			Reply = <<(encode_broker_result(Result))/binary, Cookie:32>>,
			send_reply(RetPath, ?UPDATE_CNF, Reply),
			io_loop(Handle);

		Error ->
			send_reply(RetPath, ?UPDATE_CNF, encode_broker_result(Error))
	end.


do_resume(Cookie, RetPath, Body) ->
	{Doc, Body1} = parse_uuid(Body),
	{Rev, Body2} = parse_uuid(Body1),
	{Creator, Body3} = parse_string(Body2),
	{Stores, <<>>} = parse_uuid_list(Body3),
	RealCreator = case Creator of
		<<>> -> keep;
		_    -> Creator
	end,
	case broker:resume(Doc, Rev, RealCreator, broker:get_stores(Stores)) of
		{ok, _Errors, Handle} = Result ->
			Reply = <<(encode_broker_result(Result))/binary, Cookie:32>>,
			send_reply(RetPath, ?RESUME_CNF, Reply),
			io_loop(Handle);

		Error ->
			send_reply(RetPath, ?RESUME_CNF, encode_broker_result(Error))
	end.


do_forget(Body, RetPath) ->
	{Doc, Body1} = parse_uuid(Body),
	{Rev, Body2} = parse_uuid(Body1),
	{Stores, <<>>} = parse_uuid_list(Body2),
	Reply = broker:forget(Doc, Rev, broker:get_stores(Stores)),
	send_reply(RetPath, ?FORGET_CNF, encode_broker_result(Reply)).


do_delete_doc(Body, RetPath) ->
	{Doc, Body1} = parse_uuid(Body),
	{Rev, Body2} = parse_uuid(Body1),
	{Stores, <<>>} = parse_uuid_list(Body2),
	Reply = broker:delete_doc(Doc, Rev, broker:get_stores(Stores)),
	send_reply(RetPath, ?DELETE_DOC_CNF, encode_broker_result(Reply)).


do_delete_rev(Body, RetPath) ->
	{Rev, Body1} = parse_uuid(Body),
	{Stores, <<>>} = parse_uuid_list(Body1),
	Reply = broker:delete_rev(Rev, broker:get_stores(Stores)),
	send_reply(RetPath, ?DELETE_REV_CNF, encode_broker_result(Reply)).


do_sync(Body, RetPath) ->
	<<Doc:16/binary, Depth:64, Body1/binary>> = Body,
	{Stores, <<>>} = parse_uuid_list(Body1),
	Reply = case broker:sync(Doc, Depth, broker:get_stores(Stores)) of
		{ok, _ErrInfo, Rev} = Ok ->
			<<(encode_broker_result(Ok))/binary, Rev/binary>>;
		Error ->
			encode_broker_result(Error)
	end,
	send_reply(RetPath, ?SYNC_DOC_CNF, Reply).


do_replicate_doc(Body, RetPath) ->
	<<Doc:16/binary, Depth:64, Body1/binary>> = Body,
	{SrcStores, Body2} = parse_uuid_list(Body1),
	{DstStores, <<>>} = parse_uuid_list(Body2),
	Reply = broker:replicate_doc(Doc, Depth, broker:get_stores(SrcStores),
		broker:get_stores(DstStores)),
	send_reply(RetPath, ?REPLICATE_DOC_CNF, encode_broker_result(Reply)).


do_replicate_rev(Body, RetPath) ->
	<<Doc:16/binary, Depth:64, Body1/binary>> = Body,
	{SrcStores, Body2} = parse_uuid_list(Body1),
	{DstStores, <<>>} = parse_uuid_list(Body2),
	Reply = broker:replicate_rev(Doc, Depth, broker:get_stores(SrcStores),
		broker:get_stores(DstStores)),
	send_reply(RetPath, ?REPLICATE_REV_CNF, encode_broker_result(Reply)).


do_mount(Body, RetPath) ->
	Reply = case parse_store(Body) of
		{ok, {Id, _Descr, _Guid, Tags}} ->
			case proplists:is_defined(removable, Tags) of
				true  -> volman:mount(Id);
				false -> {error, einval}
			end;

		{error, _Reason} = Error ->
			Error
	end,
	send_reply(RetPath, ?MOUNT_CNF, encode_direct_result(Reply)).


do_unmount(Body, RetPath) ->
	Reply = case parse_store(Body) of
		{ok, {Id, _Descr, _Guid, Tags}} ->
			case proplists:is_defined(removable, Tags) of
				true  -> volman:unmount(Id);
				false -> {error, einval}
			end;

		{error, _Reason} = Error ->
			Error
	end,
	send_reply(RetPath, ?UNMOUNT_CNF, encode_direct_result(Reply)).


do_watch_progress_req(Body, RetPath, #state{progreg=ProgReg} = S) ->
	<<EnableInt:8>> = Body,
	Enable = EnableInt =/= 0,
	S2 = case Enable of
		ProgReg ->
			S;
		true ->
			work_monitor:register_proc(self()),
			S#state{progreg=true};
		false ->
			work_monitor:deregister_proc(self()),
			S#state{progreg=false}
	end,
	send_reply(RetPath, ?WATCH_PROGRESS_CNF, encode_direct_result(ok)),
	S2.

do_sys_info_req(Body, RetPath) ->
	{Param, <<>>} = parse_string(Body),
	Reply = case sys_info:lookup(Param) of
		{ok, Result} ->
			<<(encode_direct_result(ok))/binary, (encode_string(Result))/binary>>;
		{error, _} = Error ->
			encode_direct_result(Error)
	end,
	send_reply(RetPath, ?SYS_INFO_CNF, Reply).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% IO handler loop
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

io_loop(Handle) ->
	receive
		{?READ_REQ, ReqData, RetPath} ->
			<<Part:4/binary, Offset:64, Length:32>> = ReqData,
			Reply = case broker:read(Handle, Part, Offset, Length) of
				{ok, _Errors, Data} = Result ->
					<<(encode_broker_result(Result))/binary, Data/binary>>;
				Error ->
					encode_broker_result(Error)
			end,
			send_reply(RetPath, ?READ_CNF, Reply),
			io_loop(Handle);

		{?WRITE_REQ, ReqData, RetPath} ->
			<<Part:4/binary, Offset:64, Data/binary>> = ReqData,
			Reply = broker:write(Handle, Part, Offset, Data),
			send_reply(RetPath, ?WRITE_CNF, encode_broker_result(Reply)),
			io_loop(Handle);

		{?TRUNC_REQ, ReqData, RetPath} ->
			<<Part:4/binary, Offset:64>> = ReqData,
			Reply = broker:truncate(Handle, Part, Offset),
			send_reply(RetPath, ?TRUNC_CNF, encode_broker_result(Reply)),
			io_loop(Handle);

		{?CLOSE_REQ, <<>>, RetPath} ->
			Reply = broker:close(Handle),
			send_reply(RetPath, ?CLOSE_CNF, encode_broker_result(Reply));

		{?COMMIT_REQ, <<>>, RetPath} ->
			case broker:commit(Handle) of
				{ok, _, Rev} = Result ->
					send_reply(RetPath, ?COMMIT_CNF,
						<<(encode_broker_result(Result))/binary, Rev/binary>>);

				{retry, _, _} = Result->
					send_reply(RetPath, ?COMMIT_CNF, encode_broker_result(Result));

				Error ->
					send_reply(RetPath, ?COMMIT_CNF, encode_broker_result(Error))
			end,
			io_loop(Handle);

		{?SUSPEND_REQ, <<>>, RetPath} ->
			Reply = case broker:suspend(Handle) of
				{ok, _Errors, Rev} = Result ->
					<<(encode_broker_result(Result))/binary, Rev/binary>>;
				Error ->
					encode_broker_result(Error)
			end,
			send_reply(RetPath, ?SUSPEND_CNF, Reply),
			io_loop(Handle);

		{?SET_PARENTS_REQ, Body, RetPath} ->
			{Parents, <<>>} = parse_uuid_list(Body),
			Reply = broker:set_parents(Handle, Parents),
			send_reply(RetPath, ?SET_PARENTS_CNF, encode_broker_result(Reply)),
			io_loop(Handle);

		{?GET_PARENTS_REQ, <<>>, RetPath} ->
			Reply = case broker:get_parents(Handle) of
				{ok, _Errors, Parents} = Result ->
					<<(encode_broker_result(Result))/binary,
						(encode_list(Parents))/binary>>;
				Error ->
					encode_broker_result(Error)
			end,
			send_reply(RetPath, ?GET_PARENTS_CNF, Reply),
			io_loop(Handle);

		{?SET_TYPE_REQ, Body, RetPath} ->
			{Type, <<>>} = parse_string(Body),
			Reply = broker:set_type(Handle, Type),
			send_reply(RetPath, ?SET_TYPE_CNF, encode_broker_result(Reply)),
			io_loop(Handle);

		{?GET_TYPE_REQ, <<>>, RetPath} ->
			Reply = case broker:get_type(Handle) of
				{ok, _Errors, Type} = Result ->
					<<(encode_broker_result(Result))/binary,
						(encode_string(Type))/binary>>;
				Error ->
					encode_broker_result(Error)
			end,
			send_reply(RetPath, ?GET_TYPE_CNF, Reply),
			io_loop(Handle);

		closed ->
			broker:close(Handle);

		Else ->
			io:format("io_loop: Invalid request: ~w~n", [Else]),
			io_loop(Handle)
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

encode_broker_result({ok, Errors, _Result}) ->
	encode_broker_result({ok, Errors});

encode_broker_result({ok, []}) ->
	<<0:8>>;

encode_broker_result({ok, Errors}) ->
	ErrorList = encode_list(
		fun({Store, Error}) ->
			<<Store/binary, (encode_error_code(Error))/binary>>
		end,
		Errors),
	<<1:8, ErrorList/binary>>;

encode_broker_result({error, Reason, Errors}) ->
	ErrorList = encode_list(
		fun({Store, Error}) ->
			<<Store/binary, (encode_error_code(Error))/binary>>
		end,
		Errors),
	<<2:8, (encode_error_code(Reason))/binary, ErrorList/binary>>.


start_worker(S, Fun) ->
	Cookie = S#state.next,
	Server = self(),
	Worker = spawn_link(fun() -> Fun(Cookie), Server ! {done, Cookie} end),
	{ok, S#state{
		cookies = dict:store(Cookie, Worker, S#state.cookies),
		next    = Cookie + 1}}.


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

