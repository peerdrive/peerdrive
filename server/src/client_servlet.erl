%% Hotchpotch
%% Copyright (C) 2010  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
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

-module(client_servlet).
-export([init/1, handle_packet/2, handle_info/2, terminate/1]).
-include("store.hrl").

-record(state, {socket, cookies, next}).
-record(retpath, {socket, ref}).

-define(INIT_REQ,           16#0000).
-define(INIT_CNF,           16#0001).
-define(ENUM_REQ,           16#0010).
-define(ENUM_CNF,           16#0011).
-define(LOOKUP_REQ,         16#0020).
-define(LOOKUP_CNF,         16#0021).
-define(STAT_REQ,           16#0030).
-define(STAT_CNF,           16#0031).
-define(PEEK_REQ,           16#0040).
-define(PEEK_CNF,           16#0041).
-define(CREATE_REQ,         16#0050).
-define(CREATE_CNF,         16#0051).
-define(FORK_REQ,           16#0060).
-define(FORK_CNF,           16#0061).
-define(UPDATE_REQ,         16#0070).
-define(UPDATE_CNF,         16#0071).
-define(RESUME_REQ,         16#0080).
-define(RESUME_CNF,         16#0081).
-define(READ_REQ,           16#0090).
-define(READ_CNF,           16#0091).
-define(TRUNC_REQ,          16#00A0).
-define(TRUNC_CNF,          16#00A1).
-define(WRITE_REQ,          16#00B0).
-define(WRITE_CNF,          16#00B1).
-define(GET_TYPE_REQ,       16#00C0).
-define(GET_TYPE_CNF,       16#00C1).
-define(SET_TYPE_REQ,       16#00D0).
-define(SET_TYPE_CNF,       16#00D1).
-define(GET_PARENTS_REQ,    16#00E0).
-define(GET_PARENTS_CNF,    16#00E1).
-define(SET_PARENTS_REQ,    16#00F0).
-define(SET_PARENTS_CNF,    16#00F1).
-define(COMMIT_REQ,         16#0100).
-define(COMMIT_CNF,         16#0101).
-define(SUSPEND_REQ,        16#0110).
-define(SUSPEND_CNF,        16#0111).
-define(ABORT_REQ,          16#0120).
-define(ABORT_CNF,          16#0121).
-define(WATCH_ADD_REQ,      16#0130).
-define(WATCH_ADD_CNF,      16#0131).
-define(WATCH_REM_REQ,      16#0140).
-define(WATCH_REM_CNF,      16#0141).
-define(FORGET_REQ,         16#0150).
-define(FORGET_CNF,         16#0151).
-define(DELETE_DOC_REQ,     16#0160).
-define(DELETE_DOC_CNF,     16#0161).
-define(DELETE_REV_REQ,     16#0170).
-define(DELETE_REV_CNF,     16#0171).
-define(SYNC_DOC_REQ,       16#0180).
-define(SYNC_DOC_CNF,       16#0181).
-define(REPLICATE_DOC_REQ,  16#0190).
-define(REPLICATE_DOC_CNF,  16#0191).
-define(REPLICATE_REV_REQ,  16#01A0).
-define(REPLICATE_REV_CNF,  16#01A1).
-define(MOUNT_REQ,          16#01B0).
-define(MOUNT_CNF,          16#01B1).
-define(UNMOUNT_REQ,        16#01C0).
-define(UNMOUNT_CNF,        16#01C1).
-define(WATCH_IND,          16#0002).
-define(PROGRESS_IND,       16#0012).

-define(WATCH_CAUSE_MOD, 0). % Doc has been modified
-define(WATCH_CAUSE_ADD, 1). % Doc/Rev appeared
-define(WATCH_CAUSE_REP, 2). % Doc/Rev was spread to another store
-define(WATCH_CAUSE_DIM, 3). % Doc/Rev removed from a store
-define(WATCH_CAUSE_REM, 4). % Doc/Rev has disappeared

-define(PROGRESS_TYPE_SYNC,    0).
-define(PROGRESS_TYPE_REP_DOC, 1).
-define(PROGRESS_TYPE_REP_REV, 2).

-define(EOK,       	0).
-define(EINVAL,    	3).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Servlet callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Socket) ->
	work_monitor:register_proc(self()),
	process_flag(trap_exit, true),
	#state{socket=Socket, cookies=dict:new(), next=0}.


terminate(State) ->
	work_monitor:deregister_proc(self()),
	change_monitor:remove(),
	dict:fold(
		fun(_Cookie, Worker, _Acc) -> Worker ! closed end,
		ok,
		State#state.cookies).


handle_info({work_event, Tag, Progress}, S) ->
	Num = case Progress of
		started -> 0;
		done    -> 16#ffff;
		_       -> Progress
	end,
	Data = case Tag of
		{sync, FromGuid, ToGuid} ->
			<<?PROGRESS_TYPE_SYNC:8, Num:16, FromGuid/binary, ToGuid/binary>>;

		{rep_doc, Doc, Stores} ->
			BinStores = lists:foldl(
				fun(Store, Acc) -> <<Acc/binary, Store/binary>> end,
				<<>>,
				Stores),
			<<?PROGRESS_TYPE_REP_DOC:8, Num:16, Doc/binary, BinStores/binary>>;

		{rep_rev, Rev, Stores} ->
			BinStores = lists:foldl(
				fun(Store, Acc) -> <<Acc/binary, Store/binary>> end,
				<<>>,
				Stores),
			<<?PROGRESS_TYPE_REP_REV:8, Num:16, Rev/binary, BinStores/binary>>
	end,
	send_indication(S#state.socket, ?PROGRESS_IND, Data),
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

		?LOOKUP_REQ ->
			spawn_link(fun () -> do_loopup(Body, RetPath) end),
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
	<<_Minor:8, Major:8, 0:16>> = Body,
	case Major of
		0 -> send_reply(RetPath, ?INIT_CNF, <<?EOK:32, 0:32,
			16#1000:32>>);
		_ -> send_reply(RetPath, ?INIT_CNF, <<?EINVAL:32, 0:32,
			16#1000:32>>)
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


do_loopup(Body, RetPath) ->
	% parse request
	<<Doc:16/binary, Body1/binary>> = Body,
	{Stores, <<>>} = parse_uuid_list(Body1),

	% execute
	{Revs, PreRevs} = broker:lookup(Doc, Stores),
	RevsBin = do_lookup_encode(Revs),
	PreRevsBin = do_lookup_encode(PreRevs),
	send_reply(RetPath, ?LOOKUP_CNF, <<RevsBin/binary, PreRevsBin/binary>>).


do_lookup_encode(Revs) ->
	encode_list(
		fun({Rev, Stores}) ->
			StoresBin = encode_list(fun(Store) -> Store end, Stores),
			<<Rev/binary, StoresBin/binary>>
		end,
		Revs).


do_stat(Body, RetPath) ->
	% parse request
	<<Rev:16/binary, Body1/binary>> = Body,
	{Stores, <<>>} = parse_uuid_list(Body1),

	% execute
	Reply = case broker:stat(Rev, Stores) of
		{ok, _Errors, {Stat, Volumes}} = Result ->
			#rev_stat{
				flags   = Flags,
				parts   = Parts,
				parents = Parents,
				mtime   = Mtime,
				type    = TypeCode,
				creator = CreatorCode
			} = Stat,
			ReplyParts = encode_list(
				fun ({FourCC, Size, Hash}) ->
					<<FourCC/binary, Size:64, Hash/binary>>
				end,
				Parts),
			ReplyParents = encode_list(fun (Parent) -> Parent end, Parents),
			ReplyVolumes = encode_list(fun (Volume) -> Volume end, Volumes),
			<<
				(encode_broker_result(Result))/binary,
				Flags:32,
				ReplyParts/binary,
				ReplyParents/binary,
				ReplyVolumes/binary,
				Mtime:64,
				(encode_string(TypeCode))/binary,
				(encode_string(CreatorCode))/binary
			>>;

		Error ->
			encode_broker_result(Error)
	end,
	send_reply(RetPath, ?STAT_CNF, Reply).


do_peek(Cookie, RetPath, Body) ->
	{Rev, Body1} = parse_uuid(Body),
	{Stores, <<>>} = parse_uuid_list(Body1),
	case broker:peek(Rev, Stores) of
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
	case broker:create(Type, Creator, Stores) of
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
	case broker:fork(Rev, Creator, Stores) of
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
	case broker:update(Doc, Rev, RealCreator, Stores) of
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
	case broker:resume(Doc, Rev, RealCreator, Stores) of
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
	Reply = broker:forget(Doc, Rev, Stores),
	send_reply(RetPath, ?FORGET_CNF, encode_broker_result(Reply)).


do_delete_doc(Body, RetPath) ->
	{Doc, Body1} = parse_uuid(Body),
	{Rev, Body2} = parse_uuid(Body1),
	{Stores, <<>>} = parse_uuid_list(Body2),
	Reply = broker:delete_doc(Doc, Rev, Stores),
	send_reply(RetPath, ?DELETE_DOC_CNF, encode_broker_result(Reply)).


do_delete_rev(Body, RetPath) ->
	{Rev, Body1} = parse_uuid(Body),
	{Stores, <<>>} = parse_uuid_list(Body1),
	Reply = broker:delete_rev(Rev, Stores),
	send_reply(RetPath, ?DELETE_REV_CNF, encode_broker_result(Reply)).


do_sync(Body, RetPath) ->
	<<Doc:16/binary, Depth:64, Body1/binary>> = Body,
	{Stores, <<>>} = parse_uuid_list(Body1),
	Reply = case broker:sync(Doc, Depth, Stores) of
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
	Reply = broker:replicate_doc(Doc, Depth, SrcStores, DstStores),
	send_reply(RetPath, ?REPLICATE_DOC_CNF, encode_broker_result(Reply)).


do_replicate_rev(Body, RetPath) ->
	<<Doc:16/binary, Depth:64, Body1/binary>> = Body,
	{SrcStores, Body2} = parse_uuid_list(Body1),
	{DstStores, <<>>} = parse_uuid_list(Body2),
	Reply = broker:replicate_rev(Doc, Depth, SrcStores, DstStores),
	send_reply(RetPath, ?REPLICATE_REV_CNF, encode_broker_result(Reply)).


do_mount(Body, RetPath) ->
	{Store, <<>>} = parse_string(Body),
	Reply = case (catch binary_to_existing_atom(Store, utf8)) of
		Id when is_atom(Id) ->
			case lists:keysearch(Id, 1, volman:enum()) of
				{value, {Id, _Descr, _Guid, Tags}} ->
					case proplists:is_defined(removable, Tags) of
						true  -> volman:mount(Id);
						false -> {error, einval}
					end;

				false ->
					{error, einval}
			end;

		{'EXIT', _} ->
			{error, enoent}
	end,
	send_reply(RetPath, ?MOUNT_CNF, encode_direct_result(Reply)).


do_unmount(Body, RetPath) ->
	{Store, <<>>} = parse_string(Body),
	Reply = case (catch binary_to_existing_atom(Store, utf8)) of
		Id when is_atom(Id) ->
			case lists:keysearch(Id, 1, volman:enum()) of
				{value, {Id, _Descr, _Guid, Tags}} ->
					case proplists:is_defined(removable, Tags) of
						true  -> volman:unmount(Id);
						false -> {error, einval}
					end;

				false ->
					{error, einval}
			end;

		{'EXIT', _} ->
			{error, enoent}
	end,
	send_reply(RetPath, ?UNMOUNT_CNF, encode_direct_result(Reply)).


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

		{?ABORT_REQ, <<>>, RetPath} ->
			Reply = broker:abort(Handle),
			send_reply(RetPath, ?ABORT_CNF, encode_broker_result(Reply));

		{?COMMIT_REQ, <<>>, RetPath} ->
			case broker:commit(Handle) of
				{ok, _, Rev} = Result ->
					send_reply(RetPath, ?COMMIT_CNF,
						<<(encode_broker_result(Result))/binary, Rev/binary>>);

				{retry, _, _} = Result->
					send_reply(RetPath, ?COMMIT_CNF, encode_broker_result(Result)),
					io_loop(Handle);

				Error ->
					send_reply(RetPath, ?COMMIT_CNF, encode_broker_result(Error))
			end;

		{?SUSPEND_REQ, <<>>, RetPath} ->
			Reply = case broker:suspend(Handle) of
				{ok, _Errors, Rev} = Result ->
					<<(encode_broker_result(Result))/binary, Rev/binary>>;
				Error ->
					encode_broker_result(Error)
			end,
			send_reply(RetPath, ?SUSPEND_CNF, Reply);

		{?SET_PARENTS_REQ, Body, RetPath} ->
			{Parents, <<>>} = parse_uuid_list(Body),
			Reply = broker:set_parents(Handle, Parents),
			send_reply(RetPath, ?SET_PARENTS_CNF, encode_broker_result(Reply)),
			io_loop(Handle);

		{?GET_PARENTS_REQ, <<>>, RetPath} ->
			Reply = case broker:get_parents(Handle) of
				{ok, _Errors, Parents} = Result ->
					<<(encode_broker_result(Result))/binary,
						(encode_list(fun(P) -> P end, Parents))/binary>>;
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
			broker:abort(Handle);

		Else ->
			io:format("io_loop: Invalid request: ~w~n", [Else]),
			io_loop(Handle)
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_uuid(<<Uuid:16/binary, Rest/binary>>) ->
	{Uuid, Rest}.


parse_string(<<Length:16, Body/binary>>) ->
	<<String:Length/binary, Rest/binary>> = Body,
	{String, Rest}.


parse_list(ParseElement, <<Count:8, Body/binary>>) ->
	parse_list_loop(Count, Body, [], ParseElement).


parse_list_loop(0, Body, List, _ParseElement) ->
	{lists:reverse(List), Body};

parse_list_loop(Count, Body, List, ParseElement) ->
	{Element, Rest} = ParseElement(Body),
	parse_list_loop(Count-1, Rest, [Element|List], ParseElement).


parse_uuid_list(Packet) ->
	parse_list(fun(Body) -> parse_uuid(Body) end, Packet).


encode_list(EncodeElement, List) ->
	lists:foldl(
		fun(Element, Acc) ->
			Bin = EncodeElement(Element),
			<<Acc/binary, Bin/binary>>
		end,
		<<(length(List)):8>>,
		List).


encode_string(String) when is_binary(String) ->
	<<(size(String)):16, String/binary>>;

encode_string(String) when is_atom(String) ->
	Bin = atom_to_binary(String, utf8),
	<<(size(Bin)):16, Bin/binary>>;

encode_string(String) when is_list(String) ->
	Bin = list_to_binary(String),
	<<(size(Bin)):16, Bin/binary>>.


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

encode_broker_result({retry, Reason, Errors}) ->
	ErrorList = encode_list(
		fun({Store, Error}) ->
			<<Store/binary, (encode_error_code(Error))/binary>>
		end,
		Errors),
	<<2:8, (encode_error_code(Reason))/binary, ErrorList/binary>>;

encode_broker_result({error, Reason, Errors}) ->
	ErrorList = encode_list(
		fun({Store, Error}) ->
			<<Store/binary, (encode_error_code(Error))/binary>>
		end,
		Errors),
	<<3:8, (encode_error_code(Reason))/binary, ErrorList/binary>>.


encode_direct_result(ok) ->
	<<0:32>>;

encode_direct_result({ok, _}) ->
	<<0:32>>;

encode_direct_result({error, Reason}) ->
	encode_error_code(Reason).


encode_error_code(Error) ->
	Code = case Error of
		conflict  -> 1;
		enoent    -> 2;
		einval    -> ?EINVAL;
		ebadf     -> 4;
		eambig    -> 5;
		enosys    -> 6;

		ok        -> 0;
		_         ->
			error_logger:warning_report([{module, ?MODULE},
				{reason, "Non-encodable error"}, {error, Error}]),
			16#ffffffff
	end,
	<<Code:32>>.


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

