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
-export([init/1, handle_packet/2, handle_info/2, terminate/0]).

-record(state, {socket, cookies, next}).
-record(retpath, {socket, ref}).

-define(TIMEOUT, 60000).

-define(GENERIC_CNF,        16#0001).
-define(ENUM_REQ,           16#0010).
-define(ENUM_CNF,           16#0011).
-define(LOOKUP_REQ,         16#0020).
-define(LOOKUP_CNF,         16#0021).
-define(STAT_REQ,           16#0030).
-define(STAT_CNF,           16#0031).
-define(PEEK_REQ,           16#0040).
-define(PEEK_CNF,           16#0041).
-define(FORK_REQ,           16#0050).
-define(FORK_CNF,           16#0051).
-define(UPDATE_REQ,         16#0060).
-define(UPDATE_CNF,         16#0061).
-define(READ_REQ,           16#0070).
-define(READ_CNF,           16#0071).
-define(WRITE_REQ,          16#0080).
-define(TRUNC_REQ,          16#0090).
-define(COMMIT_REQ,         16#00A0).
-define(COMMIT_CNF,         16#00A1).
-define(ABORT_REQ,          16#00B0).
-define(WATCH_ADD_REQ,      16#00C0).
-define(WATCH_REM_REQ,      16#00D0).
-define(WATCH_IND,          16#00E2).
-define(DELETE_DOC_REQ,     16#00F0).
-define(DELETE_REV_REQ,     16#0100).
-define(SYNC_DOC_REQ,       16#0110).
-define(REPLICATE_DOC_REQ,  16#0120).
-define(REPLICATE_REV_REQ,  16#0130).
-define(MOUNT_REQ,          16#0140).
-define(UNMOUNT_REQ,        16#0150).
-define(PROGRESS_IND,       16#0162).

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

init(Socket) ->
	work_monitor:register_proc(self()),
	#state{socket=Socket, cookies=dict:new(), next=0}.


terminate() ->
	work_monitor:deregister_proc(self()),
	change_monitor:remove().


handle_info({work_event, Tag, Progress}, S) ->
	Num = case Progress of
		started -> 0;
		done    -> 16#ffff;
		_       -> Progress
	end,
	Data = case Tag of
		{sync, FromGuid, ToGuid} ->
			<<?PROGRESS_TYPE_SYNC:8, Num:16/little, FromGuid/binary, ToGuid/binary>>;

		{rep_doc, Doc, Stores} ->
			BinStores = lists:foldl(
				fun(Store, Acc) -> <<Acc/binary, Store/binary>> end,
				<<>>,
				Stores),
			<<?PROGRESS_TYPE_REP_DOC:8, Num:16/little, Doc/binary, BinStores/binary>>;

		{rep_rev, Rev, Stores} ->
			BinStores = lists:foldl(
				fun(Store, Acc) -> <<Acc/binary, Store/binary>> end,
				<<>>,
				Stores),
			<<?PROGRESS_TYPE_REP_REV:8, Num:16/little, Rev/binary, BinStores/binary>>
	end,
	send_indication(S#state.socket, ?PROGRESS_IND, Data),
	S;

handle_info({done, Cookie}, S) ->
	S#state{cookies=dict:erase(Cookie, S#state.cookies)};

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
	S.


handle_packet(Packet, S) ->
	<<Request:16/little, Ref:32/little, Body/binary>> = Packet,
	RetPath = #retpath{socket=S#state.socket, ref=Ref},
	%io:format("[~w] Request:~w Ref:~w Body:~w~n", [self(), Request, Ref, Body]),
	case Request of
		?ENUM_REQ ->
			do_enum(RetPath),
			S;

		?LOOKUP_REQ ->
			<<Doc:16/binary>> = Body,
			spawn_link(fun () -> do_loopup(Doc, RetPath) end),
			S;

		?STAT_REQ ->
			<<Rev:16/binary>> = Body,
			spawn_link(fun () -> do_stat(Rev, RetPath) end),
			S;

		?PEEK_REQ ->
			<<Rev:16/binary, Body1/binary>> = Body,
			{Stores, <<>>} = parse_revisions(Body1),
			start_worker(S, fun(Cookie) -> do_peek(Cookie, RetPath, Rev, Stores) end);

		?FORK_REQ ->
			<<Rev:16/binary, Body1/binary>> = Body,
			{Stores, EncUti} = parse_revisions(Body1),
			Uti = case EncUti of
				<<>> -> keep;
				_    -> EncUti
			end,
			start_worker(S, fun(Cookie) -> do_fork(Cookie, RetPath, Rev, Stores, Uti) end);

		?UPDATE_REQ ->
			<<Doc:16/binary, Rev:16/binary, Body1/binary>> = Body,
			{Stores, EncUti} = parse_revisions(Body1),
			Uti = case EncUti of
				<<>> -> keep;
				_    -> EncUti
			end,
			start_worker(S, fun(Cookie) -> do_update(Cookie, RetPath, Doc, Rev,
				Stores, Uti) end);

		?WATCH_ADD_REQ ->
			<<EncType:8, Hash:16/binary>> = Body,
			Type = case EncType of
				0 -> doc;
				1 -> rev
			end,
			change_monitor:watch(Type, Hash),
			send_generic_reply(RetPath, ok),
			S;

		?WATCH_REM_REQ ->
			<<EncType:8, Hash:16/binary>> = Body,
			Type = case EncType of
				0 -> doc;
				1 -> rev
			end,
			change_monitor:unwatch(Type, Hash),
			send_generic_reply(RetPath, ok),
			S;

		?DELETE_DOC_REQ ->
			<<Doc:16/binary, Body1/binary>> = Body,
			{Stores, <<>>} = parse_revisions(Body1),
			spawn_link(fun () -> do_delete_doc(Doc, Stores, RetPath) end),
			S;

		?DELETE_REV_REQ ->
			<<Rev:16/binary, Body1/binary>> = Body,
			{Stores, <<>>} = parse_revisions(Body1),
			spawn_link(fun () -> do_delete_rev(Rev, Stores, RetPath) end),
			S;

		?SYNC_DOC_REQ ->
			<<Doc:16/binary, Body1/binary>> = Body,
			{Stores, <<>>} = parse_revisions(Body1),
			spawn_link(fun () -> do_sync(Doc, Stores, RetPath) end),
			S;

		?REPLICATE_DOC_REQ ->
			<<Doc:16/binary, History:8, Body1/binary>> = Body,
			{Stores, <<>>} = parse_revisions(Body1),
			replicator:replicate_uuid(Doc, Stores, as_boolean(History)),
			send_generic_reply(RetPath, ok),
			S;

		?REPLICATE_REV_REQ ->
			<<Rev:16/binary, History:8, Body1/binary>> = Body,
			{Stores, <<>>} = parse_revisions(Body1),
			replicator:replicate_rev(Rev, Stores, as_boolean(History)),
			send_generic_reply(RetPath, ok),
			S;

		?MOUNT_REQ ->
			spawn_link(fun () -> do_mount(Body, RetPath) end),
			S;

		?UNMOUNT_REQ ->
			spawn_link(fun () -> do_unmount(Body, RetPath) end),
			S;

		_ ->
			<<Cookie:32/little, Data/binary>> = Body,
			Worker = dict:fetch(Cookie, S#state.cookies),
			Worker ! {Request, Data, RetPath},
			S
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


parse_revisions(<<Count:8, Body/binary>>) ->
	parse_revisions_loop(Body, [], Count).

parse_revisions_loop(Body, Revs, 0) ->
	{Revs, Body};
parse_revisions_loop(<<Rev:16/binary, Rest/binary>>, Revs, Count) ->
	parse_revisions_loop(Rest, [Rev|Revs], Count-1).

as_boolean(Int) ->
	case Int of
		0 -> false;
		_ -> true
	end.

start_worker(S, Fun) ->
	Cookie = S#state.next,
	Server = self(),
	Worker = spawn_link(fun() -> Fun(Cookie), Server ! {done, Cookie} end),
	S#state{
		cookies = dict:store(Cookie, Worker, S#state.cookies),
		next    = Cookie + 1}.


send_reply(RetPath, Reply, Data) ->
	Raw = <<Reply:16/little, (RetPath#retpath.ref):32/little, Data/binary>>,
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
	Raw = <<Indication:16/little, 16#FFFFFFFF:32/little, Data/binary>>,
	%io:format("[~w] Indication: ~w~n", [self(), Raw]),
	case gen_tcp:send(Socket, Raw) of
		ok ->
			ok;
		{error, Reason} ->
			error_logger:warning_msg(
				"[~w] Failed to send indication: ~w~n",
				[self(), Reason])
	end.

send_generic_reply(RetPath, Reply) ->
	BinCode = case Reply of
		ok             -> 0;
		conflict       -> 1; % recoverable conflict
		{error, Error} ->
			case Error of
				conflict  -> 2; % unrecoverable conflict
				enoent    -> 3;
				einval    -> 4;
				emultiple -> 5;
				ebadf     -> 6;
				_         -> 16#ffffffff
			end;
		{ok, _} -> 0
	end,
	send_reply(RetPath, ?GENERIC_CNF, <<BinCode:32/little>>).


do_enum(RetPath) ->
	Stores = volman:enum(),
	Reply = lists:foldl(
		fun({Id, Descr, Store, Properties}, Acc) ->
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
			IdBin = list_to_binary(atom_to_list(Id)),
			NameBin = list_to_binary(Descr),
			<<Acc/binary,
				Store/binary,
				Flags:32/little,
				(size(IdBin)):16/little, IdBin/binary,
				(size(NameBin)):16/little, NameBin/binary>>
		end,
		<<(length(Stores)):8>>,
		Stores),
	send_reply(RetPath, ?ENUM_CNF, Reply).


do_loopup(Doc, RetPath) ->
	Revs = broker:lookup(Doc),
	Reply = lists:foldl(
		fun({Rev, Stores}, RevAcc) ->
			StoreBin = lists:foldl(
				fun(Store, StoreAcc) -> <<StoreAcc/binary, Store/binary>> end,
				<<(length(Stores)):8>>,
				Stores),
			<<RevAcc/binary, Rev/binary, StoreBin/binary>>
		end,
		<<(length(Revs)):8>>,
		Revs),
	send_reply(RetPath, ?LOOKUP_CNF, Reply).


do_stat(Rev, RetPath) ->
	case broker:stat(Rev) of
		{ok, Flags, Parts, Parents, Mtime, Uti, Volumes} ->
			ReplyParts = lists:foldl(
				fun ({FourCC, Size, Hash}, AccIn) ->
					<<AccIn/binary, FourCC/binary, Size:64/little, Hash/binary>>
				end,
				<<(length(Parts)):8>>,
				Parts),
			ReplyParents = lists:foldl(
				fun (Parent, AccIn) ->
					<<AccIn/binary, Parent/binary>>
				end,
				<<(length(Parents)):8>>,
				Parents),
			ReplyVolumes = lists:foldl(
				fun (Volume, AccIn) ->
					<<AccIn/binary, Volume/binary>>
				end,
				<<(length(Volumes)):8>>,
				Volumes),
			Reply = <<
				Flags:32/little,
				ReplyParts/binary,
				ReplyParents/binary,
				ReplyVolumes/binary,
				Mtime:64/little,
				Uti/binary>>,
			send_reply(RetPath, ?STAT_CNF, Reply);

		error ->
			send_generic_reply(RetPath, {error, enoent})
	end.


do_peek(Cookie, RetPath, Rev, Stores) ->
	case broker:peek(Rev, Stores) of
		{ok, Handle} ->
			send_reply(RetPath, ?PEEK_CNF, <<Cookie:32/little>>),
			io_loop(Handle);

		{error, _} = Error ->
			send_generic_reply(RetPath, Error)
	end.


do_fork(Cookie, RetPath, Rev, Stores, Uti) ->
	case broker:fork(Rev, Stores, Uti) of
		{ok, Doc, Handle} ->
			send_reply(RetPath, ?FORK_CNF, <<Cookie:32/little, Doc/binary>>),
			io_loop(Handle);

		{error, _} = Error ->
			send_generic_reply(RetPath, Error)
	end.


do_update(Cookie, RetPath, Doc, Rev, Stores, Uti) ->
	case broker:update(Doc, Rev, Stores, Uti) of
		{ok, Handle} ->
			send_reply(RetPath, ?UPDATE_CNF, <<Cookie:32/little>>),
			io_loop(Handle);

		{error, _} = Error ->
			send_generic_reply(RetPath, Error)
	end.


io_loop(Handle) ->
	receive
		{?READ_REQ, ReqData, RetPath} ->
			<<Part:4/binary, Offset:64/little, Length:32/little>> = ReqData,
			case broker:read(Handle, Part, Offset, Length) of
				{ok, Data}         -> send_reply(RetPath, ?READ_CNF, Data);
				eof                -> send_reply(RetPath, ?READ_CNF, <<>>);
				{error, _} = Error -> send_generic_reply(RetPath, Error)
			end,
			io_loop(Handle);

		{?WRITE_REQ, ReqData, RetPath} ->
			<<Part:4/binary, Offset:64/little, Data/binary>> = ReqData,
			Reply = broker:write(Handle, Part, Offset, Data),
			send_generic_reply(RetPath, Reply),
			io_loop(Handle);

		{?TRUNC_REQ, ReqData, RetPath} ->
			<<Part:4/binary, Offset:64/little>> = ReqData,
			Reply = broker:truncate(Handle, Part, Offset),
			send_generic_reply(RetPath, Reply),
			io_loop(Handle);

		{?ABORT_REQ, <<>>, RetPath} ->
			Reply = broker:abort(Handle),
			send_generic_reply(RetPath, Reply);

		{?COMMIT_REQ, ReqData, RetPath} ->
			{MergeRevs, <<>>} = parse_revisions(ReqData),
			case broker:commit(Handle, MergeRevs) of
				{ok, Rev} ->
					send_reply(RetPath, ?COMMIT_CNF, Rev);

				conflict ->
					send_generic_reply(RetPath, conflict),
					io_loop(Handle);

				{error, _} = Error ->
					send_generic_reply(RetPath, Error)
			end;

		Else ->
			io:format("io_loop: Invalid request: ~w~n", [Else]),
			io_loop(Handle)
	after
		?TIMEOUT ->
			io:format("io_loop: timeout~n")
	end.


do_delete_doc(Doc, Stores, RetPath) ->
	Reply = broker:delete_doc(Doc, Stores),
	send_generic_reply(RetPath, Reply).


do_delete_rev(Rev, Stores, RetPath) ->
	Reply = broker:delete_rev(Rev, Stores),
	send_generic_reply(RetPath, Reply).


do_sync(Doc, Stores, RetPath) ->
	Reply = broker:sync(Doc, Stores),
	send_generic_reply(RetPath, Reply).


do_mount(Store, RetPath) ->
	Id = list_to_atom(binary_to_list(Store)), % FIXME: might overflow atom table
	Reply = case lists:keysearch(Id, 1, volman:enum()) of
		{value, {Id, _Descr, _Guid, Tags}} ->
			case proplists:is_defined(removable, Tags) of
				true  -> volman:mount(Id);
				false -> {error, einval}
			end;

		false ->
			{error, einval}
	end,
	send_generic_reply(RetPath, Reply).


do_unmount(Store, RetPath) ->
	Id = list_to_atom(binary_to_list(Store)), % FIXME: might overflow atom table
	Reply = case lists:keysearch(Id, 1, volman:enum()) of
		{value, {Id, _Descr, _Guid, Tags}} ->
			case proplists:is_defined(removable, Tags) of
				true  -> volman:unmount(Id);
				false -> {error, einval}
			end;

		false ->
			{error, einval}
	end,
	send_generic_reply(RetPath, Reply).

