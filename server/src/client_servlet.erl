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

-define(ENUM_REQ,            16#0000).
-define(ENUM_CNF,            16#0001).
-define(LOOKUP_REQ,          16#0010).
-define(LOOKUP_CNF,          16#0011).
-define(STAT_REQ,            16#0020).
-define(STAT_CNF,            16#0021).
-define(STAT_REJ,            16#0022).
-define(READ_START_REQ,      16#0030).
-define(READ_START_CNF,      16#0031).
-define(READ_START_REJ,      16#0032).
-define(READ_PART_REQ,       16#0040).
-define(READ_PART_CNF,       16#0041).
-define(READ_PART_REJ,       16#0042).
-define(READ_DONE_IND,       16#0053).
-define(FORK_REQ,            16#0060).
-define(FORK_CNF,            16#0061).
-define(FORK_REJ,            16#0062).
-define(MERGE_REQ,           16#0070).
-define(MERGE_CNF,           16#0071).
-define(MERGE_REJ,           16#0072).
-define(UPDATE_REQ,          16#0080).
-define(UPDATE_CNF,          16#0081).
-define(UPDATE_REJ,          16#0082).
-define(MERGE_TRIVIAL_REQ,   16#0120).
-define(MERGE_TRIVIAL_CNF,   16#0121).
-define(MERGE_TRIVIAL_REJ,   16#0122).
-define(WRITE_TRUNC_REQ,     16#0090).
-define(WRITE_TRUNC_CNF,     16#0091).
-define(WRITE_TRUNC_REJ,     16#0092).
-define(WRITE_PART_REQ,      16#00A0).
-define(WRITE_PART_CNF,      16#00A1).
-define(WRITE_PART_REJ,      16#00A2).
-define(WRITE_COMMIT_REQ,    16#00B0).
-define(WRITE_COMMIT_CNF,    16#00B1).
-define(WRITE_COMMIT_REJ,    16#00B2).
-define(WRITE_ABORT_IND,     16#00C3).
-define(WATCH_ADD_REQ,       16#00D0).
-define(WATCH_ADD_CNF,       16#00D1).
-define(WATCH_ADD_REJ,       16#00D2).
-define(WATCH_REM_REQ,       16#00D3).
-define(WATCH_REM_CNF,       16#00D4).
-define(WATCH_REM_REJ,       16#00D5).
-define(WATCH_IND,           16#00D6).
-define(DELETE_UUID_REQ,     16#00E0).
-define(DELETE_REV_REQ,      16#00E1).
-define(DELETE_CNF,          16#00E2).
-define(DELETE_REJ,          16#00E3).
-define(REPLICATE_UUID_IND,  16#00F0).
-define(REPLICATE_REV_IND,   16#00F1).
-define(MOUNT_REQ,           16#0130).
-define(MOUNT_CNF,           16#0131).
-define(MOUNT_REJ,           16#0132).
-define(UNMOUNT_IND,         16#0123).
-define(PROGRESS_IND,        16#0146).

-define(WATCH_CAUSE_MOD, 0). % UUID has been modified
-define(WATCH_CAUSE_ADD, 1). % GUID appeared
-define(WATCH_CAUSE_REP, 2). % GUID was spread to another store
-define(WATCH_CAUSE_DIM, 3). % GUID removed from a store
-define(WATCH_CAUSE_REM, 4). % GUID has disappeared

-define(PROGRESS_TYPE_SYNC, 0).
-define(PROGRESS_TYPE_REP_UUID, 1).
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

		{rep_uuid, Uuid, Stores} ->
			BinStores = lists:foldl(
				fun(Store, Acc) -> <<Acc/binary, Store/binary>> end,
				<<>>,
				Stores),
			<<?PROGRESS_TYPE_REP_UUID:8, Num:16/little, Uuid/binary, BinStores/binary>>;

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

handle_info({watch, Cause, Type, Hash}, S) ->
	EncType = case Type of
		uuid -> 0;
		rev  -> 1
	end,
	Data = case Cause of
		modified    -> <<?WATCH_CAUSE_MOD:8, EncType:8, Hash/binary>>;
		appeared    -> <<?WATCH_CAUSE_ADD:8, EncType:8, Hash/binary>>;
		replicated  -> <<?WATCH_CAUSE_REP:8, EncType:8, Hash/binary>>;
		diminished  -> <<?WATCH_CAUSE_DIM:8, EncType:8, Hash/binary>>;
		disappeared -> <<?WATCH_CAUSE_REM:8, EncType:8, Hash/binary>>
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
			<<Uuid:16/binary>> = Body,
			spawn_link(fun () -> do_loopup(Uuid, RetPath) end),
			S;

		?STAT_REQ ->
			<<Rev:16/binary>> = Body,
			spawn_link(fun () -> do_stat(Rev, RetPath) end),
			S;

		?READ_START_REQ ->
			<<Rev:16/binary>> = Body,
			start_worker(S, fun(Cookie) -> do_start_read(Cookie, RetPath, Rev) end);

		?FORK_REQ ->
			<<Store:16/binary, StartRev:16/binary, Uti/binary>> = Body,
			start_worker(S, fun(Cookie) -> do_fork(Cookie, RetPath, Store, StartRev, Uti) end);

		?MERGE_REQ ->
			<<Uuid:16/binary, Body1/binary>> = Body,
			{StartRevs, Uti} = parse_revisions(Body1),
			start_worker(S, fun(Cookie) -> do_merge(Cookie, RetPath, Uuid, StartRevs, Uti) end);

		?MERGE_TRIVIAL_REQ ->
			<<Uuid:16/binary, DestRev:16/binary, Body1/binary>> = Body,
			{OtherRevs, <<>>} = parse_revisions(Body1),
			spawn_link(fun() -> do_merge_trivial(Uuid, DestRev, OtherRevs, RetPath) end),
			S;

		?UPDATE_REQ ->
			<<Uuid:16/binary, Rev:16/binary>> = Body,
			start_worker(S, fun(Cookie) -> do_update(Cookie, RetPath, Uuid, Rev) end);

		?WATCH_ADD_REQ ->
			<<EncType:8, Hash:16/binary>> = Body,
			Type = case EncType of
				0 -> uuid;
				1 -> rev
			end,
			change_monitor:watch(Type, Hash),
			send_reply(RetPath, ?WATCH_ADD_CNF, <<>>),
			S;

		?WATCH_REM_REQ ->
			<<EncType:8, Hash:16/binary>> = Body,
			Type = case EncType of
				0 -> uuid;
				1 -> rev
			end,
			change_monitor:unwatch(Type, Hash),
			send_reply(RetPath, ?WATCH_REM_CNF, <<>>),
			S;

		?DELETE_UUID_REQ ->
			<<Store:16/binary, Uuid:16/binary>> = Body,
			spawn_link(fun () -> do_delete_uuid(Store, Uuid, RetPath) end),
			S;

		?DELETE_REV_REQ ->
			<<Store:16/binary, Rev:16/binary>> = Body,
			spawn_link(fun () -> do_delete_rev(Store, Rev, RetPath) end),
			S;

		?REPLICATE_UUID_IND ->
			<<Uuid:16/binary, History:8, Body1/binary>> = Body,
			{Stores, <<>>} = parse_revisions(Body1),
			replicator:replicate_uuid(Uuid, Stores, as_boolean(History)),
			S;

		?REPLICATE_REV_IND ->
			<<Rev:16/binary, History:8, Body1/binary>> = Body,
			{Stores, <<>>} = parse_revisions(Body1),
			replicator:replicate_rev(Rev, Stores, as_boolean(History)),
			S;

		?MOUNT_REQ ->
			spawn_link(fun () -> do_mount(Body, RetPath) end),
			S;

		?UNMOUNT_IND ->
			do_unmount(Body),
			S;

		_ ->
			<<Cookie:32/little, Data/binary>> = Body,
			[Worker] = dict:fetch(Cookie, S#state.cookies),
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
		cookies = dict:append(Cookie, Worker, S#state.cookies),
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


do_loopup(Uuid, RetPath) ->
	Revs = broker:lookup(Uuid),
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
		{ok, Parts, Parents, Mtime, Uti, Volumes} ->
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
				ReplyParts/binary,
				ReplyParents/binary,
				ReplyVolumes/binary,
				Mtime:64/little,
				Uti/binary>>,
			send_reply(RetPath, ?STAT_CNF, Reply);

		error ->
			send_reply(RetPath, ?STAT_REJ, <<>>)
	end.


do_merge_trivial(Uuid, DestRev, OtherRevs, RetPath) ->
	case broker:merge_trivial(Uuid, DestRev, OtherRevs) of
		{ok, NewRev}     -> send_reply(RetPath, ?MERGE_TRIVIAL_CNF, NewRev);
		{error, _Reason} -> send_reply(RetPath, ?MERGE_TRIVIAL_REJ, <<>>)
	end.


do_start_read(Cookie, RetPath, Rev) ->
	case broker:read_start(Rev) of
		{ok, Reader} ->
			send_reply(RetPath, ?READ_START_CNF, <<Cookie:32/little>>),
			reader_loop(Cookie, Reader);

		{error, _} ->
			send_reply(RetPath, ?READ_START_REJ, <<>>)
	end.


reader_loop(Cookie, Reader) ->
	receive
		{?READ_PART_REQ, ReqData, RetPath} ->
			<<Part:4/binary, Offset:64/little, Length:32/little>> = ReqData,
			case broker:read_part(Reader, Part, Offset, Length) of
				{ok, Data} ->
					send_reply(RetPath, ?READ_PART_CNF, Data);
				eof ->
					send_reply(RetPath, ?READ_PART_CNF, <<>>);
				{error, _} ->
					send_reply(RetPath, ?READ_PART_REJ, <<>>)
			end,
			reader_loop(Cookie, Reader);

		{?READ_DONE_IND, _, _} ->
			broker:read_done(Reader);

		Else ->
			io:format("reader_loop: Invalid request: ~w~n", [Else]),
			reader_loop(Cookie, Reader)
	after
		?TIMEOUT ->
			io:format("reader_loop: timeout~n")
	end.


do_fork(Cookie, RetPath, Store, StartRev, Uti) ->
	case broker:fork(Store, StartRev, Uti) of
		{ok, Uuid, Writer} ->
			send_reply(RetPath, ?FORK_CNF, <<Uuid/bitstring, Cookie:32/little>>),
			writer_loop(Cookie, Writer);

		{error, _} ->
			send_reply(RetPath, ?FORK_REJ, <<>>)
	end.


do_merge(Cookie, RetPath, Uuid, StartRevs, Uti) ->
	Stores = lists:map(fun({_Guid, Ifc}) -> Ifc end, volman:stores()),
	case broker:merge(Stores, Uuid, StartRevs, Uti) of
		{ok, Writer} ->
			send_reply(RetPath, ?MERGE_CNF, <<Cookie:32/little>>),
			writer_loop(Cookie, Writer);

		{error, conflict} ->
			send_reply(RetPath, ?MERGE_REJ, <<0:8>>);

		{error, _} ->
			send_reply(RetPath, ?MERGE_REJ, <<1:8>>)
	end.


do_update(Cookie, RetPath, Uuid, Rev) ->
	case broker:update(Uuid, Rev) of
		{ok, Writer} ->
			send_reply(RetPath, ?UPDATE_CNF, <<Cookie:32/little>>),
			writer_loop(Cookie, Writer);

		{error, conflict} ->
			send_reply(RetPath, ?UPDATE_REJ, <<0:8>>);

		{error, _} ->
			send_reply(RetPath, ?UPDATE_REJ, <<1:8>>)
	end.


writer_loop(Cookie, Writer) ->
	receive
		{?WRITE_PART_REQ, ReqData, RetPath} ->
			<<Part:4/binary, Offset:64/little, Data/binary>> = ReqData,
			%io:format("WritePart: Part:~w Offset:~w Data:~w~n", [Part, Offset, Data]),
			Reply = case broker:write_part(Writer, Part, Offset, Data) of
				ok         -> ?WRITE_PART_CNF;
				{error, _} -> ?WRITE_PART_REJ
			end,
			send_reply(RetPath, Reply, <<>>),
			writer_loop(Cookie, Writer);

		{?WRITE_TRUNC_REQ, ReqData, RetPath} ->
			<<Part:4/binary, Offset:64/little>> = ReqData,
			%io:format("WriteTrunc: ~w Offset:~w~n", [Part, Offset]),
			Reply = case broker:write_trunc(Writer, Part, Offset) of
				ok         -> ?WRITE_TRUNC_CNF;
				{error, _} -> ?WRITE_TRUNC_REJ
			end,
			send_reply(RetPath, Reply, <<>>),
			writer_loop(Cookie, Writer);

		{?WRITE_COMMIT_REQ, _, RetPath} ->
			%io:format("WriteCommit: "),
			case broker:write_commit(Writer) of
				{ok, Rev} ->
					%io:format("~s~n", [util:bin_to_hexstr(Rev)]),
					send_reply(RetPath, ?WRITE_COMMIT_CNF, Rev);
				{error, conflict} ->
					%io:format("conflict~n"),
					send_reply(RetPath, ?WRITE_COMMIT_REJ, <<0:8>>);
				{error, _} ->
					%io:format("error~n"),
					send_reply(RetPath, ?WRITE_COMMIT_REJ, <<1:8>>)
			end;

		{?WRITE_ABORT_IND, _, _} ->
			broker:write_abort(Writer);

		Else ->
			io:format("writer_loop: Invalid request: ~w~n", [Else]),
			writer_loop(Cookie, Writer)
	after
		?TIMEOUT ->
			io:format("writer_loop: timeout~n")
	end.


do_delete_uuid(Store, Uuid, RetPath) ->
	case broker:delete_uuid(Store, Uuid) of
		ok ->
			send_reply(RetPath, ?DELETE_CNF, <<>>);
		{error, _} ->
			send_reply(RetPath, ?DELETE_REJ, <<>>)
	end.


do_delete_rev(Store, Rev, RetPath) ->
	case broker:delete_rev(Store, Rev) of
		ok ->
			send_reply(RetPath, ?DELETE_CNF, <<>>);
		{error, _} ->
			send_reply(RetPath, ?DELETE_REJ, <<>>)
	end.


do_mount(Store, RetPath) ->
	Id = list_to_atom(binary_to_list(Store)),
	Reply = case lists:keysearch(Id, 1, volman:enum()) of
		{value, {Id, _Descr, _Guid, Tags}} ->
			case proplists:is_defined(removable, Tags) of
				true ->
					case volman:mount(Id) of
						{ok, _}    -> ?MOUNT_CNF;
						{error, _} -> ?MOUNT_REJ
					end;

				false ->
					?MOUNT_REJ
			end;

		false ->
			?MOUNT_REJ
	end,
	send_reply(RetPath, Reply, <<>>).


do_unmount(Store) ->
	Id = list_to_atom(binary_to_list(Store)),
	case lists:keysearch(Id, 1, volman:enum()) of
		{value, {Id, _Descr, _Guid, Tags}} ->
			case proplists:is_defined(removable, Tags) of
				true  -> volman:unmount(Id);
				false -> error
			end;

		false ->
			error
	end.

