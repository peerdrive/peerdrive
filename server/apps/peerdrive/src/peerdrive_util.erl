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

-module(peerdrive_util).
-export([get_time/0, bin_to_hexstr/1, hexstr_to_bin/1, build_path/2, gen_tmp_name/1]).
-export([read_doc_struct/3, read_rev_struct/3, hash_file/1, fixup_file/1]).
-export([walk/2, read_doc_file_name/2]).
-export([folder_link/3]).
-export([cfg_sys_daemon/0, cfg_app_dir/0, cfg_mnt_dir/0, cfg_run_dir/0]).

-include("utils.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Hex conversions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

hex(N) when N < 10 ->
    $0+N;

hex(N) when N >= 10, N < 16 ->
    $a+(N-10).


to_hex(N) when N < 256 ->
    [hex(N div 16), hex(N rem 16)].


list_to_hexstr([]) ->
    [];

list_to_hexstr([H|T]) ->
    to_hex(H) ++ list_to_hexstr(T).


bin_to_hexstr(Bin) ->
    list_to_hexstr(binary_to_list(Bin)).


hexstr_to_bin(S) ->
	hexstr_to_bin(S, []).


hexstr_to_bin([], Acc) ->
	list_to_binary(lists:reverse(Acc));

hexstr_to_bin([X,Y|T], Acc) ->
	{ok, [V], []} = io_lib:fread("~16u", [X,Y]),
	hexstr_to_bin(T, [V | Acc]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Path utilities
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

build_path(RootPath, Hash) ->
	<<Modulo:8, Body/binary>> = Hash,
	RootPath ++ "/"
	++ to_hex(Modulo) ++ "/"
	++ bin_to_hexstr(Body).


gen_tmp_name(RootPath) ->
	RootPath ++ "/_" ++ peerdrive_util:bin_to_hexstr(crypto:rand_bytes(8)).


walk(Store, Path) ->
	Doc = peerdrive_store:guid(Store),
	walk_step(Store, Doc, re:split(Path, "/")).


walk_step(_Store, Doc, []) ->
	{ok, Doc};

walk_step(Store, Doc, [File | Path]) ->
	case read_doc_struct(Store, Doc, <<"/org.peerdrive.folder">>) of
		{ok, Folder} ->
			case find_folder_entry(Store, Folder, File) of
				{ok, Child} ->
					walk_step(Store, Child, Path);
				{error, _} = Error ->
					Error
			end;

		{error, _} = Error ->
			Error
	end.


find_folder_entry(_Store, [], _Name) ->
	{error, enoent};

find_folder_entry(Store, [Entry | Rest], Name) when ?IS_GB_TREE(Entry) ->
	case gb_trees:lookup(<<"">>, Entry) of
		{value, {dlink, Doc}} ->
			try read_file_name(Store, Doc) of
				Name ->
					{ok, Doc};
				_ ->
					find_folder_entry(Store, Rest, Name)
			catch
				throw:{error, _} ->
					find_folder_entry(Store, Rest, Name)
			end;

		_ ->
			find_folder_entry(Store, Rest, Name)
	end;

find_folder_entry(Store, [_ | Rest], Name) ->
	find_folder_entry(Store, Rest, Name).


read_file_name(Store, Doc) ->
	case read_doc_struct(Store, Doc, <<"/org.peerdrive.annotation/title">>) of
		{ok, Title} when is_binary(Title) ->
			Title;
		{ok, _} ->
			throw({error, eio});
		{error, _} = ReadError ->
			throw(ReadError)
	end.


read_doc_file_name(Store, Doc) ->
	try
		{ok, read_file_name(Store, Doc)}
	catch
		throw:Error -> Error
	end.


folder_link(Store, Folder, Doc) ->
	case peerdrive_store:lookup(Store, Folder) of
		{ok, Rev, _} ->
			case read_rev_struct(Store, Rev, <<"/org.peerdrive.folder">>) of
				{ok, Dir} ->
					Entry = gb_trees:from_orddict([{<<"">>, {dlink, Doc}}]),
					NewDir = [Entry | Dir],
					write_rev_struct(Store, Folder, Rev, <<"/org.peerdrive.folder">>, NewDir);

				Error ->
					Error
			end;

		{error, _} = Error ->
			Error
	end.


write_rev_struct(Store, Doc, Rev, Part, Data) ->
	case peerdrive_broker:update(Store, Doc, Rev, undefined) of
		{ok, Handle} ->
			try
				Raw = peerdrive_struct:encode(Data),
				case peerdrive_broker:set_data(Handle, Part, Raw) of
					ok ->
						case peerdrive_broker:commit(Handle) of
							{ok, _NewRev} ->
								ok;
							Error ->
								Error
						end;

					Error ->
						Error
				end
			after
				peerdrive_broker:close(Handle)
			end;

		Error ->
			Error
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Global configuration
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

cfg_sys_daemon() ->
	case init:get_argument(system) of
		{ok, _} -> true;
		error   -> false
	end.


cfg_app_dir() ->
	case os:type() of
		{unix, _} ->
			Home = os:getenv("HOME"),
			case cfg_sys_daemon() of
				false -> filename:join(Home, ".peerdrive");
				true  -> Home
			end;

		{win32, _} ->
			cfg_win32_app_dir()
	end.


cfg_mnt_dir() ->
	case cfg_sys_daemon() of
		false ->
			Home = case os:type() of
				{unix, _} -> os:getenv("HOME");
				{win32, _} -> os:getenv("USERPROFILE")
			end,
			filename:join(Home, "PeerDrive");
		true ->
			case os:type() of
				{unix, _} -> "/media/peerdrive";
				{win32, _} -> "P:\\"
			end
	end.


cfg_run_dir() ->
	case os:type() of
		{unix, _} ->
			case init:get_argument(system) of
				{ok, [[RunDir]]} -> RunDir;
				error -> "/tmp/peerdrive-" ++ os:getenv("USER")
			end;
		{win32, _} ->
			cfg_win32_app_dir()
	end.


cfg_win32_app_dir() ->
	{ok, Reg} = win32reg:open([read]),
	case cfg_sys_daemon() of
		false ->
			ok = win32reg:change_key(Reg, "\\hkcu\\Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\Shell Folders"),
			{ok, BasePath} = win32reg:value(Reg, "Local AppData");
		true ->
			ok = win32reg:change_key(Reg, "\\hklm\\SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Explorer\\Shell Folders"),
			{ok, BasePath} = win32reg:value(Reg, "Common AppData")
	end,
	ok = win32reg:close(Reg),
	filename:join(win32reg:expand(BasePath),  "PeerDrive").


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Document/Revision reading
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

read_doc_struct(Store, Doc, Selector) ->
	case peerdrive_store:lookup(Store, Doc) of
		{ok, Rev, _} -> read_rev_struct(Store, Rev, Selector);
		Error        -> Error
	end.


read_rev_struct(Store, Rev, Selector) ->
	case peerdrive_broker:peek(Store, Rev) of
		{ok, Reader} ->
			try
				case peerdrive_broker:get_data(Reader, Selector) of
					{ok, Data} -> {ok, peerdrive_struct:decode(Data)};
					Error      -> Error
				end
			after
				peerdrive_broker:close(Reader)
			end;

		{error, _Reason} = Error ->
			Error
	end.


get_time() ->
	Now = {_, _, Micro} = os:timestamp(),
	Utc = calendar:now_to_universal_time(Now),
	% Thanks to http://www.epochconverter.com/ :)
	(calendar:datetime_to_gregorian_seconds(Utc) - 719528*24*3600) * 1000000 + Micro.


% returns {ok, FileSize, Sha1} | {error, Reason}
hash_file(File) ->
	{ok, _} = file:position(File, 0),
	hash_file_loop(File, 0, peerdrive_crypto:merkle_init()).

hash_file_loop(File, AccSize, Ctx1) ->
	case file:read(File, 16#100000) of
		{ok, Data} ->
			Ctx2 = peerdrive_crypto:merkle_update(Ctx1, Data),
			hash_file_loop(File, AccSize + size(Data), Ctx2);
		eof ->
			{ok, AccSize, peerdrive_crypto:merkle_final(Ctx1)};
		Else ->
			fixup_file(Else)
	end.


fixup_file({error, Reason} = Error) ->
	case Reason of
		badarg -> {error, einval};
		terminated -> {error, eio};
		system_limit -> {error, emfile};
		_ -> Error
	end;

fixup_file(Ok) ->
	Ok.

