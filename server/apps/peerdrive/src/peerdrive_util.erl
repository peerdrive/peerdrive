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
-export([read_doc_struct/3, read_rev/3, read_rev_struct/3, hash_file/1, fixup_file/1]).
-export([walk/2, read_doc_file_name/2]).
-export([merkle/1, merkle_init/0, merkle_update/2, merkle_final/1, make_bin_16/1]).

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
	case read_doc_struct(Store, Doc, <<"PDSD">>) of
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
	Meta = case read_doc_struct(Store, Doc, <<"META">>) of
		{ok, Value1} when ?IS_GB_TREE(Value1) ->
			Value1;
		{ok, _} ->
			throw({error, eio});
		{error, _} = ReadError ->
			throw(ReadError)
	end,
	case meta_read_entry(Meta, [<<"org.peerdrive.annotation">>, <<"title">>]) of
		{ok, Title} when is_binary(Title) ->
			Title;
		{ok, _} ->
			throw({error, eio});
		error ->
			throw({error, eio})
	end.


meta_read_entry(Meta, []) ->
	{ok, Meta};
meta_read_entry(Meta, [Step|Path]) when ?IS_GB_TREE(Meta) ->
	case gb_trees:lookup(Step, Meta) of
		{value, Value} -> meta_read_entry(Value, Path);
		none           -> error
	end;
meta_read_entry(_Meta, _Path) ->
	error.


read_doc_file_name(Store, Doc) ->
	try
		{ok, read_file_name(Store, Doc)}
	catch
		throw:Error -> Error
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Document/Revision reading
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

read_doc_struct(Store, Doc, Part) ->
	case peerdrive_store:lookup(Store, Doc) of
		{ok, Rev, _} -> read_rev_struct(Store, Rev, Part);
		error        -> {error, enoent}
	end.


% returns {ok, Data} | {error, Reason}
read_rev(Store, Rev, Part) ->
	case peerdrive_broker:peek(Store, Rev) of
		{ok, Reader} ->
			try
				read_rev_loop(Reader, Part, 0, <<>>)
			after
				peerdrive_broker:close(Reader)
			end;

		{error, Reason} ->
			{error, Reason}
	end.


read_rev_loop(Reader, Part, Offset, Acc) ->
	Length = 16#10000,
	case peerdrive_broker:read(Reader, Part, Offset, Length) of
		{ok, <<>>} ->
			{ok, Acc};
		{ok, Data} ->
			read_rev_loop(Reader, Part, Offset+size(Data), <<Acc/binary, Data/binary>>);
		{error, Reason} ->
			{error, Reason}
	end.


read_rev_struct(Store, Rev, Part) ->
	case read_rev(Store, Rev, Part) of
		{ok, Data} ->
			case catch peerdrive_struct:decode(Data) of
				{'EXIT', _Reason} ->
					{error, einval};
				Struct ->
					{ok, Struct}
			end;

		{error, Reason} ->
			{error, Reason}
	end.


get_time() ->
	% Thanks to http://www.epochconverter.com/ :)
	(calendar:datetime_to_gregorian_seconds(calendar:universal_time()) -
	719528*24*3600) * 1000000.


% returns {ok, Sha1} | {error, Reason}
hash_file(File) ->
	{ok, _} = file:position(File, 0),
	hash_file_loop(File, merkle_init()).

hash_file_loop(File, Ctx1) ->
	case file:read(File, 16#100000) of
		{ok, Data} ->
			Ctx2 = merkle_update(Ctx1, Data),
			hash_file_loop(File, Ctx2);
		eof ->
			{ok, merkle_final(Ctx1)};
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


merkle(Data) ->
	Ctx1 = merkle_init(),
	Ctx2 = merkle_update(Ctx1, Data),
	merkle_final(Ctx2).


merkle_init() ->
	{<<>>, []}.


merkle_update({Buffer, HashTree}, Data) when size(Buffer) >= 4096 ->
	<<Block:4096/binary, Rest/binary>> = Buffer,
	% TODO: benchmark if constructing complete binary first is faster
	Ctx1 = crypto:sha_init(),
	Ctx2 = crypto:sha_update(Ctx1, <<0>>),
	Ctx3 = crypto:sha_update(Ctx2, Block),
	NewHashTree = merkle_push(crypto:sha_final(Ctx3), HashTree),
	merkle_update({Rest, NewHashTree}, Data);

merkle_update(State, <<>>) ->
	State;

merkle_update({<<>>, HashTree}, Data) ->
	merkle_update({Data, HashTree}, <<>>);

merkle_update({Buffer, HashTree}, Data) ->
	merkle_update({<<Buffer/binary, Data/binary>>, HashTree}, <<>>).


merkle_push(Block, []) ->
	[Block];

merkle_push(Block, [empty | Root]) ->
	[Block | Root];

merkle_push(Block, [Sibling | Root]) ->
	[empty | merkle_push(crypto:sha(<<1, Sibling/binary, Block/binary>>), Root)].


merkle_final({<<>>, []}) ->
	crypto:sha(<<0>>);

merkle_final({<<>>, HashTree}) ->
	merkle_finalize(HashTree);

merkle_final({Remaining, HashTree}) ->
	FinalTree = merkle_push(crypto:sha(<<0, Remaining/binary>>), HashTree),
	merkle_finalize(FinalTree).


merkle_finalize([Root]) ->
	Root;

merkle_finalize([empty | Root]) ->
	merkle_finalize(Root);

merkle_finalize([Partial, empty | Root]) ->
	merkle_finalize([Partial | Root]);

merkle_finalize([Partial, Sibling | Root]) ->
	merkle_finalize([crypto:sha(<<1, Sibling/binary, Partial/binary>>) | Root]).


make_bin_16(Bin) when size(Bin) == 16 ->
	Bin;

make_bin_16(Bin) when size(Bin) > 16 ->
	binary_part(Bin, 0, 16);

make_bin_16(Bin) ->
	<<Bin/binary, 0:((16-size(Bin))*8)>>.

