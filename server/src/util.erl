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

-module(util).
-export([get_time/0, bin_to_hexstr/1, hexstr_to_bin/1, build_path/2, gen_tmp_name/1]).
-export([read_rev/2, read_rev_struct/2]).

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
	RootPath ++ "/_" ++ util:bin_to_hexstr(crypto:rand_bytes(8)).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Document/Revision reading
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% returns {ok, Data} | {error, Reason}
read_rev(Rev, Part) ->
	case broker:read_start(Rev) of
		{ok, Reader} ->
			try
				read_rev_loop(Reader, Part, 0, <<>>)
			after
				broker:read_done(Reader)
			end;

		{error, Reason} ->
			{error, Reason}
	end.


read_rev_loop(Reader, Part, Offset, Acc) ->
	Length = 16#10000,
	case broker:read_part(Reader, Part, Offset, Length) of
		{ok, Data} ->
			read_rev_loop(Reader, Part, Offset+Length, <<Acc/binary, Data/binary>>);
		eof ->
			{ok, Acc};
		{error, Reason} ->
			{error, Reason}
	end.


read_rev_struct(Rev, Part) ->
	case read_rev(Rev, Part) of
		{ok, Data} ->
			case catch struct:decode(Data) of
				{'EXIT', _Reason} ->
					{error, econvert};
				Struct ->
					{ok, Struct}
			end;

		{error, Reason} ->
			{error, Reason}
	end.


get_time() ->
	{MegaSecs, Secs, _} = now(),
	(MegaSecs*1000000+Secs).

