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

-module(hotchpotch_replicator_copy).
-export([put_rev/3]).

-include("store.hrl").


% returns ok | {error, Reason}
put_rev(SourceStores, DestStore, Rev) ->
	case get_source(Rev, SourceStores) of
		{ok, SourceStore} ->
			replicate(Rev, SourceStore, DestStore);

		error ->
			{error, enoent}
	end.


get_source(_Rev, []) ->
	error;

get_source(Rev, [{_Guid, Pid} | Remaining]) ->
	case hotchpotch_store:contains(Pid, Rev) of
		true -> {ok, Pid};
		false -> get_source(Rev, Remaining)
	end.


replicate(Rev, SourceStore, DestStore) ->
	case hotchpotch_store:stat(SourceStore, Rev) of
		{ok, Stat} ->
			Revision = #revision{
				flags = Stat#rev_stat.flags,
				parts = lists:sort(
					lists:map(
						fun({FCC, _Size, Hash}) -> {FCC, Hash} end,
						Stat#rev_stat.parts)),
				parents   = lists:sort(Stat#rev_stat.parents),
				mtime     = Stat#rev_stat.mtime,
				type      = Stat#rev_stat.type,
				creator   = Stat#rev_stat.creator,
				doc_links = Stat#rev_stat.doc_links,
				rev_links = Stat#rev_stat.rev_links
			},
			case hotchpotch_store:put_rev_start(DestStore, Rev, Revision) of
				ok ->
					ok;

				{ok, MissingParts, Importer} ->
					copy_parts(Rev, SourceStore, Importer, MissingParts);

				{error, Reason} ->
					{error, Reason}
			end;

		Error ->
			Error
	end.


copy_parts(Rev, SourceStore, Importer, Parts) ->
	case hotchpotch_store:peek(SourceStore, Rev) of
		{ok, Reader} ->
			case copy_parts_loop(Parts, Reader, Importer) of
				ok ->
					hotchpotch_store:close(Reader),
					hotchpotch_store:put_rev_commit(Importer);

				{error, Reason} ->
					hotchpotch_store:close(Reader),
					hotchpotch_store:put_rev_abort(Importer),
					{error, Reason}
			end;

		{error, Reason} ->
			hotchpotch_store:put_rev_abort(Importer),
			{error, Reason}
	end.

copy_parts_loop([], _Reader, _Importer) ->
	ok;
copy_parts_loop([Part|Remaining], Reader, Importer) ->
	case copy(Part, Reader, Importer) of
		ok   -> copy_parts_loop(Remaining, Reader, Importer);
		Else -> Else
	end.

copy(Part, Reader, Importer) ->
	copy_loop(Part, Reader, Importer, 0).

copy_loop(Part, Reader, Importer, Pos) ->
	case hotchpotch_store:read(Reader, Part, Pos, 16#100000) of
		{ok, Data} ->
			case hotchpotch_store:put_rev_part(Importer, Part, Data) of
				ok ->
					if
						size(Data) == 16#100000 ->
							copy_loop(Part, Reader, Importer, Pos+16#100000);
						true ->
							ok
					end;

				{error, Reason} ->
					{error, Reason}
			end;

		eof ->
			ok;

		{error, Reason} ->
			{error, Reason}
	end.

