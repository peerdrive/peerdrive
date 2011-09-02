%% Hotchpotch
%% Copyright (C) 2011  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
%%
%% This program is free software: you can redistribute_write it and/or modify
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

-module(hotchpotch_broker_io).

-export([peek/2, create/3, fork/3, update/4, resume/4]).
-export([read/4, write/4, truncate/3, get_parents/1, merge/4, rebase/2,
	get_type/1, set_type/2, commit/1, suspend/1, close/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public broker operations...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

peek(Store, Rev) ->
	case hotchpotch_store:peek(Store, Rev) of
		{ok, Handle} ->
			{ok, {Store, Handle}};
		Error ->
			Error
	end.

create(Store, Type, Creator) ->
	case hotchpotch_store:create(Store, Type, Creator) of
		{ok, Doc, Handle} ->
			{ok, Doc, {Store, Handle}};
		Error ->
			Error
	end.

fork(Store, Rev, Creator) ->
	case hotchpotch_store:fork(Store, Rev, Creator) of
		{ok, Doc, Handle} ->
			{ok, Doc, {Store, Handle}};
		Error ->
			Error
	end.

update(Store, Doc, Rev, Creator) ->
	case hotchpotch_store:update(Store, Doc, Rev, Creator) of
		{ok, Handle} ->
			{ok, {Store, Handle}};
		Error ->
			Error
	end.

resume(Store, Doc, PreRev, Creator) ->
	case hotchpotch_store:resume(Store, Doc, PreRev, Creator) of
		{ok, Handle} ->
			{ok, {Store, Handle}};
		Error ->
			Error
	end.

read({_, Handle}, Part, Offset, Length) ->
	hotchpotch_store:read(Handle, Part, Offset, Length).

write({_, Handle}, Part, Offset, Data) ->
	hotchpotch_store:write(Handle, Part, Offset, Data).

truncate({_, Handle}, Part, Offset) ->
	hotchpotch_store:truncate(Handle, Part, Offset).

get_parents({_, Handle}) ->
	hotchpotch_store:get_parents(Handle).

merge({DstStore, Handle}, SrcStore, Rev, Depth) ->
	do_merge(Handle, DstStore, SrcStore, Rev, Depth).

rebase({_, Handle}, Rev) ->
	do_rebase(Handle, Rev).

get_type({_, Handle}) ->
	hotchpotch_store:get_type(Handle).

set_type({_, Handle}, Type) ->
	hotchpotch_store:set_type(Handle, Type).

commit({_, Handle}) ->
	do_commit(Handle, fun hotchpotch_store:commit/1).

suspend({_, Handle}) ->
	do_commit(Handle, fun hotchpotch_store:suspend/1).

close({_, Handle}) ->
	hotchpotch_store:close(Handle).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_merge(Handle, DstStore, SrcStore, Rev, Depth) ->
	case hotchpotch_store:contains(SrcStore, Rev) of
		true ->
			case hotchpotch_store:get_parents(Handle) of
				{ok, Parents} ->
					% TODO: check if the new Rev supersedes any present parent
					NewParents = lists:usort([Rev | Parents]),
					case hotchpotch_store:set_parents(Handle, NewParents) of
						ok ->
							% FIXME: _sync?
							hotchpotch_replicator:replicate_rev(SrcStore, Rev,
								DstStore, Depth),
							ok;
						{error, _} = Error ->
							Error
					end;

				{error, _} = Error ->
					Error
			end;

		false ->
			{error, enoent}
	end.


do_rebase(Handle, Rev) ->
	% TODO: only remove superseded revisions
	hotchpotch_store:set_parents(Handle, [Rev]).


do_commit(Handle, Fun) ->
	case do_commit_prepare(Handle) of
		ok    -> Fun(Handle);
		Error -> Error
	end.


do_commit_prepare(Handle) ->
	case read_rev_refs(Handle) of
		{ok, DocRefs, RevRefs} ->
			hotchpotch_store:set_links(Handle, DocRefs, RevRefs);
		Error ->
			Error
	end.


read_rev_refs(Handle) ->
	try
		{DocSet, RevSet} = lists:foldl(
			fun(FourCC, {AccDocRefs, AccRevRefs}) ->
				{NewDR, NewRR} = read_rev_extract(read_rev_part(Handle,
					FourCC)),
				{sets:union(NewDR, AccDocRefs), sets:union(NewRR, AccRevRefs)}
			end,
			{sets:new(), sets:new()},
			[<<"HPSD">>, <<"META">>]),
		{ok, sets:to_list(DocSet), sets:to_list(RevSet)}
	catch
		throw:Term -> Term
	end.


read_rev_part(Handle, Part) ->
	case hotchpotch_store:read(Handle, Part, 0, 16#1000000) of
		{ok, Binary} ->
			case catch hotchpotch_struct:decode(Binary) of
				{'EXIT', _Reason} ->
					[];
				Struct ->
					Struct
			end;

		{error, enoent} ->
			[];

		Error ->
			throw(Error)
	end.


read_rev_extract(Data) when is_record(Data, dict, 9) ->
	dict:fold(
		fun(_Key, Value, {AccDocs, AccRevs}) ->
			{Docs, Revs} = read_rev_extract(Value),
			{sets:union(Docs, AccDocs), sets:union(Revs, AccRevs)}
		end,
		{sets:new(), sets:new()},
		Data);

read_rev_extract(Data) when is_list(Data) ->
	lists:foldl(
		fun(Value, {AccDocs, AccRevs}) ->
			{Docs, Revs} = read_rev_extract(Value),
			{sets:union(Docs, AccDocs), sets:union(Revs, AccRevs)}
		end,
		{sets:new(), sets:new()},
		Data);

read_rev_extract({dlink, Doc}) ->
	{sets:from_list([Doc]), sets:new()};

read_rev_extract({rlink, Rev}) ->
	{sets:new(), sets:from_list([Rev])};

read_rev_extract(_) ->
	{sets:new(), sets:new()}.

