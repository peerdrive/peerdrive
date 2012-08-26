%% PeerDrive
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

-module(peerdrive_broker_io).

-export([peek/2, create/3, fork/3, update/4, resume/4]).
-export([read/4, write/4, truncate/3, get_parents/1, merge/4, rebase/2,
	get_type/1, set_type/2, commit/1, commit/2, suspend/1, suspend/2, close/1,
	get_flags/1, set_flags/2]).

-include("utils.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public broker operations...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

peek(Store, Rev) ->
	case peerdrive_store:peek(Store, Rev) of
		{ok, Handle} ->
			{ok, {Store, Handle}};
		Error ->
			Error
	end.

create(Store, Type, Creator) ->
	case peerdrive_store:create(Store, Type, Creator) of
		{ok, Doc, Handle} ->
			{ok, Doc, {Store, Handle}};
		Error ->
			Error
	end.

fork(Store, Rev, Creator) ->
	case peerdrive_store:fork(Store, Rev, Creator) of
		{ok, Doc, Handle} ->
			{ok, Doc, {Store, Handle}};
		Error ->
			Error
	end.

update(Store, Doc, Rev, Creator) ->
	case peerdrive_store:update(Store, Doc, Rev, Creator) of
		{ok, Handle} ->
			{ok, {Store, Handle}};
		Error ->
			Error
	end.

resume(Store, Doc, PreRev, Creator) ->
	case peerdrive_store:resume(Store, Doc, PreRev, Creator) of
		{ok, Handle} ->
			{ok, {Store, Handle}};
		Error ->
			Error
	end.

read({_, Handle}, Part, Offset, Length) ->
	peerdrive_store:read(Handle, Part, Offset, Length).

write({_, Handle}, Part, Offset, Data) when Part == <<"FILE">>;
                                            Part == <<"META">>;
                                            Part == <<"PDSD">>;
                                            Part == <<"ENCR">> ->
	peerdrive_store:write(Handle, Part, Offset, Data);

write(_, _, _, _) ->
	{error, einval}.

truncate({_, Handle}, Part, Offset) when Part == <<"FILE">>;
                                         Part == <<"META">>;
                                         Part == <<"PDSD">>;
                                         Part == <<"ENCR">> ->
	peerdrive_store:truncate(Handle, Part, Offset);

truncate(_, _, _) ->
	{error, einval}.

get_parents({_, Handle}) ->
	peerdrive_store:get_parents(Handle).

merge({DstStore, Handle}, SrcStore, Rev, Options) ->
	do_merge(Handle, DstStore, SrcStore, Rev, Options).

rebase({_, Handle}, Rev) ->
	do_rebase(Handle, Rev).

get_flags({_, Handle}) ->
	peerdrive_store:get_flags(Handle).

set_flags({_, Handle}, Flags) ->
	peerdrive_store:set_flags(Handle, Flags).

get_type({_, Handle}) ->
	peerdrive_store:get_type(Handle).

set_type({_, Handle}, Type) ->
	peerdrive_store:set_type(Handle, Type).

commit({_, Handle}) ->
	do_commit(Handle, fun() -> peerdrive_store:commit(Handle) end).

commit({_, Handle}, Comment) ->
	do_commit(Handle, fun() -> peerdrive_store:commit(Handle, Comment) end).

suspend({_, Handle}) ->
	do_commit(Handle, fun() -> peerdrive_store:suspend(Handle) end).

suspend({_, Handle}, Comment) ->
	do_commit(Handle, fun() -> peerdrive_store:suspend(Handle, Comment) end).

close({_, Handle}) ->
	peerdrive_store:close(Handle).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_merge(Handle, DstStore, SrcStore, Rev, Options) ->
	case peerdrive_store:contains(SrcStore, Rev) of
		true ->
			case peerdrive_store:get_parents(Handle) of
				{ok, Parents} ->
					% TODO: check if the new Rev supersedes any present parent
					NewParents = lists:usort([Rev | Parents]),
					case peerdrive_store:set_parents(Handle, NewParents) of
						ok ->
							peerdrive_replicator:replicate_rev_sync(SrcStore, Rev,
								DstStore, Options),
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
	peerdrive_store:set_parents(Handle, [Rev]).


do_commit(Handle, Fun) ->
	case do_commit_prepare(Handle) of
		ok    -> Fun();
		Error -> Error
	end.


do_commit_prepare(Handle) ->
	case read_rev_refs(Handle) of
		{ok, DocRefs, RevRefs} ->
			peerdrive_store:set_links(Handle, DocRefs, RevRefs);
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
			[<<"PDSD">>, <<"META">>, <<"ENCR">>]),
		{ok, sets:to_list(DocSet), sets:to_list(RevSet)}
	catch
		throw:Term -> Term
	end.


read_rev_part(Handle, Part) ->
	case peerdrive_store:read(Handle, Part, 0, 16#1000000) of
		{ok, Binary} ->
			case catch peerdrive_struct:decode(Binary) of
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


read_rev_extract(Data) when ?IS_GB_TREE(Data) ->
	lists:foldl(
		fun(Value, {AccDocs, AccRevs}) ->
			{Docs, Revs} = read_rev_extract(Value),
			{sets:union(Docs, AccDocs), sets:union(Revs, AccRevs)}
		end,
		{sets:new(), sets:new()},
		gb_trees:values(Data));

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

