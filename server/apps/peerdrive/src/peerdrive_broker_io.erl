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
-export([read/4, write/4, truncate/3, merge/4, rebase/2, set_type/2, commit/1,
	commit/2, suspend/1, suspend/2, close/1, set_flags/2, get_data/2,
	set_data/3, fstat/1, set_mtime/3]).

-include("store.hrl").
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

get_data({_, Handle}, Selector) ->
	peerdrive_store:get_data(Handle, Selector);
get_data(_, _) ->
	{error, ebadf}.

set_data({_, Handle}, Selector, Data) ->
	peerdrive_store:set_data(Handle, Selector, Data);
set_data(_, _, _) ->
	{error, ebadf}.

read({_, Handle}, Part, Offset, Length) ->
	peerdrive_store:read(Handle, Part, Offset, Length);
read(_, _, _, _) ->
	{error, ebadf}.

write({_, Handle}, Attachment, Offset, Data) ->
	peerdrive_store:write(Handle, Attachment, Offset, Data);
write(_, _, _, _) ->
	{error, ebadf}.

truncate({_, Handle}, Attachment, Offset) ->
	peerdrive_store:truncate(Handle, Attachment, Offset);
truncate(_, _, _) ->
	{error, ebadf}.

fstat({_, Handle}) ->
	peerdrive_store:fstat(Handle);
fstat(_) ->
	{error, ebadf}.

merge({DstStore, Handle}, SrcStore, Rev, Options) ->
	do_merge(Handle, DstStore, SrcStore, Rev, Options);
merge(_, _, _, _) ->
	{error, ebadf}.

rebase({_, Handle}, Rev) ->
	do_rebase(Handle, Rev);
rebase(_, _) ->
	{error, ebadf}.

set_flags({_, Handle}, Flags) ->
	peerdrive_store:set_flags(Handle, Flags);
set_flags(_, _) ->
	{error, ebadf}.

set_type({_, Handle}, Type) ->
	peerdrive_store:set_type(Handle, Type);
set_type(_, _) ->
	{error, ebadf}.

set_mtime({_, Handle}, Attachment, MTime) ->
	peerdrive_store:set_mtime(Handle, Attachment, MTime);
set_mtime(_, _, _) ->
	{error, ebadf}.

commit({_, Handle}) ->
	peerdrive_store:commit(Handle);
commit(_) ->
	{error, ebadf}.

commit({_, Handle}, Comment) ->
	peerdrive_store:commit(Handle, Comment);
commit(_, _) ->
	{error, ebadf}.

suspend({_, Handle}) ->
	peerdrive_store:suspend(Handle);
suspend(_) ->
	{error, ebadf}.

suspend({_, Handle}, Comment) ->
	peerdrive_store:suspend(Handle, Comment);
suspend(_, _) ->
	{error, ebadf}.

close({_, Handle}) ->
	peerdrive_store:close(Handle);
close(Handle) ->
	peerdrive_replicator:close(Handle).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_merge(Handle, DstStore, SrcStore, Rev, Options) ->
	case peerdrive_store:contains(SrcStore, Rev) of
		true ->
			case peerdrive_store:fstat(Handle) of
				{ok, #rev{parents=Parents}} ->
					% TODO: check if the new Rev supersedes any present parent
					NewParents = lists:usort([Rev | Parents]),
					case peerdrive_replicator:replicate_rev_sync(SrcStore, Rev, DstStore, Options) of
						{ok, RepHandle} ->
							try
								peerdrive_store:set_parents(Handle, NewParents)
							after
								peerdrive_replicator:close(RepHandle)
							end;
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

