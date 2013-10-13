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

-module(peerdrive_broker).

-export([
	create/3, delete_rev/2, delete_doc/3, forget/3, fork/3, fstat/1,
	lookup_doc/2, lookup_rev/2, read/4, peek/2, replicate_rev/4,
	replicate_doc/4, resume/4, set_flags/2, set_type/2, stat/2, suspend/1,
	suspend/2, forward_doc/6, update/4, close/1, commit/1, commit/2, write/4,
	truncate/3, rebase/2, merge/4, get_data/2, set_data/3, get_links/2,
	set_mtime/3]).

-include("store.hrl").

%% @doc Lookup a document.
%%
%% Returns any known current revisions and any pending preliminary revisions of
%% the document. Searches on the given stores or on all mounted stores if no
%% store was specified.
%%
%% @spec lookup_doc(Doc, Stores) -> {[{Rev, [Store]}], [{PreRev, [Store]}]}
%%       Doc, Rev, PreRev, Store = guid()
%%       Stores = [pid()]
lookup_doc(Doc, Stores) ->
	{RevDict, PreRevDict} = lists:foldl(
		fun(StorePid, {AccRev, AccPreRev}) ->
			case peerdrive_store:lookup(StorePid, Doc) of
				{ok, Rev, PreRevs} ->
					StoreGuid = peerdrive_store:guid(StorePid),
					NewAccPreRev = lists:foldl(
						fun(PreRev, Acc) -> dict:append(PreRev, StoreGuid, Acc) end,
						AccPreRev,
						PreRevs),
					NewAccRev = dict:append(Rev, StoreGuid, AccRev),
					{NewAccRev, NewAccPreRev};

				{error, _} ->
					{AccRev, AccPreRev}
			end
		end,
		{dict:new(), dict:new()},
		Stores),
	{dict:to_list(RevDict), dict:to_list(PreRevDict)}.


%% @doc Lookup a revision.
%%
%% Returns the list of stores which contain the specified Rev. Searches on the
%% given stores or on all mounted stores if no store was specified.
%%
%% @spec lookup_rev(Rev, Stores) -> [Store]
%%       Rev, Store = guid()
%%       Stores = [pid()]
lookup_rev(Rev, Stores) ->
	lists:foldl(
		fun(Store, Acc) ->
			case peerdrive_store:contains(Store, Rev) of
				true  -> [peerdrive_store:guid(Store) | Acc];
				false -> Acc
			end
		end,
		[],
		Stores).


%% @doc Get status information about a revision.
%%
%% Returns information about a revision if it is found on any of the specified
%% stores, or `{error, enoent}' if no such revision is found. The returned
%% information is a #rev{} record.
%%
%% @spec stat(Rev, SearchStores) -> Result
%%       Result = {ok, Stat} | {error, enoent}
%%       Rev = guid()
%%       SearchStores = [pid()]
%%       Stat = #rev{}
%%       Reason = ecode()
stat(Rev, SearchStores) ->
	try
		lists:foreach(
			fun(Store) ->
				case peerdrive_store:stat(Store, Rev) of
					{ok, Stat} -> throw(Stat);
					{error, _Reason} -> ok
				end
			end,
			SearchStores),
		{error, enoent}
	catch
		throw:Result -> {ok, Result}
	end.


get_links(Rev, SearchStores) ->
	try
		lists:foreach(
			fun(Store) ->
				case peerdrive_store:get_links(Store, Rev) of
					{ok, Links} -> throw(Links);
					{error, _Reason} -> ok
				end
			end,
			SearchStores),
		{error, enoent}
	catch
		throw:Result -> {ok, Result}
	end.


%% @doc Start reading a specific revision.
%% @spec peek(Store, Rev) -> Result
%%       Result = {ok, Handle} | {error, Reason}
%%       Rev = guid()
%%       Store = pid()
%%       Handle = handle()
%%       Reason = ecode()
peek(Store, Rev) ->
	peerdrive_broker_io:peek(Store, Rev).


%% @doc Create a new, empty document.
%%
%% Returns `{ok, Doc, Handle}' which represents the created document and a
%% handle for the subsequent write calls to fill the revision. The initial
%% revision identifier will be returned by commit/1 which will always succeed
%% for newly created documents (despite IO errors).
%%
%% The handle can only be commited but not suspended because the new document
%% will not show up in the store until a successful commit.
%%
%% @spec create(Store, Type, Creator) -> Result
%%       Result = {ok, Doc, Handle} | {error, Reason}
%%       Store = pid()
%%       Doc = guid()
%%       Type, Creator = binary()
%%       Handle = handle()
%%       Reason = ecode()
create(Store, Type, Creator) ->
	peerdrive_broker_io:create(Store, Type, Creator).


%% @doc Fork a new document from an existing revision.
%%
%% Returns `{ok, Doc, Handle}' which represents the created document and a
%% handle for the subsequent write calls to update the revision. The initial
%% content will be taken from StartRev.
%%
%% The handle can only be commited but not suspended because the new document
%% will not show up in the store until a successful commit.
%%
%% @spec fork(Store, StartRev, Creator) -> Result
%%       Result = {ok, Doc, Handle} | {error, Reason}
%%       Store = pid()
%%       StartRev, Doc = guid()
%%       Creator = binary()
%%       Handle = handle()
%%       Reason = ecode()
fork(Store, StartRev, Creator) ->
	peerdrive_broker_io:fork(Store, StartRev, Creator).


%% @doc Update a document.
%%
%% Write to the document identified by Doc starting from StartRev. The StartRev
%% does not need to be the current revision of the document, e.g. when you want
%% to save the current state as preliminary revision even if the document was
%% updated in between.
%%
%% @spec update(Store, Doc, StartRev, Creator) -> Result
%%       Result = {ok, Handle} | {error, Reason}
%%       Doc, StartRev = guid()
%%       Creator = undefined | binary()
%%       Store = pid()
%%       Handle = handle()
%%       Reason = ecode()
update(Store, Doc, StartRev, Creator) ->
	peerdrive_broker_io:update(Store, Doc, StartRev, Creator).


%% @doc Resume writing to a document
%%
%% This will open a preliminary revision that was previously suspended. The
%% content will be exactly the same as when it was suspended, including its
%% parents. StartRev will be kept as pending preliminary revision (until
%% overwritten by either commit/1 or suspend/1).
%%
%% @spec resume(Store, Doc, PreRev, Creator) -> Result
%%       Result = {ok, Handle} | {error, Reason}
%%       Doc, PreRev = guid()
%%       Creator = undefined | binary()
%%       Store = pid()
%%       Handle = handle()
%%       Reason = ecode()
resume(Store, Doc, PreRev, Creator) ->
	peerdrive_broker_io:resume(Store, Doc, PreRev, Creator).


get_data(Handle, Selector) ->
	peerdrive_broker_io:get_data(Handle, Selector).


set_data(Handle, Selector, Data) ->
	peerdrive_broker_io:set_data(Handle, Selector, Data).


%% @doc Read a part of a document
%%
%% Returns the requested data. Trying to read a non-existing part yields
%% {error, enoent}. May return less data if the end of the part was hit.
%%
%% @spec read(Handle, Part, Offset, Length) ->  Result
%%       Result = {ok, Data} | {error, Reason}
%%       Handle = handle()
%%       Part, Offset, Length = int()
%%       Data = binary()
%%       Reason = ecode()
read(Handle, Part, Offset, Length) ->
	peerdrive_broker_io:read(Handle, Part, Offset, Length).


%% @doc Write a part of a document
%%
%% Writes the given data at the requested offset of the part. If the part does
%% not exist yet it will be created.
%%
%% @spec write(Handle, Part, Offset, Data) -> Result
%%       Result = ok | {error, Reason}
%%       Handle = handle()
%%       Part = Data = binary()
%%       Offset = integer()
%%       Reason = ecode()
write(Handle, Part, Offset, Data) ->
	peerdrive_broker_io:write(Handle, Part, Offset, Data).


%% @doc Truncate part
%%
%% Truncates part at the given offset. If the part does not exist yet it will
%% be created.
%%
%% @spec truncate(Handle, Part, Offset) -> Result
%%       Result = ok | {error, Reason}
%%       Handle = handle()
%%       Part = binary()
%%       Offset = integer()
%%       Reason = ecode()
truncate(Handle, Part, Offset) ->
	peerdrive_broker_io:truncate(Handle, Part, Offset).


fstat(Handle) ->
	peerdrive_broker_io:fstat(Handle).

set_flags(Handle, Flags) ->
	peerdrive_broker_io:set_flags(Handle, Flags).

set_type(Handle, Uti) ->
	peerdrive_broker_io:set_type(Handle, Uti).

set_mtime(Handle, Attachment, MTime) ->
	peerdrive_broker_io:set_mtime(Handle, Attachment, MTime).


%% @doc Merge document with another revision
%%
%% Add `Rev' from `Store' as another parent of the document. The revision is
%% replicated asynchronously to the store of the document. If the revision is a
%% successor of any parent then this(these) parent(s) will be replaced by the
%% new revision.
%%
%% @spec merge(Handle, Store, Rev, Options) -> Result
%%       Result = ok | {error, Reason}
%%       Handle = handle()
%%       Store = pid()
%%       Rev = guid()
%%       Options = [{depth, interger()} | verbose]
%%       Reason = ecode()
merge(Handle, Store, Rev, Options) ->
	peerdrive_broker_io:merge(Handle, Store, Rev, Options).


rebase(Handle, Parent) ->
	peerdrive_broker_io:rebase(Handle, Parent).


%% @doc Commit a new revision
%%
%% One of the parents of this new revision must point to the current revision
%% of the document, otherwise the function will fail with a `econflict' error
%% code. The handle remains writable in this case and the caller may either
%% rebase the revision and try again or suspend the handle to keep the changes.
%%
%% If the new revision could be committed then its identifier will be returned.
%% If the handle was resumed from a preliminary revision then this preliminary
%% revision will be removed from the list of pending preliminary revisions. In
%% case of severe errors the function will fail with an apropriate error code.
%%
%% The handle will be read only after the call if the commit succeeds. In case
%% the commit fails, e.g. due to a conflict the handle will still be writable.
%%
%% @spec commit(Handle) -> Result
%%       Result = {ok, Rev} | {error, Reason}
%%       Handle = handle()
%%       Rev = guid()
%%       Reason = ecode()
commit(Handle) ->
	peerdrive_broker_io:commit(Handle).

commit(Handle, Comment) ->
	peerdrive_broker_io:commit(Handle, Comment).


%% @doc Suspend a handle
%%
%% This function will create a temporary revision with the changes made so far
%% and will enqueue it as a pending preliminary revisions of the affected
%% document. The operation should only fail on io-errors or when trying to
%% suspend a handle which was obtained from create/3 or fork/3.
%%
%% The handle can be resumed later by calling resume/4. If the resulting handle
%% is successfully commited or again suspended then the original preliminary
%% revision is removed from the document. The preliminary revision can also be
%% removed explicitly by calling forget/3.
%%
%% The handle will be read only after the call if the operation succeeds. In
%% case the operation fails the handle will still be writable.
%%
%% @spec suspend(Handle) -> Result
%%       Result = {ok, Rev} | {error, Reason}
%%       Handle = handle()
%%       Rev = guid()
%%       Reason = ecode()
suspend(Handle) ->
	peerdrive_broker_io:suspend(Handle).

suspend(Handle, Comment) ->
	peerdrive_broker_io:suspend(Handle, Comment).


%% @doc Close a handle
%%
%% Discards the handle and throws away any changes. The handle will be invalid
%% after the call.
%%
%% @spec close(Handle) -> ok
%%       Handle = #handle{}
close(Handle) ->
	peerdrive_broker_io:close(Handle).


%% @doc Remove a pending preliminary revision from a document.
%%
%% @spec forget(Store, Doc, PreRev) -> Result
%%       Result = ok | {error, Reason}
%%       Doc, PreRev = guid()
%%       Store = pid()
%%       Reason = ecode()
forget(Store, Doc, PreRev) ->
	peerdrive_store:forget(Store, Doc, PreRev).


%% @doc Delete a document and release any referenced revisions.
%%
%% Normally documents should not be deleted explicitly but indirectly through
%% automatic garbage collection when they are no longer referenced inside the
%% store.
%%
%% @spec delete_doc(Store, Doc, Rev) -> Result
%%       Result = ok | {error, Reason}
%%       Store = pid()
%%       Doc, Rev = guid()
%%       Reason = ecode()
delete_doc(Store, Doc, Rev) ->
	peerdrive_store:delete_doc(Store, Doc, Rev).


%% @doc Delete a revision and release any referenced parent revisions
%%
%% This is normally used to explicitly delete old, unused revisions of a
%% document.
%%
%% @spec delete_rev(Store, Rev) -> Result
%%       Result = ok | {error, Reason}
%%       Store = pid()
%%       Rev = guid()
%%       Reason = ecode()
delete_rev(Store, Rev) ->
	peerdrive_store:delete_rev(Store, Rev).


%% @doc Forward a document revision
%%
%% Forwards the document Doc from FromRev to ToRev. Any intermediate revisions
%% are searched on SrcStore and replicated to the documents store.
%%
%% @spec forward_doc(Store, Doc, FromRev, ToRev, SrcStore, Options) -> Result
%%       Result = ok | {error, Reason}
%%       Doc, Rev = guid()
%%       Options = [{depth, interger()} | verbose]
%%       Stores = [guid()]
%%       Reason = ecode()
forward_doc(Store, Doc, FromRev, ToRev, SrcStore, Options) ->
	case search_path([SrcStore, Store], FromRev, ToRev) of
		{ok, Path} ->
			do_forward_doc(Store, Doc, SrcStore, Path, ToRev, Options);
		error ->
			{error, einval}
	end.


%% @doc Replicate a document to new stores.
%%
%% The Doc must be unambiguous, that is it must have the same revision on all
%% source stores. The Doc may already exist on the destination stores. An empty
%% (src- or dst-)stores list is replaced by the list of all mounted stores.
%%
%% @spec replicate_doc(SrcStore, Doc, DstStore, Options) -> Result
%%       Doc = guid()
%%       Options = [{depth, interger()} | verbose]
%%       SrcStore, DstStore = guid()
%%       Result = {ok, Handle} | {error, Reason}
replicate_doc(SrcStore, Doc, DstStore, Options) ->
	peerdrive_replicator:replicate_doc_sync(SrcStore, Doc, DstStore, Options).


%% @doc Replicate a revision to another store.
%%
%% The revision must be referenced by another document on the destination
%% stores, otherwise the revision is immediately eligible for garbage collection
%% or the destination store may refuse to replicate the revision entirely.
%%
%% @spec replicate_rev(SrcStore, Rev, DstStore, Options) -> Result
%%       Rev = guid()
%%       Options = [{depth, interger()} | verbose]
%%       SrcStore, DstStore = guid()
%%       Result = {ok, Handle} | {error, Reason}
replicate_rev(SrcStore, Rev, DstStore, Options) ->
	peerdrive_replicator:replicate_rev_sync(SrcStore, Rev, DstStore, Options).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

search_path(Stores, FromRev, ToRev) ->
	search_path(Stores, FromRev, ToRev, [ToRev]).


search_path(_Stores, FromRev, FromRev, Path) ->
	{ok, Path};

search_path(Stores, FromRev, ToRev, Path) ->
	case stat(ToRev, Stores) of
		{ok, #rev{parents=Parents}} ->
			lists:foldl(
				fun
					(Rev, error) ->
						search_path(Stores, FromRev, Rev, [Rev | Path]);
					(_Rev, Found) ->
						Found
				end,
				error,
				Parents);

		{error, _} ->
			error
	end.


do_forward_doc(DstStore, Doc, SrcStore, RevPath, ToRev, Options) ->
	case peerdrive_store:forward_doc(DstStore, Doc, RevPath, undefined) of
		ok ->
			% Do an explicit asynchronous replication if verbose operation
			% requested
			case proplists:get_bool(verbose, Options) of
				true ->
					peerdrive_replicator:replicate_rev(SrcStore, ToRev,
						DstStore, Options);
				false ->
					ok
			end,
			ok;

		{ok, MissingRevs, Handle} ->
			try
				case proplists:get_bool(verbose, Options) of
					true ->
						% Verbose replicating is very heavy. Just do it for the
						% final Rev. This will anyways bring in the other
						% missing revs.
						case replicate_rev(SrcStore, ToRev, DstStore, Options) of
							{ok, RepHdl1} -> close(RepHdl1), ok;
							Err1 -> throw(Err1)
						end,
						MissingOpt = [{depth, peerdrive_util:get_time()}];
					false ->
						MissingOpt = Options
				end,
				lists:foreach(
					fun(Rev) ->
						case replicate_rev(SrcStore, Rev, DstStore, MissingOpt) of
							{ok, RepHdp2} -> close(RepHdp2), ok;
							Err2 -> throw(Err2)
						end
					end,
					MissingRevs),
				case peerdrive_store:commit(Handle) of
					{ok, ToRev} -> ok;
					Err3 -> throw(Err3)
				end
			catch
				throw:Error -> Error
			after
				peerdrive_store:close(Handle)
			end;

		{error, _Reason} = Error ->
			Error
	end.

