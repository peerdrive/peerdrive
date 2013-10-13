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

-module(peerdrive_store).

-export([guid/1, statfs/1, contains/2, lookup/2, stat/2, sync/1]).
-export([put_doc/3, forward_doc/4, put_rev/6, put_rev_part/3, remember_rev/4]).
-export([close/1, commit/1, commit/2, create/3, fork/3, fstat/1, peek/2, read/4,
	resume/4, set_mtime/3, set_parents/2, set_type/2, truncate/3, update/4,
	write/4, suspend/1, suspend/2, get_links/2, set_flags/2, get_data/2,
	set_data/3]).
-export([delete_rev/2, delete_doc/3, forget/3]).
-export([sync_get_changes/3, sync_get_anchor/3, sync_set_anchor/4, sync_finish/2]).
-export([hash_revision/1]).

-include("store.hrl").

%% @doc Get GUID of a store
%% @spec guid(Store::pid()) -> guid()
guid(Store) ->
	call_store(Store, guid, <<0:128>>).

%% @doc Get file system statistics
%% @spec statfs(Store::pid()) -> {ok, #fs_stat{}} | {error, Reason}
statfs(Store) ->
	call_store(Store, statfs).

%% @doc Lookup a document.
%%
%% Returns the current revision and any pending preliminary revisions if the
%% document is found in the store, or `error' if no such document exists.
%%
%% @spec lookup(Store, Doc) -> {ok, Rev, PreRevs} | {error, Reason}
%%       Store = pid()
%%       Doc = Rev = guid()
%%       PreRevs = [guid()]
%%       Reason = enoent | enxio
lookup(Store, Doc) ->
	call_store(Store, {lookup, Doc}).

%% @doc Check if a revision exists in the store
%% @spec contains(Store, Rev) -> bool()
%%       Store = pid()
%%       Rev = guid()
contains(Store, Rev) ->
	call_store(Store, {contains, Rev}, false).

%% @doc Stat a revision.
%%
%% Returns information about a revision if it is found on the store, or
%% `{error, enonent}' if no such revision exists. The returned information is a
%% #rev{} record.
%%
%% @spec stat(Store, Rev) -> {ok, Stat} | {error, enoent}
%%       Store = pid()
%%       Rev = guid()
%%       Stat = #rev{}
stat(Store, Rev) ->
	call_store(Store, {stat, Rev}).

%% @doc Query links of revision
%%
%% Return the set of all links of the structured data of a revision.
%%
%% @spec get_links(Store, Rev) -> Result
%%       Store = pid()
%%       Rev = guid()
%%       Result = {ok, {DocLinks, RevLinks}} | {error, Reason}
get_links(Store, Rev) ->
	call_store(Store, {get_links, Rev}).

%% @doc Commit all dirty data to disk
%% @spec sync(Store::pid()) -> Result
%%       Result -> ok | {error, Reason::ecode()}
sync(Store) ->
	call_store(Store, sync).

%% @doc Start reading a revision.
%%
%% Returns the handle when ready, or an error code.
%%
%% @spec peek(Store, Rev) -> {ok, Handle} | {error, Reason}
%%       Store = pid()
%%       Handle = pid()
%%       Rev = guid()
%%       Reason = ecode()
peek(Store, Rev) ->
	call_store(Store, {peek, Rev}).

%% @doc Create a new, empty document.
%%
%% Returns the new DId and a handle for the following read/write functions to
%% fill it. The handle can only be commited or aborted but not suspended
%% because the new document will not show up in the store until a sucsessful
%% commit.
%%
%% @spec create(Store, Type, Creator) -> {ok, Doc, Handle} | {error, Reason}
%%         Store = pid()
%%         Handle = pid()
%%         Doc = guid()
%%         Type, Creator = binary()
%%         Reason = ecode()
create(Store, Type, Creator) ->
	call_store(Store, {create, Type, Creator}).

%% @doc Derive a new document from an existing revision
%%
%% Returns the new DId and a handle for the following read/write functions to
%% update the new document. The new revision will start with the content of the
%% StartRev revision. The handle can only be commited or aborted but not
%% suspended because the new document will not show up in the store until a
%% sucsessful commit.
%%
%% @spec fork(Store, StartRev, Creator) -> {ok, Doc, Handle} | {error, Reason}
%%         Store = pid()
%%         Handle = pid()
%%         StartRev, Doc = guid()
%%         Creator = binary()
%%         Reason = ecode()
fork(Store, StartRev, Creator) ->
	call_store(Store, {fork, StartRev, Creator}).

%% @doc Update an existing document
%%
%% The new revision will start with the content of the StartRev revision and
%% will have StartRev as its sole parent. If Doc points already to another
%% revision then the call will fail.
%%
%% @spec update(Store, Doc, StartRev, Creator) -> {ok, Handle} | {error, Reason}
%%        Store = pid()
%%        Handle = pid()
%%        Doc, StartRev = guid()
%%        Creator = undefined | binary()
%%        Reason = ecode()
update(Store, Doc, StartRev, Creator) ->
	call_store(Store, {update, Doc, StartRev, Creator}).

%% @doc Resume writing to a document
%%
%% This will open a preliminary revision that was previously suspended. The
%% content will be exactly the same as when it was suspended, including its
%% parents. StartRev will be kept as pending preliminary revision (until
%% overwritten by either commit/2 or suspend/1).
%%
%% @spec resume(Store, Doc, PreRev, Creator) -> {ok, Handle} | {error, Reason}
%%        Store = pid()
%%        Handle = pid()
%%        Doc, PreRev = guid()
%%        Creator = undefined | binary()
%%        Reason = ecode()
resume(Store, Doc, PreRev, Creator) ->
	call_store(Store, {resume, Doc, PreRev, Creator}).


%% @doc Get structured data of a document
%%
%% Returns the structured data in the common binary encoding. Use `Selector' to
%% retrieve only a subset of the data. The selection is done by path-like
%% syntax, e.g. <<"/foo/bar#3">>.
get_data(Handle, Selector) ->
	call_store(Handle, {get_data, Selector}).


%% @doc Set structured data of a document
%%
%% Set the whole structured data (or a subset specified by `Selector') to `Data'.
%% The data must be given in the common binary encoding.
%%
%% The selection is done by path-like syntax, e.g. <<"/foo#3/bar#+">>. Any
%% non-existing key in a `/'-step implicitly creates the key in the dictionary.
%% To append to a list use the `#+'-step, otherwise a `#'-step must reference
%% an existing list entry.
set_data(Handle, Selector, Data) ->
	call_store(Handle, {set_data, Selector, Data}).


%% @doc Read from a binary attachment of a document
%%
%% Returns the requested data. Non-existing attachment are implicitly empty.
%% May return less data if the end of the part was hit.
%%
%% @spec read(Handle, Attachment, Offset, Length) -> {ok, Data} | {error, Reason}
%%       Handle = pid()
%%       Attachment = Data = binary()
%%       Offset = Length = integer()
%%       Reason = ecode()
read(Handle, Attachment, Offset, Length) ->
	call_store(Handle, {read, Attachment, Offset, Length}).

%% @doc Write a binary attachment of a document
%%
%% Writes the given data at the requested offset of the attachment. If the
%% attachment does not exist yet it will be created.
%%
%% @spec write(Handle, Attachment, Offset, Data) -> ok | {error, Reason}
%%       Handle = pid()
%%       Attachment = Data = binary()
%%       Offset = integer()
%%       Reason = ecode()
write(Handle, Attachment, Offset, Data) ->
	call_store(Handle, {write, Attachment, Offset, Data}).

%% @doc Truncate attachment to a specific length
%%
%% Cause the named attachment to be truncated to a size of precisely Length
%% bytes.
%%
%% If the attachment previously was larger than this size, the extra data is lost.
%% If the attachment previously was shorter, it is extended, and the extended part
%% reads as null bytes.
%%
%% If the attachment does not exist yet it will be created if Length is greater
%% than zero. If the attachment is truncated at offset zero it is deleted
%% implicitly.
%%
%% @spec truncate(Handle, Attachment, Length) -> ok | {error, Reason}
%%       Handle = pid()
%%       Attachment = binary()
%%       Length = integer()
%%       Reason = ecode()
truncate(Handle, Attachment, Length) ->
	call_store(Handle, {truncate, Attachment, Length}).

% {ok, #rev{}} | {error, Reason}
fstat(Handle) ->
	call_store(Handle, fstat).

% ok | {error, Reason}
set_flags(Handle, Flags) ->
	call_store(Handle, {set_flags, Flags}).

% ok | {error, Reason}
set_type(Handle, Type) ->
	call_store(Handle, {set_type, Type}).

% ok | {error, Reason}
set_parents(Handle, Parents) ->
	call_store(Handle, {set_parents, Parents}).

% ok | {error, Reason}
set_mtime(Handle, Attachment, MTime) ->
	call_store(Handle, {set_mtime, Attachment, MTime}).

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
%% @spec commit(Handle) -> {ok, Rev} | {error, Reason}
%%       Handle = pid()
%%       Rev = guid()
%%       Reason = ecode()
commit(Handle) ->
	call_store(Handle, commit).

commit(Handle, Comment) ->
	call_store(Handle, {commit, Comment}).

%% @doc Suspend a handle
%%
%% This function will create a temporary revision with the changes made so far
%% and will enqueue it as a pending preliminary revisions of the affected
%% document. The operation should only fail on io-errors or when trying to
%% suspend a handle which was obtained from create/4 or fork/4.
%%
%% The handle can be resumed later by calling resume/4. If the resulting handle
%% is successfully commited or again suspended then the original preliminary
%% revision is removed from the document. The preliminary revision can also be
%% removed explicitly by calling forget/3.
%%
%% The handle will be read only after the call if the operation succeeds. In
%% case the operation fails the handle will still be writable.
%%
%% @spec suspend(Handle) -> {ok, Rev} | {error, Reason}
%%       Handle = pid()
%%       Rev = guid()
%%       Reason = ecode()
suspend(Handle) ->
	call_store(Handle, {suspend, undefined}).

suspend(Handle, Comment) ->
	call_store(Handle, {suspend, Comment}).

%% @doc Close a handle
%%
%% Discards the handle. Throws away any changes which have not been committed
%% yet. The handle will be invalid after the call.
%%
%% @spec close(Handle) -> ok
%%       Handle = pid()
close(Handle) ->
	call_store(Handle, close, ok).

%% @doc Remove a pending preliminary revision from a document.
%%
%% @spec forget(Store, Doc, PreRev) -> ok | {error, Reason}
%%       Store = pid()
%%       Doc, PreRev = guid()
%%       Reason = ecode()
forget(Store, Doc, PreRev) ->
	call_store(Store, {forget, Doc, PreRev}).

%% @doc Delete a document and release any referenced revisions.
%%
%% Normally documents should not be deleted explicitly but indirectly through
%% automatic garbage collection when they are no longer referenced inside the
%% store.
%%
%% @spec delete_doc(Store, Doc, Rev) -> ok | {error, Reason}
%%       Store = pid()
%%       Doc, Rev = guid()
%%       Reason = ecode()
delete_doc(Store, Doc, Rev) ->
	call_store(Store, {delete_doc, Doc, Rev}).

%% @doc Delete a revision and release any referenced parent revisions
%%
%% This is normally used to explicitly delete old, unused revisions of a
%% document.
%%
%% @spec delete_rev(Store, Rev) -> ok | {error, Reason}
%%       Store = pid()
%%       Rev = guid()
%%       Reason = ecode()
delete_rev(Store, Rev) ->
	call_store(Store, {delete_rev, Rev}).

%% @doc Put a document in the store
%%
%% The function will return a Handle and the caller has the chance to upload
%% the revision before committing. After the commit the Doc it is created and
%% points to Rev. If the Doc exits it must point to Rev, otherwise the call
%% will fail. As long as the handle is kept open the Doc and the Rev are
%% guaranteed to be not garbage collected.
%%
%% @spec put_doc(Store, Doc, Rev) -> Result
%%       Store = pid()
%%       Doc = Rev = guid()
%%       Result = {ok, Handle} | {error, Reason}
%%       Handle = pid()
%%       Reason = ecode()
put_doc(Store, Doc, Rev) ->
	call_store(Store, {put_doc, Doc, Rev}).


%% @doc Fast-forward a document
%%
%% This function forwards Doc from the current revision to a new revision where
%% the current revision must be an ancestor of the new revision. The current
%% revision, all revisions in between and the final revision are given as a
%% list in RevPath. The list must be ordered from the the current Rev to the
%% final, latest Rev.
%%
%% If the store has already all involved revisions then the Doc is forwarded
%% and the function just returns `ok'. Otherwise a list of missing revisions is
%% returned which have to be uploaded in any order with put_rev/3 before
%% the forward can be committed by commit/1.
%%
%% If the document is successfully forwarded OldPreRev is atomically removed
%% from the list of preliminary revisions (if given).
%%
%% @spec forward_doc(Store, Doc, RevPath, OldPreRev) -> Result
%%       Store = pid()
%%       Doc = guid()
%%       RevPath = MissingRevs = [guid()]
%%       OldPreRev = undefined | guid()
%%       Result = ok | {ok, MissingRevs, Handle} | {error, Reason}
%%       Handle = pid()
%%       Reason = ecode()
forward_doc(Store, Doc, RevPath, OldPreRev) ->
	call_store(Store, {forward_doc, Doc, RevPath, OldPreRev}).


%% @doc Add a preliminary revision to a document
%%
%% Add PreRev to the current list of pending preliminary revisions of Doc. The
%% function returns a handle which gives the caller the chance to actually
%% upload the revision. After committing the handle PreRev is added as
%% preliminary revision, optionally replacing OldPreRev atomically.
%%
%% @spec remember_rev(Store, Doc, PreRev, OldPreRev) -> Result
%%       Store = pid()
%%       Doc = Rev = guid()
%%       OldPreRev = undefined | guid()
%%       Result = ok | {ok, Handle} | {error, Reason}
%%       Handle = pid()
%%       Reason = ecode()
remember_rev(Store, Doc, PreRev, OldPreRev) ->
	call_store(Store, {remember_rev, Doc, PreRev, OldPreRev}).


%% @doc Put/import a revision into the store.
%%
%% The function takes the specification of the whole revision and returns the
%% list of missing attachments which the caller has to supply by subsequent
%% put_rev_part/3 calls. The revision will not be garbage collected as long as
%% the handle is kept open.
%%
%% @spec put_rev(Store, Rev, Revision, Data, DocLinks, RevLinks) -> Result
%%       Store = pid()
%%       Rev = guid()
%%       Revision = #rev{}
%%       Data = binary()
%%       DocLinks = RevLinks = [guid()]
%%       Result = {ok, MissingAttachments, Handle} | {error, Reason}
%%       MissingAttachments = [binary()]
%%       Handle = pid()
%%       Reason = ecode()
put_rev(Store, Rev, Revision, Data, DocLinks, RevLinks) ->
	call_store(Store, {put_rev, Rev, Revision, Data, DocLinks, RevLinks}).

%% @doc Add data to a revision that's imported
%%
%% @spec put_rev_part(Importer, Attachment, Data) -> ok | {error, Reason}
%%       Importer = pid()
%%       Attachment = Data = binary()
%%       Reason = ecode()
put_rev_part(Importer, Attachment, Data) ->
	call_store(Importer, {put_part, Attachment, Data}).

%% @doc Get changes since the last sync point of peer store
%%
%% The caller is also recorded as the synchronizing process. Only one sync
%% process is allowed per peer store. The store will trap the exit of the
%% caller but the caller may also release the lock by calling sync_finish/2.
%%
%% @spec sync_get_changes(Store, PeerGuid, Anchor) -> Result
%%       Store = pid()
%%       PeerGuid = guid()
%%       Anchor = integer()
%%       Result = {ok, Backlog} | {error, Reason}
%%       Backlog = [{Doc::guid(), SeqNum::integer()}]
sync_get_changes(Store, PeerGuid, Anchor) ->
	call_store(Store, {sync_get_changes, PeerGuid, Anchor}).


%% @doc Release the lock for the peer store synchronizing process.
%% @spec sync_finish(Store, PeerGuid) -> ok | {error, Reason}
sync_finish(Store, PeerGuid) ->
	call_store(Store, {sync_finish, PeerGuid}).


%% @doc Get last sync point between two stores
%% @spec sync_get_anchor(Store, FromSId, ToSId) -> Result
%%       Store = pid()
%%       FromSId = ToSId = guid()
%%       Result = {ok, SeqNum::integer()} | {error, Reason}
sync_get_anchor(Store, FromSId, ToSId) ->
	call_store(Store, {sync_get_anchor, FromSId, ToSId}).


%% @doc Set sync point of two stores to new generation
%% @spec sync_set_anchor(Store, FromSId, ToSId, SeqNum) -> Result
%%       Store = pid()
%%       FromSId = ToSId = guid()
%%       SeqNum = integer()
%%       Result = ok | {error, Reason}
sync_set_anchor(Store, FromSId, ToSId, SeqNum) ->
	call_store(Store, {sync_set_anchor, FromSId, ToSId, SeqNum}).


hash_revision(#rev{flags=Flags, crtime=CrTime, mtime=Mtime} = Revision) ->
	BinData = hash_revision_hash((Revision#rev.data)#rev_dat.hash),
	Attachments = Revision#rev.attachments,
	BinAttachments = lists:foldl(
		fun (#rev_att{name=Name, hash=Hash, crtime=CrT, mtime=MT}, AccIn) ->
			<<AccIn/binary, (hash_revision_string(Name))/binary,
				(hash_revision_hash(Hash))/binary, CrT:64/little, MT:64/little>>
		end,
		<<(length(Attachments)):32/little>>,
		Attachments),
	BinParents = hash_revision_list(Revision#rev.parents),
	BinType = hash_revision_string(Revision#rev.type),
	BinCreator = hash_revision_string(Revision#rev.creator),
	BinComment = hash_revision_string(Revision#rev.comment),
	peerdrive_crypto:sha(<<Flags:32/little, BinData/binary, BinAttachments/binary,
		BinParents/binary, CrTime:64/little, Mtime:64/little, BinType/binary,
		BinCreator/binary, BinComment/binary>>).

hash_revision_list(List) ->
	lists:foldl(
		fun (Hash, AccIn) ->
			<<AccIn/binary, (hash_revision_hash(Hash))/binary>>
		end,
		<<(length(List)):32/little>>,
		List).

hash_revision_string(String) ->
	<<(size(String)):32/little, String/binary>>.

hash_revision_hash(Hash) ->
	<<(size(Hash)):8/little, Hash/binary>>.


call_store(Store, Request) ->
	call_store(Store, Request, {error, enxio}).


call_store(Store, Request, FailReply) ->
	try
		gen_server:call(Store, Request, infinity)
	catch
		exit:_ -> FailReply
	end.

