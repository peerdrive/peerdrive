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

-module(store).

-export([guid/1, contains/2, lookup/2, stat/2]).
-export([put_doc/4, put_rev_start/3, put_rev_part/3, put_rev_abort/1,
	put_rev_commit/1]).
-export([abort/1, commit/2, create/4, fork/4, get_parents/1, get_type/1,
	peek/2, read/4, resume/4, set_parents/2, set_type/2, truncate/3, update/4,
	write/4, suspend/2]).
-export([delete_rev/2, delete_doc/3, forget/3]).
-export([sync_get_changes/2, sync_set_anchor/3]).
-export([hash_revision/1]).

-include("store.hrl").

%% @doc Get GUID of a store
%% @spec guid(Store::#store{}) -> guid()
guid(#store{this=Store, guid=Guid}) ->
	Guid(Store).

%% @doc Lookup a document.
%%
%% Returns the current revision and any pending preliminary revisions if the
%% document is found in the store, or `error' if no such document exists.
%%
%% @spec lookup(Store, Doc) -> {ok, Rev, PreRevs} | error
%%       Store = #store{}
%%       Doc = Rev = guid()
%%       PreRevs = [guid()]
lookup(#store{this=Store, lookup=Lookup}, Doc) ->
	Lookup(Store, Doc).

%% @doc Check if a revision exists in the store
%% @spec contains(Store, Rev) -> bool()
%%       Store = #store{}
%%       Rev = guid()
contains(#store{this=Store, contains=Contains}, Rev) ->
	Contains(Store, Rev).

%% @doc Stat a revision.
%%
%% Returns information about a revision if it is found on the store, or `error'
%% if no such revision exists. The returned information is a #stat{} record
%% containing the following fields:
%%
%%   flags = integer()
%%   parts = [{FourCC::binary(), Size::interger(), Hash::guid()}]
%%   parents = [guid()]
%%   mtime = integer()
%%   type = binary()
%%   creator = binary()
%%
%% @spec stat(Store, Rev) -> {ok, Stat} | {error, Reason}
%%       Store = #store{}
%%       Rev = guid()
%%       Stat = #stat{}
%%       Reason = ecode()
stat(#store{this=Store, stat=Stat}, Rev) ->
	Stat(Store, Rev).

%% @doc Start reading a revision.
%%
%% Returns the handle when ready, or an error code.
%%
%% @spec peek(Store, Rev) -> {ok, Handle} | {error, Reason}
%%       Store = #store{}
%%       Handle = #handle{}
%%       Rev = guid()
%%       Reason = ecode()
peek(#store{this=Store, peek=Peek}, Rev) ->
	Peek(Store, Rev).

%% @doc Create a new, empty document.
%%
%% Returns a handle for the following read/write functions to fill it. The
%% handle can only be commited or aborted but not suspended because the new
%% document will not show up in the store until a sucsessful commit.
%%
%% @spec create(Store, Doc, Type, Creator) -> {ok, Handle} | {error, Reason}
%%         Store = #store{}
%%         Handle = #handle{}
%%         Doc = guid()
%%         Type, Creator = binary()
%%         Reason = ecode()
create(#store{this=Store, create=Create}, Doc, Type, Creator) ->
	Create(Store, Doc, Type, Creator).

%% @doc Derive a new document from an existing revision
%%
%% Returns a handle for the following read/write functions to update the new
%% document. The new revision will start with the content of the StartRev
%% revision. The handle can only be commited or aborted but not suspended
%% because the new document will not show up in the store until a sucsessful
%% commit.
%%
%% @spec fork(Store, Doc, StartRev, Creator) -> {ok, Handle} | {error, Reason}
%%         Store = #store{}
%%         Handle = #handle{}
%%         StartRev, Doc = guid()
%%         Creator = binary()
%%         Reason = ecode()
fork(#store{this=Store, fork=Fork}, Doc, StartRev, Creator) ->
	Fork(Store, Doc, StartRev, Creator).

%% @doc Update an existing document
%%
%% The new revision will start with the content of the StartRev revision and
%% will have StartRev as its sole parent. If Doc points already to another
%% revision then the call will fail.
%%
%% @spec update(Store, Doc, StartRev, Creator) -> {ok, Handle} | {error, Reason}
%%        Store = #store{}
%%        Handle = #handle{}
%%        Doc, StartRev = guid()
%%        Creator = keep | binary()
%%        Reason = ecode()
update(#store{this=Store, update=Update}, Doc, StartRev, Creator) ->
	Update(Store, Doc, StartRev, Creator).

%% @doc Resume writing to a document
%%
%% This will open a preliminary revision that was previously suspended. The
%% content will be exactly the same as when it was suspended, including its
%% parents. StartRev will be kept as pending preliminary revision (until
%% overwritten by either commit/2 or suspend/1).
%%
%% @spec resume(Store, Doc, PreRev, Creator) -> {ok, Handle} | {error, Reason}
%%        Store = #store{}
%%        Handle = #handle{}
%%        Doc, PreRev = guid()
%%        Creator = keep | binary()
%%        Reason = ecode()
resume(#store{this=Store, resume=Resume}, Doc, StartRev, Creator) ->
	Resume(Store, Doc, StartRev, Creator).

%% @doc Read a part of a document
%%
%% Returns the requested data. Trying to read a non-existing part yields
%% {error, enoent}. May return less data if the end of the part was hit.
%%
%% @spec read(Handle, Part, Offset, Length) -> {ok, Data} | {error, Reason}
%%       Handle = #handle{}
%%       Part = Data = binary()
%%       Offset = Length = integer()
%%       Reason = ecode()
read(#handle{this=Handle, read=Read}, Part, Offset, Length) ->
	Read(Handle, Part, Offset, Length).

%% @doc Write a part of a document
%%
%% Writes the given data at the requested offset of the part. If the part does
%% not exist yet it will be created.
%%
%% @spec write(Handle, Part, Offset, Data) -> ok | {error, Reason}
%%       Handle = #handle
%%       Part = Data = binary()
%%       Offset = integer()
%%       Reason = ecode()
write(#handle{this=Handle, write=Write}, Part, Offset, Data) ->
	Write(Handle, Part, Offset, Data).

%% @doc Truncate part
%%
%% Truncates part at the given offset. If the part does not exist yet it will
%% be created.
%%
%% @spec truncate(Handle, Part, Offset) -> ok | {error, Reason}
%%       Handle = #handle
%%       Part = binary()
%%       Offset = integer()
%%       Reason = ecode()
truncate(#handle{this=Handle, truncate=Truncate}, Part, Offset) ->
	Truncate(Handle, Part, Offset).

% {ok, binary()} | {error, Reason}
get_type(#handle{this=Handle, get_type=GetType}) ->
	GetType(Handle).

% ok | {error, Reason}
set_type(#handle{this=Handle, set_type=SetType}, Uti) ->
	SetType(Handle, Uti).

% {ok, [guid()]} | {error, Reason}
get_parents(#handle{this=Handle, get_parents=GetParents}) ->
	GetParents(Handle).

% ok | {error, Reason}
set_parents(#handle{this=Handle, set_parents=SetParents}, Parents) ->
	SetParents(Handle, Parents).

%% @doc Commit a new revision
%%
%% One of the parents of this new revision must point to the current revision
%% of the document, otherwise the function will fail with `conflict'. The handle
%% is still valid in this case and the caller may either rebase the revision
%% and try again or suspend the handle to keep the changes.
%%
%% If the new revision could be committed then its identifier will be returned.
%% If the handle was resumed from a preliminary revision then this preliminary
%% revision will be removed from the list of pending preliminary revisions. In
%% case of severe errors the function will fail with an apropriate error code.
%%
%% The handle will be invalid after the call if the commit succeeds or fails
%% with a hard error. In case it fails due to a conflict the handle will still
%% be valid.
%%
%% @spec commit(Handle, Mtime) -> {ok, Rev} | conflict | {error, Reason}
%%       Handle = #handle{}
%%       Mtime = integer()
%%       Rev = guid()
%%       Reason = ecode()
commit(#handle{this=Handle, commit=Commit}, Mtime) ->
	Commit(Handle, Mtime).

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
%% The handle will be invalid after the call regardless of the result.
%%
%% @spec suspend(Handle, Mtime) -> {ok, Rev} | {error, Reason}
%%       Handle = #handle{}
%%       Mtime = integer()
%%       Rev = guid()
%%       Reason = ecode()
suspend(#handle{this=Handle, suspend=Suspend}, Mtime) ->
	Suspend(Handle, Mtime).

%% @doc Abort the creation of a new revision
%%
%% Discards the handle and throws away any changes. The handle will be invalid
%% after the call.
%%
%% @spec abort(Handle) -> ok
%%       Handle = #handle{}
abort(#handle{this=Handle, abort=Abort}) ->
	Abort(Handle).

%% @doc Remove a pending preliminary revision from a document.
%%
%% @spec forget(Store, Doc, PreRev) -> ok | {error, Reason}
%%       Store = #store{}
%%       Doc, PreRev = guid()
%%       Reason = ecode()
forget(#store{this=Store, forget=Forget}, Doc, PreRev) ->
	Forget(Store, Doc, PreRev).

%% @doc Delete a document and release any referenced revisions.
%%
%% Normally documents should not be deleted explicitly but indirectly through
%% automatic garbage collection when they are no longer referenced inside the
%% store.
%%
%% @spec delete_doc(Store, Doc, Rev) -> ok | {error, Reason}
%%       Store = #store{}
%%       Doc, Rev = guid()
%%       Reason = ecode()
delete_doc(#store{this=Store, delete_doc=DeleteDoc}, Doc, Rev) ->
	DeleteDoc(Store, Doc, Rev).

%% @doc Delete a revision and release any referenced parent revisions
%%
%% This is normally used to explicitly delete old, unused revisions of a
%% document.
%%
%% @spec delete_rev(Store, Rev) -> ok | {error, Reason}
%%       Store = #store{}
%%       Rev = guid()
%%       Reason = ecode()
delete_rev(#store{this=Store, delete_rev=DeleteRev}, Rev) ->
	DeleteRev(Store, Rev).

%% @doc Put/update a document in the store
%%
%% Let's a document point to a new revision. If the Doc does not exist yet then it
%% is created and points to NewRev. If the Doc exits it must either point
%% OldRev or NewRev, otherwise the call will fail.
%%
%% @spec put_doc(Store, Doc, OldRev, NewRev) -> ok | {error, Reason}
%%       Store = #store{}
%%       Doc = OldRev = NewRev = guid()
%%       Reason = ecode()
put_doc(#store{this=Store, put_doc=PutDoc}, Doc, OldRev, NewRev) ->
	PutDoc(Store, Doc, OldRev, NewRev).

%% @doc Put/import a revision into the store.
%%
%% The function takes the specification of the whole revision and returns the
%% list of missing parts which the caller has to supply by subsequent
%% put_rev_part/3 calls. If all parts are already available in the store then
%% the function just returns `ok'.
%%
%% @spec put_rev_start(Store, Rev, Revision) -> Result
%%       Store = #store{}
%%       Rev = guid()
%%       Revision = #revision
%%       Result = ok | {ok, MissingParts, Importer} | {error, Reason}
%%       MissingParts = [FourCC]
%%       Importer = #importer
%%       Reason = ecode()
put_rev_start(#store{this=Store, put_rev_start=PutRevStart}, Rev, Revision) ->
	PutRevStart(Store, Rev, Revision).

%% @doc Add data to a revision that's imported
%%
%% @spec put_rev_part(Importer, Part, Data) -> ok | {error, Reason}
%%       Importer = #importer
%%       Part = Data = binary()
%%       Reason = ecode()
put_rev_part(#importer{this=Importer, put_part=PutPart}, Part, Data) ->
	PutPart(Importer, Part, Data).

%% @doc Abort importing a revision
%% @spec put_rev_abort(Importer::#importer) -> none()
put_rev_abort(#importer{this=Importer, abort=Abort}) ->
	Abort(Importer).

%% @doc Finish importing a revision
%% @spec put_rev_commit(Importer::pid()) -> ok | {error, Reason}
%%       Importer = #importer
%%       Reason = ecode()
put_rev_commit(#importer{this=Importer, commit=Commit}) ->
	Commit(Importer).


%% @doc Get changes since the last sync point of peer store
%% @spec sync_get_changes(Store, PeerGuid) ->
%%       Store = #store
%%       PeerGuid = guid()
sync_get_changes(#store{this=Store, sync_get_changes=SyncGetChanges}, PeerGuid) ->
	SyncGetChanges(Store, PeerGuid).


%% @doc Set sync point of peer store to new generation
%% @spec sync_set_anchor(Store, PeerGuid, SeqNum) ->
%%       Store = #store
%%       PeerGuid = guid()
%%       SeqNum = integer()
sync_set_anchor(#store{this=Store, sync_set_anchor=SyncSetAnchor}, PeerGuid, SeqNum) ->
	SyncSetAnchor(Store, PeerGuid, SeqNum).


hash_revision(#revision{flags=Flags, mtime=Mtime} = Revision) ->
	Parts = Revision#revision.parts,
	BinParts = lists:foldl(
		fun ({FourCC, Hash}, AccIn) ->
			<<AccIn/binary, FourCC/binary, Hash/binary>>
		end,
		<<(length(Parts)):8>>,
		Parts),
	Parents = Revision#revision.parents,
	BinParents = lists:foldl(
		fun (Parent, AccIn) ->
			<<AccIn/binary, Parent/binary>>
		end,
		<<(length(Parents)):8>>,
		Parents),
	Type = Revision#revision.type,
	BinType = <<(size(Type)):32/little, Type/binary>>,
	Creator = Revision#revision.creator,
	BinCreator = <<(size(Creator)):32/little, Creator/binary>>,
	binary_part(
		crypto:sha(<<Flags:32/little, BinParts/binary, BinParents/binary,
			Mtime:64/little, BinType/binary, BinCreator/binary>>),
		0,
		16).

