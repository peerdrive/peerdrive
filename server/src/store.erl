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
-export([put_uuid/4, put_rev_start/3, put_rev_part/3, put_rev_abort/1,
	put_rev_commit/1]).
-export([abort/1, commit/3, fork/4, peek/2, read/4, truncate/3, update/4, write/4]).
-export([delete_rev/2, delete_doc/2]).
-export([sync_get_changes/2, sync_set_anchor/3]).
-export([hash_object/1]).

-include("store.hrl").

%% @doc Get GUID of a store
%% @spec guid(Store::#store) -> guid()
guid(#store{this=Store, guid=Guid}) ->
	Guid(Store).

%% @doc Lookup a document.
%%
%% Returns `{ok, Rev}' if the document is found on the store, or `error' if no such
%% UUID exists.
%%
%% @spec lookup(Store, Doc) -> {ok, Rev} | error
%%       Store = #store
%%       Doc = Rev = guid()
lookup(#store{this=Store, lookup=Lookup}, Doc) ->
	Lookup(Store, Doc).

%% @doc Check if a revision exists in the store
%% @spec contains(Store, Rev) -> bool()
%%       Store = #store
%%       Rev = guid()
contains(#store{this=Store, contains=Contains}, Rev) ->
	Contains(Store, Rev).

%% @doc Stat a revision.
%%
%% Returns information about a revision if it is found on the store, or `error'
%% if no such revision exists.
%%
%% @spec stat(Store, Rev) -> {ok, Flags, Parts, Parents, Mtime, Uti} | error
%%       Store = pid()
%%       Rev = guid()
%%       Parts = [{FourCC::binary(), Size::interger(), Hash::guid()}]
%%       Parents = [guid()]
%%       Mtime = integer()
%%       Uti = binary()
stat(#store{this=Store, stat=Stat}, Rev) ->
	Stat(Store, Rev).

%% @doc Start reading a document revision.
%%
%% Returns the handle when ready, or an error code.
%%
%% @spec peek(Store, Rev) -> {ok, Handle} | {error, Reason}
%%       Store = #store
%%       Handle = #handle
%%       Rev = guid()
%%       Reason = ecode()
peek(#store{this=Store, peek=Peek}, Rev) ->
	Peek(Store, Rev).

%% @doc Create a new document
%%
%% Returns `{ok, Handle}' which represents the created document and a handle
%% for the following read/write functions to fill it. The initial
%% revision identifier will be returned by commit/3 which should always succeed
%% for newly created documents.
%%
%% @spec fork(Store, Doc, StartRev, Uti) -> {ok, Handle} | {error, Reason}
%%         Store = #store
%%         Handle = #handle
%%         StartRev, Doc = guid()
%%         Uti = keep | binary()
%%         Reason = ecode()
fork(#store{this=Store, fork=Fork}, Doc, StartRev, Uti) ->
	Fork(Store, Doc, StartRev, Uti).

%% @doc Write to an existing document
%%
%% The new revision will start with the content of the StartRev revision. If
%% Doc points already to another revision then the call will fail.
%%
%% @spec update(Store, Doc, StartRev) -> {ok, Handle} | {error, Reason}
%%        Store = #store
%%        Handle = #handle
%%        Doc = guid()
%%        StartRevs = guid()
%%        Uti = keep | binary()
%%        Reason = ecode()
update(#store{this=Store, update=Update}, Doc, StartRev, Uti) ->
	Update(Store, Doc, StartRev, Uti).

%% @doc Read a part of a document
%%
%% @spec read(Reader, Part, Offset, Length) -> {ok, Data} | eof | {error, Reason}
%%       Reader = #reader
%%       Part = Data = binary()
%%       Offset = Length = integer()
%%       Reason = ecode()
read(#handle{this=Handle, read=Read}, Part, Offset, Length) ->
	Read(Handle, Part, Offset, Length).

% ok | {error, Reason}
write(#handle{this=Handle, write=Write}, Part, Offset, Data) ->
	Write(Handle, Part, Offset, Data).

% ok | {error, Reason}
truncate(#handle{this=Handle, truncate=Truncate}, Part, Offset) ->
	Truncate(Handle, Part, Offset).

% {ok, Rev} | conflict | {error, Reason}
commit(#handle{this=Handle, commit=Commit}, Mtime, MergeRevs) ->
	Commit(Handle, Mtime, MergeRevs).

% ok
abort(#handle{this=Handle, abort=Abort}) ->
	Abort(Handle).

% ok | {error, Reason}
delete_doc(#store{this=Store, delete_doc=DeleteDoc}, Doc) ->
	DeleteDoc(Store, Doc).

% ok | {error, Reason}
delete_rev(#store{this=Store, delete_rev=DeleteRev}, Rev) ->
	DeleteRev(Store, Rev).

%% @doc Put/update a UUID in the store
%%
%% Let's a UUID point to a new revision. If the UUID does not exist yet then it
%% is created and points to NewRev. If the UUID exits it must either point
%% OldRev or NewRev, otherwise the call will fail.
%%
%% @spec put_uuid(Store, Uuid, OldRev, NewRev) -> ok | {error, Reason}
%%       Store = #store
%%       Uuid = OldRev = NewRev = guid()
%%       Reason = ecode()
put_uuid(#store{this=Store, put_uuid=PutUuid}, Uuid, OldRev, NewRev) ->
	PutUuid(Store, Uuid, OldRev, NewRev).

%% @doc Put/import a revision into the store.
%%
%% The function takes the specification of the whole revision and returns the
%% list of missing parts which the caller has to supply by subsequent
%% put_rev_part/3 calls. If all parts are already available in the store then
%% the function just returns `ok'.
%%
%% @spec put_rev_start(Store, Rev, Object) -> Result
%%       Store = #store
%%       Rev = guid()
%%       Object = #object
%%       Result = ok | {ok, MissingParts, Importer} | {error, Reason}
%%       MissingParts = [FourCC]
%%       Importer = #importer
%%       Reason = ecode()
put_rev_start(#store{this=Store, put_rev_start=PutRevStart}, Rev, Object) ->
	PutRevStart(Store, Rev, Object).

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


hash_object(#object{flags=Flags, parts=Parts, parents=Parents, mtime=Mtime, uti=Uti}) ->
	BinParts = lists:foldl(
		fun ({FourCC, Hash}, AccIn) ->
			<<AccIn/binary, FourCC/binary, Hash/binary>>
		end,
		<<(length(Parts)):8>>,
		Parts),
	BinParents = lists:foldl(
		fun (Parent, AccIn) ->
			<<AccIn/binary, Parent/binary>>
		end,
		<<(length(Parents)):8>>,
		Parents),
	BinUti = <<(size(Uti)):32/little, Uti/binary>>,
	erlang:md5(<<Flags:32/little, BinParts/binary, BinParents/binary,
		Mtime:64/little, BinUti/binary>>).

