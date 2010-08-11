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

-module(broker).

-export([
	delete_rev/2, delete_doc/2, fork/3, lookup/1,
	read/4, peek/2, replicate_rev/2, replicate_uuid/2,
	stat/1, update/2, abort/1, commit/2, write/4,
	truncate/3]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Hotchpotch operations...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Lookup a UUID.
%% @spec lookup(Uuid) -> [{Rev, [StoreGuid]}]
%%       Rev, StoreGuid = guid()
lookup(Uuid) ->
	RevDict = lists:foldl(
		fun({StoreGuid, StoreIfc}, Dict) ->
			case store:lookup(StoreIfc, Uuid) of
				{ok, Rev} -> dict:append(Rev, StoreGuid, Dict);
				error     -> Dict
			end
		end,
		dict:new(),
		volman:stores()),
	dict:to_list(RevDict).


%% @doc Get status information about a revision.
%% @spec stat(Rev) -> {ok, Flags, Parts, Parents, Mtime, Uti, Volumes} | error
%%       Parts = [{FourCC, Size, Hash}]
%%       FourCC, Hash, Uti = binary()
%%       Size, Mtime = integer()
%%       Parents = Volumes = [guid()]
stat(Rev) ->
	lists:foldl(
		fun({Guid, Ifc}, Result) ->
			case Result of
				{ok, Flags, Parts, Parents, Mtime, Uti, Volumes} ->
					case store:contains(Ifc, Rev) of
						true ->
							{ok, Flags, Parts, Parents, Mtime, Uti, [Guid|Volumes]};
						false ->
							Result
					end;

				error ->
					case store:stat(Ifc, Rev) of
						{ok, Flags, Parts, Parents, Mtime, Uti} ->
							{ok, Flags, Parts, Parents, Mtime, Uti, [Guid]};
						error ->
							error
					end
			end
		end,
		error,
		volman:stores()).


%% @doc Start reading a specific revision.
%% @spec peek(Rev, Stores) -> {ok, Handle} | {error, Reason}
%%       Rev = guid()
%%       Stores = [guid()]
%%       Handle = handle()
%%       Reason = ecode()
peek(Rev, Stores) ->
	User = self(),
	broker_io:start({peek, Rev, Stores, User}).


%% @doc Fork a new document from an existing revision.
%%
%% Returns `{ok, Doc, Writer}' which represents the created document and a handle
%% for the following write_* functions to fill the object. The initial revision
%% identifier will be returned by write_done/1 which will always succeed for
%% newly created documents (despite IO errors).
%%
%% To create a empty new document set StartRev to <<0:128>>. To derive a document
%% from an existing one set StartRev to a revision which is available on the Stores.
%%
%% @spec fork(StartRev, Stores, Uti) -> {ok, Doc, Handle} | {error, Reason}
%%       Stores = [guid()]
%%       StartRev, Doc = guid()
%%       Uti = binary()
%%       Handle = handle()
%%       Reason = ecode()
fork(StartRev, Stores, Uti) ->
	User = self(),
	Doc = crypto:rand_bytes(16),
	case broker_io:start({fork, Doc, StartRev, Stores, Uti, User}) of
		{ok, Handle} ->
			{ok, Doc, Handle};
		{error, _} = Error ->
			Error
	end.


%% @doc Update a document.
%%
%% Write to the document identified by Doc. If the document exists on more
%% than one store then only the stores pointing to Rev will be updated. All
%% affected stores are updated simultaniously.
%%
%% @spec update(Doc, Rev, Stores, Uti) -> {ok, Handle} | {error, Reason}
%%       Doc, Rev = guid()
%%       Stores = [guid()]
%%       Uti = binary()
%%       Handle = handle()
%%       Reason = ecode()
update(Doc, Rev, Stores, Uti) ->
	User = self(),
	broker_io:start({update, Doc, Rev, Stores, Uti, User}).


%% @doc Read a part of a document
%% @spec read(Handle, Part, Offset, Length) ->  {ok, Data} | eof | {error, Reason}
%%       Handle = handle()
%%       Part, Offset, Length = int()
%%       Data = binary()
%%       Reason = ecode()
read(Handle, Part, Offset, Length) ->
	broker_io:read(Handle, Part, Offset, Length).

% ok | {error, Reason}
write(Handle, Part, Offset, Data) ->
	broker_io:write(Handle, Part, Offset, Data).

% ok | {error, Reason}
truncate(Handle, Part, Offset) ->
	broker_io:truncate(Handle, Part, Offset).

% {ok, Rev} | conflict | {error, Reason}
commit(Handle, MergeRevs) ->
	broker_io:commit(Handle).

% ok
abort(Handle) ->
	broker_io:abort(Handle).


% ok | {error, Reason}
delete_doc(Store, Doc) ->
	case volman:store(Store) of
		{ok, StoreIfc} ->
			store:delete_doc(StoreIfc, Doc);
		error ->
			{error, enoent}
	end.


% ok | {error, Reason}
delete_rev(Store, Rev) ->
	case volman:store(Store) of
		{ok, StoreIfc} ->
			store:delete_rev(StoreIfc, Rev);
		error ->
			{error, enoent}
	end.


%% @doc Replicate a Uuid to a new store.
%%
%% The Uuid must be unambiguous, that is it must have the same revision on all
%% stores in the system. The Uuid may already exist on the destination store.
%%
%% @spec replicate_uuid(Uuid, Store) -> ok | {error, Reason}
replicate_uuid(Uuid, ToStore) ->
	case volman:store(ToStore) of
		{ok, StoreIfc} ->
			case lookup(Uuid) of
				[] ->
					{error, enoent};

				[{Rev, _}] ->
					store:put_uuid(StoreIfc, Uuid, Rev, Rev);

				_ ->
					{error, conflict}
			end;

		error ->
			{error, enoent}
	end.


%% @doc Replicate a revision to another store.
%%
%% The revision must be referenced by another document on the destination
%% store, otherwise the revision is immediately eligible for garbage collection
%% or the destination store may refuse to replicate the revision entirely.
%%
%% @spec replicate_rev(Rev, Store) -> ok | {error, Reason}
replicate_rev(Rev, ToStore) ->
	case volman:store(ToStore) of
		{ok, StoreIfc} ->
			broker_replicator:put_rev(StoreIfc, Rev);

		error ->
			{error, enoent}
	end.

