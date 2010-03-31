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
	delete_rev/2, delete_uuid/2, fork/3, lookup/1, merge/4, merge_trivial/3,
	read_done/1, read_part/4, read_start/1, replicate_rev/2, replicate_uuid/2,
	stat/1, update/2, write_abort/1, write_commit/1, write_part/4,
	write_trunc/3]).


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
%% @spec stat(Rev) -> {ok, Parts, Parents, Mtime, Uti, Volumes} | error
%%       Parts = [{FourCC, Size, Hash}]
%%       FourCC, Hash, Uti = binary()
%%       Size, Mtime = integer()
%%       Parents = Volumes = [guid()]
stat(Rev) ->
	lists:foldl(
		fun({Guid, Ifc}, Result) ->
			case Result of
				{ok, Parts, Parents, Mtime, Uti, Volumes} ->
					case store:contains(Ifc, Rev) of
						true ->
							{ok, Parts, Parents, Mtime, Uti, [Guid|Volumes]};
						false ->
							Result
					end;

				error ->
					case store:stat(Ifc, Rev) of
						{ok, Parts, Parents, Mtime, Uti} ->
							{ok, Parts, Parents, Mtime, Uti, [Guid]};
						error ->
							error
					end
			end
		end,
		error,
		volman:stores()).


%% @doc Start reading a document identified by Rev.
%% @spec read_start(Rev) -> {ok, Reader} | {error, Reason}
%%       Rev = guid()
%%       Reader = reader()
%%       Reason = term()
read_start(Rev) ->
	User = self(),
	read_start_loop(Rev, User, volman:stores()).

read_start_loop(_Rev, _User, []) ->
	{error, enoent};
read_start_loop(Rev, User, [{_Guid, StoreIfc} | Stores]) ->
	case store:read_start(StoreIfc, Rev, User) of
		{ok, Reader} ->
			{ok, Reader};
		_Else ->
			read_start_loop(Rev, User, Stores)
	end.


%% @doc Read a part of a document
%% @spec read_part(Reader, Part, Offset, Length) ->  {ok, Data} | eof | {error, Reason}
%%       Reader = reader()
%%       Part, Offset, Length = int()
%%       Data = binary()
%%       Reason = term()
read_part(Reader, Part, Offset, Length) ->
	store:read_part(Reader, Part, Offset, Length).


%% @doc Finish the read process.
%% @spec read_done(Reader)
%%       Reader = reader()
read_done(Reader) ->
	store:read_done(Reader).


%% @doc Fork a new document from an existing revision.
%%
%% Returns `{ok, Uuid, Writer}' which represents the created UUID and a handle
%% for the following write_* functions to fill the object. The initial revision
%% identifier will be returned by write_done/1 which will always succeed for
%% newly created documents (despite IO errors).
%%
%% To create a empty new document set StartRev to <<0:128>>. To derive a document
%% from an existing one set StartRev to a revision which is available on Store.
%%
%% @spec fork(Store, StartRev, Uti) -> {ok, Uuid, Writer} | {error, Reason}
%%       Store, StartRev, Uuid = guid()
%%       Uti = binary()
%%       Writer = writer()
%%       Reason = enoent | term()
fork(Store, StartRev, Uti) ->
	User = self(),
	case volman:store(Store) of
		{ok, StoreIfc} ->
			case broker_writer:start({fork, StoreIfc, StartRev, Uti, User}) of
				{ok, Writer} ->
					{ok, broker_writer:get_uuid(Writer), Writer};
				{error, Reason} ->
					{error, Reason}
			end;
		error ->
			{error, enoent}
	end.


%% @doc Merge different document revisions.
%%
%% Write to the document identified by Uuid. Revs must be a superset of all
%% available revisions of the document. If Revs is a strict superset then the
%% new revision will represent a merge of a previously cloned document.
%% Revs should have at least two elements, otherwise update/2 should be
%% used.
%%
%% The newly created document revision will start completely empty, just like
%% as if fork/3 would have been called! The new revision will be stored on
%% all stores where the Uuid exists.
%%
%% @spec merge(Stores, Uuid, Revs, Uti) -> {ok, Writer} | {error, Reason}
%%       Stores = [#store]
%%       Uuid = guid()
%%       Revs = [guid()]
%%       Uti = binary()
%%       Writer = writer()
%%       Reason = conflict | enoent | term()
merge(Stores, Uuid, Revs, Uti) ->
	User = self(),
	% Lookup all stores&revisions where the Uuid exists
	{StartStores, FoundRevs} = lists:foldl(
		fun(StoreIfc, {AccStores, AccRevs}) ->
			case store:lookup(StoreIfc, Uuid) of
				{ok, Rev} -> {[StoreIfc|AccStores], [Rev|AccRevs]};
				error     -> {AccStores, AccRevs}
			end
		end,
		{[], []},
		Stores),
	% Check if Revs is a superset of all revisions
	case FoundRevs of
		[] ->
			{error, enoent};

		_ ->
			AvailRevs = sets:from_list(FoundRevs),
			StartRevs = sets:from_list(Revs),
			case sets:is_subset(AvailRevs, StartRevs) of
				true  -> broker_writer:start({merge, Uuid, StartStores, Revs, Uti, User});
				false -> {error, conflict}
			end
	end.


%% @doc Perform a trivial merge
%%
%% A trivial merge is a merge where the resulting content equals an existing
%% revision. Threre are two possible scenarios (in "git"-terms):
%%
%%   - Fast-forward: If `OtherRevs' is `[]' then the Uuid is updated on all
%%                   stores to point to `DestRev'. DestRev will be replicated
%%                   to all affected stores.
%%   - "ours"-merge: In this case `OtherRevs' points to other revisions. A new
%%                   revision will be created with exactly the same content as
%%                   `DestRev'.
%%
%%@spec merge_trivial(Uuid, DestRev, OtherRevs) -> {ok, Rev} | {error, Reason}
%%      Uuid, DestRev, Rev = guid()
%%      OtherRevs = [guid()]
%%      Reason = term()
merge_trivial(Uuid, DestRev, OtherRevs) ->
	Stores = lists:foldl(
		fun({_Guid, Store}, Acc) -> [Store|Acc] end,
		[],
		volman:stores()),
	case broker_merger:merge(Uuid, DestRev, OtherRevs, Stores) of
		{ok, Rev} ->
			vol_monitor:trigger_mod_uuid(local, Uuid),
			{ok, Rev};
		Else ->
			Else
	end.


%% @doc Write to a document.
%%
%% Write to the document identified by Uuid. If the document exists on more
%% than one store then only the stores pointing to Rev will be updated. All
%% affected stores are updated simultaniously.
%%
%% @spec update(Uuid, Rev) -> {ok, Writer} | {error, Reason}
%%       Uuid, Rev = guid()
%%       Writer = writer()
%%       Reason = ecode()
update(Uuid, Rev) ->
	User = self(),
	% Lookup all stores&revisions where the Uuid exists and points to Rev
	StartStores = lists:foldl(
		fun({_Guid, StoreIfc}, AccStores) ->
			case store:lookup(StoreIfc, Uuid) of
				{ok, Rev}       -> [StoreIfc|AccStores];
				{ok, _OtherRev} -> AccStores;
				error           -> AccStores
			end
		end,
		[],
		volman:stores()),
	if
		StartStores == [] ->
			{error, enoent};
		true ->
			broker_writer:start({update, Uuid, StartStores, Rev, User})
	end.


% ok | {error, Reason}
write_part(Writer, Part, Offset, Data) ->
	broker_writer:write_part(Writer, Part, Offset, Data).

% ok | {error, Reason}
write_trunc(Writer, Part, Offset) ->
	broker_writer:write_trunc(Writer, Part, Offset).

% {ok, Rev} | {error, conflict} | {error, Reason}
write_commit(Writer) ->
	broker_writer:write_commit(Writer).

% ok
write_abort(Writer) ->
	broker_writer:write_abort(Writer).


% ok | {error, Reason}
delete_uuid(Store, Uuid) ->
	case volman:store(Store) of
		{ok, StoreIfc} ->
			store:delete_uuid(StoreIfc, Uuid);
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

