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
	create/3, delete_rev/2, delete_doc/3, forget/3, fork/3, get_parents/1,
	get_type/1, lookup/2, read/4, peek/2, replicate_rev/2, replicate_doc/2,
	resume/4, set_parents/2, set_type/2, stat/2, suspend/1, update/4, abort/1,
	commit/1, write/4, truncate/3, sync/2]).

-export([consolidate_error/1, consolidate_success/2, consolidate_success/1,
	consolidate_filter/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Hotchpotch operations...
%%
%% The broker does normally work on more than one store simultaneously. The
%% operations result is therefore not a success/fail condition anymore. Instead
%% the operation could succeed on some stores and fail on others.
%%
%% All operations which can exhibit this behaviour will return their result in
%% the following form:
%%
%%   {ok, ErrInfo} | {ok, ErrInfo, Result} -- (partial) success
%%   {retry, Error, ErrInfo}               -- failed, but retry (commit)
%%   {error, Error, ErrInfo}               -- failed
%%
%% Error is the primary error which caused the operation to fail. `ErrInfo' is
%% a list of {Store, Error} tuples which indicate the failure code on a
%% specific store.
%%
%% If there is no unambiguous error then Error will be `eambig' and ErrInfo
%% should be inspected for individual error causes.  The list will typically
%% not contain `enoent' errors as these are not treated as error on the broker
%% level. OTOH if all stores return `enoent' then the primary error will be
%% enoent.
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Lookup a document.
%%
%% Returns any known current revisions and any pending preliminary revisions of
%% the document. Searches on the given stores or on all mounted stores if no
%% store was specified.
%%
%% @spec lookup(Doc, Stores) -> {[{Rev, [Store]}], [{PreRev, [Store]}]}
%%       Doc, Rev, PreRev, Store = guid()
%%       Stores = [guid()]
lookup(Doc, Stores) ->
	{RevDict, PreRevDict} = lists:foldl(
		fun({StoreGuid, StoreIfc}, {AccRev, AccPreRev}) ->
			case store:lookup(StoreIfc, Doc) of
				{ok, Rev, PreRevs} ->
					NewAccPreRev = lists:foldl(
						fun(PreRev, Acc) -> dict:append(PreRev, StoreGuid, Acc) end,
						AccPreRev,
						PreRevs),
					NewAccRev = dict:append(Rev, StoreGuid, AccRev),
					{NewAccRev, NewAccPreRev};

				error ->
					{AccRev, AccPreRev}
			end
		end,
		{dict:new(), dict:new()},
		get_stores(Stores)),
	{dict:to_list(RevDict), dict:to_list(PreRevDict)}.


%% @doc Get status information about a revision.
%%
%% Returns information about a revision if it is found on any of the specified
%% stores, or `error' if no such revision is found. The returned information is
%% a #stat{} record containing the following fields:
%%
%%   flags = integer()
%%   parts = [{FourCC::binary(), Size::interger(), Hash::guid()}]
%%   parents = [guid()]
%%   mtime = integer()
%%   type = binary()
%%   creator = binary()
%%
%% If an empty list of stores was given then all mounted stores are searched.
%%
%% @spec stat(Rev, SearchStores) -> Result
%%       Result = {ok, ErrInfo, {Stat, FoundStores}} | {error, Reason, ErrInfo}
%%       Rev = guid()
%%       SearchStores, FoundStores = [guid()]
%%       Stat = #stat{}
%%       ErrInfo = [{Store::guid(), Reason::ecode()}]
%%       Reason = ecode()
stat(Rev, SearchStores) ->
	{Stat, FoundStores, ErrInfo} = lists:foldl(
		fun({Guid, Ifc}, {SoFar, FoundStores, ErrInfo} = Acc) ->
			case SoFar of
				undef ->
					case store:stat(Ifc, Rev) of
						{ok, Stat} ->
							{Stat, [Guid], ErrInfo};
						{error, Reason} ->
							{undef, [], [{Guid, Reason} | ErrInfo]}
					end;

				Stat ->
					case store:contains(Ifc, Rev) of
						true  -> {Stat, [Guid|FoundStores], ErrInfo};
						false -> Acc
					end
			end
		end,
		{undef, [], []},
		get_stores(SearchStores)),
	case Stat of
		undef -> consolidate_error(ErrInfo);
		_     -> consolidate_success(ErrInfo, {Stat, FoundStores})
	end.


%% @doc Start reading a specific revision.
%% @spec peek(Rev, Stores) -> Result
%%       Result = {ok, ErrInfo, Handle} | {error, Reason, ErrInfo}
%%       Rev = guid()
%%       Stores = [guid()]
%%       Handle = handle()
%%       Reason = ecode()
peek(Rev, Stores) ->
	broker_io:start({peek, Rev, get_stores(Stores)}).


%% @doc Create a new, empty document.
%%
%% Returns a `{Doc, Handle}' which represents the created document and a
%% handle for the subsequent write calls to fill the revision. The initial
%% revision identifier will be returned by commit/1 which will always succeed
%% for newly created documents (despite IO errors).
%%
%% The handle can only be commited or aborted but not suspended because the new
%% document will not show up in the store until a successful commit.
%%
%% @spec create(Type, Creator, Stores) -> Result
%%       Result = {ok, ErrInfo, {Doc, Handle}} | {error, Reason, ErrInfo}
%%       Stores = [guid()]
%%       Doc = guid()
%%       Type, Creator = binary()
%%       Handle = handle()
%%       Reason = ecode()
create(Type, Creator, Stores) ->
	Doc = crypto:rand_bytes(16),
	StoreIfcs = get_stores(Stores),
	case broker_io:start({create, Doc, Type, Creator, StoreIfcs}) of
		{ok, ErrInfo, Handle} ->
			{ok, ErrInfo, {Doc, Handle}};
		{error, _, _} = Error ->
			Error
	end.


%% @doc Fork a new document from an existing revision.
%%
%% Returns `{ok, Doc, Handle}' which represents the created document and a
%% handle for the subsequent write calls to update the revision. The initial
%% content will be taken from StartRev.
%%
%% The handle can only be commited or aborted but not suspended because the new
%% document will not show up in the store until a successful commit.
%%
%% @spec fork(StartRev, Creator, Stores) -> Result
%%       Result = {ok, ErrInfo, {Doc, Handle}} | {error, Reason, ErrInfo}
%%       Stores = [guid()]
%%       StartRev, Doc = guid()
%%       Creator = binary()
%%       Handle = handle()
%%       Reason = ecode()
fork(StartRev, Creator, Stores) ->
	Doc = crypto:rand_bytes(16),
	StoreIfcs = get_stores(Stores),
	case broker_io:start({fork, Doc, StartRev, Creator, StoreIfcs}) of
		{ok, ErrInfo, Handle} ->
			{ok, ErrInfo, {Doc, Handle}};
		{error, _} = Error ->
			Error
	end.


%% @doc Update a document.
%%
%% Write to the document identified by Doc. If the document exists on more
%% than one store then only the stores pointing to Rev will be updated. All
%% affected stores are updated simultaniously.
%%
%% @spec update(Doc, StartRev, Creator, Stores) -> Result
%%       Result = {ok, ErrInfo, Handle} | {error, Reason, ErrInfo}
%%       Doc, StartRev = guid()
%%       Creator = binary()
%%       Stores = [guid()]
%%       Handle = handle()
%%       Reason = ecode()
update(Doc, StartRev, Creator, Stores) ->
	StoreIfcs = get_stores(Stores),
	broker_io:start({update, Doc, StartRev, Creator, StoreIfcs}).


%% @doc Resume writing to a document
%%
%% This will open a preliminary revision that was previously suspended. The
%% content will be exactly the same as when it was suspended, including its
%% parents. StartRev will be kept as pending preliminary revision (until
%% overwritten by either commit/1 or suspend/1). All affected stores are
%% updated simultaniously.
%%
%% @spec resume(Doc, PreRev, Creator, Stores) -> Result
%%       Result = {ok, ErrInfo, Handle} | {error, Reason, ErrInfo}
%%       Doc, PreRev = guid()
%%       Creator = binary()
%%       Stores = [guid()]
%%       Handle = handle()
%%       Reason = ecode()
resume(Doc, PreRev, Creator, Stores) ->
	StoreIfcs = get_stores(Stores),
	broker_io:start({resume, Doc, PreRev, Creator, StoreIfcs}).


%% @doc Read a part of a document
%%
%% Returns the requested data. Trying to read a non-existing part yields
%% {error, enoent}. May return less data if the end of the part was hit.
%%
%% @spec read(Handle, Part, Offset, Length) ->  Result
%%       Result = {ok, ErrInfo, Data} | {error, Reason, ErrInfo}
%%       Handle = handle()
%%       Part, Offset, Length = int()
%%       Data = binary()
%%       Reason = ecode()
read(Handle, Part, Offset, Length) ->
	broker_io:read(Handle, Part, Offset, Length).


%% @doc Write a part of a document
%%
%% Writes the given data at the requested offset of the part. If the part does
%% not exist yet it will be created.
%%
%% @spec write(Handle, Part, Offset, Data) -> Result
%%       Result = {ok, ErrInfo} | {error, Reason, ErrInfo}
%%       Handle = handle()
%%       Part = Data = binary()
%%       Offset = integer()
%%       Reason = ecode()
write(Handle, Part, Offset, Data) ->
	broker_io:write(Handle, Part, Offset, Data).


%% @doc Truncate part
%%
%% Truncates part at the given offset. If the part does not exist yet it will
%% be created.
%%
%% @spec truncate(Handle, Part, Offset) -> Result
%%       Result = {ok, ErrInfo} | {error, Reason, ErrInfo}
%%       Handle = handle()
%%       Part = binary()
%%       Offset = integer()
%%       Reason = ecode()
truncate(Handle, Part, Offset) ->
	broker_io:truncate(Handle, Part, Offset).


get_type(Handle) ->
	broker_io:get_type(Handle).

set_type(Handle, Uti) ->
	broker_io:set_type(Handle, Uti).

get_parents(Handle) ->
	broker_io:get_parents(Handle).

set_parents(Handle, Parents) ->
	broker_io:set_parents(Handle, Parents).


%% @doc Commit a new revision
%%
%% One of the parents of this new revision must point to the current revision
%% of the document, otherwise the function will fail with `confict'. The handle
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
%% @spec commit(Handle) -> Result
%%       Result = {ok, ErrInfo, Rev} | {retry, conflict, ErrInfo} |
%%                {error, Reason, ErrInfo}
%%       Handle = handle()
%%       Rev = guid()
%%       Reason = ecode()
commit(Handle) ->
	broker_io:commit(Handle).


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
%% The handle will be invalid after the call regardless of the result.
%%
%% @spec suspend(Handle) -> Result
%%       Result = {ok, ErrInfo, Rev} | {error, Reason, ErrInfo}
%%       Handle = handle()
%%       Rev = guid()
%%       Reason = ecode()
suspend(Handle) ->
	broker_io:suspend(Handle).


%% @doc Abort the creation of a new revision
%%
%% Discards the handle and throws away any changes. The handle will be invalid
%% after the call.
%%
%% @spec abort(Handle) -> {ok, ErrInfo}
%%       Handle = #handle{}
abort(Handle) ->
	broker_io:abort(Handle).


%% @doc Remove a pending preliminary revision from a document.
%%
%% @spec forget(Doc, PreRev, Stores) -> Result
%%       Result = {ok, ErrInfo} | {error, Reason, ErrInfo}
%%       Doc, PreRev = guid()
%%       Stores = [guid()]
%%       Reason = ecode()
forget(Doc, PreRev, Stores) ->
	{Result, ErrInfo} = lists:foldl(
		fun({Guid, Store}, {Result, ErrInfo}) ->
			case store:forget(Store, Doc, PreRev) of
				ok              -> {ok, ErrInfo};
				{error, Reason} -> {Result, [{Guid, Reason} | ErrInfo]}
			end
		end,
		{error, []},
		get_stores(Stores)),
	case Result of
		ok -> consolidate_success(ErrInfo);
		error -> consolidate_error(ErrInfo)
	end.


%% @doc Delete a document and release any referenced revisions.
%%
%% Normally documents should not be deleted explicitly but indirectly through
%% automatic garbage collection when they are no longer referenced inside the
%% store.
%%
%% @spec delete_doc(Doc, Rev, Stores) -> Result
%%       Result = {ok, ErrInfo} | {error, Reason, ErrInfo}
%%       Stores = [guid()]
%%       Doc, Rev = guid()
%%       Reason = ecode()
delete_doc(Doc, Rev, Stores) ->
	{Result, ErrInfo} = lists:foldl(
		fun({Guid, Store}, {Result, ErrInfo}) ->
			case store:delete_doc(Store, Doc, Rev) of
				ok              -> {ok, ErrInfo};
				{error, Reason} -> {Result, [{Guid, Reason} | ErrInfo]}
			end
		end,
		{error, []},
		get_stores(Stores)),
	case Result of
		ok -> consolidate_success(ErrInfo);
		error -> consolidate_error(ErrInfo)
	end.


%% @doc Delete a revision and release any referenced parent revisions
%%
%% This is normally used to explicitly delete old, unused revisions of a
%% document.
%%
%% @spec delete_rev(Rev, Stores) -> Result
%%       Result = {ok, ErrInfo} | {error, Reason, ErrInfo}
%%       Stores = [guid()]
%%       Rev = guid()
%%       Reason = ecode()
delete_rev(Rev, Stores) ->
	{Result, ErrInfo} = lists:foldl(
		fun({Guid, Store}, {Result, ErrInfo}) ->
			case store:delete_rev(Store, Rev) of
				ok              -> {ok, ErrInfo};
				{error, Reason} -> {Result, [{Guid, Reason} | ErrInfo]}
			end
		end,
		{error, []},
		get_stores(Stores)),
	case Result of
		ok -> consolidate_success(ErrInfo);
		error -> consolidate_error(ErrInfo)
	end.


%% @doc Synchronize a document between different stores to the same revision.
%%
%% Tries to perform a fast-forward merge for the document on the given stores
%% if the revisions differ.
%%
%% @spec sync(Doc, Stores) -> Result
%%       Result = {ok, ErrInfo} | {error, Reason, ErrInfo}
%%       Doc = guid()
%%       Stores = [guid()]
%%       Reason = ecode()
sync(Doc, Stores) ->
	StoreIfcs = get_stores(Stores),
	broker_syncer:sync(Doc, StoreIfcs).


%% @doc Replicate a document to a new store.
%%
%% The Doc must be unambiguous, that is it must have the same revision on all
%% stores in the system. The Doc may already exist on the destination store.
%%
%% @spec replicate_doc(Doc, Store) -> ok | {error, Reason}
replicate_doc(Doc, ToStore) ->
	case volman:store(ToStore) of
		{ok, StoreIfc} ->
			case lookup(Doc, []) of
				{[], _PreRevs} ->
					{error, enoent};

				{[{Rev, _}], _PreRevs} ->
					store:put_doc(StoreIfc, Doc, Rev, Rev);

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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_stores(StoreList) ->
	case StoreList of
		[] ->
			volman:stores();

		_ ->
			lists:foldl(
				fun(Guid, Acc) ->
					case volman:store(Guid) of
						{ok, Ifc} -> [{Guid, Ifc} | Acc];
						error     -> Acc
					end
				end,
				[],
				StoreList)
	end.


consolidate_error(ErrInfo) ->
	case consolidate_filter(ErrInfo) of
		[]                    -> {error, enoent, []};
		[{_, Error}] = Single -> {error, Error, Single};
		Multiple              -> {error, eambig, Multiple}
	end.


consolidate_success(ErrInfo, Result) ->
	{ok, consolidate_filter(ErrInfo), Result}.

consolidate_success(ErrInfo) ->
	{ok, consolidate_filter(ErrInfo)}.


consolidate_filter(ErrInfo) ->
	lists:filter(fun({_S, E}) -> E =/= enoent end, ErrInfo).

