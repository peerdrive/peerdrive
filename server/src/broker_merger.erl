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

-module(broker_merger).
-export([merge/4]).

-include("store.hrl").

% Reply: {ok, Rev} | {error, Reason}
merge(Uuid, DestRev, OtherRevs, Stores) ->
	% find all stores where the Uuid exists and with which rev
	AllStores = lists:foldl(
		fun(Store, Acc) ->
			case store:lookup(Store, Uuid) of
				{ok, SomeRev} -> [{Store, SomeRev}|Acc];
				error         -> Acc
			end
		end,
		[],
		Stores),
	% at least one store must currently point to DestRev
	case lists:keytake(DestRev, 2, AllStores) of
		{value, {LeadStore, DestRev}, FollowStores} ->
			% create/set temporary uuid on lead store
			case create_tmp(LeadStore, DestRev, OtherRevs) of
				{ok, TmpUuid, NewRev} ->
					% replicate temporary uuid (with all parent revs) to all stores
					Reply = case replicate_tmp_uuid(TmpUuid, FollowStores) of
						ok->
							% switch to original uuid, delete temporary uuid
							switch(Uuid, NewRev, AllStores),
							{ok, NewRev};

						{error, Reason} ->
							{error, Reason}
					end,
					cleanup(TmpUuid, AllStores),
					Reply;

				{error, Reason} ->
					{error, Reason}
			end;

		false ->
			{error, enoent}
	end.


create_tmp(LeadStore, DestRev, OtherRevs) ->
	case OtherRevs of
		[] ->
			% fast-forward
			Uuid = crypto:rand_bytes(16),
			case store:put_uuid(LeadStore, Uuid, DestRev, DestRev) of
				ok              -> {ok, Uuid, DestRev};
				{error, Reason} -> {error, Reason}
			end;

		_ ->
			% 'ours' merge
			case create_merge_rev(LeadStore, DestRev, OtherRevs) of
				{ok, Uuid, NewRev} ->
					LeadGuid = store:guid(LeadStore),
					case gather_other_revs(LeadGuid, OtherRevs) of
						ok -> {ok, Uuid, NewRev};
						_  -> {error, replication_error}
					end;

				{error, Reason} ->
					{error, Reason}
			end
	end.


gather_other_revs(_Store, []) ->
	ok;
gather_other_revs(Store, [Rev|Rest]) ->
	case replicator:replicate_rev_sync(Rev, [Store], true) of
		ok   -> gather_other_revs(Store, Rest);
		Else -> Else
	end.


replicate_tmp_uuid(TmpUuid, RepStores) ->
	Stores = lists:map(fun({Store, _Rev}) -> store:guid(Store) end, RepStores),
	replicator:replicate_uuid_sync(TmpUuid, Stores, true).


switch(Uuid, NewRev, Stores) ->
	% point of no return: once we switch one store we have to do it for all
	lists:foreach(
		fun({Store, OldRev}) ->
			store:put_uuid(Store, Uuid, OldRev, NewRev)
		end,
		Stores).


cleanup(TmpUuid, Stores) ->
	lists:foreach(
		fun({Store, _Rev}) -> store:delete_uuid(Store, TmpUuid) end,
		Stores).


%% @doc Create a merge document from an existing revision
%%
%% The newly created document has exactly the same content as StartRev but it's
%% parents set to [StartRev|OtherRevs].
%%
%% @spec create_merge_rev(Store, StartRev, OtherRevs) -> {ok, Uuid, Rev} | {error, Reason}
%%         Store = #store
%%         StartRev, Uuid, Rev = guid()
%%         OtherRevs = [guid()]
%%         Reason = term()
create_merge_rev(Store, StartRev, OtherRevs) ->
	case store:stat(Store, StartRev) of
		% create new object and uuid
		{ok, OldParts, _Parents, _Mtime, Uti} ->
			Parts = lists:map(fun({FCC, _Size, Hash}) -> {FCC, Hash} end, OldParts),
			Parents = [StartRev|OtherRevs],
			NewObject = #object{
				parts   = lists:sort(Parts),
				parents = lists:sort(Parents),
				mtime   = util:get_time(),
				uti     = Uti},
			NewRev = store:hash_object(NewObject),
			Uuid = crypto:rand_bytes(16),
			case store:put_uuid(Store, Uuid, NewRev, NewRev) of
				ok ->
					case store:put_rev_start(Store, NewRev, NewObject) of
						ok ->
							% as expected
							{ok, Uuid, NewRev};

						{ok, _MissingParts, Importer} ->
							% well, I could swear you told me you have everything :(
							store:put_rev_abort(Importer),
							store:delete_uuid(Store, Uuid),
							{error, einternal};

						{error, Reason} ->
							% something bad has happened
							store:delete_uuid(Store, Uuid),
							{error, Reason}
					end;

				{error, Reason} ->
					{error, Reason}
			end;

		% err, don't known that one...
		error ->
			{error, enoent}
	end.

