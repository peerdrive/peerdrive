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

-module(broker_syncer).
-export([sync/3]).

-include("store.hrl").

% Reply: {ok, ErrInfo, Rev} | {error, Reason, ErrInfo}
sync(Doc, Depth, Stores) ->
	{AllRevs, AllStores} = lists:foldl(
		fun({_, Ifc} = Store, {AccRevs, AccStores} = Acc) ->
			case store:lookup(Ifc, Doc) of
				{ok, SomeRev, _PreRevs} ->
					{[SomeRev|AccRevs], [Store|AccStores]};
				error ->
					Acc
			end
		end,
		{[], []},
		Stores),
	case AllRevs of
		[] ->
			{error, enoent, []};

		_ ->
			case calc_dest_rev(AllRevs, AllStores) of
				{ok, DestRev} ->
					do_sync(Doc, Depth, DestRev, lists:zip(AllStores, AllRevs));
				error ->
					{error, conflict, []}
			end
	end.


%% To calculate the revision which is the parent of all other revisions
%% (fast-forward head) the history of each revision is examined and recorded.
%% After each step the algorithm looks if the so-far traversed history of any
%% revision contains all other Revs. If this is the case then the fast-forward
%% head was found.
%%
%% The algorithm ends with an error when no such revision was found yet and the
%% ends of all revision histories were reached.
calc_dest_rev(Revs, Stores) ->
	% State :: [ {BaseRev::guid(), Heads::[guid()], Path::set()} ]
	State = [ {Rev, [Rev], sets:from_list([Rev])} || Rev <- Revs ],
	calc_dest_loop(Revs, State, Stores).


calc_dest_loop(BaseRevs, State, Stores) ->
	% go back in history another step
	{NewState, AddedSth} = calc_dest_step(State, Stores),
	% fast-forward head found?
	case find_ff_head(BaseRevs, NewState) of
		{ok, _Rev} = Result ->
			Result;
		error ->
			% try another round, but only if we have some unknown heads left
			if
				AddedSth -> calc_dest_loop(BaseRevs, NewState, Stores);
				true     -> error
			end
	end.


find_ff_head(_BaseRevs, []) ->
	error;

find_ff_head(BaseRevs, [{Candidate, _Heads, Path} | Paths]) ->
	case lists:all(fun(Rev) -> sets:is_element(Rev, Path) end, BaseRevs) of
		true -> {ok, Candidate};
		false -> find_ff_head(BaseRevs, Paths)
	end.


calc_dest_step(State, Stores) ->
	lists:foldl(
		fun(RevInfo, {AccState, AccAddedSth}) ->
			{_BaseRev, Heads, _Path} = NewRevInfo = follow(RevInfo, Stores),
			{[NewRevInfo|AccState], (Heads =/= []) or AccAddedSth}
		end,
		{[], false},
		State).


follow({BaseRev, Heads, Path}, Stores) ->
	lists:foldl(
		fun(Head, {AccBase, AccHeads, AccPath} = Acc) ->
			case broker:stat(Head, Stores) of
				{ok, _ErrInfo, #rev_stat{parents=Parents}} ->
					{
						AccBase,
						Parents ++ AccHeads,
						sets:union(sets:from_list(Parents), AccPath)
					};

				{error, _Reason, _ErrInfo} ->
					Acc
			end
		end,
		{BaseRev, [], Path},
		Heads).


do_sync(Doc, Depth, DestRev, AllStores) ->
	{value, {{LeadStoreGuid, LeadStoreIfc}, DestRev}, _FollowStores} =
		lists:keytake(DestRev, 2, AllStores),
	% create/set temporary doc on lead store
	case create_tmp(LeadStoreIfc, DestRev) of
		{ok, TmpDoc} ->
			% replicate temporary doc (with all parent revs) to all stores
			Reply = case replicate_tmp_doc(TmpDoc, Depth, AllStores) of
				{ok, _ErrInfo} -> % FIXME: merge ErrInfos
					switch(Doc, DestRev, AllStores);

				Error ->
					Error
			end,
			cleanup(TmpDoc, DestRev, AllStores),
			Reply;

		{error, Reason} ->
			{error, Reason, [{LeadStoreGuid, Reason}]}
	end.


create_tmp(LeadStore, DestRev) ->
	Doc = crypto:rand_bytes(16),
	case store:put_doc(LeadStore, Doc, DestRev, DestRev) of
		ok              -> {ok, Doc};
		{error, Reason} -> {error, Reason}
	end.


replicate_tmp_doc(TmpDoc, Depth, RepStores) ->
	Stores = [Store || {Store, _Rev} <- RepStores],
	replicator:replicate_doc_sync(TmpDoc, Depth, Stores, Stores).


switch(Doc, NewRev, Stores) ->
	% point of no return: once we switch one store we have to do it for all
	ErrInfo = lists:foldl(
		fun({{Guid, Store}, OldRev}, Result) ->
			case store:put_doc(Store, Doc, OldRev, NewRev) of
				ok ->
					Result;
				{error, Reason} ->
					[{Guid, Reason} | Result]
			end
		end,
		[],
		Stores),
	vol_monitor:trigger_mod_doc(local, Doc),
	case ErrInfo of
		[] -> {ok, [], NewRev};
		_  -> broker:consolidate_error(ErrInfo)
	end.


cleanup(TmpDoc, DestRev, Stores) ->
	lists:foreach(
		fun({{_, Store}, _Rev}) -> store:delete_doc(Store, TmpDoc, DestRev) end,
		Stores).

