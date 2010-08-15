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
-export([sync/2]).

-include("store.hrl").

% Reply: ok | {error, Reason}
sync(Doc, Stores) ->
	{AllRevs, AllStores} = lists:foldl(
		fun(Store, {AccRevs, AccStores} = Acc) ->
			case store:lookup(Store, Doc) of
				{ok, SomeRev} -> {[SomeRev|AccRevs], [Store|AccStores]};
				error         -> Acc
			end
		end,
		{[], []},
		Stores),
	case calc_dest_rev(AllRevs) of
		{ok, DestRev} -> do_sync(Doc, DestRev, lists:zip(AllStores, AllRevs));
		error         -> {error, conflict}
	end.


%% To calculate the revision which is the parent of all other revisions
%% (fast-forward head) the history of each revision is examined and recorded.
%% After each step the algorithm looks if the so-far traversed history of any
%% revision contains all other Revs. If this is the case then the fast-forward
%% head was found.
%%
%% The algorithm ends with an error when no such revision was found yet and the
%% ends of all revision histories were reached.
calc_dest_rev(Revs) ->
	% State :: [ {BaseRev::guid(), Heads::[guid()], Path::set()} ]
	State = [ {Rev, [Rev], sets:from_list([Rev])} || Rev <- Revs ],
	calc_dest_loop(Revs, State).


calc_dest_loop(BaseRevs, State) ->
	% go back in history another step
	{NewState, AddedSth} = calc_dest_step(State),
	% fast-forward head found?
	case find_ff_head(BaseRevs, NewState) of
		{ok, _Rev} = Result ->
			Result;
		error ->
			% try another round, but only if we have some unknown heads left
			if
				AddedSth -> calc_dest_loop(BaseRevs, NewState);
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


calc_dest_step(State) ->
	lists:foldl(
		fun(RevInfo, {AccState, AccAddedSth}) ->
			{_BaseRev, Heads, _Path} = NewRevInfo = follow(RevInfo),
			{[NewRevInfo|AccState], (Heads =/= []) or AccAddedSth}
		end,
		{[], false},
		State).


follow({BaseRev, Heads, Path}) ->
	lists:foldl(
		fun(Head, {AccBase, AccHeads, AccPath} = Acc) ->
			case broker:stat(Head) of
				{ok, _Flags, _Parts, Parents, _Mtime, _Uti, _Volumes} ->
					{
						AccBase,
						Parents ++ AccHeads,
						sets:union(sets:from_list(Parents), AccPath)
					};

				error ->
					Acc
			end
		end,
		{BaseRev, [], Path},
		Heads).


do_sync(Doc, DestRev, AllStores) ->
	{value, {LeadStore, DestRev}, FollowStores} = lists:keytake(DestRev, 2,
		AllStores),
	% create/set temporary doc on lead store
	case create_tmp(LeadStore, DestRev) of
		{ok, TmpDoc} ->
			% replicate temporary doc (with all parent revs) to all stores
			Reply = case replicate_tmp_doc(TmpDoc, FollowStores) of
				ok->
					switch(Doc, DestRev, AllStores);

				{error, Reason} ->
					{error, Reason}
			end,
			cleanup(TmpDoc, AllStores),
			Reply;

		{error, Reason} ->
			{error, Reason}
	end.


create_tmp(LeadStore, DestRev) ->
	Doc = crypto:rand_bytes(16),
	case store:put_doc(LeadStore, Doc, DestRev, DestRev) of
		ok              -> {ok, Doc};
		{error, Reason} -> {error, Reason}
	end.


replicate_tmp_doc(TmpDoc, RepStores) ->
	Stores = lists:map(fun({Store, _Rev}) -> store:guid(Store) end, RepStores),
	replicator:replicate_doc_sync(TmpDoc, Stores, true).


switch(Doc, NewRev, Stores) ->
	% point of no return: once we switch one store we have to do it for all
	lists:foldl(
		fun({Store, OldRev}, Result) ->
			case store:put_doc(Store, Doc, OldRev, NewRev) of
				ok    -> Result;
				Error -> Error
			end
		end,
		ok,
		Stores).


cleanup(TmpDoc, Stores) ->
	lists:foreach(
		fun({Store, _Rev}) -> store:delete_doc(Store, TmpDoc) end,
		Stores).

