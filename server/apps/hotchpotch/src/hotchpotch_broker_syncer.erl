%% Hotchpotch
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

-module(hotchpotch_broker_syncer).
-export([sync/3]).

-include("store.hrl").

% Reply: {ok, ErrInfo, Rev} | {error, Reason, ErrInfo}
sync(Doc, Depth, Stores) ->
	{AllRevs, AllStores} = lists:foldl(
		fun({_, Pid} = Store, {AccRevs, AccStores} = Acc) ->
			case hotchpotch_store:lookup(Pid, Doc) of
				{ok, SomeRev, _PreRevs} ->
					{[SomeRev|AccRevs], [Store|AccStores]};
				error ->
					Acc
			end
		end,
		{[], []},
		Stores),
	case lists:usort(AllRevs) of
		[] ->
			{error, enoent, []};

		[DestRev] ->
			{ok, [], DestRev};

		Heads ->
			Graph = hotchpotch_mergebase:new(Heads, AllStores),
			try
				case hotchpotch_mergebase:ff_head(Graph) of
					{ok, DestRev} ->
						do_sync(Doc, Depth, Graph, DestRev, AllRevs,
							AllStores);

					error ->
						{error, econflict, []}
				end
			after
				hotchpotch_mergebase:delete(Graph)
			end
	end.


do_sync(Doc, Depth, Graph, DestRev, Revs, Stores) ->
	{Result, ErrInfo} = lists:foldl(
		fun({CurRev, {Guid, _}=Store}, {Result, ErrInfo}) ->
			case CurRev of
				DestRev ->
					% already on head, make sure parent revs are here too
					hotchpotch_replicator:replicate_rev_sync(DestRev, Depth,
						Stores, [Store]),
					{Result, ErrInfo};

				_ ->
					% need to forward
					{ok, RevPath} = hotchpotch_mergebase:ff_path(Graph, DestRev, CurRev),
					case forward(Store, Doc, RevPath, Depth, Stores) of
						ok              -> {ok, ErrInfo};
						{error, Reason} -> {Result, [{Guid, Reason} | ErrInfo]}
					end
			end
		end,
		{error, []},
		lists:zip(Revs, Stores)),
	case Result of
		ok -> hotchpotch_broker:consolidate_success(ErrInfo, DestRev);
		error -> hotchpotch_broker:consolidate_error(ErrInfo)
	end.


forward({_, DstPid} = DstStore, Doc, RevPath, Depth, SrcStores) ->
	case hotchpotch_store:forward_doc_start(DstPid, Doc, RevPath) of
		ok ->
			ok;

		{ok, MissingRevs, Handle} ->
			try
				lists:foreach(
					fun(Rev) -> replicate(Rev, Depth, DstStore, SrcStores) end,
					MissingRevs),
				hotchpotch_store:forward_doc_commit(Handle)
			catch
				throw:Error -> hotchpotch_store:forward_doc_abort(Handle), Error
			end;

		{error, _Reason} = Error ->
			Error
	end.


replicate(Rev, Depth, DstStore, SrcStores) ->
	% FIXME: This will traverse the history every time up to 'Depth'! Slooooow...
	case hotchpotch_replicator:replicate_rev_sync(Rev, Depth, SrcStores, [DstStore]) of
		{ok, _} -> ok;
		{error, Reason, _ErrInfo} -> throw({error, Reason})
	end.

