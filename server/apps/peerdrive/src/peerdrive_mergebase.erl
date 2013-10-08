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

-module(peerdrive_mergebase).
-export([new/2, delete/1, ff_head/1, ff_path/3, merge_bases/1]).

-include("store.hrl").

-record(graph, {g, dest, stores}).
-record(node, {stat, paths}).

new(BaseRevs, Stores) ->
	G = digraph:new([acyclic, private]),
	heads = digraph:add_vertex(G, heads), % to-be-searched heads
	Destination = lists:foldr(
		fun(Rev, Acc) ->
			case peerdrive_broker:stat(Rev, Stores) of
				{ok, Stat} ->
					Rev = digraph:add_vertex(G, Rev, #node{stat=Stat, paths=[Rev]}),
					digraph:add_edge(G, {heads, Rev}, heads, Rev, []),
					[Rev | Acc];

				{error, _} ->
					Acc
			end
		end,
		[],
		lists:usort(BaseRevs)),
	Graph = #graph{g=G, dest=Destination, stores=Stores},
	search(Graph),
	Graph.


search(#graph{g=G, dest=Dest} = Graph) ->
	case digraph:out_neighbours(G, heads) of
		[] ->
			ok;
		Heads ->
			case pop_youngest(G, Heads, Dest) of
				none ->
					ok;
				Head ->
					search_parents(Graph, Head),
					search(Graph)
			end
	end.


pop_youngest(G, Heads, Dest) ->
	{SearchHeads, DestHeads} = lists:foldl(
		fun(Head, {AccS, AccD}) ->
			{Head, #node{stat=#rev{mtime=Mtime}, paths=Paths}}
				= digraph:vertex(G, Head),
			case Paths of
				Dest -> {AccS, [{Mtime, Head}|AccD]};
				_    -> {[{Mtime, Head}|AccS], AccD}
			end
		end,
		{[], []},
		Heads),
	case SearchHeads of
		[] ->
			% nothing interresting anymore
			lists:foreach(
				fun({_, Head}) -> digraph:del_edge(G, {heads, Head}) end,
				DestHeads),
			none;

		_ ->
			SortFun = fun({A,_}, {B,_}) -> A >= B end,
			AllHeads = DestHeads ++ SearchHeads,
			{_, Head} = hd(lists:sort(SortFun, AllHeads)),
			digraph:del_edge(G, {heads, Head}),
			Head
	end.


search_parents(#graph{g=G} = Graph, Rev) ->
	{Rev, #node{stat=RevStat, paths=Paths}} = digraph:vertex(G, Rev),
	Parents = case (RevStat#rev.flags band ?REV_FLAG_EPHEMERAL) of
		0 -> RevStat#rev.parents;
		_ -> []
	end,
	lists:foreach(
		fun(Parent) ->
			case digraph:vertex(G, Parent) of
				{Parent, Node} ->
					% Rev already discovered by other path
					digraph:add_edge(G, {Rev, Parent}, Rev, Parent, []),
					update_node(Graph, Parent, Node, Paths);

				false ->
					% yet unknown Rev
					add_node(Graph, Parent, Rev, Paths)
			end

		end,
		Parents).


update_node(#graph{g=G} = Graph, Rev, Node, Paths) ->
	CurPaths = Node#node.paths,
	case lists:usort(CurPaths++Paths) of
		CurPaths ->
			ok;

		NewPaths ->
			% update this node and its parents
			digraph:add_vertex(G, Rev, Node#node{paths=NewPaths}),
			lists:foreach(
				fun(Parent) ->
					{Parent, ParentNode} = digraph:vertex(G, Parent),
					update_node(Graph, Parent, ParentNode, NewPaths)
				end,
				digraph:out_neighbours(G, Rev))
	end.


add_node(#graph{g=G, stores=Stores}, Rev, Child, Paths) ->
	case peerdrive_broker:stat(Rev, Stores) of
		{ok, Stat} ->
			Rev = digraph:add_vertex(G, Rev, #node{stat=Stat, paths=Paths}),
			digraph:add_edge(G, {heads, Rev}, heads, Rev, []),
			digraph:add_edge(G, {Child, Rev}, Child, Rev, []);

		{error, _} ->
			ok
	end.


delete(#graph{g=G}) ->
	digraph:delete(G).


%% Find the revision which can reach all other revisions
ff_head(#graph{g=G, dest=Dest}) ->
	ff_head_loop(G, Dest, Dest).


ff_head_loop(_G, _Dest, []) ->
	error;

ff_head_loop(G, Dest, [Head | Rest]) ->
	TestFun = fun(H) ->
		(H == Head) or is_list(digraph:get_path(G, Head, H))
	end,
	case lists:all(TestFun, Dest) of
		true  -> {ok, Head};
		false -> ff_head_loop(G, Dest, Rest)
	end.


ff_path(#graph{g=G}, Head, Rev) ->
	case digraph:get_short_path(G, Head, Rev) of
		false -> error;
		Path  -> {ok, lists:reverse(Path)}
	end.


merge_bases(#graph{g=G, dest=Dest}) ->
	Paths = [ {[Rev], sets:new()} || Rev <- Dest ],
	get_merge_base_loop(Paths, G).


get_merge_base_loop(Paths, G) ->
	{Heads, PathSets} = lists:unzip(Paths),
	% did we already find a common rev?
	Common = sets:intersection(PathSets),
	case sets:size(Common) of
		0 ->
			% traverse deeper into the history
			case lists:all(fun(H) -> H == [] end, Heads) of
				true ->
					% no heads anymore -> cannot traverse further
					error;

				false ->
					NewPaths = [ traverse(OldHeads, OldPath, G) ||
						{OldHeads, OldPath} <- Paths ],
					get_merge_base_loop(NewPaths, G)
			end;

		% seems so :)
		_ ->
			{ok, sets:to_list(Common)}
	end.


traverse(Heads, Path, G) ->
	lists:foldl(
		fun(Head, {AccHeads, AccPath}) ->
			Parents = digraph:out_neighbours(G, Head),
			{Parents ++ AccHeads, sets:add_element(Head, AccPath)}
		end,
		{[], Path},
		Heads).

