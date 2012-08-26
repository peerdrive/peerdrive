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

-module(peerdrive_replicator_worker).
-behavior(gen_fsm).

-include("store.hrl").

-export([start_link/4, start_link/5]).
-export([init/1, code_change/4, handle_event/3, handle_info/3,
	handle_sync_event/4, terminate/3]).
-export([working/2, paused/2, error/2]).

-record(state, {g, backlog, srcstore, dststore, depth,
	verbose, monitor, all, done, pauseonerror, from, result}).
-record(copy, {rid, parts, part, reader, imp, pos, accsize, factor}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Request, SrcStore, DstStore, Options) ->
	gen_fsm:start_link(?MODULE, {Request, none, SrcStore, DstStore, Options}, []).

start_link(Request, SrcStore, DstStore, Options, From) ->
	gen_fsm:start_link(?MODULE, {Request, From, SrcStore, DstStore, Options}, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Request, From, SrcStore, DstStore, Options}) ->
	process_flag(trap_exit, true),
	S = #state{
		g              = digraph:new([acyclic]),
		backlog        = [],
		srcstore       = SrcStore,
		dststore       = DstStore,
		depth          = proplists:get_value(depth, Options, 0),
		verbose        = proplists:get_bool(verbose, Options),
		all            = 1,
		done           = 0,
		pauseonerror   = proplists:get_bool(pauseonerror, Options),
		from           = From,
		result         = ok
	},
	digraph:add_vertex(S#state.g, root),
	S2 = schedule(estimate, handle_node(undefined, root, S)),
	S3 = case Request of
		{doc, DId} ->
			Info = {rep_doc, peerdrive_store:guid(SrcStore), DId,
				peerdrive_store:guid(DstStore)},
			discover_doc(root, DId, true, S2);

		{rev, RId} ->
			Info = {rep_rev, peerdrive_store:guid(SrcStore), RId,
				peerdrive_store:guid(DstStore)},
			discover_rev(root, RId, true, true, S2)
	end,
	{ok, Monitor} = peerdrive_work:new(Info),
	peerdrive_work:start(Monitor),
	S4 = S3#state{monitor=Monitor},
	{ok, working, S4, 0}.


terminate(_Reason, _State, #state{monitor=Monitor, from=From, result=Result}) ->
	peerdrive_work:stop(Monitor),
	peerdrive_work:delete(Monitor),
	case From of
		{Pid, Ref} ->
			Pid ! {Ref, Result};
		_Else ->
			ok
	end.


working(timeout, S) ->
	try
		S2 = step(S),
		#state{all=Count, done=Done, monitor=Monitor} = S2,
		peerdrive_work:progress(Monitor, Done * 255 div Count),
		{next_state, working, S2, 0}
	catch
		throw:done ->
			{stop, normal, S};

		throw:{error, ErrInfo} ->
			case S#state.pauseonerror of
				true ->
					peerdrive_work:error(S#state.monitor, ErrInfo),
					{next_state, error, S};
				false ->
					Result = {error, proplists:get_value(code, ErrInfo, eio)},
					{stop, normal, S#state{result=Result}}
			end
	end.


paused(_, S) ->
	{next_state, paused, S}.


error(_, S) ->
	{next_state, error, S}.


handle_info({work_req, Req}, State, #state{monitor=Monitor} = S) ->
	case State of
		working ->
			case Req of
				pause ->
					peerdrive_work:pause(Monitor),
					{next_state, paused, S};
				stop ->
					{stop, normal, S#state{result={error, eintr}}};
				_ ->
					{next_state, working, S, 0}
			end;

		Halted ->
			case Req of
				{resume, false} ->
					peerdrive_work:resume(Monitor),
					{next_state, working, S, 0};
				{resume, true} ->
					peerdrive_work:resume(Monitor),
					{next_state, working, skip(S), 0};
				stop ->
					{stop, normal, S#state{result={error, eintr}}};
				_ ->
					{next_state, Halted, S}
			end
	end;

handle_info(_, working, S) ->
	{next_state, working, S, 0};

handle_info(_, State, S) ->
	{next_state, State, S}.


handle_event(_Event, StateName, StateData) ->
	{next_state, StateName, StateData, 0}.

handle_sync_event(_Event, _From, StateName, StateData) ->
	{next_state, StateName, StateData, 0}.

code_change(_OldVsn, StateName, StateData, _Extra) ->
	{ok, StateName, StateData}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

schedule(Action, #state{backlog=Backlog} = S) ->
	NewBacklog = [Action | Backlog],
	S#state{backlog=NewBacklog}.


step(#state{backlog=[Step | Rest]} = S) ->
	S2 = S#state{backlog=Rest},
	case Step of
		{discover_doc, Parent, DId, First} ->
			do_discover_doc(Parent, DId, First, S2);
		{discover_rev, Parent, RId, First, Force} ->
			do_discover_rev(Parent, RId, First, Force, S2);
		{handle_node, Parent, Node} ->
			do_handle_node(Parent, Node, S2);
		#copy{} = CopyAction ->
			do_copy(CopyAction, S2);
		estimate ->
			do_estimate(S2)
	end;

step(#state{backlog=[]}) ->
	throw(done).


skip(#state{g=G, backlog=[Skip | NewBacklog]} = S) ->
	S2 = S#state{backlog=NewBacklog},
	case Skip of
		#copy{reader=Reader, accsize=Size} ->
			peerdrive_store:close(Reader),
			account_size(Size, S2);
		{handle_node, _Parent, Node} ->
			{Node, MyProps} = digraph:vertex(G, Node),
			Size = proplists:get_value(size, MyProps, 0),
			account_size(Size, S2);
		_ ->
			S2
	end;

skip(#state{backlog=[]} = S) ->
	S.


discover_doc(Parent, DId, S) ->
	discover_doc(Parent, DId, false, S).

discover_doc(Parent, DId, First, #state{g=G, backlog=Backlog} = S) ->
	Node = {doc, DId},
	case digraph:vertex(G, Node) of
		false ->
			NewBacklog = [{discover_doc, Parent, DId, First} | Backlog],
			S#state{backlog=NewBacklog};
		_ ->
			S
	end.


discover_rev(Parent, RId, S) ->
	discover_rev(Parent, RId, false, false, S).

discover_rev(Parent, RId, First, Force, #state{g=G, backlog=Backlog} = S) ->
	Node = {rev, RId},
	case digraph:vertex(G, Node) of
		false ->
			NewBacklog = [{discover_rev, Parent, RId, First, Force} | Backlog],
			S#state{backlog=NewBacklog};
		_ ->
			S
	end.


do_discover_doc(Parent, DId, First, S) ->
	#state{
		g=G,
		verbose=Verbose,
		srcstore=SrcStore,
		dststore=DstStore
	} = S,
	Exists = case peerdrive_store:lookup(DstStore, DId) of
		{ok, _, _} -> true;
		{error, _} -> false
	end,
	case Verbose or not Exists of
		true ->
			case peerdrive_store:lookup(SrcStore, DId) of
				{ok, RId, _PreRevs} ->
					Node = {doc, DId},
					Props = case Exists of
						true -> [exists];
						false -> []
					end,
					Node = digraph:add_vertex(G, Node, [{rev, RId} | Props]),
					['$e' | _] = digraph:add_edge(G, Parent, Node),
					discover_rev(Node, RId, First, true, S);

				{error, Reason} when First or (Reason =/= enoent) ->
					throw({error, [{code, Reason}, {doc, DId}]});
				{error, enoent} ->
					S
			end;

		false ->
			S
	end.


do_discover_rev(Parent, RId, First, Force, S) ->
	#state{
		g=G,
		verbose=Verbose,
		srcstore=SrcStore,
		dststore=DstStore,
		depth=Depth
	} = S,
	Exists = peerdrive_store:contains(DstStore, RId),
	case Verbose or not Exists of
		true ->
			case peerdrive_store:stat(SrcStore, RId) of
				{ok, #rev_stat{parents=Ancestors, mtime=Mtime} = Stat}
				when (Mtime >= Depth) or Force ->
					Parts = [ {PId, Size} || {_, Size, PId} <- Stat#rev_stat.parts ],
					Node = {rev, RId},
					Props = case Exists of
						true -> [exists];
						false -> []
					end,
					Node = digraph:add_vertex(G, Node, [{parts, Parts} | Props]),
					['$e' | _] = digraph:add_edge(G, Parent, Node),
					S2 = lists:foldl(
						fun(Ancestor, Acc) -> discover_rev(Node, Ancestor, Acc) end,
						S,
						Ancestors),
					case is_sticky(Stat) of
						true ->
							lists:foldl(
								fun(Ref, Acc) -> discover_doc(Node, Ref, Acc) end,
								lists:foldl(
									fun(Ref, Acc) -> discover_rev(Node, Ref, Acc) end,
									S2,
									Stat#rev_stat.rev_links),
								Stat#rev_stat.doc_links);
						false ->
							S2
					end;

				{ok, Stat} ->
					% add sentinel node for correct transfer size estimation
					Node = {sentinel, RId},
					Parts = [ {PId, Size} || {_, Size, PId} <- Stat#rev_stat.parts ],
					digraph:add_vertex(G, Node, [{parts, Parts}]),
					digraph:add_edge(G, Parent, Node),
					S;

				{error, Reason} when First or (Reason =/= enoent) ->
					throw({error, [{code, Reason}, {rev, RId}]});

				{error, enoent} ->
					S
			end;

		false ->
			S
	end.


handle_node(Parent, Node, #state{backlog=Backlog} = S) ->
	NewBacklog = [{handle_node, Parent, Node} | Backlog],
	S#state{backlog=NewBacklog}.


do_handle_node(Parent, Node, #state{g=G} = S) ->
	case digraph:out_edges(G, Node) of
		[] ->
			{Node, MyProps} = digraph:vertex(G, Node),
			Size = proplists:get_value(size, MyProps, 0),
			% Do the replication and keep the handle open
			case Node of
				{doc, DId} ->
					RId = proplists:get_value(rev, MyProps),
					{Handle, S2} = replicate_doc(DId, RId, Size, S);
				{rev, RId} ->
					{Handle, S2} = replicate_rev(RId, Size, S);
				_ ->
					Handle = undefined,
					S2 = S
			end,
			% Now queue the handle at the parent
			case Handle of
				undefined ->
					ok;
				_ ->
					{Parent, ParentProps} = digraph:vertex(G, Parent),
					digraph:add_vertex(G, Parent, [Handle | ParentProps])
			end,
			% Lastly, close our handles
			lists:foreach(
				fun
					({handle, ChldHndl}) -> peerdrive_store:close(ChldHndl);
					(_) -> ok
				end,
				MyProps),
			S2;

		[Edge | _] ->
			% Remove the child link and queue both
			{Edge, Node, Child, _Label} = digraph:edge(G, Edge),
			digraph:del_edge(G, Edge),
			handle_node(Node, Child, handle_node(Parent, Node, S))
	end.


replicate_doc(DId, RId, Size, S) ->
	#state{
		dststore = DstStore
	} = S,
	case peerdrive_store:put_doc(DstStore, DId, RId) of
		{ok, Handle} ->
			case peerdrive_store:commit(Handle) of
				{ok, RId} ->
					{{handle, Handle}, account_size(Size, S)};
				{error, Reason} ->
					ErrInfo = [{code, Reason}, {doc, DId}, {rev, RId}],
					throw({error, ErrInfo})
			end;

		{error, Reason} ->
			ErrInfo = [{code, Reason}, {doc, DId}, {rev, RId}],
			throw({error, ErrInfo})
	end.


replicate_rev(RId, AccountedSize, S) ->
	#state{
		srcstore = SrcStore,
		dststore = DstStore
	} = S,
	case peerdrive_store:stat(SrcStore, RId) of
		{ok, Stat} ->
			Revision = #revision{
				flags = Stat#rev_stat.flags,
				parts = lists:sort(
					lists:map(
						fun({FCC, _Size, Hash}) -> {FCC, Hash} end,
						Stat#rev_stat.parts)),
				parents   = lists:sort(Stat#rev_stat.parents),
				mtime     = Stat#rev_stat.mtime,
				type      = Stat#rev_stat.type,
				creator   = Stat#rev_stat.creator,
				doc_links = Stat#rev_stat.doc_links,
				rev_links = Stat#rev_stat.rev_links,
				comment   = Stat#rev_stat.comment
			},
			case peerdrive_store:put_rev(DstStore, RId, Revision) of
				{ok, MissingParts, Handle} ->
					RealSize = lists:foldl(
						fun(P, Acc) ->
							{_, Size, _} = lists:keyfind(P, 1, Stat#rev_stat.parts),
							Acc + Size
						end,
						0,
						MissingParts),
					copy_parts(RId, SrcStore, Handle, MissingParts,
						AccountedSize, RealSize, S);

				{error, Reason} ->
					throw({error, [{code, Reason}, {rev, RId}]})
			end;

		{error, Reason} ->
			throw({error, [{code, Reason}, {rev, RId}]})
	end.


copy_parts(RId, _SourceStore, Importer, [], AccSize, _RealSize, S) ->
	case peerdrive_store:commit(Importer) of
		{ok, _} ->
			{{handle, Importer}, account_size(AccSize, S)};
		{error, Reason} ->
			throw({error, [{code, Reason}, {rev, RId}]})
	end;

copy_parts(RId, SourceStore, Importer, Parts, AccSize, RealSize, S) ->
	case peerdrive_store:peek(SourceStore, RId) of
		{ok, Reader} ->
			Action = #copy{rid=RId, parts=Parts, reader=Reader, imp=Importer,
				accsize=AccSize, factor=(AccSize / max(RealSize, 1))},
			{{handle, Importer}, schedule(Action, S)};

		{error, Reason} ->
			throw({error, [{code, Reason}, {rev, RId}]})
	end.


do_copy(#copy{parts=[], part=undefined, imp=Importer, reader=Reader} = Action, S) ->
	case peerdrive_store:commit(Importer) of
		{ok, _} ->
			peerdrive_store:close(Reader),
			account_size(Action#copy.accsize, S);
		{error, Reason} ->
			throw({error, [{code, Reason}, {rev, Action#copy.rid}]})
	end;

do_copy(#copy{parts=[Part|Remaining], part=undefined} = Action, S) ->
	schedule(Action#copy{parts=Remaining, part=Part, pos=0}, S);

do_copy(Action, S) ->
	#copy{
		part    = Part,
		reader  = Reader,
		imp     = Importer,
		pos     = Pos,
		factor  = Factor,
		accsize = AccSize
	} = Action,
	case peerdrive_store:read(Reader, Part, Pos, 16#40000) of
		{ok, Data} ->
			case peerdrive_store:put_rev_part(Importer, Part, Data) of
				ok ->
					Done = size(Data),
					NewAction1 = if
						Done == 16#40000 ->
							Action#copy{pos=Pos+16#40000};
						true ->
							Action#copy{part=undefined}
					end,
					Account = min(round(Done * Factor), AccSize),
					NewAction2 = NewAction1#copy{accsize=AccSize-Account},
					schedule(NewAction2, account_size(Account, S));

				{error, Reason} ->
					throw({error, [{code, Reason}, {rev, Action#copy.rid}]})
			end;

		eof ->
			schedule(Action#copy{part=undefined}, S);

		{error, Reason} ->
			throw({error, [{code, Reason}, {rev, Action#copy.rid}]})
	end.


do_estimate(#state{g=G, all=All} = S) ->
	{Size, _Parts} = do_estimate(root, G),
	S#state{all=All+Size}.


do_estimate(Node, G) ->
	{SubSize, SubPIds} = lists:foldl(
		fun(SubNode, {AccSize, AccPIds}) ->
			{Size, PIds} = do_estimate(SubNode, G),
			{AccSize+Size, sets:union(AccPIds, PIds)}
		end,
		{0, sets:new()},
		digraph:out_neighbours(G, Node)),
	{_, Props} = digraph:vertex(G, Node),
	Parts = proplists:get_value(parts, Props, []),
	PIds = sets:from_list([ PId || {PId, _} <- Parts]),
	Size = case Node of
		{rev, _} ->
			MissingPIds = sets:subtract(PIds, SubPIds),
			sets:fold(
				fun(PId, Acc) ->
					Acc + proplists:get_value(PId, Parts)
				end,
				0,
				MissingPIds);
		{doc, _} ->
			1024;
		_ ->
			0
	end,
	AccountedSize = case proplists:get_bool(exists, Props) of
		true -> max(Size, 1024);
		false -> Size
	end,
	if
		AccountedSize > 0 ->
			NewProps = [{size, AccountedSize} | Props],
			digraph:add_vertex(G, Node, NewProps);
		true ->
			ok
	end,
	{AccountedSize+SubSize, sets:union(PIds, SubPIds)}.


is_sticky(#rev_stat{flags=Flags}) ->
	(Flags band ?REV_FLAG_STICKY) =/= 0.


account_size(Size, #state{done=Done} = S) ->
	S#state{done = Done + Size}.

