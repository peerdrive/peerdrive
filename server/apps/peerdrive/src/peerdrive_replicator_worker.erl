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

-record(state, {parent_backlog, req_backlog, srcstore, dststore, depth,
	verbose, monitor, count, done, pauseonerror, from, result}).

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
	Info = case Request of
		{replicate_doc, Doc, _First} ->
			{rep_doc, peerdrive_store:guid(SrcStore), Doc,
				peerdrive_store:guid(DstStore)};

		{replicate_rev, Rev, _First} ->
			{rep_rev, peerdrive_store:guid(SrcStore), Rev,
				peerdrive_store:guid(DstStore)}
	end,
	{ok, Monitor} = peerdrive_work:new(Info),
	peerdrive_work:start(Monitor),
	S = #state{
		parent_backlog = queue:new(),
		req_backlog    = queue:in(Request, queue:new()),
		srcstore       = SrcStore,
		dststore       = DstStore,
		depth          = proplists:get_value(depth, Options, 0),
		verbose        = proplists:get_bool(verbose, Options),
		monitor        = Monitor,
		count          = 1,
		done           = 0,
		pauseonerror   = proplists:get_bool(pauseonerror, Options),
		from           = From,
		result         = ok
	},
	{ok, working, S, 0}.


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
		S2 = run_queues(S),
		#state{count=Count, done=Done, monitor=Monitor} = S2,
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

push_doc(Doc, #state{req_backlog=Backlog} = S) ->
	NewBacklog = queue:in({replicate_doc, Doc, false}, Backlog),
	S#state{req_backlog=NewBacklog}.


push_rev(Rev, #state{req_backlog=Backlog} = S) ->
	NewBacklog = queue:in({replicate_rev, Rev, false}, Backlog),
	S#state{req_backlog=NewBacklog}.


push_parent(Rev, First, #state{parent_backlog=Backlog} = S) ->
	NewBacklog = queue:in({Rev, First}, Backlog),
	S#state{parent_backlog=NewBacklog}.


run_queues(#state{parent_backlog=ParentBL} = S) ->
	case queue:out(ParentBL) of
		{{value, {Rev, First}}, NewParentBL} ->
			replicate_rev(Rev, First, S#state{parent_backlog=NewParentBL});

		{empty, _ParentBL} ->
			#state{req_backlog=ReqBacklog, count=OldCount, done=Done} = S,
			case queue:out(ReqBacklog) of
				{{value, Item}, Remaining} ->
					PrevSize = queue:len(Remaining),
					S2 = S#state{req_backlog=Remaining},
					S3 = case Item of
						{replicate_doc, Doc, First} ->
							replicate_doc(Doc, First, S2);
						{replicate_rev, Rev, First} ->
							replicate_rev(Rev, First, S2)
					end,
					NextSize = queue:len(S3#state.req_backlog),
					S3#state{count=OldCount+NextSize-PrevSize, done=Done+1};

				{empty, _ReqBacklog} ->
					throw(done)
			end
	end.


skip(#state{parent_backlog=ParentBL} = S) ->
	case queue:out(ParentBL) of
		{{value, _}, NewParentBL} ->
			S#state{parent_backlog=NewParentBL};

		{empty, _ParentBL} ->
			{_, NewReqBacklog} = queue:out(S#state.req_backlog),
			S#state{req_backlog=NewReqBacklog}
	end.


replicate_doc(Doc, First, S) ->
	#state{
		srcstore = SrcStore,
		dststore = DstStore,
		verbose  = Verbose
	} = S,
	case peerdrive_store:lookup(SrcStore, Doc) of
		{ok, Rev, _PreRevs} ->
			case peerdrive_store:put_doc(DstStore, Doc, Rev) of
				ok ->
					case Verbose of
						false -> S;
						true  -> replicate_rev(Rev, First, S)
					end;

				{ok, Handle} ->
					% replicate corresponding rev
					S2 = try
						replicate_rev(Rev, First, S)
					catch
						throw:RepError ->
							peerdrive_store:put_doc_abort(Handle),
							throw(RepError)
					end,
					case peerdrive_store:put_doc_commit(Handle) of
						ok ->
							S2;
						{error, econflict} when not First ->
							% Not treated as an error but deliberately
							% drop new state
							S;
						{error, Reason} ->
							ErrInfo = [{code, Reason}, {doc, Doc}, {rev, Rev}],
							throw({error, ErrInfo})
					end;

				{error, econflict} when not First ->
					S;
				{error, Reason} ->
					ErrInfo = [{code, Reason}, {doc, Doc}, {rev, Rev}],
					throw({error, ErrInfo})
			end;

		error when First ->
			throw({error, [{code, enoent}, {doc, Doc}]});

		error ->
			S
	end.


replicate_rev(Rev, First, #state{verbose=Verbose} = S) ->
	Contains = (not Verbose) andalso peerdrive_store:contains(S#state.dststore,
		Rev),
	case Contains of
		false -> do_replicate_rev(Rev, First, S);
		true  -> S
	end.


do_replicate_rev(Rev, First, S) ->
	#state{
		srcstore = SrcStore,
		dststore = DstStore,
		depth    = Depth
	} = S,
	case peerdrive_store:stat(SrcStore, Rev) of
		{ok, #rev_stat{parents=Parents, mtime=Mtime} = Stat}
		when (Mtime >= Depth) or First ->
			case put_rev(SrcStore, DstStore, Rev) of
				ok ->
					S2 = lists:foldl(
						fun(Parent, Acc) -> push_parent(Parent, First, Acc) end,
						S,
						Parents),
					case is_sticky(Stat) of
						true ->
							lists:foldl(
								fun(Ref, Acc) -> push_doc(Ref, Acc) end,
								lists:foldl(
									fun(Ref, Acc) -> push_rev(Ref, Acc) end,
									S2,
									Stat#rev_stat.rev_links),
								Stat#rev_stat.doc_links);
						false ->
							S2
					end;

				{error, Reason} ->
					throw({error, [{code, Reason}, {rev, Rev}]})
			end;

		{ok, _} ->
			S;

		{error, Reason} when First ->
			throw({error, [{code, Reason}, {rev, Rev}]});

		{error, _} ->
			S
	end.


is_sticky(#rev_stat{flags=Flags}) ->
	(Flags band ?REV_FLAG_STICKY) =/= 0.


% returns ok | {error, Reason}
put_rev(SourceStore, DestStore, Rev) ->
	case peerdrive_store:stat(SourceStore, Rev) of
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
				rev_links = Stat#rev_stat.rev_links
			},
			case peerdrive_store:put_rev_start(DestStore, Rev, Revision) of
				ok ->
					ok;

				{ok, MissingParts, Importer} ->
					copy_parts(Rev, SourceStore, Importer, MissingParts);

				{error, Reason} ->
					{error, Reason}
			end;

		Error ->
			Error
	end.


copy_parts(Rev, SourceStore, Importer, Parts) ->
	case peerdrive_store:peek(SourceStore, Rev) of
		{ok, Reader} ->
			case copy_parts_loop(Parts, Reader, Importer) of
				ok ->
					peerdrive_store:close(Reader),
					peerdrive_store:put_rev_commit(Importer);

				{error, Reason} ->
					peerdrive_store:close(Reader),
					peerdrive_store:put_rev_abort(Importer),
					{error, Reason}
			end;

		{error, Reason} ->
			peerdrive_store:put_rev_abort(Importer),
			{error, Reason}
	end.

copy_parts_loop([], _Reader, _Importer) ->
	ok;
copy_parts_loop([Part|Remaining], Reader, Importer) ->
	case copy(Part, Reader, Importer) of
		ok   -> copy_parts_loop(Remaining, Reader, Importer);
		Else -> Else
	end.

copy(Part, Reader, Importer) ->
	copy_loop(Part, Reader, Importer, 0).

copy_loop(Part, Reader, Importer, Pos) ->
	case peerdrive_store:read(Reader, Part, Pos, 16#100000) of
		{ok, Data} ->
			case peerdrive_store:put_rev_part(Importer, Part, Data) of
				ok ->
					if
						size(Data) == 16#100000 ->
							copy_loop(Part, Reader, Importer, Pos+16#100000);
						true ->
							ok
					end;

				{error, Reason} ->
					{error, Reason}
			end;

		eof ->
			ok;

		{error, Reason} ->
			{error, Reason}
	end.

