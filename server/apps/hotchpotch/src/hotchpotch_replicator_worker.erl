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

-module(hotchpotch_replicator_worker).

-include("store.hrl").

-export([start_link/4, start_link/5]).
-export([cancel/1]).
-export([init/6]).

-record(state, {backlog, srcstore, dststore, depth, monitor, count, done}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Request, SrcStore, DstStore, Depth) ->
	proc_lib:start_link(?MODULE, init, [self(), Request, none, SrcStore,
		DstStore, Depth]).

start_link(Request, SrcStore, DstStore, Depth, From) ->
	proc_lib:start_link(?MODULE, init, [self(), Request, From, SrcStore,
		DstStore, Depth]).

cancel(Worker) ->
	Worker ! cancel.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Parent, Request, From, SrcStore, DstStore, Depth) ->
	Info = case Request of
		{replicate_doc, Doc, _First} ->
			{rep_doc, Doc, hotchpotch_store:guid(DstStore)};

		{replicate_rev, Rev, _First} ->
			{rep_rev, Rev, hotchpotch_store:guid(DstStore)}
	end,
	{ok, Monitor} = hotchpotch_hysteresis:start(Info),
	hotchpotch_hysteresis:started(Monitor),
	proc_lib:init_ack(Parent, {ok, self()}),
	S = #state{
		backlog  = queue:in(Request, queue:new()),
		srcstore = SrcStore,
		dststore = DstStore,
		depth    = Depth,
		monitor  = Monitor,
		count    = 1,
		done     = 0
	},
	Result = loop(S),
	hotchpotch_hysteresis:done(Monitor),
	hotchpotch_hysteresis:stop(Monitor),
	case From of
		{Pid, Ref} -> Pid ! {Ref, Result};
		_Else      -> ok
	end,
	normal.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

push_doc(Doc, #state{backlog=Backlog} = S) ->
	NewBacklog = queue:in({replicate_doc, Doc, false}, Backlog),
	S#state{backlog=NewBacklog}.


push_rev(Rev, #state{backlog=Backlog} = S) ->
	NewBacklog = queue:in({replicate_rev, Rev, false}, Backlog),
	S#state{backlog=NewBacklog}.


loop(State) ->
	case run_queue(State) of
		{ok, NewState} ->
			#state{count=Count, done=Done, monitor=Monitor} = NewState,
			hotchpotch_hysteresis:progress(Monitor, Done * 255 div Count),
			loop(NewState);

		{stop, _NewState} ->
			ok;

		{{error, _} = Error, _NewState} ->
			Error
	end.


run_queue(#state{backlog=Backlog, count=OldCount, done=Done} = S) ->
	case queue:out(Backlog) of
		{{value, Item}, Remaining} ->
			PrevSize = queue:len(Remaining),
			S2 = S#state{backlog=Remaining},
			{Result, S3} = case Item of
				{replicate_doc, Doc, First} ->
					replicate_doc(Doc, First, S2);
				{replicate_rev, Rev, First} ->
					replicate_rev(queue:in(Rev, queue:new()), First, S2)
			end,
			NextSize = queue:len(S3#state.backlog),
			{Result, S3#state{count=OldCount+NextSize-PrevSize, done=Done+1}};

		{empty, _Backlog} ->
			{stop, S}
	end.


replicate_doc(Doc, First, S) ->
	#state{
		srcstore = SrcStore,
		dststore = DstStore
	} = S,
	case hotchpotch_store:lookup(SrcStore, Doc) of
		{ok, Rev, _PreRevs} ->
			case hotchpotch_store:put_doc(DstStore, Doc, Rev) of
				ok ->
					{ok, S};

				{ok, Handle} ->
					% replicate corresponding rev
					case replicate_rev(queue:in(Rev, queue:new()), First, S) of
						{ok, _} = Ok ->
							case hotchpotch_store:put_doc_commit(Handle) of
								ok ->
									Ok;
								{error, econflict} when not First ->
									% Not treated as an error but deliberately
									% drop new state
									{ok, S};
								Error ->
									% Drop new state
									{Error, S}
							end;
						Error ->
							hotchpotch_store:put_doc_abort(Handle),
							Error
					end;

				{error, econflict} when not First ->
					{ok, S};
				{error, _Reason} = Error ->
					{Error, S}
			end;

		error when First ->
			{{error, enoent}, S};

		error ->
			{ok, S}
	end.


replicate_rev(Revs, First, S) ->
	receive
		cancel -> {stop, S}
	after
		0 ->
			case queue:out(Revs) of
				{{value, Rev}, RemainRevs} ->
					case hotchpotch_store:contains(S#state.dststore, Rev) of
						false ->
							case do_replicate_rev(Rev, RemainRevs, First, S) of
								{ok, NewRevs, S2} ->
									replicate_rev(NewRevs, false, S2);
								Else ->
									Else
							end;
						true ->
							replicate_rev(RemainRevs, false, S)
					end;

				{empty, _} ->
					{ok, S}
			end
	end.


do_replicate_rev(Rev, Backlog, First, S) ->
	#state{
		srcstore = SrcStore,
		dststore = DstStore,
		depth    = Depth
	} = S,
	case hotchpotch_store:stat(SrcStore, Rev) of
		{ok, #rev_stat{parents=Parents, mtime=Mtime} = Stat}
		when (Mtime >= Depth) or First ->
			case put_rev(SrcStore, DstStore, Rev) of
				ok ->
					NewBacklog = lists:foldl(
						fun(Parent, BackAcc) -> queue:in(Parent, BackAcc) end,
						Backlog,
						Parents),
					S2 = case is_sticky(Stat) of
						true ->
							lists:foldl(
								fun(Ref, Acc) -> push_doc(Ref, Acc) end,
								lists:foldl(
									fun(Ref, Acc) -> push_rev(Ref, Acc) end,
									S,
									Stat#rev_stat.rev_links),
								Stat#rev_stat.doc_links);
						false ->
							S
					end,
					{ok, NewBacklog, S2};

				{error, _} = Error ->
					{Error, S}
			end;

		{ok, _} ->
			{ok, Backlog, S};

		{error, _} = Error when First ->
			{Error, S};

		{error, _} ->
			{ok, Backlog, S}
	end.


is_sticky(#rev_stat{flags=Flags}) ->
	(Flags band ?REV_FLAG_STICKY) =/= 0.


% returns ok | {error, Reason}
put_rev(SourceStore, DestStore, Rev) ->
	case hotchpotch_store:stat(SourceStore, Rev) of
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
			case hotchpotch_store:put_rev_start(DestStore, Rev, Revision) of
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
	case hotchpotch_store:peek(SourceStore, Rev) of
		{ok, Reader} ->
			case copy_parts_loop(Parts, Reader, Importer) of
				ok ->
					hotchpotch_store:close(Reader),
					hotchpotch_store:put_rev_commit(Importer);

				{error, Reason} ->
					hotchpotch_store:close(Reader),
					hotchpotch_store:put_rev_abort(Importer),
					{error, Reason}
			end;

		{error, Reason} ->
			hotchpotch_store:put_rev_abort(Importer),
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
	case hotchpotch_store:read(Reader, Part, Pos, 16#100000) of
		{ok, Data} ->
			case hotchpotch_store:put_rev_part(Importer, Part, Data) of
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

