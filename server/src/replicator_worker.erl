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

-module(replicator_worker).
-behaviour(gen_server).

-include("store.hrl").

-export([start_link/1, start_link/2]).
-export([cancel/1]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-record(state, {backlog, from, result, monitor, count, done}).

-define(SYNC_STICKY, [<<"org.hotchpotch.sync">>, <<"sticky">>]).
-define(SYNC_HISTORY, [<<"org.hotchpotch.sync">>, <<"history">>]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Request) ->
    Result = gen_server:start_link(?MODULE, {Request, none}, []),
	%io:format("replicator_worker: start_link/1 ~w~n", [Result]),
	Result.

start_link(Request, From) ->
    Result = gen_server:start_link(?MODULE, {Request, From}, []),
	%io:format("replicator_worker: start_link/2 ~w~n", [Result]),
	Result.

cancel(Worker) ->
	gen_server:cast(Worker, cancel).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Request, From}) ->
	Tag = case Request of
		{modified, Doc, StoreGuid} ->
			{rep_doc, Doc, [StoreGuid]};

		{replicate_doc, Doc, _Depth, _SrcStores, DstStores, _Important} ->
			{rep_doc, Doc, DstStores};

		{replicate_rev, Rev, _Depth, _SrcStores, DstStores, _Important} ->
			{rep_rev, Rev, DstStores}
	end,
	{ok, Monitor} = hysteresis:start(Tag),
	hysteresis:started(Monitor),
	{
		ok,
		#state{
			backlog = queue:in(Request, queue:new()),
			from    = From,
			result  = {undecided, orddict:new()},
			monitor = Monitor,
			count   = 1,
			done    = 0
		},
		0
	}.

handle_cast(cancel, State) ->
	{stop, normal, State}.

handle_info(timeout, State) ->
	NewState = run_queue(State),
	#state{count=Count, done=Done, monitor=Monitor} = NewState,
	hysteresis:progress(Monitor, Done * 256 div Count),
	case queue:is_empty(NewState#state.backlog) of
		true  -> {stop, normal, NewState};
		false -> {noreply, NewState, 0}
	end.

terminate(_Reason, #state{from=From, result=RawResult, monitor=Monitor}) ->
	hysteresis:done(Monitor),
	hysteresis:stop(Monitor),
	case From of
		{Pid, Ref} ->
			Result = case RawResult of
				{ok, ErrInfo} ->
					{ok, orddict:to_list(ErrInfo)};
				{undecided, ErrInfo} ->
					broker:consolidate_error(orddict:to_list(ErrInfo));
				{Reason, ErrInfo} ->
					{error, Reason, orddict:to_list(ErrInfo)}
			end,
			Pid ! {Ref, Result};
		_Else      -> ok
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Doc: Document to replicate
% Stores: Destination stores
% Depth: Date of oldest revision which gets replicated
push_doc(Backlog, Doc, Depth, SrcStores, DstStores) ->
	queue:in({replicate_doc, Doc, Depth, SrcStores, DstStores, false}, Backlog).


push_rev(Backlog, Rev, Depth, SrcStores, DstStores) ->
	queue:in({replicate_rev, Rev, Depth, SrcStores, DstStores, false}, Backlog).


push_error(Backlog, _Result, false) ->
	Backlog;

push_error(Backlog, Result, true) ->
	queue:in({fail, Result}, Backlog).


push_error(Backlog, _Store, _Result, false) ->
	Backlog;

push_error(Backlog, Store, Result, true) ->
	queue:in({fail, Store, Result}, Backlog).


push_success(Backlog, false) ->
	Backlog;

push_success(Backlog, true) ->
	queue:in(success, Backlog).


run_queue(#state{backlog=Backlog, count=OldCount, done=Done} = State) ->
	case queue:out(Backlog) of
		{{value, Item}, Remaining} ->
			PrevSize = queue:len(Remaining),
			NewState = case Item of
				{modified, Doc, Store} ->
					State#state{backlog=do_modified(Remaining, Doc, Store)};

				{replicate_doc, Doc, Depth, SrcStores, DstStores, Important} ->
					State#state{
						backlog = do_replicate_doc(Remaining, Doc, Depth,
							SrcStores, DstStores, Important)
					};

				{replicate_rev, Rev, Depth, SrcStores, DstStores, Important} ->
					State#state{
						backlog = do_replicate_rev(Remaining, Rev, Depth,
							SrcStores, DstStores, Important, false)
					};

				success ->
					{_Vote, ErrInfo} = State#state.result,
					State#state{backlog=Remaining, result={ok, ErrInfo}};

				{fail, Reason} ->
					{_Vote, ErrInfo} = State#state.result,
					State#state{backlog=Remaining, result={Reason, ErrInfo}};

				{fail, Store, Reason} ->
					{Vote, ErrInfo} = State#state.result,
					State#state{
						backlog = Remaining,
						result  = {Vote, orddict:store(Store, Reason, ErrInfo)}
					}
			end,
			NextSize = queue:len(NewState#state.backlog),
			NewState#state{count=OldCount+NextSize-PrevSize, done=Done+1};

		{empty, _Backlog} ->
			State
	end.


do_modified(Backlog, Doc, Store) ->
	{_Guid, Pid} = Store,
	case store:lookup(Pid, Doc) of
		{ok, Rev, _PreRevs} ->
			SrcStores = volman:stores(),
			sticky_handling(Backlog, Rev, SrcStores, [Store], true);

		error ->
			Backlog
	end.


do_replicate_doc(Backlog, _Doc, _Depth, _SrcStores, [], _Important) ->
	Backlog;

do_replicate_doc(Backlog, Doc, Depth, SrcStores, DstStores, Important) ->
	case lookup(Doc, SrcStores) of
		[Rev] ->
			% replicate doc to all stores, queue errors when failed
			{NewBacklog, RepStores} = lists:foldl(
				fun({DestGuid, DestPid}=Store, {AccBacklog, AccRepStores}) ->
					case store:put_doc(DestPid, Doc, Rev, Rev) of
						ok ->
							{AccBacklog, [Store|AccRepStores]};
						{error, Reason} ->
							{
								push_error(AccBacklog, DestGuid, Reason, Important),
								AccRepStores
							}
					end
				end,
				{Backlog, []},
				DstStores),
			% replicate corresponding rev
			do_replicate_rev(NewBacklog, Rev, Depth, SrcStores, RepStores,
				Important, true);

		[] -> push_error(Backlog, enoent, Important);
		_  -> push_error(Backlog, conflict, Important)
	end.


do_replicate_rev(Backlog, _Rev, _Depth, _SrcStores, [], _Important, _Latest) ->
	Backlog;

do_replicate_rev(Backlog, Rev, Depth, SrcStores, DstStores, Important, Latest) ->
	case stat(Rev, SrcStores) of
		{ok, #rev_stat{parents=Parents, mtime=Mtime}} ->
			if
				(Mtime >= Depth) or Latest ->
					{NewBacklog1, RepStores} = lists:foldl(
						fun({DstGuid, DstPid}=Store, {AccBack, AccRep}) ->
							case replicator_copy:put_rev(SrcStores, DstPid, Rev) of
								ok ->
									{
										push_success(AccBack, Important),
										[Store|AccRep]
									};
								{error, Reason} ->
									{
										push_error(AccBack, DstGuid, Reason, Important),
										AccRep
									}
							end
						end,
						{Backlog, []},
						DstStores),
					NewBacklog2 = lists:foldl(
						fun(Parent, BackAcc) ->
							push_rev(BackAcc, Parent, Depth, SrcStores, RepStores)
						end,
						NewBacklog1,
						Parents),
					sticky_handling(NewBacklog2, Rev, SrcStores, RepStores, Latest);

				true ->
					Backlog
			end;

		{error, ErrInfo} ->
			lists:foldl(
				fun({Guid, Error}, AccBack) ->
					push_error(AccBack, Guid, Error, Important)
				end,
				Backlog,
				ErrInfo)
	end.


sticky_handling(Backlog, Rev, SrcStores, DstStores, Latest) ->
	case util:read_rev_struct(Rev, <<"META">>) of
		{ok, MetaData} ->
			case meta_read_bool(MetaData, ?SYNC_STICKY) of
				true ->
					% FIXME: history property should be an integer
					Depth = case meta_read_bool(MetaData, ?SYNC_HISTORY) of
						true  -> 0;
						false -> 16#7FFFFFFFFFFFFFFF
					end,
					if
						Latest ->
							lists:foldl(
								fun(Reference, BackAcc) ->
									push_doc(BackAcc, Reference, Depth, SrcStores, DstStores)
								end,
								Backlog,
								read_doc_references(Rev, SrcStores));

						not Latest ->
							lists:foldl(
								fun(Reference, BackAcc) ->
									push_rev(BackAcc, Reference, Depth, SrcStores, DstStores)
								end,
								Backlog,
								read_rev_references(Rev, SrcStores))
					end;

				false ->
					Backlog
			end;

		{error, _Reason} ->
			Backlog
	end.


lookup(Doc, Stores) ->
	RevSet = lists:foldl(
		fun({_StoreGuid, StorePid}, AccRev) ->
			case store:lookup(StorePid, Doc) of
				{ok, Rev, _PreRevs} -> sets:add_element(Rev, AccRev);
				error               -> AccRev
			end
		end,
		sets:new(),
		Stores),
	sets:to_list(RevSet).


stat(Rev, SearchStores) ->
	{Stat, ErrInfo} = lists:foldl(
		fun({Guid, Pid}, {SoFar, ErrInfo} = Acc) ->
			case SoFar of
				undef ->
					case store:stat(Pid, Rev) of
						{ok, Stat} ->
							{Stat, ErrInfo};
						{error, Reason} ->
							{undef, [{Guid, Reason} | ErrInfo]}
					end;

				_ ->
					Acc
			end
		end,
		{undef, []},
		SearchStores),
	case Stat of
		undef -> {error, ErrInfo};
		_     -> {ok, Stat}
	end.


meta_read_bool(Meta, []) when is_boolean(Meta) ->
	Meta;
meta_read_bool(_Meta, []) ->
	false;
meta_read_bool(Meta, [Step|Path]) when is_record(Meta, dict, 9) ->
	case dict:find(Step, Meta) of
		{ok, Value} -> meta_read_bool(Value, Path);
		error       -> false
	end;
meta_read_bool(_Meta, _Path) ->
	false.


read_doc_references(Rev, SearchStores) ->
	case stat(Rev, SearchStores) of
		{ok, #rev_stat{links=Links}} ->
			element(1, Links);

		{error, _} ->
			[]
	end.


read_rev_references(Rev, SearchStores) ->
	case stat(Rev, SearchStores) of
		{ok, #rev_stat{links=Links}} ->
			element(3, Links);

		{error, _} ->
			[]
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_call(_Request, _From, S)     -> {noreply, S}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.


