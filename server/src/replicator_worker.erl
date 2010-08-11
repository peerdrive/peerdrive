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
		{modified, Uuid, StoreGuid} ->
			{rep_doc, Uuid, [StoreGuid]};

		{replicate_uuid, Uuid, Stores, _History, _Important} ->
			{rep_doc, Uuid, Stores};

		{replicate_rev, Rev, Stores, _History, _Important} ->
			{rep_rev, Rev, Stores}
	end,
	{ok, Monitor} = hysteresis:start(Tag),
	hysteresis:started(Monitor),
	{
		ok,
		#state{
			backlog = queue:in(Request, queue:new()),
			from    = From,
			result  = ok,
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

terminate(_Reason, #state{from=From, result=Result, monitor=Monitor}) ->
	hysteresis:done(Monitor),
	hysteresis:stop(Monitor),
	case From of
		{Pid, Ref} -> Pid ! {Ref, Result};
		_Else      -> ok
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Uuid: UUID to replicate
% Stores: Destination stores
% History: Also replicate the history of the document
% Important: queue an error if replication fails for this UUID
push_uuid(Backlog, Uuid, Stores, History, Important) ->
	queue:in({replicate_uuid, Uuid, Stores, History, Important}, Backlog).

push_rev(Backlog, Rev, Stores, History, Important) ->
	queue:in({replicate_rev, Rev, Stores, History, Important}, Backlog).

push_error(Backlog, Result) ->
	queue:in({result, {error, Result}}, Backlog).


run_queue(#state{backlog=Backlog, count=OldCount, done=Done} = State) ->
	case queue:out(Backlog) of
		{{value, Item}, Remaining} ->
			PrevSize = queue:len(Remaining),
			NewState = case Item of
				{modified, Uuid, StoreGuid} ->
					State#state{backlog=do_modified(Remaining, Uuid, StoreGuid)};

				{replicate_uuid, Uuid, Stores, History, Important} ->
					State#state{backlog=do_replicate_uuid(Remaining, Uuid, Stores, History, Important)};

				{replicate_rev, Rev, Stores, History, Important} ->
					State#state{backlog=do_replicate_rev(Remaining, Rev, Stores, History, Important, false)};

				{result, Result} ->
					State#state{backlog=Remaining, result=Result}
			end,
			NextSize = queue:len(NewState#state.backlog),
			NewState#state{count=OldCount+NextSize-PrevSize, done=Done+1};

		{empty, _Backlog} ->
			State
	end.

do_modified(Backlog, Uuid, StoreGuid) ->
	case volman:store(StoreGuid) of
		{ok, StoreIfc} ->
			case store:lookup(StoreIfc, Uuid) of
				{ok, Rev} -> sticky_handling(Backlog, Rev, [StoreGuid], true);
				error     -> Backlog
			end;

		error ->
			Backlog
	end.

do_replicate_uuid(Backlog, Uuid, ToStores, History, Important) ->
	case broker:lookup(Uuid) of
		[{Rev, _Stores}] ->
			RepStores = lists:filter(
				fun(Dest) ->
					case broker:replicate_uuid(Uuid, Dest) of
						ok               -> true;
						{error, _Reason} -> false
					end
				end,
				ToStores),
			% replicate as far as we can
			NewBacklog = do_replicate_rev(Backlog, Rev, RepStores, History, Important, true),
			if
				Important and (RepStores =/= ToStores) ->
					push_error(NewBacklog, rep_uuid_failed);
				true ->
					NewBacklog
			end;

		[] ->
			case Important of
				true  -> push_error(Backlog, enoent);
				false -> Backlog
			end;
		_ ->
			case Important of
				true  -> push_error(Backlog, conflict);
				false -> Backlog
			end
	end.

do_replicate_rev(Backlog, Rev, ToStores, History, Important, Latest) ->
	case broker:stat(Rev) of
		{ok, _Flags, _Parts, Parents, _Mtime, _Uti, Volumes} ->
			% do actual replication to destination stores
			RepStores1 = lists:subtract(ToStores, Volumes),
			RepStores2 = lists:filter(
				fun(Dest) ->
					case broker:replicate_rev(Rev, Dest) of
						ok               -> true;
						{error, _Reason} -> false
					end
				end,
				RepStores1),
			% if History == true then queue all parents with History and ToStores
			NewBacklog1 = lists:foldl(
				fun(Parent, BackAcc) ->
					push_rev(BackAcc, Parent, RepStores2, History, Important)
				end,
				Backlog,
				case History of
					true -> Parents;
					_    -> []
				end),
			NewBacklog2 = sticky_handling(NewBacklog1, Rev, RepStores2, Latest),
			if
				Important and (RepStores1 =/= RepStores2) ->
					push_error(NewBacklog2, rep_rev_failed);
				true ->
					NewBacklog2
			end;

		error ->
			case Important of
				true  -> push_error(Backlog, enoent);
				false -> Backlog
			end
	end.

sticky_handling(Backlog, Rev, ToStores, Latest) ->
	case util:read_rev_struct(Rev, <<"META">>) of
		{ok, MetaData} ->
			case meta_read_bool(MetaData, ?SYNC_STICKY) of
				true ->
					History = meta_read_bool(MetaData, ?SYNC_HISTORY),
					{RevRefs, UuidRefs} = read_references(Rev, Latest),
					NewBacklog = lists:foldl(
						fun(Reference, BackAcc) ->
							push_uuid(BackAcc, Reference, ToStores, History, false)
						end,
						Backlog,
						UuidRefs),
					lists:foldl(
						fun(Reference, BackAcc) ->
							push_rev(BackAcc, Reference, ToStores, History, false)
						end,
						NewBacklog,
						RevRefs);

				false ->
					Backlog
			end;

		{error, _Reason} ->
			Backlog
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


read_references(Rev, Latest) ->
	case util:read_rev_struct(Rev, <<"HPSD">>) of
		{ok, Data}       -> read_references_loop(Data, Latest);
		{error, _Reason} -> {[], []}
	end.


read_references_loop(Dict, Latest) when is_record(Dict, dict, 9) ->
	dict:fold(
		fun(_Key, Value, {AccRev, AccUuid}) ->
			{Revs, Uuids} = read_references_loop(Value, Latest),
			{Revs++AccRev, Uuids++AccUuid}
		end,
		{[], []},
		Dict);
read_references_loop(List, Latest) when is_list(List) ->
	lists:foldl(
		fun(Element, {AccRev, AccUuid}) ->
			{Revs, Uuids} = read_references_loop(Element, Latest),
			{Revs++AccRev, Uuids++AccUuid}
		end,
		{[], []},
		List);
read_references_loop({rlink, Rev}, _Latest) ->
	{[Rev], []};
read_references_loop({dlink, Uuid, Revs}, Latest) ->
	case Latest of
		true  -> {[], [Uuid]};
		false -> {Revs, []}
	end;
read_references_loop(_, _) ->
	{[], []}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_call(_Request, _From, S)     -> {noreply, S}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.


