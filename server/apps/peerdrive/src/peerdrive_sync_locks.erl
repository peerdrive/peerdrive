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

-module(peerdrive_sync_locks).
-behaviour(gen_server).

-export([start_link/0]).
-export([lock/1, unlock/1]).

-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2,
	terminate/2]).

-record(state, {locks, peers}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	gen_server:start_link({local, peerdrive_sync_locks}, ?MODULE, [], []).


%% @doc Locks a document for synchronization. Will block until the lock has
%%      been taken.
lock(Doc) ->
	gen_server:call(peerdrive_sync_locks, {lock, Doc}, infinity).


%% @doc Unlocks a previously locked document.
unlock(Doc) ->
	gen_server:call(peerdrive_sync_locks, {unlock, Doc}, infinity).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	process_flag(trap_exit, true),
	{ok, #state{locks=dict:new(), peers=dict:new()}}.


handle_call({lock, Doc}, From, State) ->
	do_lock(Doc, From, State);

handle_call({unlock, Doc}, From, State) ->
	do_unlock(Doc, From, State).


handle_info({'EXIT', Pid, _Reason}, State) ->
	NewState = do_trap_exit(Pid, State),
	{noreply, NewState};

handle_info(_, State) ->
	{noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_cast(_, State) -> {noreply, State}.
terminate(_, _) -> ok.
code_change(_, State, _) -> {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Service implementation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_lock(Doc, {Caller, _} = From, #state{locks=Locks, peers=Peers} = S) ->
	case dict:find(Doc, Locks) of
		{ok, WaitQueue} ->
			NewLocks = dict:store(Doc, WaitQueue ++ [From], Locks),
			{noreply, S#state{locks=NewLocks}};

		error ->
			NewLocks = dict:store(Doc, [], Locks),
			NewPeers = dict:append(Caller, Doc, Peers),
			link(Caller),
			{reply, ok, S#state{locks=NewLocks, peers=NewPeers}}
	end.


do_unlock(Doc, From, S) ->
	{reply, ok, do_unlock_internal(Doc, From, S)}.


do_unlock_internal(Doc, {Caller, _}, #state{locks=Locks, peers=Peers} = S) ->
	NewPeers = case lists:delete(Doc, dict:fetch(Caller, Peers)) of
		[] ->
			unlink(Caller),
			dict:erase(Caller, Peers);
		Others ->
			dict:store(Caller, Others, Peers)
	end,
	case dict:fetch(Doc, Locks) of
		[] ->
			NewLocks = dict:erase(Doc, Locks),
			S#state{locks=NewLocks, peers=NewPeers};

		[Next | Rest] ->
			{NextPid, _} = Next,
			RelockLocks = dict:store(Doc, Rest, Locks),
			RelockPeers = dict:append(NextPid, Doc, NewPeers),
			gen_server:reply(Next, ok),
			S#state{locks=RelockLocks, peers=RelockPeers}
	end.


do_trap_exit(Pid, #state{peers=Peers} = S) ->
	case dict:find(Pid, Peers) of
		error ->
			S;

		{ok, Docs} ->
			lists:foldl(
				fun(Doc, Acc) -> do_unlock_internal(Doc, {Pid,0}, Acc) end,
				S,
				Docs)
	end.

