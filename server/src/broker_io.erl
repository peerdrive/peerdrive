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

-module(broker_io).
-behaviour(gen_server).

-export([start/1]).
-export([read/4, write/4, truncate/3, commit/2, abort/1]).

-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-record(state, {handles, user, doc}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public broker operations...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


% Reply: {ok, Broker} | {error, Reason}
% The init function will link to the user process when successfull.
start(Operation) ->
	gen_server:start(?MODULE, Operation, []).

% {ok, Data} | eof | {error, Reason}
read(Broker, Part, Offset, Length) ->
	gen_server:call(Broker, {read, Part, Offset, Length}).

% ok | {error, Reason}
write(Broker, Part, Offset, Data) ->
	gen_server:call(Broker, {write, Part, Offset, Data}).

% ok | {error, Reason}
truncate(Broker, Part, Offset) ->
	gen_server:call(Broker, {truncate, Part, Offset}).

% {ok, Hash} | conflict | {error, Reason}
commit(Broker, RebaseRevs) ->
	gen_server:call(Broker, {commit, RebaseRevs}).

% ok
abort(Broker) ->
	gen_server:call(Broker, abort).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @spec: init(Op) -> {ok, #state} | {stop, Reason}

init({peek, Rev, Stores, User}) ->
	case do_peek(Rev, Stores) of
		{ok, Handle} ->
			process_flag(trap_exit, true),
			link(User),
			{ok, #state{handles=[Handle], user=User}};

		{error, Reason} ->
			{stop, Reason}
	end;

init({fork, Doc, StartRev, Stores, Uti, User}) ->
	case do_fork(Doc, StartRev, Stores, Uti) of
		{ok, Handles} ->
			process_flag(trap_exit, true),
			link(User),
			{ok, #state{handles=Handles, user=User, doc=Doc}};

		{error, Reason} ->
			{stop, Reason}
	end;

init({update, Doc, Rev, MergeRevs, Stores, Uti, User}) ->
	case do_update(Doc, Rev, MergeRevs, Stores, Uti) of
		{ok, Handles} ->
			process_flag(trap_exit, true),
			link(User),
			{ok, #state{handles=Handles, user=User, doc=Doc}};

		{error, Reason} ->
			{stop, Reason}
	end.


% {ok, Data} | eof | {error, Reason}
handle_call({read, Part, Offset, Length}, _From, S) ->
	Reply = case S#state.handles of
		[]         -> {error, enoent};
		[Handle|_] -> store:read(Handle, Part, Offset, Length)
	end,
	{reply, Reply, S};

% ok | {error, Reason}
handle_call({write, Part, Offset, Data}, _From, S) ->
	distribute(fun(Handle) -> store:write(Handle, Part, Offset, Data) end, S);

% ok | {error, Reason}
handle_call({truncate, Part, Offset}, _From, S) ->
	distribute(fun(Handle) -> store:truncate(Handle, Part, Offset) end, S);

% {ok, Rev} | conflict | {error, Reason}
handle_call({commit, RebaseRevs}, _From, S) ->
	do_commit(RebaseRevs, S);

% ok
handle_call(abort, _From, S) ->
	do_abort(S#state.handles),
	{stop, normal, ok, S}.


handle_info({'EXIT', From, Reason}, #state{user=User} = S) ->
	case From of
		User ->
			% upstream process died
			do_abort(S#state.handles),
			{stop, orphaned, S};

		_Handle ->
			% one of the handles died, abnormally?
			case Reason of
				normal ->
					% don't care
					{noreply, S};
				_Abnormal ->
					% well, this one was unexpected...
					Handles = lists:filter(
						fun(N) -> is_process_alive(N) end,
						S#state.handles),
					{noreply, S#state{handles=Handles}}
			end
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


handle_cast(_, State)    -> {stop, enotsup, State}.
code_change(_, State, _) -> {ok, State}.
terminate(_, _)          -> ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_peek(_Rev, []) ->
	{error, enoent};

do_peek(Rev, [StoreIfc | Stores]) ->
	case store:peek(StoreIfc, Rev) of
		{ok, Handle} -> {ok, Handle};
		_Else        -> do_peek(Rev, Stores)
	end.


do_fork(Doc, StartRev, Stores, Uti) ->
	case do_fork_loop(Doc, StartRev, Uti, Stores, [], []) of
		{[], Errors} ->
			{error, consolidate_errors(Errors)};
		{Handles, _} ->
			{ok, Handles}
	end.


do_fork_loop(_Doc, _StartRev, _Uti, [], Handles, Errors) ->
	{Handles, Errors};

do_fork_loop(Doc, StartRev, Uti, [Store | Stores], Handles, Errors) ->
	case store:fork(Store, Doc, StartRev, Uti) of
		{ok, Handle} ->
			do_fork_loop(Doc, StartRev, Uti, Stores, [Handle|Handles], Errors);
		{error, Reason} ->
			do_fork_loop(Doc, StartRev, Uti, Stores, Handles, [Reason|Errors])
	end.


do_update(Doc, Rev, MergeRevs, Stores, Uti) ->
	case do_update_loop(Doc, Rev, MergeRevs, Uti, Stores, [], []) of
		{[], Errors} ->
			{error, consolidate_errors(Errors)};
		{Handles, _} ->
			{ok, Handles}
	end.


do_update_loop(_Doc, _Rev, _MergeRevs, _Uti, [], Handles, Errors) ->
	{Handles, Errors};

do_update_loop(Doc, Rev, MergeRevs, Uti, [Store | Stores], Handles, Errors) ->
	case store:update(Store, Doc, Rev, MergeRevs, Uti) of
		{ok, Handle} ->
			do_update_loop(Doc, Rev, MergeRevs, Uti, Stores, [Handle|Handles], Errors);
		{error, Reason} ->
			do_update_loop(Doc, Rev, MergeRevs, Uti, Stores, Handles, [Reason|Errors])
	end.


do_commit(RebaseRevs, S) ->
	Mtime = util:get_time(),
	{Revs, ConflictHandles, Errors} = lists:foldl(
		fun(Handle, {AccRevs, AccConflicts, AccErrors}) ->
			case store:commit(Handle, Mtime, RebaseRevs) of
				{ok, Rev}       -> {[Rev|AccRevs], AccConflicts, AccErrors};
				conflict        -> {AccRevs, [Handle|AccConflicts], AccErrors};
				{error, Reason} -> {AccRevs, AccConflicts, [Reason|AccErrors]}
			end
		end,
		{[], [], []},
		S#state.handles),
	case lists:usort(Revs) of
		[Rev] ->
			% as expected
			do_abort(ConflictHandles),
			vol_monitor:trigger_mod_doc(local, S#state.doc),
			{stop, normal, {ok, Rev}, S#state{handles=[]}};

		[] ->
			% no writer commited...
			case ConflictHandles of
				[] ->
					% all were hard errors...
					{stop, normal, {error, consolidate_errors(Errors)}, S#state{handles=[]}};

				Remaining ->
					{reply, conflict, S#state{handles=Remaining}}
			end;

		_ ->
			% internal error: handles did not came to the same revision! WTF?
			do_abort(ConflictHandles),
			{stop, normal, {error, einternal}, S#state{handles=[]}}
	end.


do_abort(Handles) ->
	lists:foreach(fun(Handle) -> store:abort(Handle) end, Handles).


% TODO: parallelize this
distribute(Fun, S) ->
	{Errors, Handles} = lists:foldr(
		fun(Handle, {AccErrors, AccHandles}) ->
			case Fun(Handle) of
				ok ->
					{AccErrors, [Handle|AccHandles]};
				{error, Error} ->
					store:abort(Handle),
					{[Error|AccErrors], AccHandles}
			end
		end,
		{[], []},
		S#state.handles),
	Reply = case Errors of
		[] -> ok;
		_  -> {error, consolidate_errors(Errors)}
	end,
	{reply, Reply, S#state{handles=Handles}}.


consolidate_errors(Errors) ->
	case lists:filter(fun(E) -> E =/= enoent end, lists:usort(Errors)) of
		[]      -> enoent;
		[Error] -> Error;
		_       -> emultiple
	end.

