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

-module(broker_writer).
-behaviour(gen_server).

-export([start/1]).
-export([write_part/4, write_trunc/3, write_commit/1, write_abort/1, get_uuid/1]).

-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-record(state, {uuid, writers, user}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public broker operations...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


% Reply: {ok, Writer} | {error, Reason}
% The init function will link to `broker' when successfull.
start(Operation) ->
	gen_server:start(?MODULE, Operation, []).

% ok | {error, Reason}
write_part(Writer, Part, Offset, Data) ->
	gen_server:call(Writer, {write, Part, Offset, Data}).

% ok | {error, Reason}
write_trunc(Writer, Part, Offset) ->
	gen_server:call(Writer, {truncate, Part, Offset}).

% {ok, Hash} | {error, conflict} | {error, Reason}
write_commit(Writer) ->
	gen_server:call(Writer, commit).

% ok
write_abort(Writer) ->
	gen_server:call(Writer, abort).

% Uuid
get_uuid(Writer) ->
	gen_server:call(Writer, get_uuid).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @spec: init(Op) -> {ok, #state} | {stop, Reason}
% Creating is quite simple. Just pass on to the destination store...
% Merging/Updating is a bit more involved as it may affect several stores...

init({fork, StartStore, StartRev, Uti, User}) ->
	case store:write_start_fork(StartStore, StartRev, Uti) of
		{ok, Uuid, Writer} ->
			process_flag(trap_exit, true),
			link(User),
			{ok, #state{uuid=Uuid, writers=[Writer], user=User}};

		{error, Reason} ->
			{stop, Reason}
	end;

init({merge, Uuid, StartStores, StartRevs, Uti, User}) ->
	case do_start_merge(StartStores, Uuid, StartRevs, Uti) of
		{ok, Writers} ->
			process_flag(trap_exit, true),
			link(User),
			{ok, #state{uuid=Uuid, writers=Writers, user=User}};

		{error, Reason} ->
			{stop, Reason}
	end;

init({update, Uuid, StartStores, Rev, User}) ->
	case do_start_update(StartStores, Uuid, Rev) of
		{ok, Writers} ->
			process_flag(trap_exit, true),
			link(User),
			{ok, #state{uuid=Uuid, writers=Writers, user=User}};

		{error, Reason} ->
			{stop, Reason}
	end.


% ok | {error, Reason}
% FIXME: return not just the first error...
handle_call({write, Part, Offset, Data}, _From, S) ->
	{Reply, Writers} = lists:foldl(
		fun(Writer, {SoFar, AccWriters}) ->
			case store:write_part(Writer, Part, Offset, Data) of
				ok ->
					{SoFar, [Writer|AccWriters]};
				Error ->
					store:write_abort(Writer),
					case SoFar of
						ok -> {Error, AccWriters};
						_  -> {SoFar, AccWriters}
					end
			end
		end,
		{ok, []},
		S#state.writers),
	{reply, Reply, S#state{writers=Writers}};

% ok | {error, Reason}
% FIXME: return not just the first error...
handle_call({truncate, Part, Offset}, _From, S) ->
	{Reply, Writers} = lists:foldl(
		fun(Writer, {SoFar, AccWriters}) ->
			case store:write_trunc(Writer, Part, Offset) of
				ok ->
					{SoFar, [Writer|AccWriters]};
				Error ->
					store:write_abort(Writer),
					case SoFar of
						ok -> {Error, AccWriters};
						_  -> {SoFar, AccWriters}
					end
			end
		end,
		{ok, []},
		S#state.writers),
	{reply, Reply, S#state{writers=Writers}};

% {ok, Rev} | {error, Reason}
handle_call(commit, _From, S) ->
	Mtime = util:get_time(),
	{Revs, Errors} = lists:foldl(
		fun(Writer, {AccRevs, AccErrors}) ->
			case store:write_commit(Writer, Mtime) of
				{ok, Rev}       -> {[Rev|AccRevs], AccErrors};
				{error, Reason} -> {AccRevs, [Reason|AccErrors]}
			end
		end,
		{[], []},
		S#state.writers),
	Reply = case lists:usort(Revs) of
		[Rev] ->
			% as expected
			% TODO: what about remaining errors?
			vol_monitor:trigger_mod_uuid(local, S#state.uuid),
			{ok, Rev};

		[] ->
			% no writer commited... get error code
			case Errors of
				[] ->
					% no writer was left
					{error, enoent};

				[Reason|_OtherErrors] ->
					% TODO: could be done better
					{error, Reason}
			end;

		_ ->
			% internal error: writers did not came to the same revision! WTF?
			{error, einternal}
	end,
	{stop, normal, Reply, S};

% ok
handle_call(abort, _From, S) ->
	do_abort(S#state.writers),
	{stop, normal, ok, S};

% Uuid
handle_call(get_uuid, _From, S) ->
	{reply, S#state.uuid, S}.


handle_info({'EXIT', From, Reason}, #state{user=User} = S) ->
	case From of
		User ->
			% upstream process died
			do_abort(S#state.writers),
			{stop, orphaned, S};

		_Writer ->
			% one of the writers died, abnormally?
			case Reason of
				normal ->
					% don't care
					{noreply, S};
				_Abnormal ->
					% well, this one was unexpected...
					Writers = lists:filter(
						fun(N) -> is_process_alive(N) end,
						S#state.writers),
					{noreply, S#state{writers=Writers}}
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

do_start_merge(StartStores, Uuid, StartRevs, Uti) ->
	% TODO: more clever error propagation wouldn't hurt
	case do_start_merge_loop(StartStores, Uuid, StartRevs, Uti, [], []) of
		{[], [Reason|_OtherErrors]} ->
			{error, Reason};
		{Writers, _} ->
			{ok, Writers}
	end.

do_start_merge_loop([], _Uuid, _StartRevs, _Uti, Writers, Errors) ->
	{Writers, Errors};
do_start_merge_loop([Store|Stores], Uuid, StartRevs, Uti, Writers, Errors) ->
	case store:write_start_merge(Store, Uuid, StartRevs, Uti) of
		{ok, Writer} ->
			do_start_merge_loop(Stores, Uuid, StartRevs, Uti, [Writer|Writers], Errors);
		{error, Reason} ->
			do_start_merge_loop(Stores, Uuid, StartRevs, Uti, Writers, [Reason|Errors])
	end.


do_start_update(StartStores, Uuid, StartRev) ->
	% TODO: more clever error propagation wouldn't hurt
	case do_start_update_loop(StartStores, Uuid, StartRev, [], []) of
		{[], [Reason|_OtherErrors]} ->
			{error, Reason};
		{Writers, _} ->
			{ok, Writers}
	end.

do_start_update_loop([], _Uuid, _StartRev, Writers, Errors) ->
	{Writers, Errors};
do_start_update_loop([Store|Stores], Uuid, StartRev, Writers, Errors) ->
	case store:write_start_update(Store, Uuid, StartRev) of
		{ok, Writer} ->
			do_start_update_loop(Stores, Uuid, StartRev, [Writer|Writers], Errors);
		{error, Reason} ->
			do_start_update_loop(Stores, Uuid, StartRev, Writers, [Reason|Errors])
	end.


do_abort(Writers) ->
	lists:foreach(fun(Writer) -> store:write_abort(Writer) end, Writers).
