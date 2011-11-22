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

-module(peerdrive_work).
-behaviour(gen_fsm).

-export([start_link/0]).
-export([new/1, delete/1, start/1, stop/1, progress/2, pause/1, error/2, resume/1]).
-export([idle/2, starting/2, busy/2, deaf/2, interrupted/2]).
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(STARTUP, 300).
-define(UPDATE, 100).

-record(state, {parent, info, tag, timeout, informed, actual}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	Ok = gen_event:start_link({local, ?MODULE}),
	peerdrive_work_roster:register(),
	Ok.


new(Info) ->
	gen_fsm:start(?MODULE, {self(), Info}, []).


delete(Fsm) ->
	gen_fsm:send_all_state_event(Fsm, terminate).


start(Fsm) ->
	gen_fsm:send_event(Fsm, start).


stop(Fsm) ->
	gen_fsm:send_event(Fsm, stop).


progress(Fsm, Progress) ->
	gen_fsm:send_event(Fsm, {progress, Progress}).


pause(Fsm) ->
	gen_fsm:send_event(Fsm, pause).


error(Fsm, Info) ->
	gen_fsm:send_event(Fsm, {error, Info}).


resume(Fsm) ->
	gen_fsm:send_event(Fsm, resume).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Parent, Info}) ->
	process_flag(trap_exit, true),
	link(Parent),
	{ok, idle, #state{parent=Parent, info=Info}}.


idle(start, S) ->
	Timer = gen_fsm:send_event_after(?STARTUP, publish),
	S2 = S#state{timeout=Timer, informed=0, actual=0},
	{next_state, starting, S2}.


starting(stop, #state{timeout=Timer} = State) ->
	gen_fsm:cancel_timer(Timer),
	{next_state, idle, State};

starting(publish, #state{parent=Parent, actual=Actual, info=Info} = S) ->
	Tag = notify_started(Parent, Info),
	if
		Actual /= 0 -> notify_progress(Tag, running, Actual, []);
		true        -> ok
	end,
	{next_state, busy, S#state{tag=Tag, informed=Actual}};

starting({progress, Progress}, S) ->
	{next_state, starting, S#state{actual=Progress}};

starting(pause, #state{parent=Parent, info=Info, actual=Actual, timeout=Timer} = S) ->
	gen_fsm:cancel_timer(Timer),
	Tag = notify_started(Parent, Info),
	notify_progress(Tag, paused, Actual, []),
	{next_state, interrupted, S#state{tag=Tag, informed=Actual}};

starting({error, ErrInfo}, #state{parent=Parent, info=Info, actual=Actual, timeout=Timer} = S) ->
	gen_fsm:cancel_timer(Timer),
	Tag = notify_started(Parent, Info),
	notify_progress(Tag, error, Actual, ErrInfo),
	{next_state, interrupted, S#state{tag=Tag, informed=Actual}}.


busy({progress, Progress}, #state{tag=Tag, informed=Informed} = S) ->
	if
		Progress =/= Informed ->
			Timer = gen_fsm:send_event_after(?UPDATE, reactivate),
			notify_progress(Tag, running, Progress, []),
			{next_state, deaf, S#state{timeout=Timer, actual=Progress, informed=Progress}};

		true ->
			{next_state, busy, S}
	end;

busy(stop, #state{tag=Tag} = State) ->
	notify_done(Tag),
	{next_state, idle, State};

busy(pause, #state{tag=Tag, actual=Progress} = S) ->
	notify_progress(Tag, paused, Progress, []),
	{next_state, interrupted, S};

busy({error, Info}, #state{tag=Tag, actual=Progress} = S) ->
	notify_progress(Tag, error, Progress, Info),
	{next_state, interrupted, S}.


deaf(stop, #state{timeout=Timer, tag=Tag} = State) ->
	gen_fsm:cancel_timer(Timer),
	notify_done(Tag),
	{next_state, idle, State};

deaf({progress, Progress}, State) ->
	{next_state, deaf, State#state{actual=Progress}};

deaf(reactivate, #state{tag=Tag, actual=Actual, informed=Informed} = S) ->
	if
		Actual =/= Informed ->
			notify_progress(Tag, running, Actual, []),
			Timer = gen_fsm:send_event_after(?UPDATE, reactivate),
			{next_state, deaf, S#state{timeout=Timer, informed=Actual}};

		true ->
			{next_state, busy, S}
	end;

deaf(pause, #state{timeout=Timer, tag=Tag, actual=Progress} = S) ->
	gen_fsm:cancel_timer(Timer),
	notify_progress(Tag, paused, Progress, []),
	{next_state, interrupted, S#state{informed=Progress}};

deaf({error, Info}, #state{timeout=Timer, tag=Tag, actual=Progress} = S) ->
	gen_fsm:cancel_timer(Timer),
	notify_progress(Tag, error, Progress, Info),
	{next_state, interrupted, S#state{informed=Progress}}.


interrupted(resume, #state{tag=Tag, actual=Actual} = S) ->
	notify_progress(Tag, running, Actual, []),
	Timer = gen_fsm:send_event_after(?UPDATE, reactivate),
	{next_state, deaf, S#state{timeout=Timer, informed=Actual}};

interrupted(stop, #state{tag=Tag} = S) ->
	notify_done(Tag),
	{next_state, idle, S}.


handle_event(terminate, _StateName, StateData) ->
	{stop, normal, StateData}.


handle_info({'EXIT', _Pid, Reason}, _StateName, StateData) ->
	{stop, Reason, StateData}.


terminate(_Reason, idle, _StateData) ->
	ok;

terminate(_Reason, starting, _StateData) ->
	ok;

terminate(_Reason, _OtherStateName, #state{tag=Tag}) ->
	notify_done(Tag).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

notify_started(Proc, Info) ->
	Tag = peerdrive_work_tags:create(),
	gen_event:notify(?MODULE, {work_event, started, Tag, {Proc, Info}}),
	Tag.


notify_done(Tag) ->
	gen_event:notify(?MODULE, {work_event, done, Tag, ok}).


notify_progress(Tag, State, Progress, Info) ->
	gen_event:notify(?MODULE, {work_event, progress, Tag, {State, Progress, Info}}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_sync_event(_Event, _From, StateName, StateData) ->
	{next_state, StateName, StateData}.


code_change(_OldVsn, StateName, StateData, _Extra) ->
	{ok, StateName, StateData}.

