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

-module(hysteresis).
-behaviour(gen_fsm).

-export([start/1, stop/1, started/1, done/1, progress/2]).
-export([idle/2, starting/2, busy/2, deaf/2]).
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(STARTUP, 300).
-define(UPDATE, 100).

-record(state, {tag, timeout, informed, actual}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start(Tag) ->
	gen_fsm:start(?MODULE, {self(), Tag}, []).


stop(Fsm) ->
	gen_fsm:send_all_state_event(Fsm, terminate).


started(Fsm) ->
	gen_fsm:send_event(Fsm, start).


done(Fsm) ->
	gen_fsm:send_event(Fsm, stop).


progress(Fsm, Progress) ->
	gen_fsm:send_event(Fsm, {progress, Progress}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Parent, Tag}) ->
	process_flag(trap_exit, true),
	link(Parent),
	{ok, idle, #state{tag=Tag}}.


idle(start, State) ->
	Timer = gen_fsm:send_event_after(?STARTUP, publish),
	{next_state, starting, State#state{timeout=Timer, informed=0, actual=0}}.


starting(stop, #state{timeout=Timer} = State) ->
	gen_fsm:cancel_timer(Timer),
	{next_state, idle, State};

starting(publish, #state{tag=Tag, actual=Actual} = State) ->
	work_monitor:started(Tag),
	if
		Actual /= 0 -> work_monitor:progress(Tag, Actual);
		true        -> ok
	end,
	{next_state, busy, State#state{informed=Actual}};

starting({progress, Progress}, State) ->
	{next_state, starting, State#state{actual=Progress}}.


busy(stop, #state{tag=Tag} = State) ->
	work_monitor:done(Tag),
	{next_state, idle, State};

busy({progress, Progress}, #state{tag=Tag, informed=Informed} = State) ->
	if
		Progress =/= Informed ->
			Timer = gen_fsm:send_event_after(?UPDATE, reactivate),
			work_monitor:progress(Tag, Progress),
			{next_state, deaf, State#state{timeout=Timer, actual=Progress, informed=Progress}};
			
		true ->
			{next_state, busy, State}
	end.


deaf(stop, #state{timeout=Timer, tag=Tag} = State) ->
	gen_fsm:cancel_timer(Timer),
	work_monitor:done(Tag),
	{next_state, idle, State};

deaf({progress, Progress}, State) ->
	{next_state, deaf, State#state{actual=Progress}};

deaf(reactivate, #state{tag=Tag, actual=Actual, informed=Informed} = State) ->
	if
		Actual =/= Informed ->
			work_monitor:progress(Tag, Actual),
			Timer = gen_fsm:send_event_after(?UPDATE, reactivate),
			{next_state, deaf, State#state{timeout=Timer, informed=Actual}};
			
		true ->
			{next_state, busy, State}
	end.


handle_event(terminate, _StateName, StateData) ->
	{stop, normal, StateData}.


handle_info({'EXIT', _Pid, Reason}, _StateName, StateData) ->
	{stop, Reason, StateData}.
	

terminate(_Reason, idle, _StateData) ->
	ok;

terminate(_Reason, starting, _StateData) ->
	ok;

terminate(_Reason, _OtherStateName, #state{tag=Tag}) ->
	work_monitor:done(Tag).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_sync_event(_Event, _From, StateName, StateData) ->
	{next_state, StateName, StateData}.


code_change(_OldVsn, StateName, StateData, _Extra) ->
	{ok, StateName, StateData}.

