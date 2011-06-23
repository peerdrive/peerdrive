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

-module(hotchpotch_work_monitor).
-behaviour(gen_event).

-export([register_proc/1, deregister_proc/1, started/1, done/1, progress/2]).
-export([start_link/0]).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	gen_event:start_link({local, ?MODULE}).


started(Info) ->
	Tag = hotchpotch_work_tags:create(),
	gen_event:notify(?MODULE, {work_event, started, Tag, Info}),
	Tag.


done(Tag) ->
	gen_event:notify(?MODULE, {work_event, done, Tag, ok}).


progress(Tag, Progress) ->
	gen_event:notify(?MODULE, {work_event, progress, Tag, Progress}).


register_proc(Id) ->
	gen_event:add_sup_handler(?MODULE, {?MODULE, Id}, self()).


deregister_proc(Id) ->
	Handler = {?MODULE, Id},
	gen_event:delete_handler(?MODULE, Handler, []),
	receive
		{gen_event_EXIT, Handler, _Reason} -> ok
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


init(Pid) ->
	{ok, Pid}.


handle_event(Event, State) ->
	Pid = State,
	Pid ! Event,
	{ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_call(_Request, State) -> {ok, badarg, State}.
handle_info(_Info, State) -> {ok, State}.
terminate(_Arg, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

