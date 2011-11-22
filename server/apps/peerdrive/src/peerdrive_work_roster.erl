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

-module(peerdrive_work_roster).
-behaviour(gen_event).

-export([register/0, all/0, pause/1, stop/1, resume/2]).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2,
	code_change/3]).

-record(entry, {tag, proc, info, progress, state, err_info}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

register() ->
	gen_event:add_handler(peerdrive_work, ?MODULE, []).


all() ->
	gen_event:call(peerdrive_work, ?MODULE, all).


pause(Tag) ->
	gen_event:call(peerdrive_work, ?MODULE, {send, Tag, pause}).


stop(Tag) ->
	gen_event:call(peerdrive_work, ?MODULE, {send, Tag, stop}).


resume(Tag, Skip) ->
	gen_event:call(peerdrive_work, ?MODULE, {send, Tag, {resume, Skip}}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	{ok, gb_trees:empty()}.


handle_event({work_event, progress, Tag, {State, Progress, Info}}, S) ->
	Entry = gb_trees:get(Tag, S),
	NewEntry = Entry#entry{progress=Progress, state=State, err_info=Info},
	{ok, gb_trees:enter(Tag, NewEntry, S)};

handle_event({work_event, started, Tag, {Proc, Info}}, S) ->
	Entry = #entry{tag=Tag, proc=Proc, info=Info, progress=0, state=running,
		err_info=[]},
	{ok, gb_trees:insert(Tag, Entry, S)};

handle_event({work_event, done, Tag, ok}, S) ->
	{ok, gb_trees:delete(Tag, S)}.


handle_call(all, S) ->
	Reply = [ {Tag, Info, State, Progress, ErrInfo} || #entry{tag=Tag,
		info=Info, progress=Progress, state=State, err_info=ErrInfo}
		<- gb_trees:values(S) ],
	{ok, Reply, S};

handle_call({send, Tag, Msg}, S) ->
	case gb_trees:lookup(Tag, S) of
		{value, #entry{proc=Proc}} ->
			Proc ! {work_req, Msg};
		none ->
			ok
	end,
	{ok, ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_info(_Info, State) -> {ok, State}.
terminate(_Arg, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.


