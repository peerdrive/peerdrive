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

-module(peerdrive_vol_monitor).
-behaviour(gen_server).

-export([register_proc/0, deregister_proc/0, add_filter/2, rem_filter/1,
		 trigger_add_store/1, trigger_rem_store/1, trigger_add_rev/2,
		 trigger_rm_rev/2, trigger_add_doc/2, trigger_rm_doc/2,
		 trigger_mod_doc/2]).
-export([start_link/0]).
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2, code_change/3]).

-record(state, {procs, filters}).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


register_proc() ->
	case gen_server:call(?MODULE, {reg, self()}) of
		ok -> ok;
		error -> erlang:error(already_registered)
	end.

deregister_proc() ->
	case gen_server:call(?MODULE, {unreg, self()}) of
		ok -> ok;
		error -> erlang:error(not_registered)
	end.

add_filter(Store, Fun) ->
	case gen_server:call(?MODULE, {add_filter, self(), Store, Fun}) of
		ok -> ok;
		error -> erlang:error(badarg)
	end.

rem_filter(Store) ->
	case gen_server:call(?MODULE, {rem_filter, self(), Store}) of
		ok -> ok;
		error -> erlang:error(badarg)
	end.


trigger_add_store(Store) ->
	gen_server:cast(?MODULE, {notify, add_store, Store, undefined}).

trigger_rem_store(Store) ->
	gen_server:cast(?MODULE, {notify, rem_store, Store, undefined}).

trigger_add_rev(Store, Rev) ->
	gen_server:cast(?MODULE, {notify, add_rev, Store, Rev}).

trigger_rm_rev(Store, Rev) ->
	gen_server:cast(?MODULE, {notify, rem_rev, Store, Rev}).

trigger_add_doc(Store, Doc) ->
	gen_server:cast(?MODULE, {notify, add_doc, Store, Doc}).

trigger_rm_doc(Store, Doc) ->
	gen_server:cast(?MODULE, {notify, rem_doc, Store, Doc}).

trigger_mod_doc(Store, Doc) ->
	gen_server:cast(?MODULE, {notify, mod_doc, Store, Doc}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback handlers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	process_flag(trap_exit, true),
	S = #state{
		procs = [],
		filters = []
	},
	{ok, S}.


handle_cast({notify, Event, Store, Elem}, #state{procs=Procs, filters=Filters} = S) ->
	Msg = {vol_event, Event, Store, Elem},
	FinalMsg = case orddict:find(Store, Filters) of
		error -> Msg;
		{ok, {_Pid, Fun}} -> Fun(Msg)
	end,
	lists:foreach(fun(Pid) -> Pid ! FinalMsg end, Procs),
	{noreply, S}.


handle_call({reg, Pid}, _From, #state{procs=Procs} = S) ->
	case lists:member(Pid, Procs) of
		false ->
			link(Pid),
			{reply, ok, S#state{procs=[Pid | Procs]}};
		true ->
			{reply, error, S}
	end;

handle_call({unreg, Pid}, _From, #state{procs=Procs} = S) ->
	case lists:member(Pid, Procs) of
		true ->
			lists:any(fun({_Store, {FiltPid, _Fun}}) -> FiltPid == Pid end,
				S#state.filters) orelse unlink(Pid),
			{reply, ok, S#state{procs=lists:delete(Pid, Procs)}};
		false ->
			{reply, error, S}
	end;

handle_call({add_filter, Pid, Store, Fun}, _From, #state{filters=Filters} = S) ->
	case orddict:is_key(Store, Filters) of
		false ->
			link(Pid),
			NewFilters = orddict:store(Store, {Pid, Fun}, Filters),
			{reply, ok, S#state{filters=NewFilters}};
		true ->
			{reply, error, S}
	end;

handle_call({rem_filter, Pid, Store}, _From, S) ->
	#state{filters=Filters, procs=Procs} = S,
	case orddict:is_key(Store, Filters) of
		true ->
			lists:member(Pid, Procs) orelse unlink(Pid),
			{reply, ok, S#state{filters=orddict:erase(Store, Filters)}};
		false ->
			{reply, error, S}
	end.


handle_info({'EXIT', Pid, _Reason}, S) ->
	Filters = orddict:filter(
		fun(_Store, {FiltPid, _Fun}) -> FiltPid =/= Pid end,
		S#state.filters),
	Procs = lists:delete(Pid, S#state.procs),
	{noreply, S#state{procs=Procs, filters=Filters}}.


terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

