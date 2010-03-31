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

-module(store_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([spawn_store/4, stop_store/1]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, store_sup}, ?MODULE, []).

%% @doc Spawn a store
%%
%% Returns the interface instad of the pid.
spawn_store(Id, Disposition, Module, Arg) ->
	Restart = case proplists:is_defined(removable, Disposition) of
		false -> permanent;
		true  -> transient
	end,
	ChildSpec = {
		Id,
		{Module, start_link, [Id, Arg]},
		Restart,
		30000,
		worker,
		[Module]
	},
	Status = case supervisor:start_child(store_sup, ChildSpec) of
		{error, already_present} ->
			supervisor:restart_child(store_sup, Id);
		Else ->
			Else
	end,
	case Status of
		{ok, _Pid, Interface} ->
			{ok, Interface};
		{error, Error} ->
			{error, Error}
	end.

stop_store(Id) ->
	case supervisor:terminate_child(store_sup, Id) of
		ok                 -> ok;
		{error, not_found} -> ok;
		Error              -> Error
	end.

init([]) ->
	RestartStrategy    = one_for_one,
	MaxRestarts        = 1,
	MaxTimeBetRestarts = 60,
	ChildSpecs         = [ ],
	{ok, {{RestartStrategy, MaxRestarts, MaxTimeBetRestarts}, ChildSpecs}}.

