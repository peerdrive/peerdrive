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

-module(peerdrive_main_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, peerdrive_main_sup}, ?MODULE, []).

init([]) ->
	RestartStrategy    = one_for_all,
	MaxRestarts        = 1,
	MaxTimeBetRestarts = 60,

	ChildSpecs = [
		{
			peerdrive_sys_info,
			{peerdrive_sys_info, start_link, []},
			permanent,
			1000,
			worker,
			[peerdrive_sys_info]
		},
		{
			peerdrive_pool_sup,
			{peerdrive_pool_sup, start_link, []},
			permanent,
			infinity,
			supervisor,
			[peerdrive_pool_sup]
		},
		{
			peerdrive_registry,
			{peerdrive_registry, start_link, []},
			permanent,
			1000,
			worker,
			[peerdrive_registry]
		},
		{
			peerdrive_worker_sup,
			{peerdrive_worker_sup, start_link, []},
			permanent,
			infinity,
			supervisor,
			[peerdrive_worker_sup]
		},
		{
			peerdrive_interfaces_sup,
			{peerdrive_interfaces_sup, start_link, []},
			permanent,
			infinity,
			supervisor,
			[peerdrive_interfaces_sup]
		}
	],

	{ok, {{RestartStrategy, MaxRestarts, MaxTimeBetRestarts}, ChildSpecs}}.

