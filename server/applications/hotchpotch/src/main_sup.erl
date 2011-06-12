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

-module(main_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, main_sup}, ?MODULE, []).

init([]) ->
	RestartStrategy    = one_for_all,
	MaxRestarts        = 1,
	MaxTimeBetRestarts = 60,

	ChildSpecs = [
		{
			sys_info,
			{sys_info, start_link, []},
			permanent,
			1000,
			worker,
			[sys_info]
		},
		{
			pool_sup,
			{pool_sup, start_link, []},
			permanent,
			infinity,
			supervisor,
			[pool_sup]
		},
		{
			worker_sup,
			{worker_sup, start_link, []},
			permanent,
			infinity,
			supervisor,
			[worker_sup]
		},
		{
			interfaces_sup,
			{interfaces_sup, start_link, []},
			permanent,
			infinity,
			supervisor,
			[interfaces_sup]
		}
	],

	{ok, {{RestartStrategy, MaxRestarts, MaxTimeBetRestarts}, ChildSpecs}}.

