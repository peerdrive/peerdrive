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

-module(hotchpotch_pool_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, hotchpotch_pool_sup}, ?MODULE, []).

init([]) ->
	RestartStrategy    = one_for_all,
	MaxRestarts        = 1,
	MaxTimeBetRestarts = 60,

	ChildSpecs = [
		{
			hotchpotch_revcache,
			{hotchpotch_revcache, start_link, []},
			permanent,
			10000,
			worker,
			[hotchpotch_revcache]
		},
		{
			hotchpotch_store_sup,
			{hotchpotch_store_sup, start_link, []},
			permanent,
			infinity,
			supervisor,
			[hotchpotch_store_sup]
		},
		{
			hotchpotch_vol_monitor,
			{hotchpotch_vol_monitor, start_link, []},
			permanent,
			1000,
			worker,
			[hotchpotch_vol_monitor]
		},
		{
			hotchpotch_volman,
			{hotchpotch_volman, start_link, []},
			permanent,
			1000,
			worker,
			[hotchpotch_volman]
		},
		{
			hotchpotch_change_monitor,
			{hotchpotch_change_monitor, start_link, []},
			permanent,
			60000,
			worker,
			[hotchpotch_change_monitor]
		}
	],

	{ok, {{RestartStrategy, MaxRestarts, MaxTimeBetRestarts}, ChildSpecs}}.


