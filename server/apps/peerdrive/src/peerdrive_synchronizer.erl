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

-module(peerdrive_synchronizer).
-behaviour(supervisor).

-export([start_link/0]).
-export([start_sync/3, stop_sync/2]).
-export([init/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	supervisor:start_link({local, peerdrive_synchronizer}, ?MODULE, []).

start_sync(Mode, Store, Peer) ->
	peerdrive_sync_sup:start_sync(Mode, Store, Peer).

stop_sync(Store, Peer) ->
	peerdrive_sync_sup:stop_sync(Store, Peer).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	RestartStrategy    = one_for_all,
	MaxRestarts        = 1,
	MaxTimeBetRestarts = 60,
	ChildSpecs = [
		{
			peerdrive_sync_locks,
			{peerdrive_sync_locks, start_link, []},
			permanent,
			1000,
			worker,
			[peerdrive_sync_locks]
		},
		{
			peerdrive_sync_sup,
			{peerdrive_sync_sup, start_link, []},
			permanent,
			infinity,
			supervisor,
			[peerdrive_sync_sup]
		}
	],
	{ok, {{RestartStrategy, MaxRestarts, MaxTimeBetRestarts}, ChildSpecs}}.

