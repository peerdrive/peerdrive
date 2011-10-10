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

-module(peerdrive_sync_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([start_sync/3, stop_sync/2]).
-export([init/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).


start_sync(Mode, Store, Peer) ->
	Id = <<Store/binary, Peer/binary>>,
	ChildSpec = {
		Id,
		{peerdrive_sync_worker, start_link, [Mode, Store, Peer]},
		transient,
		10000,
		worker,
		[peerdrive_sync_worker]
	},
	case supervisor:start_child(?MODULE, ChildSpec) of
		{ok, _Child} ->
			ok;
		{error, already_present} ->
			ok = supervisor:delete_child(?MODULE, Id),
			case supervisor:start_child(?MODULE, ChildSpec) of
				{ok, _Child} ->
					ok;
				{error, _} = Error->
					Error
			end;
		{error, {already_started, _}} ->
			{error, ebusy};
		{error, _} = Error->
			Error
	end.


stop_sync(Store, Peer) ->
	case supervisor:terminate_child(?MODULE, <<Store/binary, Peer/binary>>) of
		ok ->
			ok;
		{error, not_found} ->
			{error, einval}
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	{ok, { {one_for_one, 1, 10}, [] }}.

