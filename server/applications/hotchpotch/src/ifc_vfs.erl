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

-module(ifc_vfs).
-behaviour(supervisor).

-export([get_supervisor_spec/2, start_link/1]).
-export([init/1]).


get_supervisor_spec(Id, Options) ->
	{
		Id,
		{ifc_vfs, start_link, [Options]},
		permanent,
		infinity,
		supervisor,
		[ifc_vfs]
	}.


start_link(Options) ->
	supervisor:start_link({local, fuse}, ?MODULE, Options).


init(Options) ->
	RestartStrategy    = one_for_all,
	MaxRestarts        = 1,
	MaxTimeBetRestarts = 60,
	NativeSpec = case erlang:system_info(system_architecture) of
		"win32" ->
			{
				ifc_vfs_dokan,
				{ifc_vfs_dokan, start_link, Options},
				permanent,
				10000,
				worker,
				[ifc_vfs_dokan]
			};

		_ ->
			{
				ifc_vfs_fuse,
				{ifc_vfs_fuse, start_link, Options},
				permanent,
				10000,
				worker,
				[ifc_vfs_fuse]
			}
	end,
	ChildSpecs = [
		{
			ifc_vfs_broker,
			{ifc_vfs_broker, start_link, []},
			permanent,
			10000,
			worker,
			[ifc_vfs_broker]
		},
		NativeSpec
	],
	{ok, {{RestartStrategy, MaxRestarts, MaxTimeBetRestarts}, ChildSpecs}}.

