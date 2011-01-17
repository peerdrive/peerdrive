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

-module(ifc_fuse).
-behaviour(supervisor).

-export([get_supervisor_spec/2, start_link/1]).
-export([init/1]).


get_supervisor_spec(Id, Options) ->
	{
		Id,
		{ifc_fuse, start_link, [Options]},
		permanent,
		infinity,
		supervisor,
		[ifc_fuse]
	}.


start_link(Options) ->
	supervisor:start_link({local, fuse}, ?MODULE, Options).


init(Options) ->
	RestartStrategy    = one_for_all,
	MaxRestarts        = 1,
	MaxTimeBetRestarts = 60,
	ChildSpecs = [
		{
			ifc_fuse_store,
			{ifc_fuse_store, start_link, []},
			permanent,
			10000,
			worker,
			[ifc_fuse_store]
		},
		{
			ifc_fuse_client,
			{ifc_fuse_client, start_link, Options},
			permanent,
			10000,
			worker,
			[ifc_fuse_client]
		}
	],
	{ok, {{RestartStrategy, MaxRestarts, MaxTimeBetRestarts}, ChildSpecs}}.

