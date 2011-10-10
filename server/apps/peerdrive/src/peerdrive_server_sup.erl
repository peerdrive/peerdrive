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

-module(peerdrive_server_sup).
-behaviour(supervisor).

-export([get_supervisor_spec/4, start_link/4]).
-export([init/1]).


get_supervisor_spec(Id, Module, Port, Options) ->
	{
		Id,
		{peerdrive_server_sup, start_link, [Id, Module, Port, Options]},
		permanent,
		infinity,
		supervisor,
		[peerdrive_server_sup]
	}.


start_link(Id, Module, Port, Options) ->
	supervisor:start_link(
		{local, list_to_atom(Id ++ "_peerdrive_server_sup")},
		?MODULE,
		{Id, Module, Port, Options}).


init({Id, Module, Port, Options}) ->
	RestartStrategy    = one_for_all,
	MaxRestarts        = 1,
	MaxTimeBetRestarts = 60,

	ServletId  = list_to_atom(Id ++ "_peerdrive_servlet_sup"),
	ListenerId = list_to_atom(Id ++ "_peerdrive_listener"),

	ChildSpecs = [
		{
			ServletId,
			{peerdrive_servlet_sup, start_link, [ServletId, Module, Options]},
			permanent,
			infinity,
			supervisor,
			[peerdrive_servlet_sup]
		},
		{
			ListenerId,
			{peerdrive_listener, start_link, [ListenerId, ServletId, Port, Options]},
			permanent,
			1000,
			worker,
			[peerdrive_listener]
		}
	],

	{ok, {{RestartStrategy, MaxRestarts, MaxTimeBetRestarts}, ChildSpecs}}.


