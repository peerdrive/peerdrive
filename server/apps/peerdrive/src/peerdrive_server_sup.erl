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

-export([get_supervisor_spec/3, get_servlet_sup_pid/1, start_link/3]).
-export([init/1]).


get_supervisor_spec(Module, Port, Options) ->
	{
		make_ref(),
		{peerdrive_server_sup, start_link, [Module, Port, Options]},
		permanent,
		infinity,
		supervisor,
		[peerdrive_server_sup]
	}.


get_servlet_sup_pid(ServerSup) ->
	Children = supervisor:which_children(ServerSup),
	{servlets, Child, _Type, _Modules} = lists:keyfind(servlets, 1, Children),
	true = is_pid(Child),
	Child.


start_link(Module, Port, Options) ->
	supervisor:start_link(?MODULE, {Module, Port, Options}).


init({Module, Port, Options}) ->
	RestartStrategy    = one_for_all,
	MaxRestarts        = 1,
	MaxTimeBetRestarts = 60,

	ChildSpecs = [
		{
			servlets,
			{peerdrive_servlet_sup, start_link, [Module, Options]},
			permanent,
			infinity,
			supervisor,
			[peerdrive_servlet_sup]
		},
		{
			listener,
			{peerdrive_listener, start_link, [self(), Module, Port, Options]},
			permanent,
			1000,
			worker,
			[peerdrive_listener]
		}
	],

	{ok, {{RestartStrategy, MaxRestarts, MaxTimeBetRestarts}, ChildSpecs}}.


