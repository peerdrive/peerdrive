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

-module(server_sup).
-behaviour(supervisor).

-export([get_supervisor_spec/2, start_link/3]).
-export([init/1]).


get_supervisor_spec(Id, Options) ->
	{
		Id,
		{server_sup, start_link, [Id] ++ Options},
		permanent,
		infinity,
		supervisor,
		[server_sup]
	}.


start_link(Id, Module, Port) ->
	supervisor:start_link(
		{local, list_to_atom(Id ++ "_server_sup")},
		?MODULE,
		{Id, Module, Port}).


init({Id, Module, Port}) ->
	RestartStrategy    = one_for_all,
	MaxRestarts        = 1,
	MaxTimeBetRestarts = 60,

	ServletId  = list_to_atom(Id ++ "_servlet_sup"),
	ListenerId = list_to_atom(Id ++ "_listener"),

	ChildSpecs = [
		{
			ServletId,
			{servlet_sup, start_link, [ServletId, Module]},
			permanent,
			infinity,
			supervisor,
			[servlet_sup]
		},
		{
			ListenerId,
			{listener, start_link, [ListenerId, ServletId, Port]},
			permanent,
			1000,
			worker,
			[listener]
		}
	],

	{ok, {{RestartStrategy, MaxRestarts, MaxTimeBetRestarts}, ChildSpecs}}.


