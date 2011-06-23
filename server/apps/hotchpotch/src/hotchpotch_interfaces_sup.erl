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

-module(hotchpotch_interfaces_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, hotchpotch_interfaces_sup}, ?MODULE, []).

init([]) ->
	RestartStrategy    = one_for_one,
	MaxRestarts        = 1,
	MaxTimeBetRestarts = 60,
	case application:get_env(hotchpotch, interfaces) of
		{ok, Interfaces} ->
			RealInterfaces = lists:filter(
				fun(Spec) ->
					case check_ifc_spec(Spec) of
						true ->
							true;
						false ->
							error_logger:warning_msg("Dropping invalid interface spec: ~p~n", [Spec]),
							false
					end
				end,
				Interfaces),
			ChildSpecs = [map_ifc_spec(S) || S <- RealInterfaces],
			{ok, {{RestartStrategy, MaxRestarts, MaxTimeBetRestarts}, ChildSpecs}};

		undefined ->
			error_logger:error_msg("Interfaces section missing in configuration!~n"),
			{error, nointerfaces}
	end.


check_ifc_spec({Mod, Options}) when is_atom(Mod) and is_list(Options) ->
	lists:member(Mod, [native, netstore, vfs]) andalso
	lists:all(
		fun
			({Tag, _Value}) when is_atom(Tag) -> true;
			(Tag) when is_atom(Tag) -> true;
			(_) -> false
		end,
		Options);

check_ifc_spec(_) ->
	false.


map_ifc_spec({native, Options}) ->
	Port = proplists:get_value(port, Options, 4567),
	hotchpotch_server_sup:get_supervisor_spec("native", hotchpotch_ifc_client,
		Port, Options);

map_ifc_spec({netstore, Options}) ->
	Port = proplists:get_value(port, Options, 4568),
	hotchpotch_server_sup:get_supervisor_spec("netstore", hotchpotch_ifc_netstore,
		Port, Options);

map_ifc_spec({vfs, Options}) ->
	hotchpotch_ifc_vfs:get_supervisor_spec("vfs", Options).

