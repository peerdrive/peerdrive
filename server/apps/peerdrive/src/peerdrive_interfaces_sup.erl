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

-module(peerdrive_interfaces_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, peerdrive_interfaces_sup}, ?MODULE, []).

init([]) ->
	RestartStrategy    = one_for_one,
	MaxRestarts        = 1,
	MaxTimeBetRestarts = 60,
	RealInterfaces = case application:get_env(peerdrive, interfaces) of
		{ok, Interfaces} ->
			lists:filter(
				fun(Spec) ->
					case check_ifc_spec(Spec) of
						true ->
							true;
						false ->
							error_logger:warning_msg("Dropping invalid interface spec: ~p~n", [Spec]),
							false
					end
				end,
				Interfaces);

		undefined ->
			VfsEntry = case peerdrive_util:cfg_sys_daemon() of
				false ->
					MntDir = peerdrive_util:cfg_mnt_dir(),
					case filelib:ensure_dir(filename:join(MntDir, "dummy")) of
						ok ->
							[{vfs, []}];
						{error, Reason} ->
							error_logger:error_msg("Cannot create mount dir: ~p~n",
								[Reason]),
							[]
					end;
				true ->
					[]
			end,
			[{native, []} | VfsEntry]
	end,
	ChildSpecs = [
		{
			peerdrive_ifc_vfs_broker,
			{peerdrive_ifc_vfs_broker, start_link, []},
			permanent,
			10000,
			worker,
			[peerdrive_ifc_vfs_broker]
		}
		| [map_ifc_spec(S) || S <- RealInterfaces]
	],
	{ok, {{RestartStrategy, MaxRestarts, MaxTimeBetRestarts}, ChildSpecs}}.


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
	Port = proplists:get_value(port, Options, 0),
	peerdrive_server_sup:get_supervisor_spec(peerdrive_ifc_client,
		Port, Options ++ [{ip, "127.0.0.1"}]);

map_ifc_spec({netstore, Options}) ->
	Port = proplists:get_value(port, Options, 4568),
	peerdrive_server_sup:get_supervisor_spec(peerdrive_ifc_netstore,
		Port, Options);

map_ifc_spec({vfs, Options}) ->
	case erlang:system_info(system_architecture) of
		"win32" ->
			{
				make_ref(),
				{peerdrive_ifc_vfs_dokan, start_link, [Options]},
				permanent,
				10000,
				worker,
				[peerdrive_ifc_vfs_dokan]
			};

		_ ->
			{
				make_ref(),
				{peerdrive_ifc_vfs_fuse, start_link, [Options]},
				permanent,
				10000,
				worker,
				[peerdrive_ifc_vfs_fuse]
			}
	end.

