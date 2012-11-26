%% otp_app_install.erl - Rebar plugin to install OTP applications
%%
%% Copyright (C) 2012  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
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

-module(otp_app_install).
-export([install/2]).

-include("rebar.hrl").

install(Config, File) ->
	case rebar_app_utils:is_app_dir() of
		{true, _} ->
			AppFile = case rebar_app_utils:is_app_src(File) of
				true  -> rebar_app_utils:app_src_to_app(File);
				false -> File
			end,
			do_install(Config, AppFile);

		false ->
			ok % skip non-app directories
	end.

do_install(Config, AppFile) ->
	LibDir = rebar_config:get_global(Config, target,
		filename:join(code:root_dir(), "lib")),

	%% Load the app file and validate it.
	case rebar_app_utils:load_app_file(Config, AppFile) of
		{ok, Config1, AppName, AppData} ->
			{vsn, Vsn} = lists:keyfind(vsn, 1, AppData),
			TargetDir = filename:join(LibDir, atom_to_list(AppName) ++ "-" ++ Vsn),
			ok = mk_target_dir(Config1, TargetDir),
			ok = copy_sub_dir(TargetDir, "ebin"),
			ok = copy_sub_dir(TargetDir, "priv"),
			copy_sub_dir(TargetDir, "include");

		{error, Reason} ->
			?ABORT("Failed to load app file ~s: ~p\n", [AppFile, Reason])
	end.

mk_target_dir(Config, TargetDir) ->
	case filelib:is_dir(TargetDir) of
		false ->
			case filelib:ensure_dir(filename:join(TargetDir, "dummy")) of
				ok ->
					ok;
				{error, Reason} ->
					?ERROR("Failed to make target dir ~p: ~s\n",
						   [TargetDir, file:format_error(Reason)]),
					?FAIL
			end;

		true ->
			%% Output directory already exists; if force=1, wipe it out
			case rebar_config:get_global(Config, force, "0") of
				"1" ->
					rebar_file_utils:rm_rf(TargetDir),
					ok = file:make_dir(TargetDir);
				_ ->
					?ERROR("Install target directory ~p already exists!\n",
						   [TargetDir]),
					?FAIL
			end
	end.

copy_sub_dir(TargetDir, SubDir) ->
	case filelib:is_dir(SubDir) of
		true ->
			rebar_file_utils:cp_r([SubDir], TargetDir);
		false ->
			ok
	end.

