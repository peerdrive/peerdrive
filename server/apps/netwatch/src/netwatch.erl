%%
%% This file is part of PeerDrive.
%% Copyright (C) 2012  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
%%
%% PeerDrive is free software: you can redistribute it and/or modify it under
%% the terms of the GNU Lesser General Public License as published by the Free
%% Software Foundation, either version 3 of the License, or (at your option)
%% any later version.
%%
%% PeerDrive is distributed in the hope that it will be useful,
%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
%% GNU Lesser General Public License for more details.
%%
%% You should have received a copy of the GNU Lesser General Public License
%% along with PeerDrive. If not, see <http://www.gnu.org/licenses/>.
%%
-module(netwatch).

-export([start/0, stop/1]).

-define(DRV_NAME, "netwatch_drv").

-ifdef(HAVE_NETWATCH_DRV).
start() ->
	try
		case erl_ddll:load(code:priv_dir(?MODULE), ?DRV_NAME) of
			ok -> ok;
			{error, already_loaded} -> ok;
			LoadErr -> throw(LoadErr)
		end,
		open_port({spawn_driver, ?DRV_NAME}, [binary])
	catch
		throw:Error -> Error
	end.


stop(Port) ->
	catch port_close(Port),
	erl_ddll:unload(?DRV_NAME).
-else.
start() -> ok.
stop(_) -> ok.
-endif.
