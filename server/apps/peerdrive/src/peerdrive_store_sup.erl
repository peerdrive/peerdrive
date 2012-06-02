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

-module(peerdrive_store_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([spawn_store/4, reap_store/1]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).


spawn_store(Module, Src, ParsedOpt, ParsedCreds) ->
	Ref = make_ref(),
	ChildSpec = {
		Ref,
		{Module, start_link, [Src, ParsedOpt, ParsedCreds]},
		transient,
		30000,
		worker,
		[Module]
	},
	case supervisor:start_child(?MODULE, ChildSpec) of
		{ok, Pid} ->
			{ok, Ref, Pid};
		{error, {Reason, _ChildSpec}} ->
			{error, Reason};
		{error, Reason} ->
			{error, Reason}
	end.


reap_store(Ref) ->
	case supervisor:terminate_child(?MODULE, Ref) of
		ok ->
			supervisor:delete_child(?MODULE, Ref),
			ok;
		{error, not_found} ->
			ok
	end.


init([]) ->
	RestartStrategy    = one_for_one,
	MaxRestarts        = 1,
	MaxTimeBetRestarts = 60,
	ChildSpecs         = [ ],
	{ok, {{RestartStrategy, MaxRestarts, MaxTimeBetRestarts}, ChildSpecs}}.

