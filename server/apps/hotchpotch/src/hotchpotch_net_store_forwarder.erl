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

-module(hotchpotch_net_store_forwarder).
-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-record(state, {store, handle}).

-include("store.hrl").
-include("netstore.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Store, Handle, User) ->
	State = #state{store=Store, handle=Handle},
	gen_server:start_link(?MODULE, {State, User}, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({State, User}) ->
	process_flag(trap_exit, true),
	link(User),
	{ok, State}.


handle_call(commit, _From, S) ->
	Reply = relay_request(?FF_DOC_COMMIT_REQ, S),
	{stop, normal, Reply, S};

handle_call(abort, _From, S) ->
	relay_request(?FF_DOC_ABORT_REQ, S),
	{stop, normal, ok, S}.


handle_info({'EXIT', From, Reason}, #state{store=Store} = S) ->
	case From of
		Store ->
			{stop, {orphaned, Reason}, S};
		_User ->
			relay_request(?FF_DOC_ABORT_REQ, S),
			{stop, normal, S}
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


handle_cast(_, State)    -> {stop, enotsup, State}.
code_change(_, State, _) -> {ok, State}.
terminate(_, _)          -> ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

relay_request(Request, #state{handle=Handle, store=Store}) ->
	case hotchpotch_net_store:io_request(Store, Request, <<Handle:32>>) of
		{ok, <<>>} -> ok;
		Error      -> Error
	end.

