%% PeerDrive
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

-module(peerdrive_crypt_store_guard).
-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-record(state, {store, handle, key, user}).

-include("store.hrl").
-include("cryptstore.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Handle, Key, User) ->
	State = #state{store=self(), handle=Handle, user=User, key=Key},
	gen_server:start_link(?MODULE, {State, User}, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({State, User}) ->
	process_flag(trap_exit, true),
	link(User),
	{ok, State}.


handle_call(commit, _From, #state{handle=Handle, key=Key} = S) ->
	Reply = case peerdrive_store:commit(Handle) of
		{ok, EncRId} -> {ok, peerdrive_crypt_store:dec_xid(Key, EncRId)};
		Error        -> Error
	end,
	{reply, Reply, S};

handle_call(close, _From, #state{handle=Handle} = S) ->
	peerdrive_store:close(Handle),
	{stop, normal, ok, S};

handle_call(_, _, S) ->
	{reply, {error, ebadf}, S}.


handle_info({'EXIT', From, Reason}, #state{store=Store, user=User} = S) ->
	case From of
		User ->
			peerdrive_store:close(S#state.handle),
			{stop, normal, S};
		Store ->
			peerdrive_store:close(S#state.handle),
			{stop, Reason, S};
		_ ->
			{noreply, S}
	end.


terminate(_Reason, #state{user=User}) ->
	unlink(User).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


handle_cast(_, State)    -> {stop, enotsup, State}.
code_change(_, State, _) -> {ok, State}.

