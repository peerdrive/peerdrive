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

-module(peerdrive_file_store_put).
-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-record(state, {store, did, rid, done, user}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(DId, RId, User) ->
	State = #state{
		store = self(),
		did   = DId,
		rid   = RId,
		done  = false,
		user  = User
	},
	gen_server:start_link(?MODULE, {State, User}, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({State, User}) ->
	process_flag(trap_exit, true),
	link(User),
	{ok, State}.


% returns `ok | {error, Reason}'
handle_call(commit, _From, #state{done=false} = S) ->
	case do_commit(S) of
		{ok, _} = Ok ->
			{reply, Ok, S#state{done=true}};
		Error ->
			{reply, Error, S}
	end;

% returns nothing
handle_call(close, _From, S) ->
	{stop, normal, ok, S};

handle_call(_, _, S) ->
	{reply, {error, ebadf}, S}.


handle_info({'EXIT', From, Reason}, #state{store=Store} = S) ->
	case From of
		Store -> {stop, Reason, S};
		_User -> {stop, normal, S}
	end.


terminate(_Reason, #state{store=Store, did=DId, rid=RId, user=User}) ->
	unlink(User),
	peerdrive_file_store:doc_unlock(Store, DId),
	peerdrive_file_store:rev_unlock(Store, RId).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


handle_cast(_, State)    -> {noreply, State}.
code_change(_, State, _) -> {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_commit(#state{store=Store, did=DId, rid=RId}) ->
	peerdrive_file_store:put_doc_commit(Store, DId, RId).

