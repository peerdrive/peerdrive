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

-module(peerdrive_auto_mounter).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-include("utils.hrl").
-include("volman.hrl").

-record(state, {store, doc, fstab}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	#peerdrive_store{pid=SysStore} = peerdrive_volman:sys_store(),
	peerdrive_vol_monitor:register_proc(),
	case peerdrive_util:walk(SysStore, <<"fstab">>) of
		{ok, Doc} ->
			case read_fstab(SysStore, Doc) of
				{ok, FsTab} ->
					S = #state{store=SysStore, doc=Doc, fstab=FsTab},
					auto_mount(S),
					{ok, S};

				{error, Reason} ->
					{stop, Reason}
			end;

		{error, enoent} ->
			S = #state{store=SysStore, fstab=gb_trees:empty()},
			{ok, S};

		{error, Reason} ->
			{stop, Reason}
	end.


terminate(_Reason, _State) ->
	peerdrive_vol_monitor:deregister_proc().


handle_call(_Request, _From, S) -> {reply, badarg, S}.
handle_info(_, S) -> {noreply, S}.
handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local stuff
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

read_fstab(Store, Doc) ->
	case peerdrive_util:read_doc_struct(Store, Doc, <<"PDSD">>) of
		{ok, FsTab} when ?IS_GB_TREE(FsTab) ->
			{ok, FsTab};
		{ok, _} ->
			{error, eio};
		{error, _} = Error ->
			Error
	end.


auto_mount(#state{fstab=FsTab}) ->
	lists:foreach(fun auto_mount_store/1, gb_trees:to_list(FsTab)).


auto_mount_store({Label, Spec}) when ?IS_GB_TREE(Spec) ->
	try
		case gb_trees:lookup(<<"auto">>, Spec) of
			{value, true} ->
				Src = unicode:characters_to_list(get_value(<<"src">>, Spec)),
				Type = unicode:characters_to_list(get_value(<<"type">>,
					<<"file">>, Spec)),
				Options = unicode:characters_to_list(get_value(<<"options">>,
					<<"">>, Spec)),
				Credentials = unicode:characters_to_list(get_value(
					<<"credentials">>, <<"">>, Spec)),
				peerdrive_volman:mount(Src, Options, Credentials, Type,
					unicode:characters_to_list(Label));
			_ ->
				ok
		end
	catch
		throw:Error ->
			error_logger:error_report([{module, ?MODULE},
				{function, auto_mount_store}, {error, Error}])
	end;

auto_mount_store(_) ->
	ok.


get_value(Key, Tree) ->
	case gb_trees:lookup(Key, Tree) of
		{value, Value} -> Value;
		none -> throw(einval)
	end.


get_value(Key, Default, Tree) ->
	case gb_trees:lookup(Key, Tree) of
		{value, Value} -> Value;
		none -> Default
	end.

