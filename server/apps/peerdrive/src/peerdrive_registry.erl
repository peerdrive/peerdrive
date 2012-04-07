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

-module(peerdrive_registry).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).
-export([get_uti_from_extension/1, get_uti_from_extension/2]).

-include("utils.hrl").

-record(state, {store, doc, reg}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


get_uti_from_extension(Ext) ->
	get_uti_from_extension(Ext, <<"public.data">>).


get_uti_from_extension(Ext, Default) ->
	gen_server:call(?MODULE, {uti_from_ext, Ext, Default}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	{_SysDoc, SysStore} = peerdrive_volman:sys_store(),
	peerdrive_vol_monitor:register_proc(),
	case peerdrive_util:walk(SysStore, <<"registry">>) of
		{ok, Doc} ->
			case read_registry(SysStore, Doc) of
				{ok, Registry} ->
					S = #state{store=SysStore, doc=Doc, reg=Registry},
					{ok, S};

				{error, Reason} ->
					{stop, Reason}
			end;

		{error, enoent} ->
			S = #state{store=SysStore, reg=gb_trees:empty()},
			{ok, S};

		{error, Reason} ->
			{stop, Reason}
	end.


handle_call({uti_from_ext, Ext, Default}, _From, S) ->
	Reply = uti_from_ext(Ext, Default, S#state.reg),
	{reply, Reply, S}.


handle_info({vol_event, mod_doc, _Store, Doc}, #state{doc=Doc} = S) ->
	case read_registry(S#state.store, Doc) of
		{ok, Registry} ->
			{noreply, monitor, S#state{reg=Registry}};

		{error, Reason} ->
			{stop, Reason, S}
	end;

handle_info(_, S) ->
	{noreply, S}.


terminate(_Reason, _State) ->
	peerdrive_vol_monitor:deregister_proc().


handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local stuff
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

read_registry(Store, Doc) ->
	case peerdrive_util:read_doc_struct(Store, Doc, <<"PDSD">>) of
		{ok, Registry} when ?IS_GB_TREE(Registry) ->
			{ok, Registry};
		{ok, _} ->
			{error, eio};
		{error, _} = Error ->
			Error
	end.


uti_from_ext(Ext, Default, Reg) when is_list(Ext) ->
	uti_from_ext(unicode:characters_to_binary(Ext), Default, Reg);

uti_from_ext(Ext, Default, Reg) ->
	try
		lists:foreach(
			fun({Uti, Spec}) ->
				case gb_trees:lookup(<<"extensions">>, Spec) of
					{value, Extensions} ->
						lists:member(Ext, Extensions) andalso throw(Uti);
					none ->
						ok
				end
			end,
			gb_trees:to_list(Reg)),
		Default
	catch
		throw:Result -> Result
	end.

