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

-module(peerdrive_vol_monitor).
-behaviour(gen_event).

-export([register_proc/1, deregister_proc/1, trigger_add_store/1,
		 trigger_rem_store/1, trigger_add_rev/2, trigger_rm_rev/2,
		 trigger_add_doc/2, trigger_rm_doc/2, trigger_mod_doc/2]).
-export([start_link/0]).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
	gen_event:start_link({local, ?MODULE}).


trigger_add_store(StoreGuid) ->
	gen_event:notify(?MODULE, {trigger_add_store, StoreGuid}).

trigger_rem_store(StoreGuid) ->
	gen_event:notify(?MODULE, {trigger_rem_store, StoreGuid}).

trigger_add_rev(StoreGuid, Rev) ->
	gen_event:notify(?MODULE, {trigger_add_rev, StoreGuid, Rev}).

trigger_rm_rev(StoreGuid, Rev) ->
	gen_event:notify(?MODULE, {trigger_rm_rev, StoreGuid, Rev}).

trigger_add_doc(StoreGuid, Doc) ->
	gen_event:notify(?MODULE, {trigger_add_doc, StoreGuid, Doc}).

trigger_rm_doc(StoreGuid, Doc) ->
	gen_event:notify(?MODULE, {trigger_rm_doc, StoreGuid, Doc}).

trigger_mod_doc(StoreGuid, Doc) ->
	gen_event:notify(?MODULE, {trigger_mod_doc, StoreGuid, Doc}).


register_proc(Id) ->
	gen_event:add_sup_handler(?MODULE, {?MODULE, Id}, self()).

deregister_proc(Id) ->
	Handler = {?MODULE, Id},
	case gen_event:delete_handler(?MODULE, Handler, []) of
		ok ->
			receive
				{gen_event_EXIT, Handler, normal} -> ok
			end;
		Error ->
			Error
	end.



init(Pid) ->
	{ok, Pid}.


handle_event(Event, State) ->
	Pid = State,
	Pid ! Event,
	{ok, State}.


handle_call(_Request, State) -> {ok, badarg, State}.
handle_info(_Info, State) -> {ok, State}.
terminate(_Arg, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

