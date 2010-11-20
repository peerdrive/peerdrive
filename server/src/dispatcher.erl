%% Hotchpotch
%% Copyright (C) 2010  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
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

-module(dispatcher).

-export([start_link/0]).
-export([init/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	proc_lib:start_link(?MODULE, init, [self()]).


init(Parent) ->
	vol_monitor:register_proc(dispatcher),
	proc_lib:init_ack(Parent, {ok, self()}),
	loop().


loop() ->
	receive
		{trigger_mod_doc, local, _Uuid} ->
			loop();

		{trigger_mod_doc, StoreGuid, Uuid} ->
			replicator:event_modified(Uuid, StoreGuid),
			loop();

		{trigger_add_store, Store} ->
			added_store(Store),
			loop();

		_ ->
			loop()
	end.


added_store(Store) ->
	lists:foreach(
		fun({Mode, From, To}) -> synchronizer:sync(Mode, From, To) end,
		get_sync_rules(Store)).


get_sync_rules(Store) ->
	lists:foldl(
		fun(RawRule, Acc) ->
			case parse_rule(RawRule) of
				error ->
					Acc;

				{Mode, Store, To} ->
					case volman:store(To) of
						{ok, _Ifc} -> [{Mode, Store, To} | Acc];
						error      -> Acc
					end;

				{Mode, From, Store} ->
					case volman:store(From) of
						{ok, _Ifc} -> [{Mode, From, Store} | Acc];
						error      -> Acc
					end;

				{_Mode, _From, _To} ->
					Acc
			end
		end,
		[],
		load_sync_rules()).


parse_rule(Rule) when is_record(Rule, dict, 9) ->
	case dict:find(<<"from">>, Rule) of
		{ok, StoreHex} ->
			Store = util:hexstr_to_bin(binary_to_list(StoreHex)),
			case dict:find(<<"to">>, Rule) of
				{ok, PeerHex} ->
					Peer = util:hexstr_to_bin(binary_to_list(PeerHex)),
					case dict:find(<<"mode">>, Rule) of
						{ok, <<"ff">>}        -> {ff, Store, Peer};
						{ok, <<"automerge">>} -> {automerge, Store, Peer};
						{ok, <<"savemerge">>} -> {savemerge, Store, Peer};
						{ok, _} -> error;
						error -> error
					end;

				error ->
					error
			end;

		error ->
			error
	end;

parse_rule(_) ->
	error.


load_sync_rules() ->
	{ok, SysStoreGuid} = find_sys_store(volman:enum()),
	{ok, SysStoreIfc} = volman:store(SysStoreGuid),
	{ok, Root} = read_doc(SysStoreIfc, SysStoreGuid),
	case dict:find(<<"syncrules">>, Root) of
		{ok, {dlink, Doc}} ->
			case read_doc(SysStoreIfc, Doc) of
				{ok, Rules}      -> Rules;
				{error, _Reason} -> []
			end;

		{ok, _} -> [];
		error   -> []
	end.


find_sys_store([]) ->
	error;
find_sys_store([{_Id, _Descr, Guid, Tags} | Remaining]) ->
	case proplists:is_defined(system, Tags) of
		true  -> {ok, Guid};
		false -> find_sys_store(Remaining)
	end.


read_doc(StoreIfc, Doc) ->
	case store:lookup(StoreIfc, Doc) of
		{ok, Rev, _} -> util:read_rev_struct(Rev, <<"HPSD">>);
		error        -> {error, enoent}
	end.

