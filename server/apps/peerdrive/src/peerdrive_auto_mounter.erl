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

-record(state, {store :: pid(), doc :: peerdrive:doc(), fstab :: gb_tree(), watch}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	#peerdrive_store{pid=SysStore, sid=SysSId} = peerdrive_volman:sys_store(),
	peerdrive_vol_monitor:register_proc(),
	Watch = netwatch:start(),
	S1 = #state{store=SysStore, watch=Watch},
	case peerdrive_util:walk(SysStore, <<"fstab">>) of
		{ok, Doc} ->
			case read_fstab(SysStore, Doc) of
				{ok, FsTab} ->
					S2 = S1#state{doc=Doc, fstab=FsTab},
					auto_mount(S2),
					{ok, S2};

				{error, Reason} ->
					{stop, Reason}
			end;

		{error, enoent} ->
			case create_fstab(SysStore, SysSId) of
				{ok, Doc} ->
					S2 = S1#state{doc=Doc, fstab=gb_trees:empty()},
					{ok, S2};
				{error, Reason} ->
					{stop, Reason}
			end;

		{error, Reason} ->
			{stop, Reason}
	end.


terminate(_Reason, S) ->
	netwatch:stop(S#state.watch),
	peerdrive_vol_monitor:deregister_proc().


handle_info({vol_event, mod_doc, _Store, Doc}, #state{doc=Doc} = S) ->
	case read_fstab(S#state.store, Doc) of
		{ok, FsTab} ->
			{noreply, S#state{fstab=FsTab}};

		{error, Reason} ->
			{stop, Reason, S}
	end;

handle_info({Watch, ifup}, #state{watch=Watch} = S) ->
	% wait some time for the network to settle...
	erlang:send_after(7000, self(), check_net_stores),
	{noreply, S};

handle_info({Watch, ifdown}, #state{watch=Watch} = S) ->
	ping_net_stores(),
	{noreply, S};

handle_info(check_net_stores, S) ->
	check_net_stores(S#state.fstab),
	{noreply, S};

handle_info(_, S) ->
	{noreply, S}.


handle_call(_Request, _From, S) -> {reply, badarg, S}.
handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local stuff
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec read_fstab(pid(), peerdrive:doc()) -> {ok, gb_tree()} | {error, atom()}.
read_fstab(Store, Doc) ->
	case peerdrive_util:read_doc_struct(Store, Doc, <<"/org.peerdrive.fstab">>) of
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
				Type = unicode:characters_to_list(get_value(<<"type">>,
					<<"file">>, Spec)),
				mount_store(Label, Type, Spec);
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


create_fstab(Store, Root) ->
	try
		{ok, Doc, Handle} = check(peerdrive_broker:create(Store,
			<<"org.peerdrive.fstab">>, <<"">>)),
		try
			Meta = gb_trees:enter(<<"title">>, <<"fstab">>, gb_trees:empty()),
			Data = gb_trees:enter(<<"org.peerdrive.fstab">>, gb_trees:empty(),
				gb_trees:enter(<<"org.peerdrive.annotation">>, Meta, gb_trees:empty())),
			ok = check(peerdrive_broker:set_data(Handle, <<"">>,
				peerdrive_struct:encode(Data))),
			{ok, _Rev} = check(peerdrive_broker:commit(Handle)),
			ok = check(peerdrive_util:folder_link(Store, Root, Doc)),
			{ok, Doc}
		after
			peerdrive_broker:close(Handle)
		end
	catch
		throw:Error -> Error
	end.


check({error, _} = Error) ->
	throw(Error);
check(Term) ->
	Term.


check_net_stores(FsTab) ->
	lists:foreach(fun check_net_store/1, gb_trees:to_list(FsTab)).

check_net_store({Label, Spec}) when ?IS_GB_TREE(Spec) ->
	try
		get_value(<<"auto">>, false, Spec)
			andalso (get_value(<<"type">>, <<"file">>, Spec) =:= <<"net">>)
			andalso not is_mounted(Label)
			andalso mount_store(Label, "net", Spec)
	catch
		throw:Error ->
			error_logger:error_report([{module, ?MODULE},
				{function, auto_mount_store}, {error, Error}])
	end;

check_net_store(_) ->
	ok.


mount_store(Label, Type, Spec) ->
	Src = unicode:characters_to_list(get_value(<<"src">>, Spec)),
	Options = unicode:characters_to_list(get_value(<<"options">>,
		<<"">>, Spec)),
	Credentials = unicode:characters_to_list(get_value(
		<<"credentials">>, <<"">>, Spec)),
	peerdrive_volman:mount(Src, Options, Credentials, Type,
		unicode:characters_to_list(Label)).


is_mounted(BinLabel) ->
	Label = unicode:characters_to_list(BinLabel),
	Stores = peerdrive_volman:enum(),
	is_mounted(Label, Stores).

is_mounted(_Label, []) ->
	false;
is_mounted(Label, [Store | Rest]) ->
	Store#peerdrive_store.label == Label orelse is_mounted(Label, Rest).


ping_net_stores() ->
	NetStores = [ Store || Store <- peerdrive_volman:enum(),
		Store#peerdrive_store.type =:= "net" ],
	lists:foreach(fun(Store) -> spawn(fun() -> ping_net_store(Store) end) end,
		NetStores).


ping_net_store(#peerdrive_store{pid=Pid, sid=SId, src=Src}) ->
	Res = re:run(Src, "^(.+)@([-.[:alnum:]]+)(:[0-9]+)?$",
		[{capture, all_but_first, list}]),
	{Address, Port} = case Res of
		{match, [_Name, Ip]} ->
			{Ip, 4568};
		{match, [_Name, Ip, [_|PortStr]]} ->
			Prt = try
				list_to_integer(PortStr)
			catch
				error:badarg -> throw(einval)
			end,
			{Ip, Prt};
		_ ->
			throw(einval)
	end,
	Self = self(),
	% Try to connect to TCP port
	spawn(fun() -> Self ! {connect, connect_to_port(Address, Port)} end),
	% Also call store to see if it still responds
	spawn(fun() -> Self ! {statfs, peerdrive_store:statfs(Pid)} end),
	receive
		{connect, ok} ->
			ok;
		{connect, error} ->
			peerdrive_volman:unmount(SId);
		{statfs, _} ->
			ok % netstore will stop by itself on a transport error
	after
		% 15 seconds should be enough
		15000 ->
			peerdrive_volman:unmount(SId)
	end.


connect_to_port(Address, Port) ->
	case gen_tcp:connect(Address, Port, [binary, {active, false}]) of
		{ok, Socket} ->
			gen_tcp:close(Socket),
			ok;
		{error, _} ->
			error
	end.
