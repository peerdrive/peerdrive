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

-module(peerdrive_volman).
-behaviour(gen_server).

-export([start_link/0]).
-export([enum/0, enum_all/0, store/1, sys_store/0, mount/5, unmount/1]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-record(store, {ref, pid, spec}).
-record(state, {sys_store, stores}).

-include("volman.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Enumerate all known stores, excluding the system store.
%%
%% Returns information about all mounted stores in the system.
%%
%% @spec enum() -> Result
%%       Result = [#peerdrive_store{}]
enum() ->
	gen_server:call(?MODULE, enum, infinity).

%% @doc Enumerate all known stores, including the system store.
%%
%% @spec enum_all() -> Result
%%       Result = [#peerdrive_store{}]
enum_all() ->
	gen_server:call(?MODULE, enum_all, infinity).

%% @doc Get pid of a specific store
%% @spec store(Guid) -> {ok, pid()} | error
%%       Guid = guid()
store(SId) ->
	Enum = gen_server:call(?MODULE, enum_all, infinity),
	case lists:keyfind(SId, #peerdrive_store.sid, Enum) of
		#peerdrive_store{pid=Pid} ->
			{ok, Pid};
		false ->
			error
	end.

%% @doc Get Guids and pid 's of system store
%% @spec sys_store() -> #peerdrive_store{}
sys_store() ->
	gen_server:call(?MODULE, sys_store, infinity).

%% @doc Mount a store
%% @spec mount(Src, Options, Type, Label) -> Result
%%       Result = {ok, SId} | {error, Reason}
%%       Src, Options, Type, Label = string()
%%       SId = guid()
%%       Reason = ecode()
mount(Src, Options, Credentials, Type, Label) when is_list(Src),
                                                   is_list(Options),
												   is_list(Credentials),
                                                   is_list(Type),
												   is_list(Label) ->
	gen_server:call(?MODULE, {mount, Src, Options, Credentials, Type, Label},
		infinity).

%% @doc Unmount a store
%% @spec unmount(SId) -> ok | {error, Reason}
%%       SId = guid()
unmount(SId) ->
	gen_server:call(?MODULE, {unmount, SId}, infinity).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks implementation...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	process_flag(trap_exit, true),
	try
		{Src, Options, Type} = get_sys_store_spec(),
		SysStore = case mount_internal(Src, Options, "", Type, "sys") of
			{ok, Store} ->
				link(Store#store.pid),
				Store;

			{error, Error} ->
				error_logger:error_report(["Could not mount system store",
					{error, Error}, {src, Src}, {options, Options},
					{type, Type}]),
				throw(Error)
		end,
		{ok, #state{sys_store=SysStore, stores=[]}}
	catch
		throw:Reason ->
			{stop, Reason}
	end.


handle_call(enum, _From, #state{stores=Stores} = S) ->
	Reply = [ Spec || #store{spec=Spec} <- Stores ],
	{reply, Reply, S};

handle_call(enum_all, _From, #state{sys_store=SysStore, stores=Stores} = S) ->
	Reply = [ Spec || #store{spec=Spec} <- [SysStore | Stores] ],
	{reply, Reply, S};

handle_call(sys_store, _From, #state{sys_store=SysStore} = S) ->
	{reply, SysStore#store.spec, S};

handle_call({mount, Src, Options, Credentials, Type, Label}, From, S) ->
	do_mount(From, Src, Options, Credentials, Type, Label, S);

handle_call({reg, Store}, _From, S) ->
	do_reg(Store, S);

handle_call({unmount, SId}, _From, S) ->
	do_unmount(SId, S).


handle_info({'EXIT', Pid, _Reason}, #state{stores=Stores} = S) ->
	case lists:keytake(Pid, #store.pid, Stores) of
		{value, #store{ref=Ref, spec=#peerdrive_store{sid=SId}}, Remaining} ->
			peerdrive_vol_monitor:trigger_rem_store(SId),
			peerdrive_store_sup:reap_store(Ref),
			{noreply, S#state{stores=Remaining}};

		false ->
			{noreply, S}
	end.


handle_cast(_Request, S)            -> {noreply, S}.
terminate(_Reason, _State)          -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% local functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_mount(From, Src, Options, Credentials, Type, Label, S) ->
	spawn_link(fun() ->
		Reply = case mount_internal(Src, Options, Credentials, Type, Label) of
			{ok, Store} ->
				case gen_server:call(?MODULE, {reg, Store}, infinity) of
					ok ->
						{ok, (Store#store.spec)#peerdrive_store.sid};
					{error, _} = Error ->
						peerdrive_store_sup:reap_store(Store#store.ref),
						Error
				end;

			{error, _} = Error ->
				Error
		end,
		gen_server:reply(From, Reply)
	end),
	{noreply, S}.


do_unmount(WhichSId, #state{stores=Stores} = S) ->
	Victim = [ Ref || #store{ref=Ref, spec=#peerdrive_store{sid=SId}} <- Stores,
		SId == WhichSId ],
	case Victim of
		[Ref] ->
			peerdrive_store_sup:reap_store(Ref),
			{reply, ok, S};
		[] ->
			{reply, {error, einval}, S}
	end.


mount_internal(Src, Options, Creds, Type, Label) ->
	try
		ParsedOpt = parse_options(Options),
		ParsedCreds = parse_options(Creds),
		Module = get_module_for_type(Type),
		Store = case peerdrive_store_sup:spawn_store(Module, Src, ParsedOpt, ParsedCreds) of
			{ok, Ref, Pid} ->
				#store{
					ref  = Ref,
					pid  = Pid,
					spec = #peerdrive_store{
						pid     = Pid,
						label   = Label,
						sid     = peerdrive_store:guid(Pid),
						src     = Src,
						options = ParsedOpt,
						type    = Type
					}
				};
			{error, Reason} ->
				throw(Reason)
		end,
		{ok, Store}
	catch
		throw:Error ->
			{error, Error}
	end.


do_reg(#store{pid=Pid, spec=#peerdrive_store{sid=SId, label=Label}} = Store, S) ->
	#state{stores=Stores} = S,
	try
		[ throw(eexist) ||
			#store{spec=#peerdrive_store{sid=OtherSId, label=OtherLabel}} <- Stores,
			(OtherSId == SId) or (OtherLabel == Label) ],
		link(Pid),
		peerdrive_vol_monitor:trigger_add_store(SId),
		S2 = S#state{stores=[Store|Stores]},
		{reply, ok, S2}
	catch
		throw:Error ->
			{reply, {error, Error}, S}
	end.


get_sys_store_spec() ->
	case application:get_env(peerdrive, sys_store) of
		{ok, {Src, Options, Type} = Spec} when is_list(Src), is_list(Options),
		                                       is_list(Type) ->
			Spec;

		{ok, Else} ->
			error_logger:error_msg("Invalid syste store spec: ~p~n", [Else]),
			throw(einval);

		undefined ->
			SysDir = filename:join(peerdrive_util:cfg_app_dir(), "sys"),
			case filelib:ensure_dir(filename:join(SysDir, "dummy")) of
				ok ->
					{SysDir, "", "file"};
				{error, Reason} ->
					error_logger:error_msg("Cannot create system store dir: ~p~n",
						[Reason]),
					throw(Reason)
			end
	end.


% todo: add escaping of ',' and '='
parse_options("") ->
	[];
parse_options(Options) ->
	List = [
		case re:split(Opt, "=") of
			[Opt] -> {Opt, true};
			[Key, Value] -> {Key, Value};
			_ -> throw(einval)
		end
		|| Opt <- re:split(Options, ",")
	],
	proplists:compact(List).


get_module_for_type("file") ->
	peerdrive_file_store;
get_module_for_type("net") ->
	peerdrive_net_store;
get_module_for_type("crypt") ->
	peerdrive_crypt_store;
get_module_for_type(_) ->
	throw(einval).

