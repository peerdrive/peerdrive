%% Hotchpotch
%% Copyright (C) 2010  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
%%
%% This program is free software: you can redistribute_write it and/or modify
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

-module(broker_io).
-behaviour(gen_server).

-export([start/1]).
-export([read/4, write/4, truncate/3, get_parents/1, set_parents/2, get_type/1,
	set_type/2, commit/1, suspend/1, close/1]).

-export([init/1, init/2, handle_call/3, handle_cast/2, code_change/3,
	handle_info/2, terminate/2]).

-record(state, {handles, user, doc}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public broker operations...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start(Operation) ->
	proc_lib:start_link(?MODULE, init, [self(), Operation]).

read(Broker, Part, Offset, Length) ->
	gen_server:call(Broker, {read, Part, Offset, Length}).

write(Broker, Part, Offset, Data) ->
	gen_server:call(Broker, {write, Part, Offset, Data}).

truncate(Broker, Part, Offset) ->
	gen_server:call(Broker, {truncate, Part, Offset}).

get_parents(Broker) ->
	gen_server:call(Broker, get_parents).

set_parents(Broker, Parents) ->
	gen_server:call(Broker, {set_parents, Parents}).

get_type(Broker) ->
	gen_server:call(Broker, get_type).

set_type(Broker, Type) ->
	gen_server:call(Broker, {set_type, Type}).

commit(Broker) ->
	gen_server:call(Broker, commit).

suspend(Broker) ->
	gen_server:call(Broker, suspend).

close(Broker) ->
	gen_server:call(Broker, close).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Parent, Operation) ->
	case init_operation(Operation) of
		{ok, ErrInfo, State} ->
			proc_lib:init_ack(Parent, {ok, ErrInfo, self()}),
			process_flag(trap_exit, true),
			link(Parent),
			gen_server:enter_loop(?MODULE, [], State#state{user=Parent});

		{error, _Reason, _ErrInfo} = Error ->
			proc_lib:init_ack(Parent, Error),
			Error
	end.


init_operation({peek, Rev, Stores}) ->
	do_peek(Rev, Stores);

init_operation({create, Doc, Type, Creator, Stores}) ->
	do_create(Doc, Type, Creator, Stores);

init_operation({fork, Doc, StartRev, Creator, Stores}) ->
	do_fork(Doc, StartRev, Creator, Stores);

init_operation({update, Doc, StartRev, Creator, Stores}) ->
	do_update(Doc, StartRev, Creator, Stores);

init_operation({resume, Doc, PreRev, Creator, Stores}) ->
	do_resume(Doc, PreRev, Creator, Stores).


handle_call({read, Part, Offset, Length}, _From, S) ->
	distribute_read(fun(Handle) -> store:read(Handle, Part, Offset, Length) end, S);

handle_call({write, Part, Offset, Data}, _From, S) ->
	distribute_write(fun(Handle) -> store:write(Handle, Part, Offset, Data) end, S);

handle_call({truncate, Part, Offset}, _From, S) ->
	distribute_write(fun(Handle) -> store:truncate(Handle, Part, Offset) end, S);

handle_call(get_parents, _From, S) ->
	distribute_read(fun(Handle) -> store:get_parents(Handle) end, S);

handle_call({set_parents, Parents}, _From, S) ->
	distribute_write(fun(Handle) -> store:set_parents(Handle, Parents) end, S);

handle_call(get_type, _From, S) ->
	distribute_read(fun(Handle) -> store:get_type(Handle) end, S);

handle_call({set_type, Type}, _From, S) ->
	distribute_write(fun(Handle) -> store:set_type(Handle, Type) end, S);

handle_call(commit, _From, S) ->
	do_commit(fun store:commit/2, S);

handle_call(suspend, _From, S) ->
	do_commit(fun store:suspend/2, S);

handle_call(close, _From, S) ->
	do_close(S#state.handles),
	{stop, normal, {ok, []}, S}.


handle_info({'EXIT', From, Reason}, #state{user=User} = S) ->
	case From of
		User ->
			% upstream process died
			do_close(S#state.handles),
			{stop, orphaned, S};

		_Handle ->
			% one of the handles died, abnormally?
			case Reason of
				normal ->
					% don't care
					{noreply, S};

				Abnormal ->
					% well, this one was unexpected...
					error_logger:warning_msg("broker_io: handle died: ~p~n",
						[Abnormal]),
					% FIXME: N is a #handle{}
					Handles = lists:filter(
						fun({_, N}) -> is_process_alive(N) end,
						S#state.handles),
					{noreply, S#state{handles=Handles}}
			end
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


init(_) -> {stop, enotsup}.
handle_cast(_, State)    -> {stop, enotsup, State}.
code_change(_, State, _) -> {ok, State}.
terminate(_, _)          -> ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_peek(Rev, Stores) ->
	do_peek_loop(Rev, Stores, []).


do_peek_loop(_Rev, [], Errors) ->
	broker:consolidate_error(Errors);

do_peek_loop(Rev, [{Guid, Ifc} | Stores], Errors) ->
	case store:peek(Ifc, Rev) of
		{ok, Handle} ->
			State = #state{ handles=[{Guid, Handle}] },
			broker:consolidate_success(Errors, State);
		{error, Reason} ->
			do_peek_loop(Rev, Stores, [{Guid, Reason} | Errors])
	end.


do_create(Doc, Type, Creator, Stores) ->
	start_handles(
		fun(Ifc) -> store:create(Ifc, Doc, Type, Creator) end,
		Doc,
		Stores).


do_fork(Doc, StartRev, Creator, Stores) ->
	start_handles(
		fun(Ifc) -> store:fork(Ifc, Doc, StartRev, Creator) end,
		Doc,
		Stores).


do_update(Doc, StartRev, Creator, Stores) ->
	start_handles(
		fun(Ifc) -> store:update(Ifc, Doc, StartRev, Creator) end,
		Doc,
		Stores).


do_resume(Doc, PreRev, Creator, Stores) ->
	start_handles(
		fun(Ifc) -> store:resume(Ifc, Doc, PreRev, Creator) end,
		Doc,
		Stores).


do_commit(Fun, S) ->
	Mtime = util:get_time(),
	{Revs, RWHandles, ROHandles, Errors} = lists:foldl(
		fun({Store, StoreHandle}=Handle, {AccRevs, AccRW, AccRO, AccErrors}) ->
			case Fun(StoreHandle, Mtime) of
				{ok, Rev} ->
					{[Rev|AccRevs], AccRW, [Handle|AccRO], AccErrors};
				{error, Reason} ->
					{AccRevs, [Handle|AccRW], AccRO, [{Store, Reason}|AccErrors]}
			end
		end,
		{[], [], [], []},
		S#state.handles),
	case lists:usort(Revs) of
		[Rev] ->
			% as expected
			do_close(RWHandles),
			vol_monitor:trigger_mod_doc(local, S#state.doc),
			{reply, broker:consolidate_success(Errors, Rev),
				S#state{handles=ROHandles}};

		[] when ROHandles =:= [] ->
			% no writer commited...
			{reply, broker:consolidate_error(Errors),
				S#state{handles=RWHandles}};

		RevList ->
			% internal error: handles did not came to the same revision! WTF?
			error_logger:error_report([
				{module, ?MODULE},
				{error, 'revision discrepancy'},
				{doc, util:bin_to_hexstr(S#state.doc)},
				{revs, lists:map(fun util:bin_to_hexstr/1, RevList)},
				{committers, lists:map(fun({G,_}) -> util:bin_to_hexstr(G) end, ROHandles)}
			]),
			do_close(ROHandles),
			do_close(RWHandles),
			{reply, {error, einternal, []}, S#state{handles=[]}}
	end.


do_close(Handles) ->
	lists:foreach(fun({_Guid, Handle}) -> store:close(Handle) end, Handles).


start_handles(Fun, Doc, Stores) ->
	case start_handles_loop(Fun, Stores, [], []) of
		{ok, ErrInfo, Handles} ->
			{ok, ErrInfo, #state{doc=Doc, handles=Handles}};
		Error ->
			Error
	end.


start_handles_loop(_Fun, [], Handles, Errors) ->
	case Handles of
		[] -> broker:consolidate_error(Errors);
		_  -> broker:consolidate_success(Errors, Handles)
	end;

start_handles_loop(Fun, [{Guid, Ifc} | Stores], Handles, Errors) ->
	case Fun(Ifc) of
		{ok, Handle} ->
			start_handles_loop(Fun, Stores, [{Guid, Handle} | Handles], Errors);
		{error, Reason} ->
			start_handles_loop(Fun, Stores, Handles, [{Guid, Reason} | Errors])
	end.


distribute_read(Fun, S) ->
	Reply = case S#state.handles of
		[] ->
			{error, enoent, []};

		[{Guid, Handle} | _] ->
			% TODO: ask more than just the first store if it fails...
			case Fun(Handle) of
				{ok, Result}    -> {ok, [], Result};
				{error, Reason} -> {error, Reason, [{Guid, Reason}]}
			end
	end,
	{reply, Reply, S}.


distribute_write(Fun, S) ->
	% TODO: parallelize this loop
	{Success, Fail} = lists:foldr(
		fun({Store, Handle}, {AccSuccess, AccFail}) ->
			case Fun(Handle) of
				ok ->
					{[{Store, Handle} | AccSuccess], AccFail};

				{error, Reason} ->
					{AccSuccess, [{Store, Reason, Handle} | AccFail]}
			end
		end,
		{[], []},
		S#state.handles),

	% let's see what we got...
	case Success of
		[] ->
			% nobody did anything -> no state change -> keep going
			ErrInfo = lists:map(
				fun({Store, Reason, _}) -> {Store, Reason} end,
				Fail),
			{reply, broker:consolidate_error(ErrInfo), S};

		_ ->
			% at least someone did what he was told
			ErrInfo = lists:map(
				fun({Store, Reason, Handle}) ->
					store:close(Handle),
					{Store, Reason}
				end,
				Fail),
			{reply, broker:consolidate_success(ErrInfo), S#state{handles=Success}}
	end.

