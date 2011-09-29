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

%% Simplifies the store interface to a level which is needed by the fuse
%% module.  It will also try to merge consecutive commits.

-module(hotchpotch_ifc_vfs_broker).

-export([start_link/0]).
-export([lookup/2, stat/2, open_rev/2, open_doc/3, truncate/3, read/4, write/4,
	abort/1, close/1, get_type/1, set_type/2]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2,
	terminate/2]).

-include("store.hrl").


-define(VFS_CC, <<"org.hotchpotch.vfs">>).   % FUSE creator code
-define(VFS_WB_TIMEOUT, 5000).               % write back timeout in ms

% handles: dict: {wr, Store, Doc} | {ro, Store, Rev} -> {Rev, BrokerHandle, RefCnt}
% known:   dict: {Store, Doc} -> {PreRev, Timestamp, Open}
% timref:  Timer handle
-record(state, {handles, known, timref}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


lookup(Store, Doc) ->
	gen_server:call(?MODULE, {lookup, Store, Doc}, infinity).

stat(Store, Rev) ->
	hotchpotch_broker:stat(Rev, [Store]).

open_rev(Store, Rev) ->
	gen_server:call(?MODULE, {open_rev, Store, Rev}, infinity).

open_doc(Store, Doc, Write) ->
	gen_server:call(?MODULE, {open_doc, Store, Doc, Write}, infinity).

truncate(Handle, Part, Offset) ->
	gen_server:call(?MODULE, {truncate, Handle, Part, Offset}, infinity).

read(Handle, Part, Offset, Length) ->
	gen_server:call(?MODULE, {read, Handle, Part, Offset, Length}, infinity).

write(Handle, Part, Offset, Data) ->
	gen_server:call(?MODULE, {write, Handle, Part, Offset, Data}, infinity).

get_type(Handle) ->
	gen_server:call(?MODULE, {get_type, Handle}, infinity).

set_type(Handle, Uti) ->
	gen_server:call(?MODULE, {set_type, Handle, Uti}, infinity).

close(Handle) ->
	gen_server:call(?MODULE, {close, Handle}, infinity).

abort(Handle) ->
	gen_server:call(?MODULE, {abort, Handle}, infinity).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Genserver callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	{ok, #state{handles=dict:new(), known=dict:new(), timref=undefined}}.


handle_call({read, Handle, Part, Offset, Length}, _From, S) ->
	Reply = do_read(Handle, Part, Offset, Length, S),
	{reply, Reply, S};

handle_call({truncate, Handle, Part, Offset}, _From, S) ->
	Reply = do_truncate(Handle, Part, Offset, S),
	{reply, Reply, S};

handle_call({write, Handle, Part, Offset, Data}, _From, S) ->
	Reply = do_write(Handle, Part, Offset, Data, S),
	{reply, Reply, S};

handle_call({get_type, Handle}, _From, S) ->
	Reply = do_get_type(Handle, S),
	{reply, Reply, S};

handle_call({set_type, Handle, Uti}, _From, S) ->
	Reply = do_set_type(Handle, Uti, S),
	{reply, Reply, S};

handle_call(Request, _From, S) ->
	{Reply, S2} = case Request of
		{lookup, Store, Doc} ->
			do_lookup(Store, Doc, S);
		{open_rev, Store, Rev} ->
			do_open_rev(Store, Rev, S);
		{open_doc, Store, Doc, Write} ->
			do_open_doc(Store, Doc, Write, S);
		{close, Handle} ->
			do_close(Handle, S);
		{abort, Handle} ->
			do_abort(Handle, S)
	end,
	{reply, Reply, S2}.


handle_info({timeout, _TimerRef, check}, S) ->
	S2 = check_timer_expired(S),
	{noreply, S2}.


handle_cast(_Request, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_, State, _) -> {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Actual method implementations
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_lookup(Store, Doc, S) ->
	case lookup_internal(Store, Doc, S) of
		{ok, Rev, _IsPre, _IsOpen, S2} ->
			{{ok, Rev}, S2};

		{error, S2} ->
			{error, S2}
	end.


% open revision (always read only)
do_open_rev(Store, Rev, S) ->
	FuseHandle = {ro, Store, Rev},
	case open_handle(FuseHandle, S) of
		{ok, Rev, S2} ->
			{{ok, Rev, FuseHandle}, S2};

		error ->
			case hotchpotch_broker:peek(Store, Rev) of
				{ok, Handle} ->
					S2 = create_handle(FuseHandle, Rev, Handle, S),
					{{ok, Rev, FuseHandle}, S2};

				{error, Reason} ->
					{{error, Reason}, S}
			end
	end.


% open document read only
do_open_doc(Store, Doc, false, S) ->
	case lookup_internal(Store, Doc, S) of
		{ok, Rev, _IsPre, _IsOpen, S2} ->
			do_open_rev(Store, Rev, S2);

		{error, S2} ->
			{{error, enoent}, S2}
	end;

% open document read-write
do_open_doc(Store, Doc, true, S) ->
	FuseHandle = {rw, Store, Doc},
	case open_handle(FuseHandle, S) of
		{ok, Rev, S2} ->
			{{ok, Rev, FuseHandle}, S2};

		error ->
			case lookup_internal(Store, Doc, S) of
				{ok, Rev, IsPre, false, S2} ->
					StoreReply = if
						IsPre ->
							hotchpotch_broker:resume(Store, Doc, Rev, undefined);
						true ->
							hotchpotch_broker:update(Store, Doc, Rev, ?VFS_CC)
					end,
					case StoreReply of
						{ok, Handle} ->
							S3 = create_handle(FuseHandle, Rev, Handle, S2),
							S4 = if
								IsPre -> mark_open(Store, Doc, S3);
								true  -> S3
							end,
							{{ok, Rev, FuseHandle}, S4};

						{error, Reason} ->
							{{error, Reason}, S2}
					end;

				{ok, _Rev, _IsPre, true, S2} ->
					error_logger:error_msg("fuse_store: Open document but no handle found!~n"),
					{{error, eacces}, S2};

				{error, S2} ->
					{{error, enoent}, S2}
			end
	end.


do_truncate(FuseHandle, Part, Offset, S) ->
	case lookup_handle(FuseHandle, S) of
		{ok, Handle} ->
			hotchpotch_broker:truncate(Handle, Part, Offset);
		error ->
			{error, ebadf}
	end.


do_get_type(FuseHandle, S) ->
	case lookup_handle(FuseHandle, S) of
		{ok, Handle} ->
			hotchpotch_broker:get_type(Handle);
		error ->
			{error, ebadf}
	end.


do_set_type(FuseHandle, Uti, S) ->
	case lookup_handle(FuseHandle, S) of
		{ok, Handle} ->
			hotchpotch_broker:set_type(Handle, Uti);
		error ->
			{error, ebadf}
	end.


do_read(FuseHandle, Part, Offset, Length, S) ->
	case lookup_handle(FuseHandle, S) of
		{ok, Handle} ->
			hotchpotch_broker:read(Handle, Part, Offset, Length);
		error ->
			{error, ebadf}
	end.


do_write(FuseHandle, Part, Offset, Data, S) ->
	case lookup_handle(FuseHandle, S) of
		{ok, Handle} ->
			hotchpotch_broker:write(Handle, Part, Offset, Data);
		error ->
			{error, ebadf}
	end.


do_close({rw, Store, Doc}=FuseHandle, S) ->
	case close_handle(FuseHandle, S) of
		{State, Handle, S2} ->
			Result = hotchpotch_broker:suspend(Handle),
			hotchpotch_broker:close(Handle),
			case Result of
				{ok, Rev} ->
					S3 = mark_committed(Store, Doc, Rev, S2),
					case State of
						closed ->
							{{ok, Rev}, S3};

						keep ->
							case hotchpotch_broker:resume(Store, Doc, Rev, undefined) of
								{ok, NewHandle} ->
									S4 = reopen_handle(FuseHandle, Rev, NewHandle, S3),
									{{ok, Rev}, mark_open(Store, Doc, S4)};

								{error, _} ->
									% Could create the checkpoint but not
									% reopen the broker handle.  This means the
									% 'close' has succeeded but all other
									% references to the handle are now invalid.
									{{ok, Rev}, forget_handle(FuseHandle, S3)}
							end
					end;

				{error, Reason} ->
					% close failed, mark internal state as closed and forget
					S3 = mark_closed(Store, Doc, S2),
					S4 = forget_handle(FuseHandle, S3),
					{{error, Reason}, S4}
			end;

		error ->
			{{error, ebadf}, S}
	end;

do_close({ro, _Store, Rev}=FuseHandle, S) ->
	case close_handle(FuseHandle, S) of
		{closed, Handle, S2} ->
			hotchpotch_broker:close(Handle),
			{{ok, Rev}, S2};

		{keep, _Handle, S2} ->
			{{ok, Rev}, S2};

		error ->
			{{error, ebadf}, S}
	end.


do_abort(FuseHandle, S) ->
	case close_handle(FuseHandle, S) of
		{closed, Handle, S2} ->
			S3 = case FuseHandle of
				{rw, Store, Doc}   -> mark_closed(Store, Doc, S2);
				{ro, _Store, _Rev} -> S2
			end,
			hotchpotch_broker:close(Handle),
			{ok, S3};

		{keep, _Handle, S2} ->
			error_logger:error_msg("fuse_store: Tried to abort shared handle!~n"),
			{ok, S2};

		error ->
			{{error, ebadf}, S}
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

lookup_internal(Store, Doc, S) ->
	case hotchpotch_broker:lookup_doc(Doc, [Store]) of
		{[{Rev, _}], BrokerPreRevs} ->
			PreRevs = lists:map(fun({PreRev, _}) -> PreRev end, BrokerPreRevs),
			case check_known(Store, Doc, Rev, PreRevs, S) of
				{ok, _KnownRev, _IsPre, _IsOpen, _S2} = Ok ->
					Ok;

				{error, S2} ->
					{ok, Rev, false, false, S2}
			end;

		{[], []} ->
			{error, forget(Store, Doc, S)}
	end.


check_known(Store, Doc, Rev, PreRevs, #state{known=Known}=S) ->
	Key = {Store, Doc},
	case dict:find(Key, Known) of
		{ok, {PreRev, _Ts, Open}} ->
			case lists:member(PreRev, PreRevs) of
				true ->
					S2 = S#state{known=dict:store(Key, {PreRev, ts(), Open}, Known)},
					{ok, PreRev, true, Open, S2};

				false ->
					% The doc still exists but someone has removed our prerev!
					% => Just forget our version.
					S2 = S#state{known=dict:erase(Key, Known)},
					{error, S2}
			end;

		error ->
			% We don't have a PreRev for this Doc yet. However, check if there
			% are stale PreRevs...
			case check_prerevs(Store, Doc, Rev, PreRevs) of
				{ok, PreRev} ->
					Ts = ts(),
					S2 = S#state{known=dict:store(Key, {PreRev, Ts, false}, Known)},
					{ok, PreRev, true, false, check_timer_running(S2, Ts)};

				none ->
					{error, S}
			end
	end.


check_prerevs(_Store, _Doc, _Rev, []) ->
	none;

check_prerevs(Store, Doc, Rev, [PreRev | PreRevs]) ->
	case hotchpotch_broker:stat(PreRev, [Store]) of
		{ok, #rev_stat{creator=?VFS_CC, parents=Parents}} ->
			case lists:member(Rev, Parents) of
				true ->
					{ok, PreRev};
				false ->
					hotchpotch_broker:forget(Store, Doc, PreRev),
					check_prerevs(Store, Doc, Rev, PreRevs)
			end;

		_ ->
			check_prerevs(Store, Doc, Rev, PreRevs)
	end.


create_handle(FuseHandle, Rev, BrokerHandle, #state{handles=Handles}=S) ->
	S#state{handles = dict:store(FuseHandle, {Rev, BrokerHandle, 1}, Handles)}.


open_handle(FuseHandle, #state{handles=Handles}=S) ->
	case dict:find(FuseHandle, Handles) of
		{ok, {Rev, Handle, RefCnt}} ->
			S2 = S#state{handles=dict:store(FuseHandle, {Rev, Handle, RefCnt+1}, Handles)},
			{ok, Rev, S2};

		error ->
			error
	end.


lookup_handle(FuseHandle, #state{handles=Handles}) ->
	case dict:find(FuseHandle, Handles) of
		{ok, {_Rev, Handle, _RefCnt}} ->
			{ok, Handle};
		error ->
			error
	end.


close_handle(FuseHandle, #state{handles=Handles}=S) ->
	case dict:find(FuseHandle, Handles) of
		{ok, {_Rev, Handle, 1}} ->
			S2 = S#state{handles=dict:erase(FuseHandle, Handles)},
			{closed, Handle, S2};

		{ok, {Rev, Handle, RefCnt}} ->
			S2 = S#state{handles=dict:store(FuseHandle, {Rev, Handle, RefCnt-1}, Handles)},
			{keep, Handle, S2};

		error ->
			error
	end.


reopen_handle(FuseHandle, NewRev, NewHandle, #state{handles=Handles} = S) ->
	NewHandles = dict:update(
		FuseHandle,
		fun({_Rev, _Handle, RefCnt}) -> {NewRev, NewHandle, RefCnt} end,
		Handles),
	S#state{handles=NewHandles}.


forget_handle(FuseHandle, #state{handles=Handles} = S) ->
	S#state{handles=dict:erase(FuseHandle, Handles)}.


mark_open(Store, Doc, #state{known=Known}=S) ->
	NewKnown = dict:update(
		{Store, Doc},
		fun({PreRev, _Timestamp, _Open}) -> {PreRev, ts(), true} end,
		Known),
	S#state{known=NewKnown}.


mark_committed(Store, Doc, Rev, #state{known=Known}=S) ->
	Ts = ts(),
	S2 = S#state{known = dict:store({Store, Doc}, {Rev, Ts, false}, Known)},
	check_timer_running(S2, Ts).


mark_closed(Store, Doc, #state{known=Known}=S) ->
	Key = {Store, Doc},
	case dict:find(Key, Known) of
		{ok, {Rev, _Ts, _Open}} ->
			Ts = ts(),
			S2 = S#state{
				known   = dict:store(Key, {Rev, Ts, false}, Known)
			},
			check_timer_running(S2, Ts);

		error ->
			S
	end.


forget(Store, Doc, #state{known=Known}=S) ->
	S#state{known=dict:erase({Store, Doc}, Known)}.


ts() ->
	hotchpotch_util:get_time() div 1000 + ?VFS_WB_TIMEOUT.


check_timer_running(#state{timref=TimRef}=S, Ts) ->
	case TimRef of
		undefined ->
			Now = hotchpotch_util:get_time() div 1000,
			NewTim = erlang:start_timer(Ts-Now, self(), check),
			S#state{timref=NewTim};

		_ ->
			S
	end.


check_timer_expired(S) ->
	{Expired, S2} = check_expired(S),
	commit_prerevs(Expired),
	S2.


check_expired(#state{known=Known}=S) ->
	Now = hotchpotch_util:get_time() div 1000,
	{Expired, Timeout} = dict:fold(
		fun
			(_, {_Rev, _Ts, true}, Acc) ->
				% currently open -> doesn't expire
				Acc;

			({Store, Doc}, {Rev, Ts, false}, {AccExp, AccTmo}=Acc) ->
				if
					Ts =< Now       -> {[{Store, Doc, Rev} | AccExp], AccTmo};
					AccTmo == never -> {AccExp, Ts};
					Ts < AccTmo     -> {AccExp, Ts};
					true            -> Acc
				end
		end,
		{[], never},
		Known),
	NewKnown = case Expired of
		[] -> Known;
		_  -> dict:filter(fun(_, {_Rev, Ts, _Open}) -> Ts > Now end, Known)
	end,
	NewTimRef = case Timeout of
		never -> undefined;
		_     -> erlang:start_timer(Timeout-Now, self(), check)
	end,
	{Expired, S#state{timref=NewTimRef, known=NewKnown}}.


commit_prerevs(Expired) ->
	lists:foreach(
		fun({Store, Doc, Rev}) -> commit_prerev(Store, Doc, Rev) end,
		Expired).


commit_prerev(Store, Doc, Rev) ->
	case hotchpotch_broker:resume(Store, Doc, Rev, undefined) of
		{ok, Handle} ->
			try
				commit_prerev_loop(Store, Doc, Handle)
			after
				hotchpotch_broker:close(Handle)
			end;

		{error, enoent} ->
			% has been deleted in between
			ok;

		{error, Reason} ->
			error_logger:warning_msg("FUSE: Could not resume: ~w~n", [Reason])
	end.


commit_prerev_loop(Store, Doc, Handle) ->
	case hotchpotch_broker:commit(Handle) of
		{ok, _Rev} ->
			ok;

		{error, econflict} ->
			case hotchpotch_broker:lookup_doc(Doc, [Store]) of
				{[{CurRev, _}], _PreRevs} ->
					hotchpotch_broker:rebase(Handle, CurRev),
					commit_prerev_loop(Store, Doc, Handle);
				{[], []} ->
					% doesn't exist anymore
					ok
			end;

		{error, Reason} ->
			error_logger:warning_msg("FUSE: Could not commit: ~w~n", [Reason])
	end.

