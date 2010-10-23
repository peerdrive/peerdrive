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

%% Simplifies the store interface to a level which is needed by the fuse
%% module.  It will also try to merge consecutive commits.
-module(fuse_store).

-export([start_link/0]).
-export([lookup/2, stat/2, open_rev/2, open_doc/3, truncate/3, read/4, write/4,
	commit/1, abort/1]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2,
	terminate/2]).

-include("store.hrl").


-define(FUSE_CC, <<"org.hotchpotch.fuse">>).  % FUSE creator code
-define(FUSE_WB_TIMEOUT, 5).                  % write back timeout in seconds

% next:    Next free handle
% handles: dict: Handle -> {Handle, Store, Doc}
% known:   dict: {Store, Doc} -> {PreRev, Timestamp, Open}
% timref:  Timer handle
-record(state, {next, handles, known, timref}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


lookup(Store, Doc) ->
	gen_server:call(?MODULE, {lookup, Store, Doc}).

stat(Store, Rev) ->
	store:stat(Store, Rev).

open_rev(Store, Rev) ->
	gen_server:call(?MODULE, {open_rev, Store, Rev}).

open_doc(Store, Doc, Write) ->
	gen_server:call(?MODULE, {open_doc, Store, Doc, Write}).

truncate(Handle, Part, Offset) ->
	gen_server:call(?MODULE, {truncate, Handle, Part, Offset}).

read(Handle, Part, Offset, Length) ->
	gen_server:call(?MODULE, {read, Handle, Part, Offset, Length}).

write(Handle, Part, Offset, Data) ->
	gen_server:call(?MODULE, {write, Handle, Part, Offset, Data}).

commit(Handle) ->
	gen_server:call(?MODULE, {commit, Handle}).

abort(Handle) ->
	gen_server:call(?MODULE, {abort, Handle}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Genserver callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	{ok, #state{next=0, handles=dict:new(), known=dict:new(), timref=undefined}}.


handle_call({read, Handle, Part, Offset, Length}, _From, S) ->
	Reply = do_read(Handle, Part, Offset, Length, S),
	{reply, Reply, S};

handle_call({truncate, Handle, Part, Offset}, _From, S) ->
	Reply = do_truncate(Handle, Part, Offset, S),
	{reply, Reply, S};

handle_call({write, Handle, Part, Offset, Data}, _From, S) ->
	Reply = do_write(Handle, Part, Offset, Data, S),
	{reply, Reply, S};

handle_call(Request, _From, S) ->
	{Reply, S2} = case Request of
		{lookup, Store, Doc} ->
			do_lookup(Store, Doc, S);
		{open_rev, Store, Rev} ->
			do_open_rev(Store, Rev, S);
		{open_doc, Store, Doc, Write} ->
			do_open_doc(Store, Doc, Write, S);
		{commit, Handle} ->
			do_commit(Handle, S);
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


do_open_rev(Store, Rev, S) ->
	case store:peek(Store, Rev) of
		{ok, Handle} ->
			{FuseHandle, S2} = alloc_handle(Handle, Store, undefined, S),
			{{ok, Rev, FuseHandle}, S2};

		{error, _Reason} = Error ->
			{Error, S}
	end.


do_open_doc(Store, Doc, Write, S) ->
	case lookup_internal(Store, Doc, S) of
		{ok, Rev, IsPre, false, S2} ->
			StoreReply = if
				Write and IsPre ->
					store:resume(Store, Doc, Rev, keep);
				Write ->
					store:update(Store, Doc, Rev, ?FUSE_CC);
				true ->
					store:peek(Store, Rev)
			end,
			case StoreReply of
				{ok, Handle} ->
					{FuseHandle, S3} = alloc_handle(Handle, Store, Doc, S2),
					S4 = if
						Write and IsPre -> mark_open(Store, Doc, S3);
						true -> S3
					end,
					{{ok, Rev, FuseHandle}, S4};

				{error, _Reason} = Error ->
					{Error, S2}
			end;

		{ok, _Rev, _IsPre, true, S2} ->
			{{error, eacces}, S2};

		{error, S2} ->
			{{error, enoent}, S2}
	end.


do_truncate(FuseHandle, Part, Offset, S) ->
	case lookup_handle(FuseHandle, S) of
		{ok, Handle, _Store, _Doc} ->
			store:truncate(Handle, Part, Offset);
		error ->
			{error, ebadf}
	end.


do_read(FuseHandle, Part, Offset, Length, S) ->
	case lookup_handle(FuseHandle, S) of
		{ok, Handle, _Store, _Doc} ->
			store:read(Handle, Part, Offset, Length);
		error ->
			{error, ebadf}
	end.


do_write(FuseHandle, Part, Offset, Data, S) ->
	case lookup_handle(FuseHandle, S) of
		{ok, Handle, _Store, _Doc} ->
			store:write(Handle, Part, Offset, Data);
		error ->
			{error, ebadf}
	end.


do_commit(FuseHandle, S) ->
	case lookup_handle(FuseHandle, S) of
		{ok, Handle, Store, Doc} ->
			case store:suspend(Handle, util:get_time()) of
				{ok, Rev} = Ok ->
					S2 = mark_closed(Store, Doc, Rev, S),
					{Ok, S2};

				{error, _Reason} = Error ->
					S2 = mark_closed(Store, Doc, S),
					{Error, S2}
			end;

		error ->
			{{error, ebadf}, S}
	end.


do_abort(FuseHandle, S) ->
	case lookup_handle(FuseHandle, S) of
		{ok, Handle, Store, Doc} ->
			store:abort(Handle),
			S2 = mark_closed(Store, Doc, S),
			{ok, S2};

		error ->
			{{error, ebadf}, S}
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

lookup_internal(Store, Doc, S) ->
	case store:lookup(Store, Doc) of
		{ok, Rev, PreRevs} ->
			case check_known(Store, Doc, Rev, PreRevs, S) of
				{ok, _KnownRev, _IsPre, _IsOpen, _S2} = Ok ->
					Ok;

				{error, S2} ->
					{ok, Rev, false, false, S2}
			end;

		error ->
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
	case store:stat(Store, PreRev) of
		{ok, #rev_stat{creator=?FUSE_CC, parents=Parents}} ->
			case lists:member(Rev, Parents) of
				true ->
					{ok, PreRev};
				false ->
					store:forget(Store, Doc, PreRev),
					check_prerevs(Store, Doc, Rev, PreRevs)
			end;

		_ ->
			check_prerevs(Store, Doc, Rev, PreRevs)
	end.


alloc_handle(Handle, Store, Doc, #state{next=Next, handles=Handles}=S) ->
	S2 = S#state{
		next    = Next+1,
		handles = dict:store(Next, {Handle, Store, Doc}, Handles)
	},
	{Next, S2}.


lookup_handle(FuseHandle, #state{handles=Handles}) ->
	case dict:find(FuseHandle, Handles) of
		{ok, {Handle, Store, Doc}} ->
			{ok, Handle, Store, Doc};
		error ->
			error
	end.


mark_open(Store, Doc, #state{known=Known}=S) ->
	NewKnown = dict:update(
		{Store, Doc},
		fun({PreRev, _Timestamp, _Open}) -> {PreRev, ts(), true} end,
		Known),
	S#state{known=NewKnown}.


mark_closed(Store, Doc, Rev, #state{known=Known}=S) ->
	Ts = ts(),
	NewKnown = dict:store({Store, Doc}, {Rev, Ts, false}, Known),
	check_timer_running(S#state{known=NewKnown}, Ts).


mark_closed(Store, Doc, #state{known=Known}=S) ->
	Key = {Store, Doc},
	case dict:find(Key, Known) of
		{ok, {Rev, _Ts, _Open}} ->
			Ts = ts(),
			S2 = S#state{known=dict:store(Key, {Rev, Ts, false}, Known)},
			check_timer_running(S2, Ts);
		error ->
			S
	end.


forget(Store, Doc, #state{known=Known}=S) ->
	S#state{known=dict:erase({Store, Doc}, Known)}.


ts() ->
	util:get_time() + ?FUSE_WB_TIMEOUT.


check_timer_running(#state{timref=TimRef}=S, Ts) ->
	case TimRef of
		undefined ->
			Now = util:get_time(),
			NewTim = erlang:start_timer((Ts-Now)*1000, self(), check),
			S#state{timref=NewTim};

		_ ->
			S
	end.


check_timer_expired(S) ->
	{Expired, S2} = check_expired(S),
	commit_prerevs(Expired),
	S2.


check_expired(#state{known=Known}=S) ->
	Now = util:get_time(),
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
		_  -> dict:filter(fun(_, {_Rev, Ts, _Open}) -> Ts =< Now end, Known)
	end,
	NewTimRef = case Timeout of
		never -> undefined;
		_     -> erlang:start_timer((Timeout-Now)*1000, self(), check)
	end,
	{Expired, S#state{timref=NewTimRef, known=NewKnown}}.


commit_prerevs(Expired) ->
	lists:foreach(
		fun({Store, Doc, Rev}) -> commit_prerev(Store, Doc, Rev) end,
		Expired).


commit_prerev(Store, Doc, Rev) ->
	case store:resume(Store, Doc, Rev, keep) of
		{ok, Handle} ->
			commit_prerev_loop(Store, Doc, Handle);

		{error, enoent} ->
			% has been deleted in between
			ok;

		{error, Reason} ->
			error_logger:warning_msg("FUSE: Could not resume: ~w~n", [Reason])
	end.


commit_prerev_loop(Store, Doc, Handle) ->
	case store:commit(Handle, util:get_time()) of
		{ok, _Rev} ->
			vol_monitor:trigger_mod_doc(local, Doc),
			ok;

		conflict ->
			case store:lookup(Store, Doc) of
				{ok, CurRev, _PreRevs} ->
					store:set_parents(Handle, [CurRev]),
					commit_prerev_loop(Store, Doc, Handle);
				error ->
					% doesn't exist anymore
					store:abort(Handle)
			end;

		{error, Reason} ->
			error_logger:warning_msg("FUSE: Could not commit: ~w~n", [Reason])
	end.

