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

-module(peerdrive_file_store).
-behaviour(gen_server).

-export([start_link/3, stop/1, fsck/1, gc/1]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

% Functions used by helper processes (io/forwarder/importer)
-export([commit/6, doc_unlock/2, forward_commit/4, part_get/2, part_lock/2,
	part_put/3, part_unlock/2, put_doc_commit/3, put_rev_commit/5,
	remember_commit/4, rev_unlock/2, suspend/6, tmp_name/1, rev_lock/2]).

-include("store.hrl").
-include_lib("kernel/include/file.hrl").

-record(state, {
	path,
	sid,
	noverify,
	gen,
	wb_tmr,
	gc_gen,
	doc_tbl,  % dets: {Doc::DId, Rev::RId, [PreRev::RId], Generation}
	rev_tbl,  % dets: {Rev::RId, #rev{}, [DocLinks::DId], [RevLinks::RId]}
	part_tbl, % dets: {Part::PId, Content::binary() | Size::int()}
	peer_tbl, % dets: {Store::Sid, Generation}
	objlocks, % dict: {doc, DId} | {part, PId} -> Count::int()
	synclocks % dict: SId --> pid()
}).

-define(WB_DIRTY_TIME, 60*1000). % delay until write back (reset on new activity)
-define(GC_MIN_UPDATES, 1000).   % gc will be scheduled after this number of updates
-define(GC_MAX_UPDATES, 10000).  % gc is forced after this number of updates

-define(THRESHOLD, 1024).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Server state management...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Path, Options, _Credentials) ->
	gen_server:start_link(?MODULE, {Path, Options}, []).


init({Path, Options}) ->
	case filelib:is_dir(Path) of
		true ->
			try
				S = #state{
					path      = filename:absname(Path),
					synclocks = dict:new(),
					objlocks  = dict:new(),
					noverify  = proplists:get_bool(<<"noverify">>, Options)
				},
				S2 = load_store(S),
				S3 = check_root_doc(S2),
				process_flag(trap_exit, true),
				{ok, S3}
			catch
				throw:Reason ->
					{stop, Reason}
			end;

		false ->
			{stop, enoent}
	end.

stop(Store) ->
	gen_server:cast(Store, stop).

fsck(Store) ->
	gen_server:cast(Store, fsck).

gc(Store) ->
	gen_server:call(Store, gc, infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions used by helper processes...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

tmp_name(Store) ->
	call_store(Store, tmp_name).

doc_unlock(Store, DId) ->
	gen_server:cast(Store, {unlock, {doc, DId}}).

rev_lock(Store, RId) ->
	call_store(Store, {lock, {rev, RId}}).

rev_unlock(Store, RId) ->
	gen_server:cast(Store, {unlock, {rev, RId}}).

part_lock(Store, PId) ->
	call_store(Store, {lock, {part, PId}}).

part_unlock(Store, PId) ->
	gen_server:cast(Store, {unlock, {part, PId}}).

part_put(Store, PId, Content) ->
	call_store(Store, {part_put, PId, Content}).

part_get(Store, PId) ->
	call_store(Store, {part_get, PId}).

commit(Store, DId, PreRId, Rev, DocLinks, RevLinks) ->
	call_store(Store, {commit, DId, PreRId, Rev, DocLinks, RevLinks}).

suspend(Store, DId, PreRId, Rev, DocLinks, RevLinks) ->
	call_store(Store, {suspend, DId, PreRId, Rev, DocLinks, RevLinks}).

forward_commit(Store, Doc, RevPath, OldPreRId) ->
	call_store(Store, {forward_commit, Doc, RevPath, OldPreRId}).

put_doc_commit(Store, DId, RId) ->
	call_store(Store, {put_doc_commit, DId, RId}).

put_rev_commit(Store, RId, Rev, DocLinks, RevLinks) ->
	call_store(Store, {put_rev_commit, RId, Rev, DocLinks, RevLinks}).

remember_commit(Store, DId, NewPreRId, OldPreRId) ->
	call_store(Store, {remember_rev_commit, DId, NewPreRId, OldPreRId}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Gen_server callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_call(guid, _From, S) ->
	{reply, S#state.sid, S};

handle_call(statfs, _From, S) ->
	{reply, do_statfs(S), S};

handle_call({lookup, DId}, _From, S) ->
	case dets:lookup(S#state.doc_tbl, DId) of
		[{_DId, RId, PreRIds, _Gen}] ->
			{reply, {ok, RId, PreRIds}, S};
		[] ->
			{reply, {error, enoent}, S}
	end;

handle_call({contains, RId}, _From, S) ->
	Reply = dets:member(S#state.rev_tbl, RId),
	{reply, Reply, S};

handle_call({stat, Rev}, _From, S) ->
	Reply = do_stat(Rev, S),
	{reply, Reply, S};

handle_call({get_links, Rev}, _From, S) ->
	Reply = do_get_links(Rev, S),
	{reply, Reply, S};

handle_call({peek, RId}, From, S) ->
	{User, _} = From,
	{Reply, S2} = do_peek(RId, User, S),
	{reply, Reply, S2};

handle_call({create, Type, Creator}, From, S) ->
	{User, _} = From,
	{Reply, S2} = do_create(Type, Creator, User, S),
	{reply, Reply, S2};

handle_call({fork, StartRId, Creator}, From, S) ->
	{User, _} = From,
	{Reply, S2} = do_fork(StartRId, Creator, User, S),
	{reply, Reply, S2};

handle_call({update, DId, StartRId, Creator}, From, S) ->
	{User, _} = From,
	{Reply, S2} = do_update(DId, StartRId, Creator, User, S),
	{reply, Reply, S2};

handle_call({resume, DId, PreRId, Creator}, From, S) ->
	{User, _} = From,
	{Reply, S2} = do_resume(DId, PreRId, Creator, User, S),
	{reply, Reply, S2};

handle_call({forget, DId, PreRId}, _From, S) ->
	{Reply, S2} = do_forget(DId, PreRId, S),
	{reply, Reply, S2};

handle_call({delete_rev, RId}, _From, S) ->
	{Reply, S2} = do_delete_rev(RId, S),
	{reply, Reply, S2};

handle_call({delete_doc, DId, RId}, _From, S) ->
	{Reply, S2} = do_delete_doc(DId, RId, S),
	{reply, Reply, S2};

handle_call({put_doc, DId, RId}, {User, _}, S) ->
	{Reply, S2} = do_put_doc(DId, RId, User, S),
	{reply, Reply, S2};

handle_call({forward_doc, DId, RevPath, OldPreRev}, {User, _}, S) ->
	{Reply, S2} = do_forward_doc(DId, RevPath, OldPreRev, User, S),
	{reply, Reply, S2};

handle_call({put_rev, RId, Rev, Data, DocLinks, RevLinks}, {User, _}, S) ->
	{Reply, S2} = do_put_rev(RId, Rev, Data, DocLinks, RevLinks, User, S),
	{reply, Reply, S2};

handle_call({remember_rev, DId, PreRId, OldPreRId}, {User, _}, S) ->
	{Reply, S2} = do_remember_rev(DId, PreRId, OldPreRId, User, S),
	{reply, Reply, S2};

handle_call({sync_get_changes, PeerSId, Anchor}, {Caller, _}, S) ->
	{Reply, S2} = do_sync_get_changes(PeerSId, Anchor, Caller, S),
	{reply, Reply, S2};

handle_call({sync_get_anchor, FromSId, ToSId}, _From, S) ->
	{reply, do_sync_get_anchor(FromSId, ToSId, S), S};

handle_call({sync_set_anchor, FromSId, ToSId, SeqNum}, _From, S) ->
	do_sync_set_anchor(FromSId, ToSId, SeqNum, S),
	{reply, ok, S};

handle_call({sync_finish, PeerSId}, {Caller, _}, S) ->
	{Reply, S2} = do_sync_finish(PeerSId, Caller, S),
	{reply, Reply, S2};

handle_call(sync, _From, S) ->
	save_store(dirty, S),
	{reply, ok, S};

handle_call(gc, _From, S) ->
	do_gc(S),
	{reply, ok, S};

% internal: lock object
handle_call({lock, ObjId}, _From, S) ->
	S2 = do_lock(ObjId, S),
	{reply, ok, S2};

% internal: generate tmp filename
handle_call(tmp_name, _From, S) ->
	{reply, peerdrive_util:gen_tmp_name(S#state.path), S};

% internal: commit a new revision
handle_call({commit, DId, PreRId, Rev, DocLinks, RevLinks}, _From, S) ->
	{Reply, S2} = do_commit(DId, PreRId, Rev, DocLinks, RevLinks, S),
	{reply, Reply, S2};

% internal: queue a preliminary revision
handle_call({suspend, DId, PreRId, Rev, DocLinks, RevLinks}, _From, S) ->
	{Reply, S2} = do_suspend(DId, PreRId, Rev, DocLinks, RevLinks, S),
	{reply, Reply, S2};

% internal: ok | {error, Reason}
handle_call({forward_commit, DId, RevPath, OldPreRId}, _From, S) ->
	{Reply, S2} = do_forward_doc_commit(DId, RevPath, OldPreRId, S),
	{reply, Reply, S2};

% internal: ok | {error, Reason}
handle_call({put_doc_commit, DId, RId}, _From, S) ->
	{Reply, S2} = do_put_doc_commit(DId, RId, S),
	{reply, Reply, S2};

% internal: ok | {error, Reason}
handle_call({put_rev_commit, RId, Rev, DocLinks, RevLinks}, _From, S) ->
	{Reply, S2} = do_put_rev_commit(RId, Rev, DocLinks, RevLinks, S),
	{reply, Reply, S2};

% internal: put part into database
handle_call({part_put, PId, Content}, _From, S) ->
	Reply = do_part_put(PId, Content, S),
	{reply, Reply, S};

% internal: get part
handle_call({part_get, PId}, _From, S) ->
	Reply = do_part_get(PId, S),
	{reply, Reply, S};

handle_call({remember_rev_commit, DId, NewPreRId, OldPreRId}, _From, S) ->
	{Reply, S2} = do_remember_rev_commit(DId, NewPreRId, OldPreRId, S),
	{reply, Reply, S2}.


% internal: unlock a part
handle_cast({unlock, ObjId}, S) ->
	S2 = do_unlock(ObjId, S),
	{noreply, S2};

handle_cast(fsck, S) ->
	do_fsck(S),
	{noreply, S};

handle_cast(stop, S) ->
	{stop, normal, S}.


handle_info({'EXIT', From, Reason}, S) ->
	case sync_trap_exit(From, S) of
		{ok, S2} ->
			% a sync process went away
			{noreply, S2};

		error ->
			% must be an associated worker process
			case Reason of
				normal   -> {noreply, S};
				shutdown -> {noreply, S};
				_ ->        {stop, {eunexpected, Reason}, S}
			end
	end;

handle_info({timeout, _, dirty}, #state{gen=Gen, gc_gen=GcGen} = S) ->
	S2 = if
		Gen - GcGen > ?GC_MIN_UPDATES ->
			do_gc(S), % implies a write back
			S#state{wb_tmr=undefined, gc_gen=Gen};

		true ->
			save_store(dirty, S),
			S#state{wb_tmr=undefined}
	end,
	{noreply, S2}.


terminate(_Reason, S) ->
	save_store(clean, S),
	close_store(S).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

code_change(_, State, _) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Service implementations...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_statfs(_S) ->
	% TODO: report real values
	{ok, #fs_stat{
		bsize  = 512,
		blocks = 2048000,
		bfree  = 2048000,
		bavail = 2048000
	}}.


do_stat(RId, #state{rev_tbl=RevTbl}) ->
	case dets:lookup(RevTbl, RId) of
		[{_, Rev, _DocLinks, _RevLinks}] ->
			{ok, Rev};

		[] ->
			{error, enoent}
	end.


do_get_links(RId, #state{rev_tbl=RevTbl}) ->
	case dets:lookup(RevTbl, RId) of
		[{_, _Rev, DocLinks, RevLinks}] ->
			{ok, {DocLinks, RevLinks}};

		[] ->
			{error, enoent}
	end.


do_peek(RId, User, #state{rev_tbl=RevTbl} = S) ->
	case dets:lookup(RevTbl, RId) of
		[{_, Rev, _DocLinks, _RevLinks}] ->
			S1 = do_lock({part, (Rev#rev.data)#rev_dat.hash}, S),
			S2 = lists:foldl(
				fun(#rev_att{hash=PId}, AccS) -> do_lock({part, PId}, AccS) end,
				S1,
				Rev#rev.attachments),
			S3 = lists:foldl(
				fun(ParentRId, AccS) -> do_lock({rev, ParentRId}, AccS) end,
				S2,
				Rev#rev.parents),
			{ok, _} = Reply = peerdrive_file_store_io:start_link(Rev, User),
			{Reply, S3};

		[] ->
			{{error, enoent}, S}
	end.


do_create(Type, Creator, User, S) ->
	DId = crypto:rand_bytes(16),
	Rev = #rev{data=?REV_DATA_EMPTY, type=Type, creator=Creator,
		crtime=peerdrive_util:get_time()},
	ok = dets:insert(S#state.part_tbl, {?REV_DATA_EMPTY_HASH, ?REV_DATA_EMPTY_BIN}),
	case start_writer(DId, undefined, Rev, User, S) of
		{{ok, Handle}, S2} ->
			{{ok, DId, Handle}, S2};
		Error ->
			Error
	end.


do_fork(StartRId, Creator, User, #state{rev_tbl=RevTbl}=S) ->
	DId = crypto:rand_bytes(16),
	case dets:lookup(RevTbl, StartRId) of
		[{_, Rev, _DocLinks, _RevLinks}] ->
			NewRev = Rev#rev{
				parents = [StartRId],
				creator = Creator,
				comment = <<>>
			},
			case start_writer(DId, undefined, NewRev, User, S) of
				{{ok, Handle}, S2} ->
					{{ok, DId, Handle}, S2};
				Error ->
					Error
			end;

		[] ->
			{{error, enoent}, S}
	end.


do_update(DId, StartRId, Creator, User, S) ->
	#state{doc_tbl=DocTbl, rev_tbl=RevTbl} = S,
	case dets:member(DocTbl, DId) of
		true ->
			case dets:lookup(RevTbl, StartRId) of
				[{_, Rev, _DocLinks, _RevLinks}] ->
					NewCreator = case Creator of
						undefined -> Rev#rev.creator;
						_ -> Creator
					end,
					NewRev = Rev#rev{
						parents = [StartRId],
						creator = NewCreator,
						comment = <<>>
					},
					start_writer(DId, undefined, NewRev, User, S);

				[] ->
					{{error, enoent}, S}
			end;

		false ->
			{{error, enoent}, S}
	end.


do_resume(DId, PreRId, Creator, User, S) ->
	#state{doc_tbl=DocTbl, rev_tbl=RevTbl} = S,
	case dets:lookup(DocTbl, DId) of
		[{_, _, PreRevs, _}] ->
			case lists:member(PreRId, PreRevs) of
				true ->
					case dets:lookup(RevTbl, PreRId) of
						[{_, Rev, _DocLinks, _RevLinks}] ->
							NewCreator = case Creator of
								undefined -> Rev#rev.creator;
								_ -> Creator
							end,
							NewRev = Rev#rev{creator = NewCreator},
							start_writer(DId, PreRId, NewRev, User, S);

						[] ->
							{{error, enoent}, S}
					end;

				false ->
					{{error, econflict}, S}
			end;

		[] ->
			{{error, enoent}, S}
	end.


do_forget(DId, PreRId, #state{sid=SId, doc_tbl=DocTbl, gen=Gen} = S) ->
	case dets:lookup(DocTbl, DId) of
		[{_, RId, CurPreRIds, _Gen}] ->
			case lists:member(PreRId, CurPreRIds) of
				true ->
					NewPreRIds = [ R || R <- CurPreRIds, R =/= PreRId ],
					peerdrive_vol_monitor:trigger_mod_doc(SId, DId),
					ok = dets:insert(DocTbl, {DId, RId, NewPreRIds, Gen}),
					{ok, next_gen(S)};

				false ->
					{{error, econflict}, S}
			end;

		[] ->
			{{error, enoent}, S}
	end.


do_delete_rev(RId, #state{sid=SId, rev_tbl=RevTbl} = S) ->
	case dets:member(RevTbl, RId) of
		true ->
			peerdrive_vol_monitor:trigger_rm_rev(SId, RId),
			ok = dets:delete(RevTbl, RId),
			{ok, next_gen(S)};

		false ->
			{{error, enoent}, S}
	end.


do_delete_doc(DId, RId, #state{doc_tbl=DocTbl, sid=SId} = S) ->
	case DId of
		SId ->
			{{error, eacces}, S};

		_ ->
			case dets:lookup(DocTbl, DId) of
				[{_, RId, _PreRevs, _Gen}] ->
					peerdrive_vol_monitor:trigger_rm_doc(SId, DId),
					ok = dets:delete(DocTbl, DId),
					{ok, next_gen(S)};

				[{_, _OtherRev, _PreRevs, _Gen}] ->
					{{error, econflict}, S};

				[] ->
					{{error, enoent}, S}
			end
	end.


do_put_doc(DId, RId, User, #state{doc_tbl=DocTbl} = S) ->
	case dets:lookup(DocTbl, DId) of
		% pointing to other rev?
		[{_, CurRId, _, _}] when CurRId =/= RId ->
			{{error, econflict}, S};

		_ ->
			S2 = do_lock({rev, RId}, do_lock({doc, DId}, S)),
			{ok, Handle} = peerdrive_file_store_put:start_link(DId,
				RId, User),
			{{ok, Handle}, S2}
	end.


do_put_doc_commit(DId, RId, #state{doc_tbl=DocTbl, gen=Gen} = S) ->
	case dets:lookup(DocTbl, DId) of
		% document does not exist (yet)...
		[] ->
			peerdrive_vol_monitor:trigger_add_doc(S#state.sid, DId),
			ok = dets:insert(DocTbl, {DId, RId, [], Gen}),
			{{ok, RId}, next_gen(S)};

		% already pointing to requested rev
		[{_, RId, _, _}] ->
			{{ok, RId}, S};

		% completely other rev
		[_] ->
			{{error, econflict}, S}
	end.


do_commit(DId, OldPreRId, Rev, DocLinks, RevLinks, #state{doc_tbl=DocTbl, gen=Gen} = S) ->
	RId = peerdrive_store:hash_revision(Rev),
	case dets:lookup(DocTbl, DId) of
		[{_, CurrentRId, CurrentPreRIds, _Gen}] ->
			case lists:member(CurrentRId, Rev#rev.parents) of
				true  ->
					NewPreRIds = [R || R <- CurrentPreRIds, R =/= OldPreRId],
					ok = dets:insert(S#state.rev_tbl, {RId, Rev, DocLinks, RevLinks}),
					ok = dets:insert(DocTbl, {DId, RId, NewPreRIds, Gen}),
					peerdrive_vol_monitor:trigger_mod_doc(S#state.sid, DId),
					{{ok, RId}, next_gen(S)};

				false ->
					{{error, econflict}, S}
			end;

		[] ->
			ok = dets:insert(S#state.rev_tbl, {RId, Rev, DocLinks, RevLinks}),
			ok = dets:insert(DocTbl, {DId, RId, [], Gen}),
			peerdrive_vol_monitor:trigger_add_doc(S#state.sid, DId),
			{{ok, RId}, next_gen(S)}
	end.


do_suspend(DId, OldPreRId, Rev, DocLinks, RevLinks, #state{doc_tbl=DocTbl, gen=Gen} = S) ->
	RId = peerdrive_store:hash_revision(Rev),
	case dets:lookup(DocTbl, DId) of
		[{_, CurrentRId, CurrentPreRIds, _Gen}] ->
			NewPreRIds = lists:usort(
				[RId] ++ [R || R <- CurrentPreRIds, R =/= OldPreRId]
			),
			ok = dets:insert(S#state.rev_tbl, {RId, Rev, DocLinks, RevLinks}),
			ok = dets:insert(DocTbl, {DId, CurrentRId, NewPreRIds, Gen}),
			peerdrive_vol_monitor:trigger_mod_doc(S#state.sid, DId),
			{{ok, RId}, next_gen(S)};

		[] ->
			{{error, enoent}, S}
	end.


do_part_put(PId, Data, #state{part_tbl=PartTbl}) when is_binary(Data) ->
	ok = dets:insert(PartTbl, {PId, Data}),
	ok;

do_part_put(PId, TmpName, #state{part_tbl=PartTbl} = S) ->
	case file:read_file_info(TmpName) of
		{ok, #file_info{size=Size}} ->
			NewName = peerdrive_util:build_path(S#state.path, PId),
			Placed = filelib:ensure_dir(NewName) == ok andalso
				file:rename(TmpName, NewName) == ok,
			case Placed of
				true ->
					ok = dets:insert(PartTbl, {PId, Size}),
					ok;
				false ->
					{error, eio}
			end;

		{error, _} = Error ->
			peerdrive_util:fixup_file(Error)
	end.


do_part_get(PId, #state{part_tbl=PartTbl} = S) ->
	case dets:lookup(PartTbl, PId) of
		[{_, Data}] when is_binary(Data) ->
			{ok, Data};
		[{_, Size}] when is_integer(Size) ->
			{ok, peerdrive_util:build_path(S#state.path, PId)};
		[] ->
			{error, enoent}
	end.


% ok | {ok, MissingRevs, Handle} | {error, Reason}
do_forward_doc(DId, RevPath, OldPreRId, User, S) when length(RevPath) >= 2 ->
	StartRId = hd(RevPath),
	case dets:lookup(S#state.doc_tbl, DId) of
		[{_, StartRId, _, _}] ->
			RevTbl = S#state.rev_tbl,
			case [RId || RId <- RevPath, not dets:member(RevTbl, RId)] of
				[] ->
					case do_forward_doc_commit(DId, RevPath, OldPreRId, S) of
						{{ok, _}, S2} -> {ok, S2};
						Error -> Error
					end;

				Missing ->
					% Some Revs are missing and need to be uploaded. Make sure
					% nothing gets garbage collected in between...
					S2 = lists:foldl(
						fun(RId, AccS) -> do_lock({rev, RId}, AccS) end,
						S,
						RevPath),
					{ok, Handle} = peerdrive_file_store_fwd:start_link(DId,
						RevPath, OldPreRId, User),
					{{ok, lists:reverse(Missing), Handle}, S2}
			end;

		[_] ->
			{{error, econflict}, S};
		[] ->
			{{error, enoent}, S}
	end;

do_forward_doc(DId, [RId], OldPreRId, _User, #state{doc_tbl=DocTbl} = S) ->
	case dets:lookup(DocTbl, DId) of
		[{_, RId, PreRIds, _}] ->
			case lists:member(OldPreRId, PreRIds) of
				false ->
					{ok, S};
				true ->
					NewPreRIds = lists:delete(OldPreRId, PreRIds),
					ok = dets:insert(DocTbl, {DId, RId, NewPreRIds, S#state.gen}),
					peerdrive_vol_monitor:trigger_mod_doc(S#state.sid, DId),
					{ok, next_gen(S)}
			end;
		[_] ->
			{{error, econflict}, S};
		[] ->
			{{error, enoent}, S}
	end;

do_forward_doc(_DId, _RevPath, _OldPreRId, _User, S) ->
	{{error, einval}, S}.


do_forward_doc_commit(DId, RevPath, OldPreRId, S) ->
	#state{doc_tbl=DocTbl, rev_tbl=RevTbl} = S,
	try
		% check if all revisions are known
		lists:foreach(
			fun(RId) -> dets:member(RevTbl, RId) orelse throw(enoent) end,
			RevPath),

		% check if the revisions are all connected to each other
		lists:foreach(
			fun({RId1, RId2}) ->
				[{_, #rev{parents=Parents}, _, _}] = dets:lookup(RevTbl, RId2),
				lists:member(RId1, Parents) orelse throw(einval)
			end,
			zip_parent_child(RevPath)),

		% try to update
		[OldRId | Path] = RevPath,
		NewRId = lists:last(Path),
		case dets:lookup(DocTbl, DId) of
			% already pointing to requested rev
			[{_, NewRId, PreRIds, _}] ->
				case lists:member(OldPreRId, PreRIds) of
					false ->
						{{ok, NewRId}, S};
					true ->
						NewPreRIds = lists:delete(OldPreRId, PreRIds),
						ok = dets:insert(DocTbl, {DId, NewRId, NewPreRIds, S#state.gen}),
						peerdrive_vol_monitor:trigger_mod_doc(S#state.sid, DId),
						{{ok, NewRId}, next_gen(S)}
				end;

			% forward old version
			[{_, OldRId, PreRIds, _}] ->
				NewPreRIds = lists:delete(OldPreRId, PreRIds),
				ok = dets:insert(DocTbl, {DId, NewRId, NewPreRIds, S#state.gen}),
				peerdrive_vol_monitor:trigger_mod_doc(S#state.sid, DId),
				{{ok, NewRId}, next_gen(S)};

			% errors
			[_] -> throw(econflict);
			[]  -> throw(enoent)
		end
	catch
		throw:Reason -> {{error, Reason}, S}
	end.


% {ok, MissingAttachments, Handle} | {error, Reason}
do_put_rev(RId, Rev, Data, DocLinks, RevLinks, User, S) ->
	NoVerify = S#state.noverify,
	case NoVerify orelse RId == peerdrive_store:hash_revision(Rev) of
		true ->
			DataPId = (Rev#rev.data)#rev_dat.hash,
			case do_put_rev_data(DataPId, Data, NoVerify, S) of
				ok ->
					Attachments = Rev#rev.attachments,
					PartTbl = S#state.part_tbl,
					Missing = [{N, PId} || #rev_att{name=N, hash=PId} <- Attachments,
						not dets:member(PartTbl, PId)],
					% Make sure nothing gets garbage collected in between...
					S1 = do_lock({part, DataPId}, S),
					S2 = lists:foldl(
						fun(#rev_att{hash=PId}, AccS) -> do_lock({part, PId}, AccS) end,
						do_lock({rev, RId}, S1),
						Attachments),
					{ok, Handle} = peerdrive_file_store_imp:start_link(RId,
						Rev, Missing, User, NoVerify, DocLinks, RevLinks),
					NeededAttachments = [Att || {Att, _} <- Missing],
					{{ok, NeededAttachments, Handle}, S2};
				Error ->
					{Error, S}
			end;

		false ->
			{{error, erefused}, S}
	end.


do_put_rev_data(PId, Data, NoVerify, #state{part_tbl=PartTbl} = S) ->
	case dets:member(PartTbl, PId) of
		true ->
			ok;
		false ->
			case NoVerify orelse PId == peerdrive_crypto:merkle(Data) of
				true  -> write_part(PId, Data, S);
				false -> {error, einval}
			end
	end.


do_put_rev_commit(RId, Rev, DocLinks, RevLinks, #state{sid=SId, rev_tbl=RevTbl} = S) ->
	ok = dets:insert(RevTbl, {RId, Rev, DocLinks, RevLinks}),
	peerdrive_vol_monitor:trigger_add_rev(SId, RId),
	{{ok, RId}, S}.


do_remember_rev(DId, NewPreRId, OldPreRId, User, S) ->
	#state{doc_tbl=DocTbl, rev_tbl=RevTbl} = S,
	case dets:member(DocTbl, DId) of
		true ->
			case dets:member(RevTbl, NewPreRId) of
				true ->
					case do_remember_rev_commit(DId, NewPreRId, OldPreRId, S) of
						{{ok, _}, S2} -> {ok, S2};
						Error -> Error
					end;

				false ->
					S2 = do_lock({rev, NewPreRId}, S),
					Ok = {ok, _} = peerdrive_file_store_rem:start_link(DId,
						NewPreRId, OldPreRId, User),
					{Ok, S2}
			end;

		false ->
			{{error, enoent}, S}
	end.


do_remember_rev_commit(DId, NewPreRId, OldPreRId, #state{doc_tbl=DocTbl} = S) ->
	case dets:lookup(DocTbl, DId) of
		[{_, RId, PreRIds, _Gen}] ->
			NewPreRIds = lists:usort(
				[NewPreRId] ++ [R || R <- PreRIds, R =/= OldPreRId]
			),
			ok = dets:insert(DocTbl, {DId, RId, NewPreRIds, S#state.gen}),
			peerdrive_vol_monitor:trigger_mod_doc(S#state.sid, DId),
			{{ok, NewPreRId}, next_gen(S)};

		[] ->
			{{error, enoent}, S}
	end.


do_sync_get_changes(PeerSId, Anchor, Caller, S) ->
	case sync_lock(PeerSId, Caller, S) of
		{ok, S2} ->
			#state{doc_tbl=DocTbl} = S2,
			Changes = dets:select(DocTbl,
				[{{'$1','_','_','$2'},[{'>','$2',Anchor}],[{{'$1','$2'}}]}]),
			Backlog = lists:sort(
				fun({_Doc1, Seq1}, {_Doc2, Seq2}) -> Seq1 =< Seq2 end,
				Changes),
			{{ok, lists:sublist(Backlog, 1024)}, S2};

		error ->
			{{error, ebusy}, S}
	end.


do_sync_get_anchor(FromSId, ToSId, #state{peer_tbl=PeerTbl}) ->
	case dets:lookup(PeerTbl, {FromSId, ToSId}) of
		[{_, Value}] -> {ok, Value};
		[] -> {error, enoent}
	end.


do_sync_set_anchor(FromSId, ToSId, SeqNum, #state{peer_tbl=PeerTbl}) ->
	ok = dets:insert(PeerTbl, {{FromSId, ToSId}, SeqNum}).


do_sync_finish(PeerSId, Caller, #state{synclocks=SLocks} = S) ->
	case dict:find(PeerSId, SLocks) of
		{ok, Caller} ->
			unlink(Caller),
			{ok, S#state{synclocks=dict:erase(PeerSId, SLocks)}};
		{ok, _Other} ->
			{{error, eacces}, S};
		error ->
			{{error, einval}, S}
	end.


do_lock(ObjId, #state{objlocks=ObjLocks} = S) ->
	S#state{objlocks = dict:update_counter(ObjId, 1, ObjLocks)}.


do_unlock(ObjId, #state{objlocks=ObjLocks} = S) ->
	NewObjLocks = case dict:fetch(ObjId, ObjLocks) of
		1     -> dict:erase(ObjId, ObjLocks);
		Depth -> dict:store(ObjId, Depth-1, ObjLocks)
	end,
	S#state{objlocks=NewObjLocks}.


do_gc(S) ->
	save_store(dirty, S),
	{StartMS, StartS, StartUS} = now(),
	GcObj1 = dets:foldl(
		fun({DId, RId, PreRIds, _}, Acc) ->
			dict:store({doc, DId}, [{rev, R} || R <- [RId | PreRIds]], Acc)
		end,
		dict:new(),
		S#state.doc_tbl),
	GcObj2 = dets:foldl(
		fun({RId, Rev, DocLinks, RevLinks}, Acc) ->
			Parents = case (Rev#rev.flags band ?REV_FLAG_EPHEMERAL) == 0 of
				true -> Rev#rev.parents;
				false -> []
			end,
			DRefs = [{doc, D} || D <- DocLinks],
			RRefs = [{rev, R} || R <- Parents ++ RevLinks],
			PRefs = [
				{part, (Rev#rev.data)#rev_dat.hash}
				| [{part, P} || #rev_att{hash=P} <- Rev#rev.attachments]
			],
			dict:store({rev, RId}, DRefs++RRefs++PRefs, Acc)
		end,
		GcObj1,
		S#state.rev_tbl),
	AllObj = dets:foldl(
		fun({PId, _}, Acc) ->
			dict:store({part, PId}, [], Acc)
		end,
		GcObj2,
		S#state.part_tbl),
	GreyList = [{doc, S#state.sid} | dict:fetch_keys(S#state.objlocks)],
	DelObj = dict:fetch_keys(gc_step(GreyList, AllObj)),
	{GcDocs, GcRevs, GcParts} = lists:foldl(
		fun
			({doc, _}, {AccD, AccR, AccP}) -> {AccD+1, AccR, AccP};
			({rev, _}, {AccD, AccR, AccP}) -> {AccD, AccR+1, AccP};
			({part, _}, {AccD, AccR, AccP}) -> {AccD, AccR, AccP+1}
		end,
		{0, 0, 0},
		DelObj),
	gc_cleanup(DelObj, S),
	{EndMS, EndS, EndUS} = now(),
	Duration = (EndMS-StartMS) * 1000000 + (EndS-StartS) +
		(EndUS-StartUS) / 1000000,
	error_logger:info_report([
		{'store', peerdrive_util:bin_to_hexstr(S#state.sid)},
		{'type', 'peerdrive_file_store'},
		{'gc_docs', GcDocs},
		{'gc_revs', GcRevs},
		{'gc_parts', GcParts},
		{'duration', Duration} ]).


do_fsck(_S) ->
	% TODO: implement. But what to do with broken objects? Deleting might
	% be a bad idea.
	%
	% Steps:
	%   * do all revs have their parts?
	%   * do all parts files exist
	%   * are the file hashes correct?
	ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local helpers...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

sync_trap_exit(From, #state{synclocks=SLocks} = S) ->
	Found = dict:fold(
		fun(_Guid, Pid, Acc) -> Acc or (Pid == From) end,
		false,
		SLocks),
	case Found of
		true ->
			NewLocks = dict:filter(fun(_, Pid) -> Pid =/= From end, SLocks),
			{ok, S#state{synclocks=NewLocks}};
		false ->
			error
	end.


sync_lock(PeerSId, Caller, #state{synclocks=SLocks} = S) ->
	case dict:find(PeerSId, SLocks) of
		{ok, Caller} ->
			{ok, S};
		{ok, _Other} ->
			error;
		error ->
			link(Caller),
			{ok, S#state{synclocks=dict:store(PeerSId, Caller, SLocks)}}
	end.


% lock document and part hashes, then start writer process
start_writer(DId, PreRId, Rev, User, S) ->
	S1 = do_lock({part, (Rev#rev.data)#rev_dat.hash}, S),
	S2 = lists:foldl(
		fun(#rev_att{hash=PId}, AccS) -> do_lock({part, PId}, AccS) end,
		S1,
		Rev#rev.attachments),
	S3 = lists:foldl(
		fun(RId, AccS) -> do_lock({rev, RId}, AccS) end,
		S2,
		Rev#rev.parents),
	S4 = do_lock({doc, DId}, S3),
	{ok, _} = Reply =
		peerdrive_file_store_io:start_link(DId, PreRId, Rev, User),
	{Reply, S4}.


load_store(#state{path=Path} = S) ->
	S2 = case file:consult(Path ++ "/info") of
		{ok, Info} ->
			{sid, Sid} = lists:keyfind(sid, 1, Info),
			{gen, Gen} = lists:keyfind(gen, 1, Info),
			case lists:keyfind(version, 1, Info) of
				{version, 4} ->
					ok;
				_ ->
					throw(enodev)
			end,
			S#state{gen=Gen, gc_gen=Gen, sid=Sid};

		{error, enoent} ->
			S#state{gen=0, gc_gen=0, sid=crypto:rand_bytes(16)};

		{error, _} ->
			throw(enodev)
	end,
	DocTbl = make_ref(),
	RevTbl = make_ref(),
	PartTbl = make_ref(),
	PeerTbl = make_ref(),
	{ok, _} = check(dets:open_file(DocTbl, [{file, Path ++ "/docs.dets"}])),
	{ok, _} = check(dets:open_file(RevTbl, [{file, Path ++ "/revs.dets"}])),
	{ok, _} = check(dets:open_file(PartTbl, [{file, Path ++ "/parts.dets"}])),
	{ok, _} = check(dets:open_file(PeerTbl, [{file, Path ++ "/peers.dets"}])),
	S2#state{doc_tbl=DocTbl, rev_tbl=RevTbl, part_tbl=PartTbl, peer_tbl=PeerTbl}.


save_store(MountState, #state{path=Path} = S) ->
	ok = dets:sync(S#state.part_tbl),
	ok = dets:sync(S#state.doc_tbl),
	ok = dets:sync(S#state.rev_tbl),
	ok = dets:sync(S#state.peer_tbl),
	{ok, File} = file:open(Path ++ "/info.new", [write]),
	try
		ok = file:write(File, "{version, 4}.\n"),
		ok = file:write(File, io_lib:print({state, MountState})),
		ok = file:write(File, ".\n"),
		ok = file:write(File, io_lib:print({sid, S#state.sid})),
		ok = file:write(File, ".\n"),
		ok = file:write(File, io_lib:print({gen, S#state.gen})),
		ok = file:write(File, ".\n"),
		ok = file:sync(File)
	after
		file:close(File)
	end,
	ok = file:rename(Path ++ "/info.new", Path ++ "/info").


close_store(S) ->
	dets:close(S#state.doc_tbl),
	dets:close(S#state.rev_tbl),
	dets:close(S#state.part_tbl),
	dets:close(S#state.peer_tbl).


check_root_doc(#state{sid=SId, gen=Gen} = S) ->
	case dets:member(S#state.doc_tbl, SId) of
		true ->
			S;
		false ->
			Name = "file_store on '" ++ S#state.path ++ "'",
			Content = [],
			Annotation1 = gb_trees:empty(),
			Annotation2 = gb_trees:enter(<<"title">>, list_to_binary(Name), Annotation1),
			Data1 = gb_trees:empty(),
			Data2 = gb_trees:enter(<<"org.peerdrive.folder">>, Content, Data1),
			Data3 = gb_trees:enter(<<"org.peerdrive.annotation">>, Annotation2, Data2),
			Data = check_root_doc_write(Data3, S),
			RootRev = #rev{
				flags     = ?REV_FLAG_STICKY,
				data      = Data,
				crtime    = peerdrive_util:get_time(),
				mtime     = peerdrive_util:get_time(),
				type      = <<"org.peerdrive.store">>,
				creator   = <<"org.peerdrive.file-store">>,
				comment   = <<"">>
			},
			RId = peerdrive_store:hash_revision(RootRev),
			ok = dets:insert(S#state.rev_tbl, {RId, RootRev, [], []}),
			ok = dets:insert(S#state.doc_tbl, {SId, RId, [], Gen}),
			S#state{gen=Gen+1}
	end.


check_root_doc_write(Data, S) ->
	BinData = peerdrive_struct:encode(Data),
	PId = peerdrive_crypto:merkle(BinData),
	ok = write_part(PId, BinData, S),
	#rev_dat{size=size(BinData), hash=PId}.


write_part(PId, Data, #state{part_tbl=PartTbl, path=Path}) ->
	case size(Data) > ?THRESHOLD of
		true ->
			Name = peerdrive_util:build_path(Path, PId),
			Written = filelib:is_regular(Name) orelse
				(filelib:ensure_dir(Name) == ok andalso file:write_file(Name, Data) == ok),
			case Written of
				true ->
					ok = dets:insert(PartTbl, {PId, size(Data)});
				false ->
					{error, eio}
			end;
		false ->
			ok = dets:insert(PartTbl, {PId, Data})
	end.


zip_parent_child([Head | Childs]) ->
	lists:reverse(zip_parent_child(Head, Childs, [])).


zip_parent_child(Parent, [Child], Acc) ->
	[{Parent, Child} | Acc];

zip_parent_child(Parent, [Child | Ancestors], Acc) ->
	zip_parent_child(Child, Ancestors, [{Parent, Child} | Acc]).


gc_step([], WhiteSet) ->
	WhiteSet;

gc_step([ObjId | GreyList], WhiteSet) ->
	NewWhiteSet = dict:erase(ObjId, WhiteSet),
	NewGreyList = case dict:find(ObjId, WhiteSet) of
		{ok, Refs} -> Refs ++ GreyList;
		error -> GreyList
	end,
	gc_step(NewGreyList, NewWhiteSet).


gc_cleanup(DelObj, S) ->
	lists:foreach(
		fun(ObjId) -> gc_cleanup_obj(ObjId, S) end,
		DelObj).


gc_cleanup_obj({doc, DId}, #state{doc_tbl=DocTbl}) ->
	ok = dets:delete(DocTbl, DId);

gc_cleanup_obj({rev, RId}, #state{rev_tbl=RevTbl}) ->
	ok = dets:delete(RevTbl, RId);

gc_cleanup_obj({part, PId}, #state{path=Path, part_tbl=PartTbl}) ->
	case dets:lookup(PartTbl, PId) of
		[{_, Size}] when is_integer(Size) ->
			file:delete(peerdrive_util:build_path(Path, PId));
		_ ->
			ok
	end,
	dets:delete(PartTbl, PId).


next_gen(#state{wb_tmr=WbTmr, gc_gen=GcGen, gen=Gen} = S) ->
	S2 = if
		Gen-GcGen == ?GC_MAX_UPDATES ->
			case erlang:cancel_timer(WbTmr) of
				Remain when is_integer(Remain) ->
					self() ! {timeout, WbTmr, dirty}; % trigger immediately
				false ->
					ok % already expired, message should be in queue
			end,
			S;

		WbTmr == undefined ->
			Timer = erlang:start_timer(?WB_DIRTY_TIME, self(), dirty),
			S#state{wb_tmr=Timer};

		true ->
			case erlang:cancel_timer(WbTmr) of
				Remain when is_integer(Remain) ->
					Timer = erlang:start_timer(?WB_DIRTY_TIME, self(), dirty),
					S#state{wb_tmr=Timer};
				false ->
					S % already expired, message should be in queue
			end
	end,
	S2#state{gen=Gen+1}.


call_store(Store, Request) ->
	try
		gen_server:call(Store, Request, infinity)
	catch
		exit:_ -> {error, enxio}
	end.


check(Result) ->
	case Result of
		{error, _} ->
			throw(enodev);
		Ok ->
			Ok
	end.

