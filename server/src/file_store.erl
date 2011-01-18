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

-module(file_store).
-behaviour(gen_server).

-export([start_link/2, stop/1, dump/1, fsck/1, gc/1]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

% Functions used by helper processes (reader/writer/...)
-export([commit/4, suspend/4, insert_rev/3, lock/2, unlock/2, unhide/2]).

-include("store.hrl").
-include("file_store.hrl").
-include_lib("kernel/include/file.hrl").

-record(state, {
	path,       % string,  base directory
	guid,       % binary,  GUID of the store
	gen,        % integer, Generation of the store
	uuids,      % dict: Document --> {Revision, PreRevs, Generation}
	revisions,  % dict: Revision --> {refcount, #revision{} | stub}
	parts,      % dict: PartHash --> refcount
	peers,      % dict: GUID --> Generation
	synclocks,  % dict: GUID --> pid()
	locks,      % orddict: PartHash --> LockCount
	hidden=[],  % list, documents exempted from garbage collection
	changed=false
}).

-define(SYNC_INTERVAL, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Server state management...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Id, {Path, Name}) ->
	gen_server:start_link({local, Id}, ?MODULE, {Id, Path, Name}, []).


init({Id, Path, Name}) ->
	case filelib:is_dir(Path) of
		true ->
			Guid      = load_guid(Path),
			Gen       = load_gen(Path),
			Uuids     = load_uuids(Path),
			Revisions = load_revisions(Path),
			Parts     = load_parts(Path),
			Peers     = load_peers(Path),
			State     = #state{
				path      = Path,
				guid      = Guid,
				gen       = Gen,
				uuids     = Uuids,
				revisions = Revisions,
				parts     = Parts,
				peers     = Peers,
				synclocks = dict:new(),
				locks     = orddict:new()
			},
			volman:reg_store(Id, Guid),
			process_flag(trap_exit, true),
			{ok, check_root_doc(State, Name)};
			
		false ->
			{stop, {enoent, Path}}
	end.

stop(Store) ->
	gen_server:cast(Store, stop).

dump(Store) ->
	gen_server:cast(Store, dump).

fsck(Store) ->
	gen_server:cast(Store, fsck).

gc(Store) ->
	gen_server:call(Store, gc).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions used by helper processes...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Commit a new revision into the store and update the Doc to point to the
%%      new revision instead.
%%
%% @spec commit(Store, Doc, PreRev, Revision) -> Result
%%       Store = pid()
%%       Doc, PreRev = guid()
%%       Revision = #revision
%%       Result = {ok, Rev::guid()} | {error, Error::ecode()}
commit(Store, Doc, PreRev, Revision) ->
	gen_server:call(Store, {commit, Doc, PreRev, Revision}).

%% @doc Queue a preliminary revision.
%%
%% @spec suspend(Store, Doc, PreRev, Revision) -> Result
%%       Store = pid()
%%       Doc, PreRev = guid()
%%       Revision = #revision
%%       Result = {ok, Rev::guid()} | {error, Error::ecode()}
suspend(Store, Doc, PreRev, Revision) ->
	gen_server:call(Store, {suspend, Doc, PreRev, Revision}).

%% @doc Import a new revision into the store.
%%
%% @spec insert_rev(Store, Rev, Revision) -> Result
%%       Store = pid()
%%       Rev = guid()
%%       Revision = #revision
%%       Result = ok | {error, ecode()}
insert_rev(Store, Rev, Revision) ->
	gen_server:call(Store, {insert_rev, Rev, Revision}).

%% @doc Lock a part to prevent it from being evicted when getting
%%      unreferenced.
lock(Store, Hash) ->
	gen_server:call(Store, {lock, Hash}).

%% @doc Unlock a previously locked part. If the part is not referenced by any
%%      revision it will be deleted.
unlock(Store, Hash) ->
	gen_server:cast(Store, {unlock, Hash}).

%% @doc Expose a document to garbage collection
unhide(Store, Doc) ->
	gen_server:cast(Store, {unhide, Doc}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Gen_server callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_call(Request, From, State) ->
	case handle_call_internal(Request, From, State) of
		{reply, Reply, NewState} = Result ->
			case NewState#state.changed of
				true  -> {reply, Reply, NewState, ?SYNC_INTERVAL};
				false -> Result
			end;

		{noreply, NewState} = Result ->
			case NewState#state.changed of
				true  -> {noreply, NewState, ?SYNC_INTERVAL};
				false -> Result
			end;

		Result ->
			Result
	end.

handle_cast(Request, State) ->
	case handle_cast_internal(Request, State) of
		{noreply, NewState} = Result ->
			case NewState#state.changed of
				true  -> {noreply, NewState, ?SYNC_INTERVAL};
				false -> Result
			end;

		Result ->
			Result
	end.


handle_info(Info, State) ->
	case handle_info_internal(Info, State) of
		{noreply, NewState} = Result ->
			case NewState#state.changed of
				true  -> {noreply, NewState, ?SYNC_INTERVAL};
				false -> Result
			end;

		Result ->
			Result
	end.


handle_call_internal(guid, _From, S) ->
	{reply, S#state.guid, S};

handle_call_internal(statfs, _From, S) ->
	{reply, do_statfs(S), S};

% returns `{ok, Rev} | error'
handle_call_internal({lookup, Doc}, _From, S) ->
	case dict:find(Doc, S#state.uuids) of
		{ok, {Rev, PreRevs, _Gen}} ->
			{reply, {ok, Rev, PreRevs}, S};
		error ->
			{reply, error, S}
	end;

handle_call_internal({contains, Rev}, _From, S) ->
	Reply = case dict:find(Rev, S#state.revisions) of
		{ok, {_, stub}} -> false;
		{ok, {_, _}}    -> true;
		error           -> false
	end,
	{reply, Reply, S};

handle_call_internal({stat, Rev}, _From, S) ->
	Reply = do_stat(Rev, S),
	{reply, Reply, S};

% returns `{ok, Handle} | {error, Reason}'
handle_call_internal({peek, Rev}, From, S) ->
	{User, _} = From,
	Reply = do_peek(S, Rev, User),
	{reply, Reply, S};

handle_call_internal({create, Doc, Type, Creator}, From, S) ->
	{User, _} = From,
	{S2, Reply} = do_write_start_create(S, Doc, Type, Creator, User),
	{reply, Reply, S2};

handle_call_internal({fork, Doc, StartRev, Creator}, From, S) ->
	{User, _} = From,
	{S2, Reply} = do_write_start_fork(S, Doc, StartRev, Creator, User),
	{reply, Reply, S2};

handle_call_internal({update, Doc, StartRev, Creator}, From, S) ->
	{User, _} = From,
	{S2, Reply} = do_write_start_update(S, Doc, StartRev, Creator, User),
	{reply, Reply, S2};

handle_call_internal({resume, Doc, PreRev, Creator}, From, S) ->
	{User, _} = From,
	{S2, Reply} = do_write_start_resume(S, Doc, PreRev, Creator, User),
	{reply, Reply, S2};

handle_call_internal({forget, Doc, PreRev}, _From, S) ->
	{S2, Reply} = do_forget(S, Doc, PreRev),
	{reply, Reply, S2};

handle_call_internal({delete_rev, Rev}, _From, S) ->
	{S2, Reply} = do_delete_rev(S, Rev),
	{reply, Reply, S2};

handle_call_internal({delete_doc, Doc, Rev}, _From, S) ->
	{S2, Reply} = do_delete_doc(S, Doc, Rev),
	{reply, Reply, S2};

handle_call_internal({put_doc, Doc, OldRev, NewRev}, _From, S) ->
	{S2, Reply} = do_put_doc(S, Doc, OldRev, NewRev),
	{reply, Reply, S2};

handle_call_internal({put_rev, Rev, Revision}, From, S) ->
	{User, _} = From,
	{S2, Reply} = do_put_rev(S, Rev, Revision, User),
	{reply, Reply, S2};

handle_call_internal({sync_get_changes, PeerGuid}, {Caller, _}, S) ->
	{S2, Reply} = do_sync_get_changes(S, PeerGuid, Caller),
	{reply, Reply, S2};

handle_call_internal({sync_set_anchor, PeerGuid, SeqNum}, _From, S) ->
	S2 = do_sync_set_anchor(S, PeerGuid, SeqNum),
	{reply, ok, S2};

handle_call_internal({sync_finish, PeerGuid}, {Caller, _}, S) ->
	{S2, Reply} = do_sync_finish(S, PeerGuid, Caller),
	{reply, Reply, S2};

handle_call_internal(gc, _From, S) ->
	S2 = do_gc(S),
	{reply, ok, S2};

% internal
handle_call_internal({lock, Hash}, _From, S) ->
	S2 = S#state{locks = orddict:update_counter(Hash, 1, S#state.locks)},
	{reply, ok, S2};

% internal: ok | {error, Reason}
handle_call_internal({insert_rev, Rev, Revision}, _From, S) ->
	{S2, Reply} = do_put_rev_commit(S, Rev, Revision),
	{reply, Reply, S2};

% internal: commit a new revision
handle_call_internal({commit, Doc, PreRev, Revision}, _From, S) ->
	{S2, Reply} = do_commit(S, Doc, PreRev, Revision),
	{reply, Reply, S2};

% internal: queue a preliminary revision
handle_call_internal({suspend, Doc, PreRev, Revision}, _From, S) ->
	{S2, Reply} = do_suspend(S, Doc, PreRev, Revision),
	{reply, Reply, S2};

% internal: add part refcount
handle_call_internal({parts_ref_inc, PartHashes}, _From, S) ->
	S2 = do_parts_ref_inc(S, PartHashes),
	{reply, ok, S2}.


% internal: decrease part refcount
handle_cast_internal({part_ref_dec, PartHash}, S) ->
	S2 = do_part_ref_dec(S, PartHash),
	{noreply, S2};

% internal: unlock a part
handle_cast_internal({unlock, Hash}, S) ->
	S2 = do_unlock(S, Hash),
	{noreply, S2};

% internal: expose to gc
handle_cast_internal({unhide, Doc}, S) ->
	S2 = do_unhide(S, Doc),
	{noreply, S2};

% internal: decrease refcount of a revision
handle_cast_internal({revision_ref_dec, Rev}, S) ->
	S2 = do_revision_ref_dec(S, Rev),
	{noreply, S2};

handle_cast_internal(dump, S) ->
	do_dump(S),
	{noreply, S};

handle_cast_internal(fsck, S) ->
	do_fsck(S),
	{noreply, S};

handle_cast_internal(stop, S) ->
	{stop, normal, S}.


handle_info_internal(timeout, S) ->
	S2 = do_gc(S),
	do_checkpoint(S2),
	{noreply, S2#state{changed=false}};

handle_info_internal({'EXIT', From, Reason}, S) ->
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
	end.


terminate(Reason, State) ->
	case Reason of
		shutdown -> do_checkpoint(State);
		normal   -> do_checkpoint(State);
		_        ->
			error_logger:error_msg("store: Unexpected termination, not syncing~n")
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


code_change(_, State, _) -> {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

check_root_doc(S, Name) ->
	case dict:is_key(S#state.guid, S#state.uuids) of
		true ->
			S;
		false ->
			RootContent = dict:new(),
			{S2, ContentHash} = crd_write_part(S, RootContent),
			Annotation1 = dict:new(),
			Annotation2 = dict:store(<<"title">>, list_to_binary(Name), Annotation1),
			Annotation3 = dict:store(<<"comment">>, list_to_binary("<<Initial store creation>>"), Annotation2),
			Sync = dict:store(<<"sticky">>, true, dict:new()),
			RootMeta1 = dict:new(),
			RootMeta2 = dict:store(<<"org.hotchpotch.annotation">>, Annotation3, RootMeta1),
			RootMeta3 = dict:store(<<"org.hotchpotch.sync">>, Sync, RootMeta2),
			{S3, MetaHash} = crd_write_part(S2, RootMeta3),
			Revision = #revision{
				parts   = [{<<"HPSD">>, ContentHash}, {<<"META">>, MetaHash}],
				parents = [],
				mtime   = util:get_time(),
				type    = <<"org.hotchpotch.store">>,
				creator = <<"org.hotchpotch.file-store">>,
				links   = {[], [], [], [], []}},
			Rev = store:hash_revision(Revision),
			S4 = S3#state{revisions=dict:store(Rev, {1, Revision}, S3#state.revisions)},
			Gen = S4#state.gen,
			S4#state{
				gen     = Gen+1,
				uuids   = dict:store(S4#state.guid, {Rev, [], Gen}, S4#state.uuids),
				changed = true
			}
	end.

crd_write_part(S, RawContent) ->
	Content = struct:encode(RawContent),
	Hash = binary_part(crypto:sha(Content), 0, 16),
	Name = util:build_path(S#state.path, Hash),
	filelib:ensure_dir(Name),
	file:write_file(Name, Content),
	S2 = do_parts_ref_inc(S, [Hash]),
	{S2, Hash}.


do_statfs(_S) ->
	% TODO: implement
	{ok, #fs_stat{
		bsize  = 4096,
		blocks = 1024,
		bfree  = 1024,
		bavail = 1024
	}}.


% returns `{ok, #stat{}} | {error, Reason}'
do_stat(Rev, #state{revisions=Revisions, path=Path}) ->
	case dict:find(Rev, Revisions) of
		% revision doesn't exist anymore on this store
		{ok, {_, stub}} ->
			{error, enoent};

		% got'ya
		{ok, {_, Revision}} ->
			Parts = lists:foldl(
				fun ({FourCC, Hash}, AccIn) ->
					case file:read_file_info(util:build_path(Path, Hash)) of
						{ok, FileInfo} ->
							[{FourCC, FileInfo#file_info.size, Hash} | AccIn];
						{error, _} ->
							AccIn
					end
				end,
				[],
				Revision#revision.parts),
			{ok, #rev_stat{
				flags   = Revision#revision.flags,
				parts   = Parts,
				parents = Revision#revision.parents,
				mtime   = Revision#revision.mtime,
				type    = Revision#revision.type,
				creator = Revision#revision.creator,
				links   = Revision#revision.links
			}};

		% completely unknown...
		error ->
			{error, enoent}
	end.

do_peek(#state{revisions=Revisions, path=Path}, Rev, User) ->
	case dict:find(Rev, Revisions) of
		{ok, {_, stub}} ->
			{error, enoent};

		{ok, {_, Revision}} ->
			% TODO: lock parts
			file_store_reader:start_link(Path, Revision, User);

		error ->
			{error, enoent}
	end.

%%% Delete %%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_forget(#state{guid=Guid, uuids=Uuids} = S, Doc, PreRev) ->
	case dict:find(Doc, Uuids) of
		{ok, {Rev, PreRevs, _Gen}} ->
			case lists:member(PreRev, PreRevs) of
				true ->
					revision_ref_dec(PreRev),
					NewPreRevs = lists:filter(
						fun(R) -> R =/= PreRev end,
						PreRevs),
					vol_monitor:trigger_mod_doc(Guid, Doc),
					Gen = S#state.gen,
					S2 = S#state{
						gen     = Gen+1,
						uuids   = dict:store(Doc, {Rev, NewPreRevs, Gen}, Uuids),
						changed = true
					},
					{S2, ok};

				false ->
					{S, {error, econflict}}
			end;

		error ->
			{S, {error, enoent}}
	end.


do_delete_rev(#state{guid=Guid, revisions=Revisions} = S, Rev) ->
	case dict:find(Rev, Revisions) of
		{ok, {_, stub}} ->
			{S, {error, enoent}};

		{ok, {RefCount, Revision}} ->
			vol_monitor:trigger_rm_rev(Guid, Rev),
			dispose_revision(Revision),
			{
				S#state{
					revisions=dict:store(Rev, {RefCount, stub}, Revisions),
					changed=true
				},
				ok
			};

		error ->
			{S, {error, enoent}}
	end.


do_delete_doc(#state{guid=Guid, uuids=Uuids} = S, Doc, Rev) ->
	case Doc of
		Guid ->
			{S, {error, eaccess}};

		_ ->
			case dict:find(Doc, Uuids) of
				{ok, {Rev, PreRevs, _Gen}} ->
					vol_monitor:trigger_rm_doc(Guid, Doc),
					revision_ref_dec(Rev),
					lists:foreach(fun(R) -> revision_ref_dec(R) end, PreRevs),
					{S#state{ uuids=dict:erase(Doc, Uuids), changed=true}, ok};

				{ok, {_OtherRev, _PreRevs, _Gen}} ->
					{S, {error, econflict}};

				error ->
					{S, {error, enoent}}
			end
	end.


%%% Dump %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% #state{
%   path:    base directory
%   gen:     integer
%   uuids:   dict: Document --> {Rev, PreRevs, Generation}
%   revisions: dict: Revision --> {refcount, #revision | stub}
%   parts:   dict: PartHash --> refcount
%   peers:   dict: GUID --> Generation
%   changed: Bool
% }
do_dump(S) ->
	io:format("Guid: ~s~n", [util:bin_to_hexstr(S#state.guid)]),
	io:format("Gen:  ~p~n", [S#state.gen]),
	io:format("Path: ~s~n", [S#state.path]),
	io:format("Documents:~n"),
	dict:fold(
		fun (Doc, {Rev, PreRevs, Gen}, _) ->
			io:format("    ~s -> ~s @ ~p~n", [util:bin_to_hexstr(Doc), util:bin_to_hexstr(Rev), Gen]),
			lists:foreach(
				fun(PreRev) ->
					io:format("                                      + ~s~n",
						[util:bin_to_hexstr(PreRev)])
				end,
				PreRevs)
		end,
		ok, S#state.uuids),
	io:format("Revisions:~n"),
	dict:fold(
		fun (Rev, {RefCount, Revision}, _) ->
			io:format("    ~s #~w~n", [util:bin_to_hexstr(Rev), RefCount]),
			do_dump_revision(Revision)
		end,
		ok, S#state.revisions),
	io:format("Parts:~n"),
	dict:fold(
		fun (Hash, RefCount, _) ->
			io:format("    ~s #~w~n", [util:bin_to_hexstr(Hash), RefCount])
		end,
		ok, S#state.parts),
	io:format("Peers:~n"),
	dict:fold(
		fun (Guid, Gen, _) ->
			io:format("    ~s @ ~w~n", [util:bin_to_hexstr(Guid), Gen])
		end,
		ok, S#state.peers).


% #revision{
%   flags:   integer()
%   parts:   [{PartFourCC, Hash}]*
%   parents: [Revision]*
%   mitme:   {MegaSecs, Secs, MicroSecs}
%   type:    Binary
%   creator: Binary
%   links:   {SDL, WDL, SRL, WRL, DocMap}
% }
do_dump_revision(Revision) ->
	if
		Revision == stub ->
			ok;
		true ->
			%io:format("        Flags:~w~n", [Revision#revision.flags]),
			io:format("        Parts:~n"),
			lists:foreach(
				fun ({FourCC, Hash}) ->
					io:format("            ~s -> ~s~n", [FourCC, util:bin_to_hexstr(Hash)])
				end,
				Revision#revision.parts),
			io:format("        Parents:~n"),
			lists:foreach(
				fun (Rev) ->
					io:format("            ~s~n", [util:bin_to_hexstr(Rev)])
				end,
				Revision#revision.parents),
			{SDL, WDL, SRL, _WRL, DocMap} = Revision#revision.links,
			io:format("        Strong Doc Links:~n"),
			lists:foreach(
				fun (Rev) ->
					io:format("            ~s~n", [util:bin_to_hexstr(Rev)])
				end,
				SDL),
			io:format("        Weak Doc Links:~n"),
			lists:foreach(
				fun (Rev) ->
					io:format("            ~s~n", [util:bin_to_hexstr(Rev)])
				end,
				WDL),
			io:format("        Strong Rev Links:~n"),
			lists:foreach(
				fun (Rev) ->
					io:format("            ~s~n", [util:bin_to_hexstr(Rev)])
				end,
				SRL),
			io:format("        Document -> Revs map:~n"),
			HexDocMap = lists:map(
				fun({Doc, Revs}) ->
					{util:bin_to_hexstr(Doc),
					lists:map(fun(R) -> util:bin_to_hexstr(R) end, Revs)}
				end,
				DocMap),
			io:format("            ~p~n", [HexDocMap])
	end.


%%% Creating & Writing %%%%%%%%%%%%%%%%

% returns `{S2, {ok, Writer} | {error, Reason}}'
do_write_start_create(S, Doc, Type, Creator, User) ->
	State = #ws{
		path      = S#state.path,
		server    = self(),
		flags     = 0,
		doc       = Doc,
		baserevs  = [],
		type      = Type,
		creator   = Creator,
		orig      = dict:new(),
		new       = dict:new(),
		readers   = dict:new(),
		links     = {[], [], [], [], []},
		locks     = []},
	{S2, Writer} = start_writer(S, State, User),
	{do_hide(S2, Doc), {ok, Writer}}.


% returns `{S2, {ok, Writer} | {error, Reason}}'
do_write_start_fork(S, Doc, StartRev, Creator, User) ->
	case dict:find(StartRev, S#state.revisions) of
		% err, don't known that one...
		error ->
			{S, {error, enoent}};
		{ok, {_, stub}} ->
			{S, {error, enoent}};

		% load old values and start writer process
		{ok, {_, Revision}} ->
			Parts = dict:from_list(Revision#revision.parts),
			State = #ws{
				path      = S#state.path,
				server    = self(),
				flags     = Revision#revision.flags,
				doc       = Doc,
				baserevs  = [StartRev],
				type      = Revision#revision.type,
				creator   = Creator,
				orig      = Parts,
				new       = dict:new(),
				readers   = dict:new(),
				links     = Revision#revision.links,
				locks     = []},
			{S2, Writer} = start_writer(S, State, User),
			{do_hide(S2, Doc), {ok, Writer}}
	end.


% returns `{S2, {ok, Writer} | {error, Reason}}' where `Reason = econflict | enoent | ...'
do_write_start_update(S, Doc, StartRev, Creator, User) ->
	case dict:is_key(Doc, S#state.uuids) of
		true ->
			case dict:find(StartRev, S#state.revisions) of
				error ->
					{S, {error, enoent}};
				{ok, {_, stub}} ->
					{S, {error, enoent}};

				{ok, {_, Revision}} ->
					Parts = dict:from_list(Revision#revision.parts),
					NewCreator = case Creator of
						keep -> Revision#revision.creator;
						_    -> Creator
					end,
					State = #ws{
						path      = S#state.path,
						server    = self(),
						flags     = Revision#revision.flags,
						doc       = Doc,
						baserevs  = [StartRev],
						type      = Revision#revision.type,
						creator   = NewCreator,
						orig      = Parts,
						new       = dict:new(),
						readers   = dict:new(),
						links     = Revision#revision.links,
						locks     = []},
					{S2, Writer} = start_writer(S, State, User),
					{S2, {ok, Writer}}
			end;

		false ->
			% unknown document
			{S, {error, enoent}}
	end.


% returns `{S2, {ok, Writer} | {error, Reason}}'
do_write_start_resume(S, Doc, PreRev, Creator, User) ->
	case dict:find(Doc, S#state.uuids) of
		{ok, {_Rev, PreRevs, _Gen}} ->
			case lists:member(PreRev, PreRevs) of
				true ->
					case dict:fetch(PreRev, S#state.revisions) of
						{_, stub} ->
							{S, {error, enoent}};

						{_, Revision} ->
							Parts = dict:from_list(Revision#revision.parts),
							NewCreator = case Creator of
								keep -> Revision#revision.creator;
								_    -> Creator
							end,
							State = #ws{
								path      = S#state.path,
								server    = self(),
								flags     = Revision#revision.flags,
								doc       = Doc,
								prerev    = PreRev,
								baserevs  = Revision#revision.parents,
								type      = Revision#revision.type,
								creator   = NewCreator,
								orig      = Parts,
								new       = dict:new(),
								readers   = dict:new(),
								links     = Revision#revision.links,
								locks     = []},
							{S2, Writer} = start_writer(S, State, User),
							{S2, {ok, Writer}}
					end;

				false ->
					{S, {error, econflict}}
			end;

		error ->
			% unknown document
			{S, {error, enoent}}
	end.


% lock part hashes, then start writer process
start_writer(S, State, User) ->
	WriterLocks = dict:fold(
		fun(_Part, Hash, Acc) -> [Hash | Acc] end,
		[],
		State#ws.orig),
	ServerLocks = lists:foldl(
		fun(Hash, Acc) -> orddict:update_counter(Hash, 1, Acc) end,
		S#state.locks,
		WriterLocks),
	{ok, Writer} = file_store_writer:start_link(State#ws{locks=WriterLocks}, User),
	{S#state{locks=ServerLocks}, Writer}.


% ok | {error, Reason}
do_put_doc(#state{uuids=Uuids} = S, Doc, OldRev, NewRev) ->
	case dict:find(Doc, Uuids) of
		% already pointing to requested rev
		{ok, {NewRev, _, _}} ->
			{S, ok};

		% free old version, store new one
		{ok, {OldRev, PreRevs, _}} ->
			revision_ref_dec(OldRev),
			vol_monitor:trigger_mod_doc(S#state.guid, Doc),
			S21 = do_revisions_ref_inc(S, [NewRev]),
			Gen = S21#state.gen,
			{
				S21#state{
					gen     = Gen+1,
					uuids   = dict:store(Doc, {NewRev, PreRevs, Gen}, S21#state.uuids),
					changed = true
				},
				ok
			};

		% completely other rev
		{ok, _} ->
			{S, {error, econflict}};

		% document does not exist (yet)...
		error ->
			vol_monitor:trigger_add_doc(S#state.guid, Doc),
			S21 = do_revisions_ref_inc(S, [NewRev]),
			Gen = S21#state.gen,
			{
				S21#state{
					gen     = Gen+1,
					uuids   = dict:store(Doc, {NewRev, [], Gen}, S21#state.uuids),
					changed = true
				},
				ok
			}
	end.


% ok | {ok, MissingParts, Importer} | {error, Reason}
do_put_rev(S, Rev, Revision, User) ->
	case dict:find(Rev, S#state.revisions) of
		error ->
			{S, {error, enotref}};

		{ok, {_, stub}} ->
			AllParts = S#state.parts,
			{PartsDone, PartsNeeded} = lists:foldl(
				fun({FourCC, Hash}, {AccDone, AccNeed}) ->
					case dict:is_key(Hash, AllParts) of
						true  -> {[Hash|AccDone], AccNeed};
						false -> {AccDone, [{FourCC, Hash}|AccNeed]}
					end
				end,
				{[], []},
				Revision#revision.parts),
			case PartsNeeded of
				[] ->
					do_put_rev_commit(S, Rev, Revision);

				_ ->
					{ok, Importer} = file_store_importer:start_link(self(),
						S#state.path, Rev, Revision, PartsDone, PartsNeeded,
						User),
					NeededFourCCs = lists:map(fun({FCC, _Hash}) -> FCC end, PartsNeeded),
					{S, {ok, NeededFourCCs, Importer}}
			end;

		{ok, _} ->
			{S, ok}
	end.

%%% Commit %%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_commit(S, Doc, OldPreRev, Revision) ->
	NewRev = store:hash_revision(Revision),
	Result = case dict:find(Doc, S#state.uuids) of
		{ok, {CurrentRev, CurrentPreRevs, _Gen}} ->
			case lists:member(CurrentRev, Revision#revision.parents) of
				true  ->
					revision_ref_dec(CurrentRev),
					NewPreRevs = lists:filter(
						fun(R) ->
							case R of
								OldPreRev -> revision_ref_dec(R), false;
								_         -> true
							end
						end,
						CurrentPreRevs),
					{ok, NewRev, NewPreRevs};
				false ->
					{error, econflict}
			end;
		error ->
			{ok, NewRev, []}
	end,
	case Result of
		{ok, Rev, PreRevs} ->
			vol_monitor:trigger_mod_doc(S#state.guid, Doc),
			Gen = S#state.gen,
			S2 = S#state{
				gen     = Gen+1,
				uuids   = dict:store(Doc, {Rev, PreRevs, Gen}, S#state.uuids)
			},
			S3 = add_revision(S2, Rev, 1, Revision),
			{S3, {ok, Rev}};

		Error ->
			{S, Error}
	end.


do_suspend(S, Doc, OldPreRev, Revision) ->
	NewRev = store:hash_revision(Revision),
	case dict:find(Doc, S#state.uuids) of
		{ok, {Rev, PreRevs, _Gen}} ->
			NewPreRevs = lists:usort(
				[NewRev] ++
				lists:filter(
					fun(R) ->
						case R of
							OldPreRev -> revision_ref_dec(R), false;
							_         -> true
						end
					end,
					PreRevs)
			),
			vol_monitor:trigger_mod_doc(S#state.guid, Doc),
			Gen = S#state.gen,
			S2 = S#state{
				gen     = Gen+1,
				uuids   = dict:store(Doc, {Rev, NewPreRevs, Gen}, S#state.uuids)
			},
			S3 = add_revision(S2, NewRev, 1, Revision),
			{S3, {ok, NewRev}};

		error ->
			{S, {error, enoent}}
	end.


do_put_rev_commit(#state{revisions=Revisions} = S, Rev, Revision) ->
	case dict:find(Rev, Revisions) of
		error ->
			{S, {error, enotref}};

		{ok, {_RefCount, stub}} ->
			S2 = add_revision(S, Rev, 0, Revision),
			vol_monitor:trigger_add_rev(S#state.guid, Rev),
			{S2, ok};

		{ok, _} ->
			{S, ok}
	end.

%%% Reference counting %%%%%%%%%%%%%%%%

% do_revisions_ref_inc(S, [Rev]) -> S2
do_revisions_ref_inc(S, Revs) ->
	Revisions1 = S#state.revisions,
	Revisions2 = lists:foldl(
		fun (Rev, Revisions) ->
			case dict:find(Rev, Revisions) of
				{ok, {RefCount, Value}} ->
					dict:store(Rev, {RefCount+1, Value}, Revisions);
				error ->
					dict:store(Rev, {1, stub}, Revisions)
			end
		end,
		Revisions1,
		Revs),
	S#state{revisions=Revisions2, changed=true}.

% do_parts_ref_inc(S, [Hash]) -> S2
do_parts_ref_inc(#state{parts=Parts1} = S, Hashes) ->
	Parts2 = lists:foldl(
		fun(PartHash, PartsAcc) ->
			RefCount = case dict:find(PartHash, PartsAcc) of
				{ok, Count} -> Count+1;
				error       -> 1
			end,
			dict:store(PartHash, RefCount, PartsAcc)
		end,
		Parts1,
		Hashes),
	S#state{parts=Parts2, changed=true}.


add_revision(S, Rev, RefCountInc, Revision) ->
	case dict:find(Rev, S#state.revisions) of
		error when (RefCountInc > 0) ->
			Parts = lists:map(fun({_FCC, Hash}) -> Hash end, Revision#revision.parts),
			S2 = do_revisions_ref_inc(S, get_rev_references(Revision)),
			S3 = do_parts_ref_inc(S2, Parts),
			S3#state{
				revisions=dict:store(Rev, {RefCountInc, Revision}, S3#state.revisions),
				changed=true};

		{ok, {RefCount, stub}} ->
			Parts = lists:map(fun({_FCC, Hash}) -> Hash end, Revision#revision.parts),
			S2 = do_revisions_ref_inc(S, get_rev_references(Revision)),
			S3 = do_parts_ref_inc(S2, Parts),
			S3#state{
				revisions=dict:store(Rev, {RefCount+RefCountInc, Revision}, S3#state.revisions),
				changed=true};

		{ok, {RefCount, Revision}} ->
			S#state{
				revisions=dict:store(Rev, {RefCount+RefCountInc, Revision},
					S#state.revisions),
				changed=true}
	end.


revision_ref_dec(Rev) ->
	gen_server:cast(self(), {revision_ref_dec, Rev}).


dispose_revision(Revision) ->
	case Revision of
		stub ->
			ok;

		#revision{parts=Parts} ->
			lists:foreach(
				fun (Rev) -> revision_ref_dec(Rev) end,
				get_rev_references(Revision)),
			lists:foreach(
				fun({_, Hash}) -> gen_server:cast(self(), {part_ref_dec, Hash}) end,
				Parts)
	end.


get_rev_references(#revision{parents=Parents, links=Links}) ->
	ParentSet = sets:from_list(Parents),
	LinkSet = sets:from_list(element(3, Links)), % Strong Rev Links
	sets:to_list(sets:union(ParentSet, LinkSet)).


get_doc_references(#revision{links=Links}) ->
	element(1, Links).


do_part_ref_dec(S, PartHash) ->
	RefCount = dict:fetch(PartHash, S#state.parts)-1,
	Parts = if
		RefCount == 0 ->
			% defer deletion if currently locked
			case orddict:find(PartHash, S#state.locks) of
				{ok, Value} when Value > 0 ->
					ok;
				_ ->
					file:delete(util:build_path(S#state.path, PartHash))
			end,
			dict:erase(PartHash, S#state.parts);
		true ->
			dict:store(PartHash, RefCount, S#state.parts)
	end,
	S#state{parts=Parts, changed=true}.


do_unlock(#state{locks=Locks, parts=Parts} = S, Hash) ->
	Depth = orddict:fetch(Hash, Locks)-1,
	Refs = case dict:find(Hash, Parts) of
		{ok, Value} -> Value;
		error       -> 0
	end,
	NewLocks = case Depth of
		0 ->
			if
				Refs == 0 ->
					file:delete(util:build_path(S#state.path, Hash));
				true ->
					ok
			end,
			orddict:erase(Hash, Locks);

		_ ->
			orddict:store(Hash, Depth, Locks)
	end,
	S#state{locks=NewLocks}.


do_revision_ref_dec(S, Rev) ->
	{RefCount, Revision} = dict:fetch(Rev, S#state.revisions),
	Revisions = if
		RefCount == 1 ->
			dispose_revision(Revision),
			case Revision of
				stub  -> ok;
				_Else -> vol_monitor:trigger_rm_rev(S#state.guid, Rev)
			end,
			dict:erase(Rev, S#state.revisions);
		true ->
			dict:store(Rev, {RefCount-1, Revision}, S#state.revisions)
	end,
	S#state{revisions=Revisions, changed=true}.


%%% Synching %%%%%%%%%%%%%%%%

% returns {ok, [{Doc, SeqNum}]} | {error, Reason}
do_sync_get_changes(S, PeerGuid, Caller) ->
	case do_sync_lock(S, PeerGuid, Caller) of
		{ok, S2} ->
			#state{uuids=Uuids, peers=Peers} = S2,
			Anchor = case dict:find(PeerGuid, Peers) of
				{ok, Value} -> Value;
				error       -> 0
			end,
			Changes = dict:fold(
				fun(Doc, {_Rev, _PreRevs, SeqNum}, Acc) ->
					if
						SeqNum > Anchor -> [{Doc, SeqNum} | Acc];
						true            -> Acc
					end
				end,
				[],
				Uuids),
			Backlog = lists:sort(
				fun({_Doc1, Seq1}, {_Doc2, Seq2}) -> Seq1 =< Seq2 end,
				Changes),
			{S2, {ok, Backlog}};

		error ->
			{S, {error, ebusy}}
	end.


do_sync_lock(#state{synclocks=SLocks} = S, PeerGuid, Caller) ->
	case dict:find(PeerGuid, SLocks) of
		{ok, Caller} ->
			{ok, S};
		{ok, _Other} ->
			error;
		error ->
			link(Caller),
			{ok, S#state{synclocks=dict:store(PeerGuid, Caller, SLocks)}}
	end.


do_sync_set_anchor(#state{peers=Peers} = S, PeerGuid, SeqNum) ->
	NewPeers = dict:store(PeerGuid, SeqNum, Peers),
	S#state{peers=NewPeers, changed=true}.


do_sync_finish(#state{synclocks=SLocks} = S, PeerGuid, Caller) ->
	case dict:find(PeerGuid, SLocks) of
		{ok, Caller} ->
			unlink(Caller),
			{S#state{synclocks=dict:erase(PeerGuid, SLocks)}, ok};
		{ok, _Other} ->
			{S, {error, eaccess}};
		error ->
			{S, {error, einval}}
	end.


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


%%% Fsck %%%%%%%%%%%%%%%%

do_fsck(#state{uuids=Uuids, revisions=Revisions, parts=Parts, path=Path}) ->
	% check if all parts exist and hashes are correct
	io:format("Checking parts...~n"),
	fsck_check_parts(Parts, Path),
	% calculate Hash refcounts and check excessive/missing Parts and if
	% refcounts correct
	io:format("Check part refcounts...~n"),
	PartRefCounts = fsck_calc_part_refcounts(Revisions),
	fsc_check_part_refcounts(Parts, PartRefCounts),
	% calculate Rev refcounts and check if correct
	io:format("Check rev refcounts...~n"),
	RevRefCounts = fsck_calc_rev_refcounts(Uuids, Revisions),
	fsck_check_rev_refcounts(Revisions, RevRefCounts),
	io:format("Done.~n").


fsck_check_parts(Parts, Path) ->
	dict:fold(
		fun(Hash, _RefCount, Acc) -> fsck_check_hash(Hash, Path), Acc end,
		ok,
		Parts).

fsck_check_hash(Hash, Path) ->
	case file:open(util:build_path(Path, Hash), [read, binary]) of
		{ok, IoDevice} ->
			case util:hash_file(IoDevice) of
				{ok, Hash} ->
					ok;
				{ok, OtherHash} ->
					io:format("  Wrong part contents. Expected ~s, got ~s~n",
						[util:bin_to_hexstr(Hash), util:bin_to_hexstr(OtherHash)]),
					error;
				{error, Reason} ->
					io:format("  Could not hash ~s: ~p~n",
						[util:bin_to_hexstr(Hash), Reason]),
					error
			end,
			file:close(IoDevice);

		{error, Reason} ->
			io:format("  Missing part ~s: ~p~n", [util:bin_to_hexstr(Hash), Reason])
	end.

fsck_calc_part_refcounts(Revisions) ->
	dict:fold(
		fun
			(_Rev, {_RefCount, stub}, Acc) -> Acc ;
			(_Rev, {_RefCount, Obj}, Acc) -> fsck_add_obj_refs(Obj, Acc)
		end,
		dict:new(),
		Revisions).

fsck_add_obj_refs(Revision, Acc) ->
	lists:foldl(
		fun({_FCC, Hash}, HashDict) ->
			dict:update_counter(Hash, 1, HashDict)
		end,
		Acc,
		Revision#revision.parts).

fsc_check_part_refcounts(Parts, PartRefCounts) ->
	Remaining = dict:fold(
		fun(Hash, RefCount, Acc) ->
			case dict:find(Hash, Acc) of
				{ok, RefCount} ->
					dict:erase(Hash, Acc);
				{ok, Wrong} ->
					io:format("  Wrong part refcount ~s: ~p <> ~p~n",
						[util:bin_to_hexstr(Hash), RefCount, Wrong]),
					dict:erase(Hash, Acc);
				error ->
					io:format("  Missing part ~s # ~p~n", [
						util:bin_to_hexstr(Hash), RefCount]),
					Acc
			end
		end,
		Parts,
		PartRefCounts),
	dict:fold(
		fun(Hash, RefCount, Acc) ->
			io:format("  Excessive part ~s # ~p~n", [util:bin_to_hexstr(Hash),
				RefCount]),
			Acc
		end,
		ok,
		Remaining).

fsck_calc_rev_refcounts(Uuids, Revisions) ->
	UCount = dict:fold(
		fun(_Doc, {Rev, PreRevs, _Gen}, Acc1) ->
			Acc2 = lists:foldl(
				fun(R, A) -> dict:update_counter(R, 1, A) end,
				Acc1,
				PreRevs),
			dict:update_counter(Rev, 1, Acc2)
		end,
		dict:new(),
		Uuids),
	dict:fold(
		fun
			(_Rev, {_RefCount, stub}, Acc) -> Acc;
			(_Rev, {_RefCount, Obj}, Acc) -> fsck_add_rev_refs(Obj, Acc)
		end,
		UCount,
		Revisions).

fsck_add_rev_refs(Obj, RevDict) ->
	Refs = get_rev_references(Obj),
	lists:foldl(
		fun(Rev, Acc) -> dict:update_counter(Rev, 1, Acc) end,
		RevDict,
		Refs).

fsck_check_rev_refcounts(Revisions, RevRefCounts) ->
	Remaining = dict:fold(
		fun(Rev, RefCount, Acc) ->
			case dict:find(Rev, Acc) of
				{ok, {RefCount, _}} ->
					dict:erase(Rev, Acc);
				{ok, {Wrong, _}} ->
					io:format("  Wrong revision refcount ~s: ~p <> ~p~n",
						[util:bin_to_hexstr(Rev), RefCount, Wrong]),
					dict:erase(Rev, Acc);
				error ->
					io:format("  Missing revision ~s # ~p~n", [
						util:bin_to_hexstr(Rev), RefCount]),
					Acc
			end
		end,
		Revisions,
		RevRefCounts),
	dict:fold(
		fun(Rev, {RefCount, _}, Acc) ->
			io:format("  Excessive revision ~s # ~p~n", [util:bin_to_hexstr(Rev),
				RefCount]),
			Acc
		end,
		ok,
		Remaining).

%%% Garbage collection %%%%%%%%%%%%%%%%

do_gc(S) ->
	#state{
		guid      = Guid,
		uuids     = Uuids,
		revisions = Revisions,
		path      = Path,
		hidden    = Hidden
	} = S,
	GreyList = lists:foldl(
		fun(HiddenDoc, Acc) ->
			case dict:find(HiddenDoc, Uuids) of
				{ok, {Rev, PreRevs, _Gen}} -> [Rev | PreRevs] ++ Acc;
				error                      -> Acc
			end
		end,
		[],
		[Guid | Hidden]),
	WhiteSet = lists:foldl(
		fun(HiddenDoc, Acc) -> dict:erase(HiddenDoc, Acc) end,
		Uuids,
		[Guid | Hidden]),
	DelDocs = dict:fetch_keys(gc_step(GreyList, WhiteSet, Revisions, Path)),
	NumCollected = length(DelDocs),
	if
		NumCollected > 0 ->
			NewUuids = lists:foldl(
				fun(Doc, Acc) ->
					{Rev, PreRevs, _Gen} = dict:fetch(Doc, Acc),
					vol_monitor:trigger_rm_doc(Guid, Doc),
					revision_ref_dec(Rev),
					lists:foreach(fun(R) -> revision_ref_dec(R) end, PreRevs),
					dict:erase(Doc, Acc)
				end,
				Uuids,
				DelDocs),
			error_logger:info_report([{store, util:bin_to_hexstr(Guid)},
				{'garbage_collected', NumCollected}]),
			S#state{uuids=NewUuids, changed=true};
		true ->
			S
	end.


gc_step([], WhiteSet, _Revisions, _Path) ->
	WhiteSet;

gc_step([Rev | GreyList], WhiteSet, Revisions, Path) ->
	case dict:find(Rev, Revisions) of
		{ok, {_RefCnt, stub}} ->
			gc_step(GreyList, WhiteSet, Revisions, Path);

		{ok, {_RefCnt, Revision}} ->
			Refs = get_doc_references(Revision),
			{NewGreyList, NewWhiteSet} = lists:foldl(
				fun gc_step_eval/2,
				{GreyList, WhiteSet},
				Refs),
			gc_step(NewGreyList, NewWhiteSet, Revisions, Path);

		error ->
			gc_step(GreyList, WhiteSet, Revisions, Path)
	end.


gc_step_eval(Doc, {GreyList, WhiteSet}=Acc) ->
	case dict:find(Doc, WhiteSet) of
		{ok, {Rev, PreRevs, _Gen}} ->
			{[Rev | PreRevs] ++ GreyList, dict:erase(Doc, WhiteSet)};
		error ->
			Acc
	end.


do_hide(#state{hidden=Hidden} = S, Doc) ->
	NewHidden = [Doc | Hidden],
	S#state{hidden=NewHidden}.


do_unhide(#state{hidden=Hidden} = S, Doc) ->
	NewHidden = lists:delete(Doc, Hidden),
	S#state{hidden=NewHidden}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Low level file management...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load_uuids(Path) ->
	load_dict(Path, "documents.dict").

load_revisions(Path) ->
	load_dict(Path, "revisions.dict").

load_parts(Path) ->
	load_dict(Path, "parts.dict").

load_guid(Path) ->
	case file:read_file(Path ++ "/guid") of
		{ok, Data}      -> binary_to_term(Data);
		{error, enoent} -> crypto:rand_bytes(16)
	end.

load_gen(Path) ->
	case file:read_file(Path ++ "/generation") of
		{ok, Data}      -> binary_to_term(Data);
		{error, enoent} -> 0
	end.

load_peers(Path) ->
	load_dict(Path, "peers.dict").

do_checkpoint(S) ->
	case S#state.changed of
		true ->
			file:write_file(S#state.path ++ "/guid", term_to_binary(S#state.guid)),
			file:write_file(S#state.path ++ "/generation", term_to_binary(S#state.gen)),
			save_dict(S#state.path, "documents.dict", S#state.uuids),
			save_dict(S#state.path, "revisions.dict", S#state.revisions),
			save_dict(S#state.path, "parts.dict",     S#state.parts),
			save_dict(S#state.path, "peers.dict",     S#state.peers);

		false ->
			ok
	end.

load_dict(Path, Name) ->
	case file:read_file(Path ++ "/" ++ Name) of
		{ok, Data} ->
			binary_to_term(Data);
		{error, _} ->
			dict:new()
	end.

save_dict(Path, Name, Dict) ->
	case file:write_file(Path ++ "/" ++ Name, term_to_binary(Dict)) of
		ok ->
			ok;
		{error, Reason} ->
			io:format("Failed to save ~w: ~s~n", [Name, Reason]),
			ok
	end.

