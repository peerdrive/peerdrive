-module (fuse).
%-behaviour (fuserl).

% Directory layout:
%   /docs/...
%   /docs/00112233445566778899AABBCCDDEEFF/asdf
%   /revs/...
%   /revs/00112233445566778899AABBCCDDEEFF/foo/bar
%   /stores/usr/...

-export ([ get_supervisor_spec/2, start_link/2, start_link/3 ]).
-export ([ code_change/3, handle_info/2, init/1, terminate/2 ]).
-export ([ getattr/4, lookup/5, forget/5,
		   opendir/5, readdir/7, releasedir/5
           %open/5,
           %read/7,
           %readlink/4
		 ]).

-include_lib ("fuserl/include/fuserl.hrl").

% inodes: dict: inode -> #inode
% cache:  dict: {ParentInode, OID} -> inode
% dirs:   dict: id -> [#direntry]
-record(state, { inodes, cache, dirs, count }).

-record(inode, {refcnt, parent, timeout, oid, attr, ifc}).
-record(ifc, {lookup, getnode, opendir}).

-define(UNKNOWN_INO, 16#ffffffff).
-define (DIRATTR (X), #stat{ st_ino = (X), 
                             st_mode = ?S_IFDIR bor 8#0555, 
                             st_nlink = 1 }).
-define (LINKATTR, #stat{ st_mode = ?S_IFLNK bor 8#0555, st_nlink = 1 }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_supervisor_spec(Id, Options) ->
	{
		Id,
		{fuse, start_link, Options},
		permanent,
		10000,
		worker,
		[fuse]
	}.


start_link(LinkedIn, Dir) ->
	start_link(LinkedIn, Dir, "").

start_link(LinkedIn, Dir, MountOpts) ->
	Server = case application:start(fuserl) of
		ok                               -> ok;
		{error,{already_started,fuserl}} -> ok;
		{error, Reason}                  -> {fuserl, Reason}
	end,
	case Server of
		ok ->
			fuserlsrv:start_link(?MODULE, LinkedIn, MountOpts, Dir, [], []);
		Else ->
			{error, Else}
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Fuserl callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	State = #state{
		inodes = gb_trees:from_orddict([ {1, root_make_node() } ]),
		cache  = gb_trees:empty(),
		dirs   = gb_trees:empty(),
		count  = 2
	},
	{ok, State}.

code_change (_OldVsn, State, _Extra) -> { ok, State }.
handle_info (_Msg, State) -> { noreply, State }.
terminate (_Reason, _State) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% FUSE callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

getattr(_, Ino, _Cont, #state{inodes=Inodes} = S) ->
	case gb_trees:lookup(Ino, Inodes) of
		{value, #inode{attr=Attr}} ->
			{#fuse_reply_attr{attr=Attr, attr_timeout_ms=1000}, S};
		none ->
			{#fuse_reply_err{ err = enoent }, S}
	end.


lookup(_, Parent, Name, _Cont, #state{inodes=Inodes, cache=Cache, count=Count} = S) ->
	case gb_trees:lookup(Parent, Inodes) of
		{value, #inode{oid=Oid, timeout=Timeout, ifc=#ifc{lookup=Lookup, getnode=GetNode}}} ->
			case Lookup(Oid, Name) of
				{entry, ChildOid} ->
					case gb_trees:lookup({Parent, ChildOid}, Cache) of
						{value, ChildIno} ->
							ChildNode = gb_trees:get(ChildIno, Inodes),
							{#fuse_reply_entry{fuse_entry_param=
								#fuse_entry_param{
									ino              = ChildIno,
									generation       = 1,
									attr_timeout_ms  = ChildNode#inode.timeout,
									entry_timeout_ms = Timeout,
									attr             = ChildNode#inode.attr
								}},
								S#state{
									inodes = gb_trees:update(ChildIno,
										ChildNode#inode{refcnt=ChildNode#inode.refcnt+1},
										Inodes)
								}
							};

						none ->
							NewCount = Count+1,
							ChildNode = GetNode(ChildOid, NewCount),
							{#fuse_reply_entry{fuse_entry_param=
								#fuse_entry_param{
									ino              = NewCount,
									generation       = 1,
									attr_timeout_ms  = ChildNode#inode.timeout,
									entry_timeout_ms = Timeout,
									attr             = ChildNode#inode.attr
								}},
								S#state{
									inodes = gb_trees:insert(NewCount,
										ChildNode#inode{refcnt=1, parent=Parent},
										Inodes),
									cache = gb_trees:insert({Parent, ChildOid},
										NewCount, Cache),
									count = NewCount
								}
							}
					end;
					
				error ->
					{#fuse_reply_err{ err = enoent }, S}
			end;

		none ->
			{#fuse_reply_err{ err = enoent }, S}
	end.


forget(_, Ino, N, _Cont, #state{inodes=Inodes, cache=Cache} = State) ->
	NewState = case gb_trees:lookup(Ino, Inodes) of
		{value, #inode{refcnt=RefCnt} = Node} ->
			case RefCnt - N of
				0 ->
					Key = {Node#inode.parent, Node#inode.oid},
					State#state{
						inodes = gb_trees:delete(Ino, Inodes),
						cache  = gb_trees:delete(Key, Cache)
					};

				NewRef ->
					State#state{
						inodes = gb_trees:update(Ino, Node#inode{refcnt=NewRef}, Inodes)
					}
			end;

		none ->
			State
	end,
	{#fuse_reply_none{}, NewState}.


opendir(_, Ino, Fi, _Cont, #state{inodes=Inodes, dirs=Dirs} = S) ->
	case gb_trees:lookup(Ino, Inodes) of
		{value, #inode{oid=Oid, parent=Parent, ifc=#ifc{opendir=OpenDir}}} ->
			case OpenDir(Oid) of
				{ok, Entries} ->
					Id = case gb_trees:is_empty(Dirs) of
						true  -> 1;
						false -> {Max, _} = gb_trees:largest(Dirs), Max+1
					end,
					{UpdatedEntries, _} = lists:mapfoldl(
						fun(E, Acc) ->
							{
								E#direntry{
									offset=Acc,
									stat=#stat{st_ino=?UNKNOWN_INO}
								},
								Acc+1
							}
						end,
						3,
						Entries),
					AllEntries = [
						#direntry{ name = ".", offset = 1, stat = ?DIRATTR(Ino) },
						#direntry{ name = "..", offset = 2, stat = ?DIRATTR(Parent) }
					] ++ UpdatedEntries,
					NewDirs = gb_trees:enter(Id, AllEntries, Dirs),
					{
						#fuse_reply_open{fuse_file_info=Fi#fuse_file_info{
							fh = Id
						}},
						S#state{dirs=NewDirs}
					};

				{error, Error} ->
					{#fuse_reply_err{ err = Error }, S}
			end;

		none ->
			{#fuse_reply_err{ err = enoent }, S}
	end.


readdir(_Ctx, _Ino, Size, Offset, Fi, _Cont, #state{dirs=Dirs} = S) ->
	Id = Fi#fuse_file_info.fh,
	Entries = gb_trees:get(Id, Dirs),
	DirEntryList = take_while(
		fun (E, {Total, Max}) -> 
			Cur = fuserlsrv:dirent_size (E),
			if 
				Total + Cur =< Max -> {continue, {Total + Cur, Max}};
				true -> stop
			end
		end,
		{0, Size},
		safe_nthtail(Offset, Entries)),
	{#fuse_reply_direntrylist{direntrylist = DirEntryList}, S}.


releasedir(_, _Ino, #fuse_file_info{fh=Id}, _Cont, #state{dirs=Dirs} = S) ->
	{#fuse_reply_err{err=ok}, S#state{dirs=gb_trees:delete(Id, Dirs)}}.



safe_nthtail (_, []) -> 
	[];
safe_nthtail (N, L) when N =< 0 ->
	L;
safe_nthtail (N, L) ->
	safe_nthtail (N - 1, tl (L)).


take_while (_, _, []) -> 
	[];
take_while (F, Acc, [ H | T ]) ->
	case F (H, Acc) of
		{ continue, NewAcc } ->
			[ H | take_while (F, NewAcc, T) ];
		stop ->
			[]
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Root directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

root_make_node() ->
	#inode{
		refcnt  = 1,
		parent  = 1,
		timeout = 1000,
		oid     = root,
		attr    = ?DIRATTR(1),
		ifc     = #ifc{
			lookup  = fun root_lookup/2,
			getnode = fun root_getnode/2,
			opendir = fun root_opendir/1
		}
	}.

root_lookup(root, <<"docs">>) ->
	{entry, docs};
root_lookup(root, <<"revs">>) ->
	{entry, revs};
root_lookup(root, <<"stores">>) ->
	{entry, stores};
root_lookup(_, _) ->
	error.

root_getnode(docs, Ino) ->
	docs_make_node(Ino);
root_getnode(revs, Ino) ->
	revs_make_node(Ino);
root_getnode(stores, Ino) ->
	stores_make_node(Ino).

root_opendir(root) ->
	{ok, [
		#direntry{ name = "docs",   stat = ?DIRATTR(0) },
		#direntry{ name = "revs",   stat = ?DIRATTR(0) },
		#direntry{ name = "stores", stat = ?DIRATTR(0) }
	]}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Generic empty directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

emptydir_make_node(Oid, Ino) ->
	#inode{
		timeout = 1000,
		oid     = Oid,
		attr    = ?DIRATTR(Ino),
		ifc     = #ifc{
			lookup  = fun(_, _) -> error end,
			getnode = fun(_, _) -> error end,
			opendir = fun(_) -> {ok, []} end
		}
	}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Docs directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

docs_make_node(Ino) ->
	emptydir_make_node(docs, Ino).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Revs directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

revs_make_node(Ino) ->
	emptydir_make_node(revs, Ino).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stores directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

stores_make_node(Ino) ->
	#inode{
		timeout = 5,
		oid     = stores,
		attr    = ?DIRATTR(Ino),
		ifc     = #ifc{
			lookup  = fun stores_lookup/2,
			getnode = fun stores_getnode/2,
			opendir = fun stores_opendir/1
		}
	}.

stores_lookup(stores, Name) ->
	{entry, list_to_atom("_store_" ++ binary_to_list(Name))}.

stores_getnode(Oid, Ino) ->
	emptydir_make_node(Oid, Ino).

stores_opendir(stores) ->
	Stores = lists:map(
		fun({Id, _Descr, _Guid, _Tags}) ->
			#direntry{ name = atom_to_list(Id), stat = ?DIRATTR(0) }
		end,
		volman:enum()),
	{ok, Stores}.

