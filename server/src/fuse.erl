-module (fuse).
-ifndef(windows).
-behaviour (fuserl).

%-define(DEBUG(X), X).
-define(DEBUG(X), ok).

-export ([ get_supervisor_spec/2, start_link/2, start_link/3 ]).
-export ([ code_change/3, handle_info/2, init/1, terminate/2 ]).
-export ([ getattr/4, setattr/7, lookup/5, forget/5,
           opendir/5, readdir/7, releasedir/5,
           create/7, open/5, read/7, write/7, release/5
		 ]).
-export ([ access/5, flush/5, fsync/6, fsyncdir/6, getlk/6, getxattr/6, link/6,
           listxattr/5, mkdir/6, mknod/7, readlink/4, removexattr/5, rename/7,
           rmdir/5, setlk/7, setxattr/7, statfs/4, symlink/6, unlink/5 ]).

-include("store.hrl").
-include_lib ("fuserl/include/fuserl.hrl").

% inodes: dict: inode -> #inode
% cache:  dict: {ParentInode, OID} -> inode
% dirs:   dict: id -> [#direntry]
-record(state, { inodes, cache, dirs, files, count }).

-record(inode, {refcnt, parent, timeout, oid, ifc, cache}).
-record(ifc, {
	getattr,
	setattr = fun(_, _, _, _) -> {error, enosys} end,
	lookup  = fun(_, _, _) -> {error, enoent} end,
	getnode = fun(_, _) -> error end,
	opendir = fun(_, _) -> {errror, enotdir} end,
	open    = fun(_, _) -> {error, eisdir} end,
	create  = fun(_, _) -> {error, eisdir} end,
	link    = fun(_, _, _, _) -> {error, enotdir} end,
	unlink  = fun(_, _, _) -> {error, enotdir} end
}).
-record(handler, {read, write, release, changed=false}).

-define(UNKNOWN_INO, 16#ffffffff).
-define (DIRATTR (X), #stat{ st_ino = (X),
                             st_mode = ?S_IFDIR bor 8#0555,
                             st_nlink = 1 }).
-define (LINKATTR, #stat{ st_mode = ?S_IFLNK bor 8#0555, st_nlink = 1 }).

-define(FUSE_CC, <<"org.hotchpotch.fuse">>).

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
		files  = gb_trees:empty(),
		count  = 2 % 1 is the root inode
	},
	{ok, State}.

code_change (_OldVsn, State, _Extra) -> { ok, State }.
handle_info (_Msg, State) -> { noreply, State }.
terminate (_Reason, _State) -> ok.


access(_, Ino, Mask, _, S) ->
	io:format("access(~p, ~p)~n", [Ino, Mask]),
	{#fuse_reply_err{err = enosys}, S}.

flush(_, Ino, Fi, _, S) ->
	io:format("flush(~p, ~p)~n", [Ino, Fi]),
	{#fuse_reply_err{err = enosys}, S}.

fsync(_, Ino, IsDataSync, Fi, _, S) ->
	io:format("fsync(~p, ~p, ~p)~n", [Ino, IsDataSync, Fi]),
	{#fuse_reply_err{err = enosys}, S}.

fsyncdir(_, Ino, IsDataSync, Fi, _, S) ->
	io:format("fsyncdir(~p, ~p, ~p)~n", [Ino, IsDataSync, Fi]),
	{#fuse_reply_err{err = enosys}, S}.

getlk(_, Ino, Fi, Lock, _, S) ->
	io:format("getlk(~p, ~p, ~p)~n", [Ino, Fi, Lock]),
	{#fuse_reply_err{err = enosys}, S}.

getxattr(_, Ino, Name, Size, _, S) ->
	io:format("getxattr(~p, ~s, ~p)~n", [Ino, Name, Size]),
	{#fuse_reply_err{err = enosys}, S}.

listxattr(_, Ino, Size, _, S) ->
	io:format("listxattr(~p, ~p)~n", [Ino, Size]),
	{#fuse_reply_err{err = enosys}, S}.

mkdir(_, Parent, Name, Mode, _, S) ->
	io:format("mkdir(~p, ~s, ~p)~n", [Parent, Name, Mode]),
	{#fuse_reply_err{err = enosys}, S}.

mknod(_, Parent, Name, Mode, Dev, _, S) ->
	io:format("mknod(~p, ~s, ~p, ~p)~n", [Parent, Name, Mode, Dev]),
	{#fuse_reply_err{err = enosys}, S}.

readlink(_, Ino, _, S) ->
	io:format("readlink(~p)~n", [Ino]),
	{#fuse_reply_err{err = enosys}, S}.

removexattr(_, Ino, Name, _, S) ->
	io:format("removexattr(~p, ~s)~n", [Ino, Name]),
	{#fuse_reply_err{err = enosys}, S}.

rmdir(_, Ino, Name, _, S) ->
	io:format("rmdir(~p, ~s)~n", [Ino, Name]),
	{#fuse_reply_err{err = enosys}, S}.

setlk(_, Ino, Fi, Lock, Sleep, _, S) ->
	io:format("setlk(~p, ~p, ~p, ~p)~n", [Ino, Fi, Lock, Sleep]),
	{#fuse_reply_err{err = enosys}, S}.

setxattr(_, Ino, Name, Value, Flags, _, S) ->
	io:format("setxattr(~p, ~s, ~s, ~p)~n", [Ino, Name, Value, Flags]),
	{#fuse_reply_err{err = enosys}, S}.

statfs(_, Ino, _, S) ->
	io:format("statfs(~p)~n", [Ino]),
	{#fuse_reply_err{err = enosys}, S}.

symlink(_, Link, Ino, Name, _, S) ->
	io:format("symlink(~s, ~p, ~s)~n", [Link, Ino, Name]),
	{#fuse_reply_err{err = enosys}, S}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% FUSE callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

getattr(_, Ino, _Cont, #state{inodes=Inodes} = S) ->
	Reply = case gb_trees:lookup(Ino, Inodes) of
		{value, #inode{oid=Oid, timeout=Timeout, ifc=#ifc{getattr=GetAttr}}} ->
			case GetAttr(Oid, Ino) of
				{ok, Attr} ->
					{#fuse_reply_attr{attr=Attr, attr_timeout_ms=Timeout}, S};
				{error, Error} ->
					{#fuse_reply_err{err=Error}, S}
			end;
		none ->
			{#fuse_reply_err{ err = enoent }, S}
	end,
	?DEBUG(io:format("getattr(~p) -> ~p~n", [Ino, element(1, Reply)])),
	Reply.


lookup(_, Parent, Name, _Cont, S) ->
	LookupOp = fun(ParentNode) ->
		#inode{oid=Oid, ifc=#ifc{lookup=Lookup}, cache=Cache} = ParentNode,
		Lookup(Oid, Name, Cache)
	end,
	Reply = case do_lookup(Parent, LookupOp, S) of
		{ok, ChildIno, ChildNode, ParentTimeout, S2} ->
			case make_entry_param(ChildIno, ChildNode, ParentTimeout) of
				{ok, EntryParam} ->
					{#fuse_reply_entry{fuse_entry_param=EntryParam}, S2};

				error ->
					{
						#fuse_reply_err{ err = enoent },
						do_forget(ChildIno, 1, S2)
					}
			end;

		{error, Error, S2} ->
			{#fuse_reply_err{ err = Error }, S2}
	end,
	?DEBUG(io:format("lookup(~p, ~s) -> ~p~n", [Parent, Name, element(1, Reply)])),
	Reply.


forget(_, Ino, N, _Cont, State) ->
	NewState = do_forget(Ino, N, State),
	{#fuse_reply_none{}, NewState}.


opendir(_, Ino, Fi, _Cont, #state{inodes=Inodes, dirs=Dirs} = S) ->
	case gb_trees:lookup(Ino, Inodes) of
		{value, #inode{oid=Oid, parent=Parent, ifc=#ifc{opendir=OpenDir}, cache=Cache}=Node} ->
			case OpenDir(Oid, Cache) of
				{ok, Entries, NewCache} ->
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
					{
						#fuse_reply_open{fuse_file_info=Fi#fuse_file_info{fh = Id}},
						S#state{
							dirs   = gb_trees:enter(Id, AllEntries, Dirs),
							inodes = gb_trees:update(Ino, Node#inode{cache=NewCache},
								Inodes)
						}
					};

				{error, Error, NewCache} ->
					{
						#fuse_reply_err{ err = Error },
						S#state{
							inodes = gb_trees:update(Ino, Node#inode{cache=NewCache},
								Inodes)
						}
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
	?DEBUG(io:format("readdir(~p) -> ~p~n", [Ino, DirEntryList])),
	{#fuse_reply_direntrylist{direntrylist = DirEntryList}, S}.


releasedir(_, _Ino, #fuse_file_info{fh=Id}, _Cont, #state{dirs=Dirs} = S) ->
	{#fuse_reply_err{err=ok}, S#state{dirs=gb_trees:delete(Id, Dirs)}}.


open(_, Ino, Fi, _Cont, #state{inodes=Inodes} = S) ->
	Reply = case gb_trees:lookup(Ino, Inodes) of
		{value, #inode{oid=Oid, ifc=#ifc{open=Open}}} ->
			case Open(Oid, Fi#fuse_file_info.flags) of
				{ok, Handler} ->
					{Id, S2} = add_file_handler(Handler, S),
					{
						#fuse_reply_open{fuse_file_info=Fi#fuse_file_info{fh = Id}},
						S2
					};

				{error, Error} ->
					{#fuse_reply_err{ err = Error }, S}
			end;

		none ->
			{#fuse_reply_err{ err = enoent }, S}
	end,
	?DEBUG(io:format("open(~p, ~p) -> ~p~n", [Ino, Fi, element(1, Reply)])),
	Reply.


create(_, Parent, Name, _Mode, Fi, _Cont, S) ->
	LookupOp = fun(ParentNode) ->
		#inode{oid=Oid, ifc=#ifc{create=Create}, cache=Cache} = ParentNode,
		Create(Oid, Name, Cache)
	end,
	Reply = case do_lookup(Parent, LookupOp, S) of
		{ok, ChildIno, ChildNode, ParentTimeout, S2} ->
			create_open(ChildIno, ChildNode, ParentTimeout, Fi, S2);

		{error, Error, S2} ->
			{#fuse_reply_err{ err = Error }, S2}
	end,
	?DEBUG(io:format("create(~s, ~p) -> ~p~n", [Name, Fi, element(1, Reply)])),
	Reply.


create_open(ChildIno, ChildNode, ParentTimeout, Fi, S) ->
	case make_entry_param(ChildIno, ChildNode, ParentTimeout) of
		{ok, EntryParam} ->
			#inode{oid=Oid, ifc=#ifc{open=Open}} = ChildNode,
			case Open(Oid, Fi#fuse_file_info.flags) of
				{ok, Handler} ->
					{Id, S2} = add_file_handler(Handler, S),
					{
						#fuse_reply_create{
							fuse_file_info   = Fi#fuse_file_info{fh=Id},
							fuse_entry_param = EntryParam
						},
						S2
					};

				{error, Error} ->
					{
						#fuse_reply_err{ err = Error },
						do_forget(ChildIno, 1, S)
					}
			end;

		error ->
			{
				#fuse_reply_err{ err = enoent },
				do_forget(ChildIno, 1, S)
			}
	end.


read(_, _Ino, Size, Offset, Fi, _Cont, #state{files=Files} = S) ->
	Id = Fi#fuse_file_info.fh,
	#handler{read=Read} = gb_trees:get(Id, Files),
	case Read(Size, Offset) of
		{ok, Data} -> {#fuse_reply_buf{buf=Data, size=erlang:size(Data)}, S};
		{error, Error} -> {#fuse_reply_err{ err = Error }, S}
	end.


write(_, _Ino, Data, Offset, Fi, _Cont, #state{files=Files} = S) ->
	Id = Fi#fuse_file_info.fh,
	#handler{write=Write, changed=Changed} = Handler = gb_trees:get(Id, Files),
	case Write(Data, Offset) of
		ok ->
			S2 = if
				Changed ->
					S;
				true ->
					H2 = Handler#handler{changed=true},
					S#state{files = gb_trees:update(Id, H2, Files)}
			end,
			{#fuse_reply_write{count=erlang:size(Data)}, S2};

		{error, Error} ->
			{#fuse_reply_err{ err = Error }, S}
	end.


release(_, _Ino, #fuse_file_info{fh=Id}, _Cont, #state{files=Files} = S) ->
	#handler{release=Release, changed=Changed} = gb_trees:get(Id, Files),
	Release(Changed),
	?DEBUG(io:format("release(~p)~n", [Id])),
	{#fuse_reply_err{err=ok}, S#state{files=gb_trees:delete(Id, Files)}}.


setattr(_, Ino, Attr, ToSet, Fi, _, #state{inodes=Inodes} = S) ->
	Reply = case Fi of
		null ->
			case gb_trees:lookup(Ino, Inodes) of
				{value, #inode{oid=Oid, timeout=Timeout, ifc=#ifc{setattr=SetAttr}}} ->
					case SetAttr(Oid, Ino, Attr, ToSet) of
						{ok, NewAttr} ->
							{#fuse_reply_attr{attr=NewAttr, attr_timeout_ms=Timeout}, S};

						{error, Error} ->
							{#fuse_reply_err{err=Error}, S}
					end;
				none ->
					{#fuse_reply_err{ err = enoent }, S}
			end;

		#fuse_file_info{} ->
			{#fuse_reply_err{err = enosys}, S}
	end,
	?DEBUG(io:format("setattr(~p, ~p, ~p, ~p) -> ~p~n", [Ino, Attr, ToSet, Fi, element(1, Reply)])),
	Reply.


rename(_, OldParent, OldName, NewParent, NewName, _, S) ->
	Inodes = S#state.inodes,
	#inode{
		oid   = OldOid,
		ifc   = #ifc{lookup=Lookup},
		cache = OldCache1
	} = OldParentNode = gb_trees:get(OldParent, Inodes),

	case Lookup(OldOid, OldName, OldCache1) of
		{entry, ChildOid, OldCache2} ->
			#inode{oid=NewOid, ifc=#ifc{link=NewLink}} =
				gb_trees:get(NewParent, Inodes),

			% first link in new parent
			case NewLink(NewOid, ChildOid, NewName, OldCache2) of
				{ok, OldCache3} ->
					S2 = S#state{inodes=gb_trees:update(OldParent,
						OldParentNode#inode{cache=OldCache3}, Inodes)},
					% then unlink form old parent
					do_unlink(OldParent, OldName, S2);

				{error, Error, OldCache3} ->
					S2 = S#state{inodes=gb_trees:update(OldParent,
						OldParentNode#inode{cache=OldCache3}, Inodes)},
					{#fuse_reply_err{ err = Error }, S2};

				{error, Error} ->
					S2 = S#state{inodes=gb_trees:update(OldParent,
						OldParentNode#inode{cache=OldCache2}, Inodes)},
					{#fuse_reply_err{ err = Error }, S2}
			end;

		{error, Error, OldCache2} ->
			S2 = S#state{inodes=gb_trees:update(OldParent,
				OldParentNode#inode{cache=OldCache2}, Inodes)},
			{#fuse_reply_err{ err = Error }, S2};

		{error, Error} ->
			{#fuse_reply_err{ err = Error }, S}
	end.


unlink(_, Parent, Name, _, S) ->
	do_unlink(Parent, Name, S).


link(_, Ino, NewParent, NewName, _, S) ->
	Inodes = S#state.inodes,
	#inode{oid=ChildOid} = gb_trees:get(Ino, Inodes),
	#inode{
		oid     = ParentOid,
		ifc     = #ifc{link=Link, getnode=GetNode},
		cache   = ParentCache,
		timeout = Timeout
	} = ParentNode = gb_trees:get(NewParent, Inodes),
	case Link(ParentOid, ChildOid, NewName, ParentCache) of
		{ok, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(NewParent,
				ParentNode#inode{cache=NewCache}, Inodes)},
			case do_lookup_new(NewParent, ChildOid, GetNode, Timeout, S2) of
				{ok, ChildIno, ChildNode, Timeout, S3} ->
					case make_entry_param(ChildIno, ChildNode, Timeout) of
						{ok, EntryParam} ->
							{#fuse_reply_entry{fuse_entry_param=EntryParam}, S3};

						error ->
							{
								#fuse_reply_err{ err = enoent },
								do_forget(ChildIno, 1, S3)
							}
					end;

				{error, Error, S3} ->
					{#fuse_reply_err{ err = Error }, S3}
			end;

		{error, Error, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(NewParent,
				ParentNode#inode{cache=NewCache}, Inodes)},
			{#fuse_reply_err{ err = Error }, S2};

		{error, Error} ->
			{#fuse_reply_err{ err = Error }, S}
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Common FUSE functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


do_unlink(Parent, Name, S) ->
	Inodes = S#state.inodes,
	#inode{
		oid   = Oid,
		ifc   = #ifc{unlink=Unlink},
		cache = Cache
	} = ParentNode = gb_trees:get(Parent, Inodes),
	case Unlink(Oid, Name, Cache) of
		{ok, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(Parent,
				ParentNode#inode{cache=NewCache}, Inodes)},
			{#fuse_reply_err{ err = ok }, S2};

		{error, Error, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(Parent,
				ParentNode#inode{cache=NewCache}, Inodes)},
			{#fuse_reply_err{ err = Error }, S2};

		{error, Error} ->
			{#fuse_reply_err{ err = Error }, S}
	end.


do_lookup(Parent, LookupOp, S) ->
	#state{inodes=Inodes, cache=Cache} = S,
	case gb_trees:lookup(Parent, Inodes) of
		{value, ParentNode} ->
			case LookupOp(ParentNode) of
				{entry, ChildOid, NewParentCache} ->
					S2 = S#state{inodes=gb_trees:update(Parent,
						ParentNode#inode{cache=NewParentCache}, Inodes)},
					#inode{timeout=Timeout, ifc=#ifc{getnode=GetNode}} = ParentNode,
					case gb_trees:lookup({Parent, ChildOid}, Cache) of
						{value, ChildIno} ->
							do_lookup_cached(ChildIno, Timeout, S2);

						none ->
							do_lookup_new(Parent, ChildOid, GetNode, Timeout, S2)
					end;

				{error, Error, NewParentCache} ->
					{
						error,
						Error,
						S#state{inodes=gb_trees:update(Parent,
							ParentNode#inode{cache=NewParentCache}, Inodes)}
					};

				{error, Error} ->
					{error, Error, S}
			end;

		none ->
			{error, enoent, S}
	end.


do_lookup_cached(ChildIno, Timeout, #state{inodes=Inodes}=S) ->
	ChildNode = gb_trees:get(ChildIno, Inodes),
	S2 = S#state{
		inodes = gb_trees:update(
			ChildIno,
			ChildNode#inode{refcnt=ChildNode#inode.refcnt+1},
			Inodes)
	},
	{ok, ChildIno, ChildNode, Timeout, S2}.


do_lookup_new(ParentIno, ChildOid, GetNode, Timeout, S) ->
	#state{inodes=Inodes, cache=Cache, count=Count} = S,
	NewCount = Count+1,
	case GetNode(ChildOid) of
		{ok, ChildNode} ->
			S2 = S#state{
				inodes = gb_trees:insert(
					NewCount,
					ChildNode#inode{refcnt=1, parent=ParentIno},
					Inodes),
				cache = gb_trees:insert({ParentIno, ChildOid}, NewCount, Cache),
				count = NewCount
			},
			{ok, NewCount, ChildNode, Timeout, S2};

		error ->
			{error, enoent, S}
	end.


make_entry_param(ChildIno, ChildNode, ParentTimeout) ->
	#inode{
		oid     = ChildOid,
		ifc     = #ifc{ getattr = GetAttr },
		timeout = ChildTimeout
	} = ChildNode,
	case GetAttr(ChildOid, ChildIno) of
		{ok, Attr} ->
			{
				ok,
				#fuse_entry_param{
					ino              = ChildIno,
					generation       = 1,
					attr_timeout_ms  = ChildTimeout,
					entry_timeout_ms = ParentTimeout,
					attr             = Attr
				}
			};

		{error, _} ->
			error
	end.


do_forget(Ino, N, #state{inodes=Inodes, cache=Cache} = State) ->
	case gb_trees:lookup(Ino, Inodes) of
		{value, #inode{refcnt=RefCnt} = Node} ->
			case RefCnt - N of
				0 ->
					?DEBUG(io:format("forget(~p)~n", [Ino])),
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
	end.


add_file_handler(Handler, #state{files=Files} = S) ->
	Id = case gb_trees:is_empty(Files) of
		true  -> 1;
		false -> {Max, _} = gb_trees:largest(Files), Max+1
	end,
	{Id, S#state{files = gb_trees:enter(Id, Handler, Files)}}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Root directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

root_make_node() ->
	#inode{
		refcnt  = 1,
		parent  = 1,
		timeout = 100000,
		oid     = root,
		ifc     = #ifc{
			getattr = fun(_,_) -> {ok, ?DIRATTR(1)} end,
			lookup  = fun root_lookup/3,
			getnode = fun root_getnode/1,
			opendir = fun root_opendir/2
		}
	}.

root_lookup(root, <<"docs">>, Cache) ->
	{entry, docs, Cache};
root_lookup(root, <<"revs">>, Cache) ->
	{entry, revs, Cache};
root_lookup(root, <<"stores">>, Cache) ->
	{entry, stores, Cache};
root_lookup(_, _, _) ->
	{error, enoent}.

root_getnode(docs) ->
	docs_make_node();
root_getnode(revs) ->
	revs_make_node();
root_getnode(stores) ->
	stores_make_node().

root_opendir(root, Cache) ->
	{ok, [
		#direntry{ name = "docs",   stat = ?DIRATTR(0) },
		#direntry{ name = "revs",   stat = ?DIRATTR(0) },
		#direntry{ name = "stores", stat = ?DIRATTR(0) }
	], Cache}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Generic empty directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

emptydir_make_node(Oid) ->
	{ok, #inode{
		timeout = 100000,
		oid     = Oid,
		ifc     = #ifc{
			getattr = fun(_, Ino) -> {ok, ?DIRATTR(Ino)} end,
			opendir = fun(_, Cache) -> {ok, [], Cache} end
		}
	}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Docs directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

docs_make_node() ->
	emptydir_make_node(docs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Revs directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

revs_make_node() ->
	emptydir_make_node(revs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stores directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

stores_make_node() ->
	{ok, #inode{
		timeout = 3000,
		oid     = stores,
		ifc     = #ifc{
			getattr = fun(_,Ino) -> {ok, ?DIRATTR(Ino)} end,
			lookup  = fun stores_lookup/3,
			getnode = fun stores_getnode/1,
			opendir = fun stores_opendir/2
		}
	}}.


stores_lookup(stores, Name, Cache) ->
	case find_entry(
		fun({Id, _Descr, Guid, _Tags}) ->
			BinId = list_to_binary(atom_to_list(Id)),
			if
				BinId == Name ->
					case volman:store(Guid) of
						{ok, Store} -> {ok, {doc, Store, Guid}};
						error       -> error
					end;

				true ->
					error
			end
		end,
		volman:enum())
	of
		{value, Oid} -> {entry, Oid, Cache};
		none         -> {error, enoent}
	end.


stores_getnode(Oid) ->
	doc_make_node(Oid).


stores_opendir(stores, Cache) ->
	Stores = lists:map(
		fun({Id, _Descr, _Guid, _Tags}) ->
			#direntry{ name = atom_to_list(Id), stat = ?DIRATTR(0) }
		end,
		lists:filter(
			fun({_Id, _Descr, _Guid, Tags}) ->
				proplists:is_defined(mounted, Tags)
			end,
			volman:enum())),
	{ok, Stores, Cache}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Documents
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

doc_make_node({doc, Store, Doc} = Oid) ->
	case store:lookup(Store, Doc) of
		{ok, Rev, _PreRevs} ->
			case store:stat(Store, Rev) of
				{ok, #rev_stat{type=Type}} ->
					case Type of
						<<"org.hotchpotch.store">> ->
							doc_make_node_dict(Oid);
						<<"org.hotchpotch.dict">> ->
							doc_make_node_dict(Oid);
						<<"org.hotchpotch.set">> ->
							doc_make_node_set(Oid);
						_ ->
							doc_make_node_file(Oid)
					end;

				{error, _} ->
					error
			end;

		error ->
			error
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Directory documents
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

doc_make_node_dict(Oid) ->
	{ok, #inode{
		timeout = 1000,
		oid     = Oid,
		ifc     = #ifc{
			getattr = fun dict_getattr/2,
			lookup  = fun dict_lookup/3,
			getnode = fun dict_getnode/1,
			opendir = fun dict_opendir/2,
			create  = fun dict_create/3,
			link    = fun dict_link/4,
			unlink  = fun dict_unlink/3
		},
		cache = {undefined, undefined}
	}}.


dict_getattr({doc, Store, Doc}, Ino) ->
	case store:lookup(Store, Doc) of
		{ok, Rev, _PreRevs} ->
			case store:stat(Store, Rev) of
				{ok, #rev_stat{mtime=Mtime}} ->
					{ok, #stat{
						st_ino   = Ino,
						st_mode  = ?S_IFDIR bor 8#0777,
						st_nlink = 1,
						st_atime = Mtime,
						st_mtime = Mtime,
						st_ctime = Mtime
					}};
				Error ->
					Error
			end;
		error ->
			{error, enoent}
	end.


dict_lookup({doc, Store, Doc}, Name, Cache) ->
	case dict_read_entries(Store, Doc, Cache) of
		{ok, Entries, NewCache} ->
			case dict:find(Name, Entries) of
				{ok, {dlink, ChildDoc, _Revs}} ->
					{entry, {doc, Store, ChildDoc}, NewCache};

				_ ->
					{error, enoent, NewCache}
			end;

		_ ->
			{error, enoent}
	end.


dict_create({doc, Store, Doc}, Name, Cache) ->
	case dict_read_entries(Store, Doc, Cache) of
		{ok, Entries, NewCache} ->
			case dict:find(Name, Entries) of
				{ok, {dlink, ChildDoc, _Revs}} ->
					{entry, {doc, Store, ChildDoc}, NewCache};

				_ ->
					{error, eacces, NewCache}
			end;

		_ ->
			{error, eacces}
	end.


dict_getnode({doc, _Store, _Doc} = Oid) ->
	doc_make_node(Oid);
dict_getnode(_) ->
	error.


dict_opendir({doc, Store, Doc}, Cache) ->
	case dict_read_entries(Store, Doc, Cache) of
		{ok, Entries, NewCache} ->
			Content = map_filter(
				fun(E) -> dict_opendir_filter(Store, E) end,
				dict:to_list(Entries)),
			{ok, Content, NewCache};

		error ->
			{error, enoent}
	end.


dict_opendir_filter(Store, {Name, {dlink, Child, _Revs}}) ->
	Oid = {doc, Store, Child},
	case doc_make_node(Oid) of
		{ok, #inode{ifc=#ifc{getattr=GetAttr}}} ->
			case GetAttr(Oid, 0) of
				{ok, Attr} ->
					{ok, #direntry{name=binary_to_list(Name), stat=Attr}};
				{error, _} ->
					skip
			end;
		error ->
			skip
	end;
dict_opendir_filter(_, _) ->
	skip.


dict_read_entries(Store, Doc, {CacheRev, CacheEntries}=Cache) ->
	case store:lookup(Store, Doc) of
		{ok, CacheRev, _PreRevs} ->
			{ok, CacheEntries, Cache};

		{ok, Rev, _PreRevs} ->
			case util:read_rev_struct(Rev, <<"HPSD">>) of
				{ok, Entries} when is_record(Entries, dict, 9) ->
					{ok, Entries, {Rev, Entries}};
				{ok, _} ->
					error;
				{error, _} ->
					error
			end;
		error ->
			error
	end.


dict_write_entries(Store, Doc, Entries, Cache) ->
	case write_struct(Store, Doc, <<"HPSD">>, Entries) of
		{ok, Rev}      -> {ok, {Rev, Entries}};
		{error, Error} -> {error, Error, Cache}
	end.


dict_link({doc, Store, Doc}, {doc, ChildStore, ChildDoc}, Name, Cache)
	when Store =:= ChildStore ->
	case store:lookup(Store, ChildDoc) of
		{ok, ChildRev, _PreRevs} ->
			case dict_read_entries(Store, Doc, Cache) of
				{ok, Entries, NewCache} ->
					NewEntries = dict:store(Name, {dlink, ChildDoc, [ChildRev]},
						Entries),
					dict_write_entries(Store, Doc, NewEntries, NewCache);

				error ->
					{error, enoent}
			end;

		error ->
			{error, enoent}
	end;

dict_link(_, _, _, _) ->
	{error, eacces}.


dict_unlink({doc, Store, Doc}, Name, Cache) ->
	case dict_read_entries(Store, Doc, Cache) of
		{ok, Entries, NewCache} ->
			NewEntries = dict:erase(Name, Entries),
			dict_write_entries(Store, Doc, NewEntries, NewCache);

		error ->
			{error, enoent}
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Set documents
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

doc_make_node_set(Oid) ->
	{ok, #inode{
		timeout = 1000,
		oid     = Oid,
		ifc     = #ifc{
			getattr = fun set_getattr/2,
			lookup  = fun set_lookup/3,
			getnode = fun set_getnode/1,
			opendir = fun set_opendir/2
		},
		cache = {undefined, undefined}
	}}.


set_getattr({doc, Store, Doc}, Ino) ->
	case store:lookup(Store, Doc) of
		{ok, Rev, _PreRevs} ->
			case store:stat(Store, Rev) of
				{ok, #rev_stat{mtime=Mtime}} ->
					{ok, #stat{
						st_ino   = Ino,
						st_mode  = ?S_IFDIR bor 8#0555,
						st_nlink = 1,
						st_atime = Mtime,
						st_mtime = Mtime,
						st_ctime = Mtime
					}};
				Error ->
					Error
			end;
		error ->
			{error, enoent}
	end.


set_lookup({doc, Store, Doc}, Name, Cache) ->
	case set_read_entries(Store, Doc, Cache) of
		{ok, Entries, NewCache} ->
			case find_entry(fun(E) -> set_lookup_cmp(Name, E) end, Entries) of
				{value, Oid} -> {entry, Oid, NewCache};
				none         -> {error, enoent, NewCache} % TODO: look up alternative names
			end;

		_ ->
			{error, enoent}
	end.


set_lookup_cmp(Name, {Title, Oid}) ->
	case list_to_binary(Title) of
		Name -> {ok, Oid};
		_    -> error
	end;

set_lookup_cmp(_, _) ->
	error.


set_getnode({doc, _Store, _Doc} = Oid) ->
	doc_make_node(Oid);
set_getnode(_) ->
	error.


set_opendir({doc, Store, Doc}, Cache) ->
	case set_read_entries(Store, Doc, Cache) of
		{ok, Entries, NewCache} ->
			Content = map_filter(fun set_opendir_filter/1, Entries),
			{ok, Content, NewCache};

		error ->
			{error, enoent}
	end.


set_opendir_filter({Name, {doc, _, _}=Oid}) ->
	case doc_make_node(Oid) of
		{ok, #inode{ifc=#ifc{getattr=GetAttr}}} ->
			case GetAttr(Oid, 0) of
				{ok, Attr} ->
					{ok, #direntry{name=Name, stat=Attr}};
				{error, _} ->
					skip
			end;
		error ->
			skip
	end;
set_opendir_filter(_) ->
	skip.


set_read_entries(Store, Doc, {CacheRev, CacheEntries}=Cache) ->
	case store:lookup(Store, Doc) of
		{ok, CacheRev, _PreRevs} ->
			{ok, CacheEntries, Cache};

		{ok, Rev, _PreRevs} ->
			case util:read_rev_struct(Rev, <<"HPSD">>) of
				{ok, Entries} when is_list(Entries) ->
					RawList = map_filter(
						fun(E) -> set_read_entries_filter(Store, E) end,
						Entries),
					List = set_sanitize_entries(RawList, 4),
					{ok, List, {Rev, List}};
				{ok, _} ->
					error;
				{error, _} ->
					error
			end;
		error ->
			error
	end.


set_read_entries_filter(Store, {dlink, Child, _Revs}) ->
	{ok, {{set_read_title(Store, Child), ""}, {doc, Store, Child}}};
set_read_entries_filter(_, _) ->
	skip.


set_read_title(Store, Doc) ->
	case set_read_title_meta(Store, Doc) of
		{ok, <<"">>} -> "." ++ util:bin_to_hexstr(Doc);
		{ok, Title}  -> sanitize(binary_to_list(Title));
		error        -> "." ++ util:bin_to_hexstr(Doc)
	end.

set_read_title_meta(Store, Doc) ->
	case store:lookup(Store, Doc) of
		{ok, Rev, _PreRevs} ->
			case util:read_rev_struct(Rev, <<"META">>) of
				{ok, Meta} ->
					case meta_read_entry(Meta, [<<"org.hotchpotch.annotation">>, <<"title">>]) of
						{ok, Title} when is_binary(Title) ->
							{ok, Title};
						{ok, _} ->
							error;
						error ->
							error
					end;
				{error, _} ->
					error
			end;
		error ->
			error
	end.


set_sanitize_entries(Entries, SuffixLen) ->
	Dict = lists:foldl(
		fun({Title, Oid}, Acc) -> dict:append(Title, Oid, Acc) end,
		dict:new(),
		Entries),
	dict:fold(
		fun({Title, Suffix}, Oids, Acc) ->
			case Oids of
				[Oid] -> [{Title++Suffix, Oid} | Acc];
				_     -> set_sanitize_entry(Title, Oids, SuffixLen) ++ Acc
			end
		end,
		[],
		Dict).


set_sanitize_entry(Title, Oids, SuffixLen) ->
	NewEntries = lists:map(
		fun({doc, _, Uuid} = Oid) ->
			Suffix = "~"++lists:sublist(util:bin_to_hexstr(Uuid), SuffixLen),
			{{Title, Suffix}, Oid}
		end,
		Oids),
	set_sanitize_entries(NewEntries, SuffixLen+4).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% All other documents
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

doc_make_node_file(Oid) ->
	{ok, #inode{
		timeout = 1000,
		oid     = Oid,
		ifc     = #ifc{
			getattr = fun file_getattr/2,
			setattr = fun file_setattr/4,
			open    = fun file_open/2
		}
	}}.


file_getattr({doc, Store, Doc}, Ino) ->
	case store:lookup(Store, Doc) of
		{ok, Rev, _PreRevs} ->
			file_getattr_rev(Store, Rev, Ino);
		error ->
			{error, enoent}
	end.


file_getattr_rev(Store, Rev, Ino) ->
	case store:stat(Store, Rev) of
		{ok, #rev_stat{parts=Parts, mtime=Mtime}} ->
			Size = case find_entry(
				fun({FCC, Size, _Hash}) ->
					case FCC of
						<<"FILE">> -> {ok, Size};
						_          -> error
					end
				end,
				Parts)
			of
				{value, FileSize} -> FileSize;
				none              -> 0
			end,
			{ok, #stat{
				st_ino   = Ino,
				st_mode  = ?S_IFREG bor 8#0666,
				st_nlink = 1,
				st_atime = Mtime,
				st_mtime = Mtime,
				st_ctime = Mtime,
				st_size  = Size
			}};

		Error ->
			Error
	end.


file_setattr({doc, Store, Doc}, Ino, Attr, ToSet) ->
	if
		(ToSet band ?FUSE_SET_ATTR_SIZE) =/= 0 ->
			file_truncate(Store, Doc, Ino, Attr#stat.st_size);

		true ->
			{error, enosys}
	end.


file_truncate(Store, Doc, Ino, Size) ->
	case store:lookup(Store, Doc) of
		{ok, Rev, _PreRevs} ->
			case store:update(Store, Doc, Rev, ?FUSE_CC) of
				{ok, Handle} ->
					case store:truncate(Handle, <<"FILE">>, Size) of
						ok ->
							case file_release_commit(Store, Doc, Handle) of
								{ok, CurRev} ->
									file_getattr_rev(Store, CurRev, Ino);
								Error ->
									Error
							end;

						{error, _} = Error ->
							store:abort(Handle),
							Error
					end;

				{error, _} ->
					{error, enoent}
			end;

		error ->
			{error, enoent}
	end.


file_open({doc, Store, Doc}, Flags) ->
	case store:lookup(Store, Doc) of
		{ok, Rev, _PreRevs} ->
			Reply = if
				(Flags band ?O_ACCMODE) =/= ?O_RDONLY ->
					store:update(Store, Doc, Rev, ?FUSE_CC);
				true ->
					store:peek(Store, Rev)
			end,
			case Reply of
				{ok, Handle} ->
					{ok, #handler{
						read = fun(Size, Offset) ->
							file_read(Handle, Size, Offset)
						end,
						write = fun(Data, Offset) ->
							file_write(Handle, Data, Offset)
						end,
						release = fun(Changed) ->
							file_release(Store, Doc, Handle, Changed)
						end
					}};

				{error, _} ->
					{error, enoent}
			end;

		error ->
			{error, enoent}
	end.


file_read(Handle, Size, Offset) ->
	case store:read(Handle, <<"FILE">>, Offset, Size) of
		{ok, _Data} = R  -> R;
		{error, enoent}  -> {ok, <<>>};
		{error, _}       -> {error, eio}
	end.


file_write(Handle, Data, Offset) ->
	store:write(Handle, <<"FILE">>, Offset, Data).


file_release(Store, Doc, Handle, Changed) ->
	case Changed of
		false -> store:abort(Handle);
		true  -> file_release_commit(Store, Doc, Handle)
	end.


file_release_commit(Store, Doc, Handle) ->
	case store:commit(Handle, util:get_time()) of
		{ok, _Rev} = Ok ->
			Ok;

		conflict ->
			case store:lookup(Store, Doc) of
				{ok, CurRev, _PreRevs} ->
					store:set_parents(Handle, [CurRev]),
					file_release_commit(Store, Doc, Handle);
				error ->
					store:abort(Handle),
					{error, enoent}
			end;

		{error, _} = Error ->
			Error
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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


map_filter(F, L) ->
	map_filter_loop(F, L, []).

map_filter_loop(_, [], Acc) ->
	Acc;
map_filter_loop(F, [H | T], Acc) ->
	case F(H) of
		{ok, Value} -> map_filter_loop(F, T, [Value | Acc]);
		skip        -> map_filter_loop(F, T, Acc)
	end.


find_entry(_, []) ->
	none;
find_entry(F, [H|T]) ->
	case F(H) of
		{ok, Result} -> {value, Result};
		error        -> find_entry(F, T)
	end.


meta_read_entry(Meta, []) ->
	{ok, Meta};
meta_read_entry(Meta, [Step|Path]) when is_record(Meta, dict, 9) ->
	case dict:find(Step, Meta) of
		{ok, Value} -> meta_read_entry(Value, Path);
		error       -> error
	end;
meta_read_entry(_Meta, _Path) ->
	error.


sanitize(S) ->
	lists:filter(fun(C) -> (C /= $/) and (C >= 31) end, S).


write_struct(Store, Doc, Part, Struct) ->
	Data = struct:encode(Struct),
	case write_struct_open(Store, Doc) of
		{ok, Handle} ->
			case store:truncate(Handle, Part, 0) of
				ok ->
					case store:write(Handle, Part, 0, Data) of
						ok ->
							write_struct_close(Store, Doc, Handle);

						{error, _} = Error ->
							store:abort(Handle),
							Error
					end;

				{error, _} = Error ->
					store:abort(Handle),
					Error
			end;

		{error, _} = Error ->
			Error
	end.


write_struct_open(Store, Doc) ->
	case store:lookup(Store, Doc) of
		{ok, Rev, _PreRevs} ->
			case store:update(Store, Doc, Rev, ?FUSE_CC) of
				{ok, _} = Ok       -> Ok;
				{error, conflict}  -> write_struct_open(Store, Doc);
				{error, _} = Error -> Error
			end;

		error ->
			{error, enoent}
	end.


write_struct_close(Store, Doc, Handle) ->
	case store:commit(Handle, util:get_time()) of
		{ok, _Rev} = Ok ->
			Ok;

		conflict ->
			case store:lookup(Store, Doc) of
				{ok, CurRev, _PreRevs} ->
					store:set_parents(Handle, [CurRev]),
					write_struct_close(Store, Doc, Handle);
				error ->
					store:abort(Handle),
					{error, enoent}
			end;

		{error, _} = Error ->
			Error
	end.

-endif.
