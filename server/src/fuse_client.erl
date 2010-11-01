-module (fuse_client).
-ifndef(windows).
-behaviour (fuserl).

%-define(DEBUG(X), X).
-define(DEBUG(X), ok).

-export([start_link/3]).
-export([code_change/3, handle_info/2, init/1, terminate/2]).
-export([create/7, forget/5, getattr/4, link/6, lookup/5, mkdir/6, open/5,
         opendir/5, read/7, readdir/7, release/5, releasedir/5, rename/7,
         rmdir/5, setattr/7, statfs/4, unlink/5, write/7]).
-export([access/5, flush/5, fsync/6, fsyncdir/6, getlk/6, getxattr/6,
         listxattr/5, mknod/7, readlink/4, removexattr/5, setlk/7, setxattr/7,
         symlink/6]).

-include("store.hrl").
-include_lib ("fuserl/include/fuserl.hrl").

-define(FUSE_CC, <<"org.hotchpotch.fuse">>).  % FUSE creator code

%% inode: integer number (for fuse) identifying a vnode
%% vnode: structure describing the file system object

-record(state, {
	inodes,  % gb_trees: inode -> #vnode
	cache,   % gb_trees: {ParentInode, OID} -> inode
	dirs,    % gb_trees: id -> [#direntry]
	files,   % gb_trees: id -> #handler
	count,   % int: next free inode
	uid,
	gid,
	umask
}).

-record(vnode, {
	refcnt,  % int:  how many time lookup'ed by fuse (reference count)
	parent,  % int:  parent inode
	timeout, % int:  timeout of this node's information in ms
	ifc,     % #ifc: file system functions of node
	oid,     % term: information to find corresponding hotchpotch object
	cache    % private information of #ifc functions
}).

-record(ifc, {
	getattr,
	truncate = fun(_, _) -> {error, enosys} end,
	lookup   = fun(_, _, _) -> {error, enoent} end,
	getnode  = fun(_, _) -> error end,
	opendir  = fun(_, _) -> {errror, enotdir} end,
	open     = fun(_, _) -> {error, eisdir} end,
	create   = fun(_, _) -> {error, eisdir} end,
	link     = fun(_, _, _, _) -> {error, enotdir} end,
	unlink   = fun(_, _, _) -> {error, enotdir} end,
	mkdir    = fun(_, _, _) -> {error, enotdir} end
}).

-record(handler, {read, write, release, changed=false}).

-define(UNKNOWN_INO, 16#ffffffff).
-define(DIRATTR, #stat{st_mode=?S_IFDIR bor 8#0555, st_nlink=1}).
-define(DIRATTR(I), #stat{
	st_ino   = (I),
	st_mode  = ?S_IFDIR bor 8#0555,
	st_nlink = 1
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Dir, MountOpts, Options) ->
	Server = case application:start(fuserl) of
		ok                               -> ok;
		{error,{already_started,fuserl}} -> ok;
		{error, Reason}                  -> {fuserl, Reason}
	end,
	LinkedIn = proplists:get_value(linkedin, Options, false),
	case Server of
		ok ->
			fuserlsrv:start_link(?MODULE, LinkedIn, MountOpts, Dir, Options, []);
		Else ->
			{error, Else}
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Fuserl callback stubs
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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

mknod(_, Parent, Name, Mode, Dev, _, S) ->
	io:format("mknod(~p, ~s, ~p, ~p)~n", [Parent, Name, Mode, Dev]),
	{#fuse_reply_err{err = enosys}, S}.

readlink(_, Ino, _, S) ->
	io:format("readlink(~p)~n", [Ino]),
	{#fuse_reply_err{err = enosys}, S}.

removexattr(_, Ino, Name, _, S) ->
	io:format("removexattr(~p, ~s)~n", [Ino, Name]),
	{#fuse_reply_err{err = enosys}, S}.

setlk(_, Ino, Fi, Lock, Sleep, _, S) ->
	io:format("setlk(~p, ~p, ~p, ~p)~n", [Ino, Fi, Lock, Sleep]),
	{#fuse_reply_err{err = enosys}, S}.

setxattr(_, Ino, Name, Value, Flags, _, S) ->
	io:format("setxattr(~p, ~s, ~s, ~p)~n", [Ino, Name, Value, Flags]),
	{#fuse_reply_err{err = enosys}, S}.

symlink(_, Link, Ino, Name, _, S) ->
	io:format("symlink(~s, ~p, ~s)~n", [Link, Ino, Name]),
	{#fuse_reply_err{err = enosys}, S}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Fuserl callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Options) ->
	State = #state{
		inodes = gb_trees:from_orddict([ {1, root_make_node() } ]),
		cache  = gb_trees:empty(),
		dirs   = gb_trees:empty(),
		files  = gb_trees:empty(),
		count  = 2, % 1 is the root inode
		uid    = proplists:get_value(uid, Options, 0),
		gid    = proplists:get_value(gid, Options, 0),
		umask  = bnot proplists:get_value(umask, Options, 8#022)
	},
	{ok, State}.


getattr(_, Ino, _Cont, #state{inodes=Inodes} = S) ->
	#vnode{
		oid     = Oid,
		timeout = Timeout,
		ifc     = #ifc{getattr=GetAttr}
	} = gb_trees:get(Ino, Inodes),
	Reply = case catch GetAttr(Oid) of
		{ok, Attr1} ->
			Attr2 = fixup_attr(Attr1, Ino, S),
			{#fuse_reply_attr{attr=Attr2, attr_timeout_ms=Timeout}, S};

		{error, Error} ->
			{#fuse_reply_err{err=Error}, S}
	end,
	?DEBUG(io:format("getattr(~p) -> ~p~n", [Ino, element(1, Reply)])),
	Reply.


lookup(_, Parent, Name, _Cont, S) ->
	LookupOp = fun(ParentNode) ->
		#vnode{oid=Oid, ifc=#ifc{lookup=Lookup}, cache=Cache} = ParentNode,
		Lookup(Oid, Name, Cache)
	end,
	Reply = case do_lookup(Parent, LookupOp, S) of
		{ok, ChildIno, ChildNode, ParentTimeout, S2} ->
			case make_entry_param(ChildIno, ChildNode, ParentTimeout, S2) of
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
	#vnode{
		oid    = Oid,
		parent = Parent,
		ifc    = #ifc{opendir=OpenDir},
		cache  = Cache
	} = Node = gb_trees:get(Ino, Inodes),
	Reply = case catch OpenDir(Oid, Cache) of
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
					inodes = gb_trees:update(Ino, Node#vnode{cache=NewCache},
						Inodes)
				}
			};

		{error, Error, NewCache} ->
			{
				#fuse_reply_err{ err = Error },
				S#state{
					inodes = gb_trees:update(Ino, Node#vnode{cache=NewCache},
						Inodes)
				}
			};

		{error, Error} ->
			{#fuse_reply_err{ err = Error }, S}
	end,
	Reply.


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
	#vnode{oid=Oid, ifc=#ifc{open=Open}} = gb_trees:get(Ino, Inodes),
	Reply = case catch Open(Oid, Fi#fuse_file_info.flags) of
		{ok, Handler} ->
			{Id, S2} = add_file_handler(Handler, S),
			{
				#fuse_reply_open{fuse_file_info=Fi#fuse_file_info{fh = Id}},
				S2
			};

		{error, Error} ->
			{#fuse_reply_err{ err = Error }, S}
	end,
	?DEBUG(io:format("open(~p, ~p) -> ~p~n", [Ino, Fi, element(1, Reply)])),
	Reply.


create(_, Parent, Name, _Mode, Fi, _Cont, S) ->
	LookupOp = fun(ParentNode) ->
		#vnode{oid=Oid, ifc=#ifc{create=Create}, cache=Cache} = ParentNode,
		Create(Oid, Name, Cache)
	end,
	Reply = case do_lookup(Parent, LookupOp, S) of
		{ok, ChildIno, ChildNode, ParentTimeout, S2} ->
			create_open(ChildIno, ChildNode, ParentTimeout, Fi, S2);

		{error, Error, S2} ->
			{#fuse_reply_err{ err = Error }, S2}
	end,
	?DEBUG(io:format("create(~s, ~p) ->~n    ~p~n", [Name, Fi, element(1, Reply)])),
	Reply.


create_open(ChildIno, ChildNode, ParentTimeout, Fi, S) ->
	case make_entry_param(ChildIno, ChildNode, ParentTimeout, S) of
		{ok, EntryParam} ->
			#vnode{oid=Oid, ifc=#ifc{open=Open}} = ChildNode,
			case catch Open(Oid, Fi#fuse_file_info.flags) of
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
	case catch Read(Size, Offset) of
		{ok, Data} -> {#fuse_reply_buf{buf=Data, size=erlang:size(Data)}, S};
		{error, Error} -> {#fuse_reply_err{ err = Error }, S}
	end.


write(_, _Ino, Data, Offset, Fi, _Cont, #state{files=Files} = S) ->
	Id = Fi#fuse_file_info.fh,
	#handler{write=Write, changed=Changed} = Handler = gb_trees:get(Id, Files),
	case catch Write(Data, Offset) of
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
			#vnode{timeout=Timeout} = VNode = gb_trees:get(Ino, Inodes),
			case catch do_setattr(Ino, VNode, Attr, ToSet, S) of
				{ok, NewAttr} ->
					{#fuse_reply_attr{attr=NewAttr, attr_timeout_ms=Timeout}, S};

				{error, Error} ->
					{#fuse_reply_err{err=Error}, S}
			end;

		#fuse_file_info{} ->
			{#fuse_reply_err{err = enosys}, S}
	end,
	?DEBUG(io:format("setattr(~p, ~p, ~p, ~p) ->~n    ~p~n", [Ino, Attr, ToSet, Fi, element(1, Reply)])),
	Reply.


rename(_, OldParent, OldName, NewParent, NewName, _, S) ->
	Inodes = S#state.inodes,
	#vnode{
		oid   = OldOid,
		ifc   = #ifc{lookup=Lookup},
		cache = OldCache1
	} = OldParentNode = gb_trees:get(OldParent, Inodes),

	Reply = case catch Lookup(OldOid, OldName, OldCache1) of
		{entry, ChildOid, OldCache2} ->
			#vnode{oid=NewOid, ifc=#ifc{link=NewLink}} =
				gb_trees:get(NewParent, Inodes),

			% first link in new parent
			case catch NewLink(NewOid, ChildOid, NewName, OldCache2) of
				{ok, OldCache3} ->
					S2 = S#state{inodes=gb_trees:update(OldParent,
						OldParentNode#vnode{cache=OldCache3}, Inodes)},
					% then unlink form old parent
					do_unlink(OldParent, OldName, S2);

				{error, Error, OldCache3} ->
					S2 = S#state{inodes=gb_trees:update(OldParent,
						OldParentNode#vnode{cache=OldCache3}, Inodes)},
					{#fuse_reply_err{ err = Error }, S2};

				{error, Error} ->
					S2 = S#state{inodes=gb_trees:update(OldParent,
						OldParentNode#vnode{cache=OldCache2}, Inodes)},
					{#fuse_reply_err{ err = Error }, S2}
			end;

		{error, Error, OldCache2} ->
			S2 = S#state{inodes=gb_trees:update(OldParent,
				OldParentNode#vnode{cache=OldCache2}, Inodes)},
			{#fuse_reply_err{ err = Error }, S2};

		{error, Error} ->
			{#fuse_reply_err{ err = Error }, S}
	end,
	?DEBUG(io:format("rename(~p, ~s, ~p, ~s) -> ~p~n", [OldParent, OldName, NewParent, NewName, element(1, Reply)])),
	Reply.


unlink(_, Parent, Name, _, S) ->
	Reply = do_unlink(Parent, Name, S),
	?DEBUG(io:format("unlink(~p, ~s) -> ~p~n", [Parent, Name, element(1, Reply)])),
	Reply.


link(_, Ino, NewParent, NewName, _, S) ->
	Inodes = S#state.inodes,
	#vnode{oid=ChildOid} = gb_trees:get(Ino, Inodes),
	#vnode{
		oid     = ParentOid,
		ifc     = #ifc{link=Link, getnode=GetNode},
		cache   = ParentCache,
		timeout = Timeout
	} = ParentNode = gb_trees:get(NewParent, Inodes),
	Reply = case catch Link(ParentOid, ChildOid, NewName, ParentCache) of
		{ok, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(NewParent,
				ParentNode#vnode{cache=NewCache}, Inodes)},
			case do_lookup_new(NewParent, ChildOid, GetNode, Timeout, S2) of
				{ok, ChildIno, ChildNode, Timeout, S3} ->
					case make_entry_param(ChildIno, ChildNode, Timeout, S3) of
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
				ParentNode#vnode{cache=NewCache}, Inodes)},
			{#fuse_reply_err{ err = Error }, S2};

		{error, Error} ->
			{#fuse_reply_err{ err = Error }, S}
	end,
	?DEBUG(io:format("link(~p, ~p, ~s) -> ~p~n", [Ino, NewParent, NewName, element(1, Reply)])),
	Reply.


mkdir(_, Parent, Name, _Mode, _, S) ->
	Inodes = S#state.inodes,
	#vnode{
		oid     = ParentOid,
		ifc     = #ifc{mkdir=MkDir, getnode=GetNode},
		cache   = ParentCache,
		timeout = Timeout
	} = ParentNode = gb_trees:get(Parent, Inodes),
	Reply = case catch MkDir(ParentOid, Name, ParentCache) of
		{ok, ChildOid, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(Parent,
				ParentNode#vnode{cache=NewCache}, Inodes)},
			case do_lookup_new(Parent, ChildOid, GetNode, Timeout, S2) of
				{ok, ChildIno, ChildNode, Timeout, S3} ->
					case make_entry_param(ChildIno, ChildNode, Timeout, S3) of
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
			S2 = S#state{inodes=gb_trees:update(Parent,
				ParentNode#vnode{cache=NewCache}, Inodes)},
			{#fuse_reply_err{ err = Error }, S2};

		{error, Error} ->
			{#fuse_reply_err{ err = Error }, S}
	end,
	?DEBUG(io:format("mkdir(~p, ~s) -> ~p~n", [Parent, Name, element(1, Reply)])),
	Reply.


rmdir(_, Parent, Name, _, S) ->
	% FIXME: unlink only directory documents, not everything
	Reply = do_unlink(Parent, Name, S),
	?DEBUG(io:format("rmdir(~p, ~s) -> ~p~n", [Parent, Name, element(1, Reply)])),
	Reply.


statfs(_, Ino, _, #state{inodes=Inodes}=S) ->
	#vnode{oid=Oid} = gb_trees:get(Ino, Inodes),
	FsStat = case Oid of
		{doc, Store, _Doc} ->
			store:statfs(Store);
		{rev, Store, _Rev} ->
			store:statfs(Store);
		_ ->
			{ok, #fs_stat{
				bsize  = 512,
				blocks = 2048,
				bfree  = 2048,
				bavail = 2048
			}}
	end,
	Reply = case FsStat of
		{ok, Stat} ->
			{
				#fuse_reply_statfs{statvfs=#statvfs{
					f_bsize  = Stat#fs_stat.bsize,
					f_frsize = Stat#fs_stat.bsize,
					f_blocks = Stat#fs_stat.blocks,
					f_bfree  = Stat#fs_stat.bfree,
					f_bavail = Stat#fs_stat.bavail
				}},
				S
			};
		{error, Reason} ->
			{#fuse_reply_err{err=Reason}, S}
	end,
	?DEBUG(io:format("statfs(~p) -> ~w~n", [Ino, element(1, Reply)])),
	Reply.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Common FUSE functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_unlink(Parent, Name, S) ->
	Inodes = S#state.inodes,
	#vnode{
		oid   = Oid,
		ifc   = #ifc{unlink=Unlink},
		cache = Cache
	} = ParentNode = gb_trees:get(Parent, Inodes),
	case catch Unlink(Oid, Name, Cache) of
		{ok, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(Parent,
				ParentNode#vnode{cache=NewCache}, Inodes)},
			{#fuse_reply_err{ err = ok }, S2};

		{error, Error, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(Parent,
				ParentNode#vnode{cache=NewCache}, Inodes)},
			{#fuse_reply_err{ err = Error }, S2};

		{error, Error} ->
			{#fuse_reply_err{ err = Error }, S}
	end.


do_lookup(Parent, LookupOp, S) ->
	#state{inodes=Inodes, cache=Cache} = S,
	ParentNode = gb_trees:get(Parent, Inodes),
	case catch LookupOp(ParentNode) of
		{entry, ChildOid, NewParentCache} ->
			S2 = S#state{inodes=gb_trees:update(Parent,
				ParentNode#vnode{cache=NewParentCache}, Inodes)},
			#vnode{timeout=Timeout, ifc=#ifc{getnode=GetNode}} = ParentNode,
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
					ParentNode#vnode{cache=NewParentCache}, Inodes)}
			};

		{error, Error} ->
			{error, Error, S}
	end.


do_lookup_cached(ChildIno, Timeout, #state{inodes=Inodes}=S) ->
	ChildNode = gb_trees:get(ChildIno, Inodes),
	S2 = S#state{
		inodes = gb_trees:update(
			ChildIno,
			ChildNode#vnode{refcnt=ChildNode#vnode.refcnt+1},
			Inodes)
	},
	{ok, ChildIno, ChildNode, Timeout, S2}.


do_lookup_new(ParentIno, ChildOid, GetNode, Timeout, S) ->
	#state{inodes=Inodes, cache=Cache, count=Count} = S,
	NewCount = Count+1,
	case catch GetNode(ChildOid) of
		{ok, ChildNode} ->
			S2 = S#state{
				inodes = gb_trees:insert(
					NewCount,
					ChildNode#vnode{refcnt=1, parent=ParentIno},
					Inodes),
				cache = gb_trees:insert({ParentIno, ChildOid}, NewCount, Cache),
				count = NewCount
			},
			{ok, NewCount, ChildNode, Timeout, S2};

		error ->
			{error, enoent, S}
	end.


make_entry_param(ChildIno, ChildNode, ParentTimeout, S) ->
	#vnode{
		oid     = ChildOid,
		ifc     = #ifc{ getattr = GetAttr },
		timeout = ChildTimeout
	} = ChildNode,
	case catch GetAttr(ChildOid) of
		{ok, Attr1} ->
			Attr2 = fixup_attr(Attr1, ChildIno, S),
			{
				ok,
				#fuse_entry_param{
					ino              = ChildIno,
					generation       = 1,
					attr_timeout_ms  = ChildTimeout,
					entry_timeout_ms = ParentTimeout,
					attr             = Attr2
				}
			};

		{error, _} ->
			error
	end.


do_forget(Ino, N, #state{inodes=Inodes, cache=Cache} = State) ->
	#vnode{refcnt=RefCnt} = Node = gb_trees:get(Ino, Inodes),
	case RefCnt - N of
		0 ->
			?DEBUG(io:format("forget(~p)~n", [Ino])),
			Key = {Node#vnode.parent, Node#vnode.oid},
			State#state{
				inodes = gb_trees:delete(Ino, Inodes),
				cache  = gb_trees:delete(Key, Cache)
			};

		NewRef ->
			State#state{
				inodes = gb_trees:update(Ino, Node#vnode{refcnt=NewRef}, Inodes)
			}
	end.


add_file_handler(Handler, #state{files=Files} = S) ->
	Id = case gb_trees:is_empty(Files) of
		true  -> 1;
		false -> {Max, _} = gb_trees:largest(Files), Max+1
	end,
	{Id, S#state{files = gb_trees:enter(Id, Handler, Files)}}.


do_setattr(Ino, VNode, NewAttr, ToSet, S) ->
	#vnode{
		oid     = Oid,
		ifc     = #ifc{getattr=GetAttr, truncate=Truncate}
	} = VNode,
	Attr1 = if
		(ToSet band ?FUSE_SET_ATTR_SIZE) =/= 0 ->
			case Truncate(Oid, NewAttr#stat.st_size) of
				{ok, TmpAttr}-> fixup_attr(TmpAttr, Ino, S);
				Error -> throw(Error)
			end;

		true ->
			case GetAttr(Oid) of
				{ok, TmpAttr} -> fixup_attr(TmpAttr, Ino, S);
				Error -> throw(Error)
			end
	end,
	if
		(((ToSet band ?FUSE_SET_ATTR_UID) =/= 0) and
		 (NewAttr#stat.st_uid =/= Attr1#stat.st_uid)) ->
			throw({error, eperm});

		(((ToSet band ?FUSE_SET_ATTR_GID) =/= 0) and
		 (NewAttr#stat.st_gid =/= Attr1#stat.st_gid)) ->
			throw({error, eperm});

		% TODO: maybe not a good idea...
		%(ToSet band ?FUSE_SET_ATTR_MODE) =/= 0 ->
		%	throw({error, eperm});

		true ->
			ok
	end,
	Attr2 = if
		(ToSet band ?FUSE_SET_ATTR_ATIME) =/= 0 ->
			Attr1#stat{st_atime=NewAttr#stat.st_atime};
		true ->
			Attr1
	end,
	Attr3 = if
		(ToSet band ?FUSE_SET_ATTR_MTIME) =/= 0 ->
			Attr2#stat{st_mtime=NewAttr#stat.st_mtime};
		true ->
			Attr2
	end,
	{ok, Attr3}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Root directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

root_make_node() ->
	#vnode{
		refcnt  = 1,
		parent  = 1,
		timeout = 3000,
		oid     = stores,
		ifc     = #ifc{
			getattr = fun(_) -> {ok, ?DIRATTR} end,
			lookup  = fun stores_lookup/3,
			getnode = fun stores_getnode/1,
			opendir = fun stores_opendir/2
		}
	}.


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
	case fuse_store:lookup(Store, Doc) of
		{ok, Rev} ->
			case fuse_store:stat(Store, Rev) of
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
	{ok, #vnode{
		timeout = 1000,
		oid     = Oid,
		ifc     = #ifc{
			getattr = fun dict_getattr/1,
			lookup  = fun dict_lookup/3,
			getnode = fun dict_getnode/1,
			opendir = fun dict_opendir/2,
			create  = fun dict_create/3,
			link    = fun dict_link/4,
			unlink  = fun dict_unlink/3,
			mkdir   = fun dict_mkdir/3
		},
		cache = {undefined, undefined}
	}}.


dict_getattr({doc, Store, Doc}) ->
	case fuse_store:lookup(Store, Doc) of
		{ok, Rev} ->
			case fuse_store:stat(Store, Rev) of
				{ok, #rev_stat{mtime=Mtime}} ->
					{ok, #stat{
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

				{ok, _} ->
					{error, eacces, NewCache};

				error ->
					case create_empty_file(Store, Name) of
						{ok, Handle, NewDoc, NewRev} ->
							try
								Update = fun(Dict) ->
									dict:store(Name, {dlink, NewDoc, [NewRev]}, Dict)
								end,
								case dict_update(Store, Doc, NewCache, Update) of
									{ok, AddCache} ->
										{entry, {doc, Store, NewDoc}, AddCache};
									{error, _Reason, _AddCache} = Error ->
										Error
								end
							after
								store:close(Handle)
							end;

						{error, Reason} ->
							{error, Reason, NewCache}
					end
			end;

		_ ->
			{error, eacces}
	end.


dict_mkdir({doc, Store, Doc}, Name, Cache) ->
	case dict_read_entries(Store, Doc, Cache) of
		{ok, Entries, NewCache} ->
			case dict:find(Name, Entries) of
				{ok, _} ->
					{error, eexist, NewCache};

				error ->
					case create_empty_directory(Store, Name) of
						{ok, Handle, NewDoc, NewRev} ->
							try
								Update = fun(Dict) ->
									dict:store(Name, {dlink, NewDoc, [NewRev]}, Dict)
								end,
								case dict_update(Store, Doc, NewCache, Update) of
									{ok, AddCache} ->
										{ok, {doc, Store, NewDoc}, AddCache};
									{error, _Reason, _AddCache} = Error ->
										Error
								end
							after
								store:close(Handle)
							end;

						{error, Reason} ->
							{error, Reason, NewCache}
					end
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
		{ok, #vnode{ifc=#ifc{getattr=GetAttr}}} ->
			case catch GetAttr(Oid) of
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
	case fuse_store:lookup(Store, Doc) of
		{ok, CacheRev} ->
			{ok, CacheEntries, Cache};

		{ok, Rev} ->
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


dict_link({doc, Store, Doc}, {doc, ChildStore, ChildDoc}, Name, Cache)
	when Store =:= ChildStore ->
	case fuse_store:lookup(Store, ChildDoc) of
		{ok, ChildRev} ->
			Update = fun(Entries) ->
				dict:store(Name, {dlink, ChildDoc, [ChildRev]}, Entries)
			end,
			dict_update(Store, Doc, Cache, Update);

		error ->
			{error, enoent}
	end;

dict_link(_, _, _, _) ->
	{error, eacces}.


dict_unlink({doc, Store, Doc}, Name, Cache) ->
	Update = fun(Entries) -> dict:erase(Name, Entries) end,
	dict_update(Store, Doc, Cache, Update).


dict_update(Store, Doc, Cache, Fun) ->
	case fuse_store:open_doc(Store, Doc, true) of
		{ok, Rev, Handle} ->
			case dict_update_cache(Handle, Rev, Cache) of
				{ok, Entries, NewCache} ->
					NewEntries = Fun(Entries),
					dict_write_entries(Handle, NewEntries, NewCache);

				Error ->
					Error
			end;

		Error ->
			Error
	end.


dict_update_cache(_Handle, Rev, {Rev, Struct} = Cache) ->
	{ok, Struct, Cache};

dict_update_cache(Handle, Rev, _Cache) ->
	case read_struct(Handle, <<"HPSD">>) of
		{ok, Struct} when is_record(Struct, dict, 9) ->
			{ok, Struct, {Rev, Struct}};
		{ok, _} ->
			{error, einval};
		Error ->
			Error
	end.


dict_write_entries(Handle, Entries, Cache) ->
	try
		case write_struct(Handle, <<"HPSD">>, Entries) of
			ok ->
				case fuse_store:commit(Handle) of
					{ok, Rev} ->
						{ok, {Rev, Entries}};
					{error, Reason} ->
						{error, Reason, Cache}
				end;

			{error, Error} ->
				{error, Error, Cache}
		end
	after
		fuse_store:close(Handle)
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Set documents
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

doc_make_node_set(Oid) ->
	{ok, #vnode{
		timeout = 1000,
		oid     = Oid,
		ifc     = #ifc{
			getattr = fun set_getattr/1,
			lookup  = fun set_lookup/3,
			getnode = fun set_getnode/1,
			opendir = fun set_opendir/2
		},
		cache = {undefined, undefined}
	}}.


set_getattr({doc, Store, Doc}) ->
	case fuse_store:lookup(Store, Doc) of
		{ok, Rev} ->
			case fuse_store:stat(Store, Rev) of
				{ok, #rev_stat{mtime=Mtime}} ->
					{ok, #stat{
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
		{ok, #vnode{ifc=#ifc{getattr=GetAttr}}} ->
			case catch GetAttr(Oid) of
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
	case fuse_store:lookup(Store, Doc) of
		{ok, CacheRev} ->
			{ok, CacheEntries, Cache};

		{ok, Rev} ->
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
	case fuse_store:lookup(Store, Doc) of
		{ok, Rev} ->
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
	{ok, #vnode{
		timeout = 1000,
		oid     = Oid,
		ifc     = #ifc{
			getattr  = fun file_getattr/1,
			truncate = fun file_truncate/2,
			open     = fun file_open/2
		}
	}}.


file_getattr({doc, Store, Doc}) ->
	case fuse_store:lookup(Store, Doc) of
		{ok, Rev} ->
			file_getattr_rev(Store, Rev);
		error ->
			{error, enoent}
	end.


file_getattr_rev(Store, Rev) ->
	case fuse_store:stat(Store, Rev) of
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


file_truncate({doc, Store, Doc}, Size) ->
	case fuse_store:open_doc(Store, Doc, true) of
		{ok, _Rev, Handle} ->
			try
				case fuse_store:truncate(Handle, <<"FILE">>, Size) of
					ok ->
						case fuse_store:commit(Handle) of
							{ok, CurRev} ->
								file_getattr_rev(Store, CurRev);
							Error ->
								Error
						end;

					{error, _} = Error ->
						Error
				end
			after
				fuse_store:close(Handle)
			end;

		{error, _} = Error ->
			Error
	end.


file_open({doc, Store, Doc}, Flags) ->
	Write = (Flags band ?O_ACCMODE) =/= ?O_RDONLY,
	case fuse_store:open_doc(Store, Doc, Write) of
		{ok, _Rev, Handle} ->
			{ok, #handler{
				read = fun(Size, Offset) ->
					file_read(Handle, Size, Offset)
				end,
				write = fun(Data, Offset) ->
					file_write(Handle, Data, Offset)
				end,
				release = fun(Changed) ->
					file_release(Handle, Changed)
				end
			}};

		{error, _} = Error ->
			Error
	end.


file_read(Handle, Size, Offset) ->
	case fuse_store:read(Handle, <<"FILE">>, Offset, Size) of
		{ok, _Data} = R  -> R;
		{error, enoent}  -> {ok, <<>>};
		{error, _}       -> {error, eio}
	end.


file_write(Handle, Data, Offset) ->
	fuse_store:write(Handle, <<"FILE">>, Offset, Data).


file_release(Handle, Changed) ->
	Reply = case Changed of
		false -> ok;
		true  -> fuse_store:commit(Handle)
	end,
	fuse_store:close(Handle),
	Reply.


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


read_struct(Handle, Part) ->
	case read_struct_loop(Handle, Part, 0, <<>>) of
		{ok, Data} ->
			case catch struct:decode(Data) of
				{'EXIT', _Reason} ->
					{error, einval};
				Struct ->
					{ok, Struct}
			end;

		{error, Reason} ->
			{error, Reason}
	end.


read_struct_loop(Handle, Part, Offset, Acc) ->
	Length = 16#10000,
	case fuse_store:read(Handle, Part, Offset, Length) of
		{ok, _Error, <<>>} ->
			{ok, Acc};
		{ok, _Errors, Data} ->
			read_struct_loop(Handle, Part, Offset+size(Data), <<Acc/binary, Data/binary>>);
		{error, Reason, _Errors} ->
			{error, Reason}
	end.


write_struct(Handle, Part, Struct) ->
	Data = struct:encode(Struct),
	case fuse_store:truncate(Handle, Part, 0) of
		ok ->
			fuse_store:write(Handle, Part, 0, Data);
		{error, _} = Error ->
			Error
	end.


create_empty_file(Store, Name) ->
	Doc = crypto:rand_bytes(16),
	MetaData = dict:store(
		<<"org.hotchpotch.annotation">>,
		dict:store(
			<<"title">>,
			Name,
			dict:store(
				<<"comment">>,
				<<"Created by FUSE interface">>,
				dict:new())),
		dict:new()),
	case store:create(Store, Doc, <<"public.text">>, ?FUSE_CC) of
		{ok, Handle} ->
			store:write(Handle, <<"META">>, 0, struct:encode(MetaData)),
			store:write(Handle, <<"FILE">>, 0, <<>>),
			case store:commit(Handle, util:get_time()) of
				{ok, Rev} ->
					{ok, Handle, Doc, Rev};
				{error, _} = Error ->
					store:close(Handle),
					Error
			end;

		Error ->
			Error
	end.


create_empty_directory(Store, Name) ->
	Doc = crypto:rand_bytes(16),
	MetaData = dict:store(
		<<"org.hotchpotch.annotation">>,
		dict:store(
			<<"title">>,
			Name,
			dict:store(
				<<"comment">>,
				<<"Created by FUSE interface">>,
				dict:new())),
		dict:new()),
	case store:create(Store, Doc, <<"org.hotchpotch.dict">>, ?FUSE_CC) of
		{ok, Handle} ->
			store:write(Handle, <<"META">>, 0, struct:encode(MetaData)),
			store:write(Handle, <<"HPSD">>, 0, struct:encode(dict:new())),
			case store:commit(Handle, util:get_time()) of
				{ok, Rev} ->
					{ok, Handle, Doc, Rev};
				{error, _} = Error ->
					store:close(Handle),
					Error
			end;

		Error ->
			Error
	end.


fixup_attr(Attr, Ino, #state{uid=Uid, gid=Gid, umask=Umask}) ->
	Attr#stat{
		st_ino  = Ino,
		st_uid  = Uid,
		st_gid  = Gid,
		st_mode = Attr#stat.st_mode band Umask
	}.

-endif.
