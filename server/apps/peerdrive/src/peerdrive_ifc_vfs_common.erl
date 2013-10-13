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

-module(peerdrive_ifc_vfs_common).

-export([init/1, getattr/2, lookup/3, forget/3, open/4, create/6, opendir/2,
	close/2, read/4, write/4, readdir/2, setattr/3, unlink/3, rename/5, link/4,
	mkdir/3, statfs/2]).

-include("store.hrl").
-include("vfs.hrl").
-include("utils.hrl").
-include("volman.hrl").

-define(VFS_CC, <<"org.peerdrive.vfs">>).  % creator code

%% inode: integer number identifying a vnode
%% vnode: structure describing the file system object
%% OID: object identifier for peerdrive doc/rev:
%%      {doc, Store::guid(), Doc::guid()} | {rev, Store::guid(), Rev::guid()}

-record(state, {
	inodes,  % gb_trees: inode -> #vnode
	imap,    % gb_trees: OID -> inode
	count,   % int: next free inode
	ephemeral % re:mp()
}).

-record(vnode, {
	refcnt,  % int:  how many time lookup'ed by fuse (reference count)
	parent,  % int:  parent inode
	timeout, % int:  timeout of this node's information in ms
	ifc,     % #ifc: file system functions of node
	oid,     % term: information to find corresponding peerdrive object
	cache    % private information of #ifc functions
}).

-record(ifc, {
	getattr,
	truncate = fun(_, _) -> {error, enosys} end,
	setmtime = fun(_, _) -> ok end,
	lookup   = fun(_, _, _) -> {error, enoent} end,
	getnode  = fun(_, _) -> error end,
	readdir  = fun(_, _) -> {errror, eio} end,
	open     = fun(_, _, _) -> {error, eisdir} end,
	create   = fun(_, _, _, _, _) -> {error, enotdir} end,
	link     = fun(_, _, _, _) -> {error, enotdir} end,
	unlink   = fun(_, _, _) -> {error, eacces} end,
	mkdir    = fun(_, _, _, _) -> {error, enotdir} end,
	rename   = fun(_, _, _, _, _) -> {error, enotdir} end
}).

-record(handler, {read, write, release, changed=false, rewritten=false}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Options) ->
	EphemeralList = [""] ++ proplists:get_value(ephemeral, Options, [
		"thumbs\\.db", "desktop\\.ini",  % Windows
		"\\.ds_store",                   % Mac
		"~\\$.*", "~.*\\.tmp"            % Temporary stuff
		]),
	EphemeralPat = string:join([ "(^" ++ E ++ "$)" || E <- EphemeralList ], "|"),
	EphemeralRE = case re:compile(EphemeralPat, [unicode, caseless]) of
		{ok, MP} ->
			MP;
		{error, ErrSpec} ->
			error_logger:warning_report([{module, ?MODULE},
				{error, "Ephemeral list did not compile"}, {reason, ErrSpec}]),
			{ok, MP} = re:compile("^$", [unicode]),
			MP
	end,
	#state{
		inodes    = gb_trees:from_orddict([ {1, root_make_node() } ]),
		imap      = gb_trees:empty(),
		count     = 2, % 1 is the root inode
		ephemeral = EphemeralRE
	}.


getattr(Ino, #state{inodes=Inodes} = S) ->
	#vnode{
		oid     = Oid,
		timeout = Timeout,
		ifc     = #ifc{getattr=GetAttr}
	} = gb_trees:get(Ino, Inodes),
	case catch GetAttr(Oid) of
		{ok, Attr} ->
			{ok, {Attr, Timeout}, S};

		{error, Reason} ->
			{error, Reason, S}
	end.


lookup(Parent, Name, S) ->
	LookupOp = fun(ParentNode) ->
		#vnode{oid=Oid, ifc=#ifc{lookup=Lookup}, cache=Cache} = ParentNode,
		Lookup(Oid, Name, Cache)
	end,
	case do_lookup(Parent, LookupOp, S) of
		{ok, ChildIno, ChildNode, ParentTimeout, S2} ->
			case make_entry(ChildIno, ChildNode, ParentTimeout) of
				{ok, Entry} ->
					{ok, Entry, S2};

				error ->
					{error, enoent, do_forget(ChildIno, 1, S2)}
			end;

		{error, _Error, _S2} = Error ->
			Error
	end.


forget(Ino, N, S) ->
	{ok, ok, do_forget(Ino, N, S)}.


open(Ino, Trunc, Mode, #state{inodes=Inodes} = S) ->
	#vnode{oid=Oid, ifc=#ifc{open=Open}} = gb_trees:get(Ino, Inodes),
	case catch Open(Oid, Trunc, Mode) of
		{ok, Handler} ->
			{ok, Handler, S};

		{error, Error} ->
			{error, Error, S}
	end.


create(Parent, Name, MustCreate, Trunc, Mode, S) ->
	LookupOp = fun(ParentNode) ->
		#vnode{oid=Oid, ifc=#ifc{create=Create}, cache=Cache} = ParentNode,
		Create(Oid, Name, Cache, MustCreate, S)
	end,
	case do_lookup(Parent, LookupOp, S) of
		{ok, ChildIno, ChildNode, ParentTimeout, S2} ->
			case make_entry(ChildIno, ChildNode, ParentTimeout) of
				{ok, Entry} ->
					case open(ChildIno, Trunc, Mode, S2) of
						{ok, Handler, S3} ->
							% FIXME: return correct 'Existed' attribute
							{ok, {Entry, false, Handler}, S3};
						{error, Reason, S3} ->
							{error, Reason, do_forget(ChildIno, 1, S3)}
					end;

				error ->
					{error, enoent, do_forget(ChildIno, 1, S2)}
			end;

		{error, _Error, _S2} = Error ->
			Error
	end.


opendir(Ino, S) ->
	case getattr(Ino, S) of
		{ok, {#vfs_attr{dir=true}, _Timeout}, S2} ->
			{ok, Ino, S2};
		{ok, _, S2} ->
			{error, enotdir, S2};
		Error ->
			Error
	end.


close(_S, #handler{release=Release, changed=Changed, rewritten=Rewritten}) ->
	Release(Changed, Rewritten),
	{ok, ok};

close(_S, _Handler) ->
	{ok, ok}.


read(_S, #handler{read=Read}, Size, Offset) ->
	case catch Read(Size, Offset) of
		{ok, Data} -> {ok, Data};
		{error, Error} -> {error, Error}
	end;

read(_S, _Handle, _Size, _Offset) ->
	{error, ebadf}.


write(S, #handler{} = Handler, Data, Offset) ->
	#handler{
		write     = Write,
		changed   = Changed,
		rewritten = Rewritten
	} = Handler,
	case catch Write(Data, Offset) of
		ok ->
			if
				Changed and (Rewritten or (Offset > 0)) ->
					{ok, size(Data)};
				true ->
					NewHandler = Handler#handler{
						changed   = true,
						rewritten = Rewritten or (Offset == 0)
					},
					{ok, size(Data), S, NewHandler}
			end;

		{error, _Reason} = Error ->
			Error
	end;

write(_S, _Handle, _Data, _Offset) ->
	{error, ebadf}.


readdir(#state{inodes=Inodes} = S, Ino) when is_integer(Ino) ->
	#vnode{
		oid    = Oid,
		ifc    = #ifc{readdir=ReadDir},
		cache  = Cache
	} = Node = gb_trees:get(Ino, Inodes),
	case catch ReadDir(Oid, Cache) of
		{ok, Entries, NewCache} ->
			AllEntries = [
				#vfs_direntry{name= <<".">>,  attr=#vfs_attr{dir=true, size=0}},
				#vfs_direntry{name= <<"..">>, attr=#vfs_attr{dir=true, size=0}}
			] ++ Entries,
			S2 = S#state{
				inodes = gb_trees:update(Ino, Node#vnode{cache=NewCache}, Inodes)
			},
			{ok, AllEntries, S2, AllEntries};

		{error, Error, NewCache} ->
			S2 = S#state{
				inodes = gb_trees:update(Ino, Node#vnode{cache=NewCache}, Inodes)
			},
			{error, Error, S2, Ino};

		{error, Error} ->
			{error, Error}
	end;

readdir(_S, Listing) when is_list(Listing) ->
	{ok, Listing};

readdir(_S, _Handle) ->
	{error, ebadf}.


setattr(Ino, Changes, #state{inodes=Inodes} = S) ->
	#vnode{
		oid     = Oid,
		timeout = Timeout,
		ifc     = #ifc{getattr=GetAttr, truncate=Truncate, setmtime=SetMTime}
	} = gb_trees:get(Ino, Inodes),
	try
		Attr1 = case proplists:get_value(size, Changes) of
			undefined ->
				case GetAttr(Oid) of
					{ok, TmpAttr} -> TmpAttr;
					Error         -> throw(Error)
				end;
			NewSize when is_integer(NewSize) ->
				case Truncate(Oid, NewSize) of
					{ok, TmpAttr} -> TmpAttr;
					Error         -> throw(Error)
				end
		end,
		Attr2 = case proplists:get_value(atime, Changes) of
			undefined ->
				Attr1;
			NewATime when is_integer(NewATime) ->
				Attr1#vfs_attr{atime=NewATime}
		end,
		Attr3 = case proplists:get_value(mtime, Changes) of
			undefined ->
				Attr2;
			NewMTime when is_integer(NewMTime) ->
				case SetMTime(Oid, NewMTime) of
					ok -> Attr2#vfs_attr{mtime=NewMTime};
					Error2 -> throw(Error2)
				end
		end,
		Attr4 = case proplists:get_value(crtime, Changes) of
			undefined ->
				Attr3;
			NewCTime when is_integer(NewCTime) ->
				Attr3#vfs_attr{crtime=NewCTime}
		end,
		{ok, {Attr4, Timeout}, S}

	catch
		throw:{error, Reason} -> {error, Reason, S}
	end.


rename(Parent, OldName, Parent, NewName, S) ->
	Inodes = S#state.inodes,
	#vnode{
		oid   = ParentOid,
		ifc   = #ifc{rename=Rename},
		cache = ParentCache
	} = ParentNode = gb_trees:get(Parent, Inodes),
	case catch Rename(ParentOid, OldName, NewName, ParentCache, S) of
		{ok, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(Parent,
				ParentNode#vnode{cache=NewCache}, Inodes)},
			{ok, ok, S2};

		{error, Error, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(Parent,
				ParentNode#vnode{cache=NewCache}, Inodes)},
			{error, Error, S2};

		{error, Error} ->
			{error, Error, S}
	end;

rename(OldParent, OldName, NewParent, NewName, S) ->
	Inodes = S#state.inodes,
	#vnode{
		oid   = OldOid,
		ifc   = #ifc{lookup=Lookup},
		cache = OldCache1
	} = OldParentNode = gb_trees:get(OldParent, Inodes),

	case catch Lookup(OldOid, OldName, OldCache1) of
		{entry, ChildOid, OldCache2} ->
			#vnode{oid=NewOid, ifc=#ifc{link=NewLink}} =
				gb_trees:get(NewParent, Inodes),

			% first link in new parent
			case catch NewLink(NewOid, ChildOid, NewName, OldCache2) of
				{ok, OldCache3} ->
					S2 = S#state{inodes=gb_trees:update(OldParent,
						OldParentNode#vnode{cache=OldCache3}, Inodes)},
					% then unlink form old parent
					unlink(OldParent, OldName, S2);

				{error, Error, OldCache3} ->
					S2 = S#state{inodes=gb_trees:update(OldParent,
						OldParentNode#vnode{cache=OldCache3}, Inodes)},
					{error, Error, S2};

				{error, Error} ->
					S2 = S#state{inodes=gb_trees:update(OldParent,
						OldParentNode#vnode{cache=OldCache2}, Inodes)},
					{error, Error, S2}
			end;

		{error, Error, OldCache2} ->
			S2 = S#state{inodes=gb_trees:update(OldParent,
				OldParentNode#vnode{cache=OldCache2}, Inodes)},
			{error, Error, S2};

		{error, Error} ->
			{error, Error, S}
	end.


link(Ino, NewParent, NewName, S) ->
	Inodes = S#state.inodes,
	#vnode{oid=ChildOid} = gb_trees:get(Ino, Inodes),
	#vnode{
		oid     = ParentOid,
		ifc     = #ifc{link=Link},
		cache   = ParentCache,
		timeout = Timeout
	} = ParentNode = gb_trees:get(NewParent, Inodes),
	case catch Link(ParentOid, ChildOid, NewName, ParentCache) of
		{ok, NewCache} ->
			Lookup = fun(_) -> {entry, ChildOid, NewCache} end,
			case do_lookup(NewParent, Lookup, S) of
				{ok, ChildIno, ChildNode, Timeout, S2} ->
					case make_entry(ChildIno, ChildNode, Timeout) of
						{ok, Entry} ->
							{ok, Entry, S2};

						error ->
							{error, enoent, do_forget(ChildIno, 1, S2)}
					end;

				{error, Error, S2} ->
					{error, Error, S2}
			end;

		{error, Error, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(NewParent,
				ParentNode#vnode{cache=NewCache}, Inodes)},
			{error, Error, S2};

		{error, Error} ->
			{error, Error, S}
	end.


unlink(Parent, Name, S) ->
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
			{ok, ok, S2};

		{error, Error, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(Parent,
				ParentNode#vnode{cache=NewCache}, Inodes)},
			{error, Error, S2};

		{error, Error} ->
			{error, Error, S}
	end.


mkdir(Parent, Name, S) ->
	Inodes = S#state.inodes,
	#vnode{
		oid     = ParentOid,
		ifc     = #ifc{mkdir=MkDir, getnode=GetNode},
		cache   = ParentCache,
		timeout = Timeout
	} = ParentNode = gb_trees:get(Parent, Inodes),
	case catch MkDir(ParentOid, Name, ParentCache, S) of
		{ok, ChildOid, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(Parent,
				ParentNode#vnode{cache=NewCache}, Inodes)},
			case do_lookup_new(Parent, ChildOid, GetNode, Timeout, S2) of
				{ok, ChildIno, ChildNode, Timeout, S3} ->
					case make_entry(ChildIno, ChildNode, Timeout) of
						{ok, Entry} ->
							{ok, Entry, S3};

						error ->
							{error, enoent, do_forget(ChildIno, 1, S3)}
					end;

				{error, Error, S3} ->
					{error, Error, S3}
			end;

		{error, Error, NewCache} ->
			S2 = S#state{inodes=gb_trees:update(Parent,
				ParentNode#vnode{cache=NewCache}, Inodes)},
			{error, Error, S2};

		{error, Error} ->
			{error, Error, S}
	end.


statfs(Ino, S) ->
	#vnode{oid=Oid} = gb_trees:get(Ino, S#state.inodes),
	Reply = case Oid of
		{doc, Store, _Doc} ->
			case peerdrive_volman:store(Store) of
				{ok, Pid} -> peerdrive_store:statfs(Pid);
				error     -> {error, enoent}
			end;
		{rev, Store, _Rev} ->
			case peerdrive_volman:store(Store) of
				{ok, Pid} -> peerdrive_store:statfs(Pid);
				error     -> {error, enoent}
			end;
		_ ->
			% TODO: report real values
			{ok, #fs_stat{
				bsize  = 512,
				blocks = 2048000,
				bfree  = 2048000,
				bavail = 2048000
			}}
	end,
	case Reply of
		{ok, Stat}      -> {ok, Stat, S};
		{error, Reason} -> {error, Reason, S}
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Common local functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


do_lookup(Parent, LookupOp, S) ->
	#state{inodes=Inodes, imap=IMap} = S,
	ParentNode = gb_trees:get(Parent, Inodes),
	case catch LookupOp(ParentNode) of
		{entry, ChildOid, NewParentCache} ->
			S2 = S#state{inodes=gb_trees:update(Parent,
				ParentNode#vnode{cache=NewParentCache}, Inodes)},
			#vnode{timeout=Timeout, ifc=#ifc{getnode=GetNode}} = ParentNode,
			case gb_trees:lookup(ChildOid, IMap) of
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
	#state{inodes=Inodes, imap=IMap, count=Count} = S,
	NewCount = Count+1,
	case catch GetNode(ChildOid) of
		{ok, ChildNode} ->
			S2 = S#state{
				inodes = gb_trees:insert(
					NewCount,
					ChildNode#vnode{refcnt=1, parent=ParentIno},
					Inodes),
				imap = gb_trees:insert(ChildOid, NewCount, IMap),
				count = NewCount
			},
			{ok, NewCount, ChildNode, Timeout, S2};

		error ->
			{error, enoent, S}
	end.


do_forget(Ino, N, #state{inodes=Inodes, imap=IMap} = State) ->
	#vnode{refcnt=RefCnt, oid=OId} = Node = gb_trees:get(Ino, Inodes),
	case RefCnt - N of
		0 ->
			State#state{
				inodes = gb_trees:delete(Ino, Inodes),
				imap   = gb_trees:delete(OId, IMap)
			};

		NewRef ->
			State#state{
				inodes = gb_trees:update(Ino, Node#vnode{refcnt=NewRef}, Inodes)
			}
	end.


make_entry(ChildIno, ChildNode, ParentTimeout) ->
	#vnode{
		oid     = ChildOid,
		ifc     = #ifc{ getattr = GetAttr },
		timeout = ChildTimeout
	} = ChildNode,
	case catch GetAttr(ChildOid) of
		{ok, Attr} ->
			{
				ok,
				#vfs_entry{
					ino       = ChildIno,
					attr_tmo  = ChildTimeout,
					entry_tmo = ParentTimeout,
					attr      = Attr
				}
			};

		{error, _} ->
			error
	end.


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
			getattr = fun(_) -> {ok, #vfs_attr{dir=true}} end,
			lookup  = fun stores_lookup/3,
			getnode = fun stores_getnode/1,
			readdir = fun stores_readdir/2
		}
	}.


stores_lookup(stores, Name, Cache) ->
	case Name of
		<<".sys">> ->
			#peerdrive_store{sid=Guid, pid=Pid} = peerdrive_volman:sys_store(),
			{entry, {doc, Pid, Guid}, Cache};

		_ ->
			case find_entry(
				fun(#peerdrive_store{label=Id, sid=Guid, pid=Pid}) ->
					BinId = unicode:characters_to_binary(Id),
					if
						BinId == Name ->
							{ok, {doc, Pid, Guid}};
						true ->
							error
					end
				end,
				peerdrive_volman:enum())
			of
				{value, Oid} -> {entry, Oid, Cache};
				none         -> {error, enoent}
			end
	end.


stores_getnode(Oid) ->
	case doc_make_node(Oid) of
		{ok, VNode} ->
			#vnode{ifc=OldIfc} = VNode,
			NewIfc = OldIfc#ifc{
				lookup  = fun(ObjId, Name, Cache) ->
					storewrap_lookup(ObjId, Name, Cache, OldIfc#ifc.lookup)
				end,
				getnode = fun(ObjId) ->
					storewrap_getnode(ObjId, OldIfc#ifc.getnode)
				end,
				readdir = fun(ObjId, Cache) ->
					storewrap_readdir(ObjId, Cache, OldIfc#ifc.readdir)
				end
			},
			{ok, VNode#vnode{ifc=NewIfc}};

		Error ->
			Error
	end.


stores_readdir(stores, Cache) ->
	Stores = lists:map(
		fun(#peerdrive_store{label=Id}) ->
			#vfs_direntry{name=unicode:characters_to_binary(Id), attr=#vfs_attr{dir=true}}
		end,
		peerdrive_volman:enum()),
	Sys = #vfs_direntry{name = <<".sys">>, attr=#vfs_attr{dir=true}},
	{ok, [Sys | Stores], Cache}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Store wrapper: adds a '.docs' directory to each store
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

storewrap_lookup({doc, Store, _SId} = Oid, Name, Cache, Lookup) ->
	case Name of
		<<".docs">> ->
			{entry, {docsdir, Store}, Cache};
		_Else ->
			Lookup(Oid, Name, Cache)
	end.


storewrap_getnode({docsdir, Store}, _GetNode) ->
	docsdir_make_node(Store);

storewrap_getnode(Oid, GetNode) ->
	GetNode(Oid).


storewrap_readdir(Oid, Cache, ReadDir) ->
	case ReadDir(Oid, Cache) of
		{ok, Entries, NewCache} ->
			DocEntry = #vfs_direntry{name= <<".docs">>, attr=#vfs_attr{dir=true}},
			{ok, [DocEntry | Entries], NewCache};

		Else ->
			Else
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% .docs directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

docsdir_make_node(Store) ->
	{ok, #vnode{
		refcnt  = 1,
		timeout = 30000,
		oid     = {docsdir, Store},
		ifc     = #ifc{
			getattr = fun(_) -> {ok, #vfs_attr{dir=true}} end,
			lookup  = fun docsdir_lookup/3,
			getnode = fun docsdir_getnode/1,
			readdir = fun docsdir_readdir/2
		}
	}}.


docsdir_lookup({docsdir, Store}, Name, Cache) ->
	case parse_name_to_uuid(Name) of
		{ok, Uuid} ->
			{entry, {docdir, Store, Uuid}, Cache};
		error ->
			{error, enoent}
	end.


docsdir_getnode({docdir, Store, Uuid}) ->
	docdir_make_node(Store, Uuid).


docsdir_readdir({docsdir, _Store}, Cache) ->
	{ok, [], Cache}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Document virtual directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

docdir_make_node(Store, Uuid) ->
	{ok, #vnode{
		refcnt  = 1,
		timeout = 1000,
		oid     = {docdir, Store, Uuid},
		cache   = {undefined, undefined},
		ifc     = #ifc{
			getattr = fun(_) -> {ok, #vfs_attr{dir=true}} end,
			lookup  = fun docdir_lookup/3,
			getnode = fun docdir_getnode/1,
			readdir = fun docdir_readdir/2
		}
	}}.


docdir_lookup({docdir, Store, Doc}, Name, Cache) ->
	case docdir_read_entry(Store, Doc, Cache) of
		{ok, Name, NewCache} ->
			{entry, {doc, Store, Doc}, NewCache};

		{ok, _OtherName, NewCache} ->
			{error, enoent, NewCache};

		error ->
			{error, enoent}
	end.


docdir_getnode(Oid) ->
	doc_make_node(Oid).


docdir_readdir({docdir, Store, Doc}, Cache) ->
	case docdir_read_entry(Store, Doc, Cache) of
		{ok, Name, NewCache} ->
			Oid = {doc, Store, Doc},
			case doc_make_node(Oid) of
				{ok, #vnode{ifc=#ifc{getattr=GetAttr}}} ->
					case catch GetAttr(Oid) of
						{ok, Attr} ->
							{ok, [#vfs_direntry{name=Name, attr=Attr}], NewCache};
						{error, _} ->
							{ok, [], NewCache}
					end;
				error ->
					{ok, [], NewCache}
			end;

		error ->
			{ok, [], Cache}
	end.


docdir_read_entry(Store, Doc, {CacheRev, CacheEntry}=Cache) ->
	case peerdrive_ifc_vfs_broker:lookup(Store, Doc) of
		{ok, CacheRev} ->
			{ok, CacheEntry, Cache};

		{ok, Rev} ->
			case read_file_name(Store, Doc, Rev) of
				{ok, Name} -> {ok, Name, {Rev, Name}};
				error      -> error
			end;

		error ->
			error
	end.


read_file_name(Store, _Doc, Rev) ->
	case peerdrive_util:read_rev_struct(Store, Rev, <<"/org.peerdrive.annotation/title">>) of
		{ok, Title} when is_binary(Title) ->
			{ok, Title};
		_ ->
			error
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Documents
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

doc_make_node({doc, Store, Doc} = Oid) ->
	case peerdrive_ifc_vfs_broker:lookup(Store, Doc) of
		{ok, Rev} ->
			case peerdrive_ifc_vfs_broker:stat(Store, Rev) of
				{ok, #rev{type=Type}} ->
					case Type of
						<<"org.peerdrive.store">> ->
							doc_make_node_folder(Oid);
						<<"org.peerdrive.folder">> ->
							doc_make_node_folder(Oid);
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
%% Folder documents
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(fe, {oid, rev, disp, title, suffix, orig=gb_trees:empty()}).

doc_make_node_folder(Oid) ->
	{ok, #vnode{
		timeout = 1000,
		oid     = Oid,
		ifc     = #ifc{
			getattr = fun folder_getattr/1,
			lookup  = fun folder_lookup/3,
			getnode = fun folder_getnode/1,
			readdir = fun folder_readdir/2,
			create  = fun folder_create/5,
			link    = fun folder_link/4,
			unlink  = fun folder_unlink/3,
			mkdir   = fun folder_mkdir/4,
			rename  = fun folder_rename/5
		},
		cache = {undefined, undefined, {0, 0, 0}}
	}}.


folder_getattr({doc, Store, Doc}) ->
	case peerdrive_ifc_vfs_broker:lookup(Store, Doc) of
		{ok, Rev} ->
			case peerdrive_ifc_vfs_broker:stat(Store, Rev) of
				{ok, #rev{mtime=Mtime, crtime=CrTime}} ->
					{ok, #vfs_attr{
						dir    = true,
						atime  = Mtime,
						mtime  = Mtime,
						ctime  = Mtime,
						crtime = CrTime
					}};
				Error ->
					Error
			end;
		error ->
			{error, enoent}
	end.


folder_lookup({doc, Store, Doc}, Name, Cache) ->
	case folder_read_entries(Store, Doc, Cache) of
		{ok, Entries, NewCache} ->
			case folder_find_name(Name, Entries) of
				{ok, Oid} -> {entry, Oid, NewCache};
				error     -> {error, enoent, NewCache}
			end;

		_ ->
			{error, enoent}
	end.


folder_getnode({doc, _Store, _Doc} = Oid) ->
	doc_make_node(Oid);
folder_getnode(_) ->
	error.


folder_readdir({doc, Store, Doc}, Cache) ->
	case folder_read_entries(Store, Doc, Cache) of
		{ok, Entries, NewCache} ->
			Content = map_filter(fun folder_readdir_filter/1, Entries),
			{ok, Content, NewCache};

		error ->
			{error, enoent}
	end.


folder_create({doc, Store, Doc}, Name, Cache, MustCreate, S) ->
	case folder_read_entries(Store, Doc, Cache) of
		{ok, Entries, NewCache} ->
			case folder_find_name(Name, Entries) of
				{ok, {doc, _, _}=ChildOid} ->
					case MustCreate of
						true ->
							{error, eexist, NewCache};
						false ->
							{entry, ChildOid, NewCache}
					end;

				{ok, _} ->
					{error, eacces, NewCache};

				error ->
					case create_empty_file(Store, Name, S) of
						{ok, Handle, NewDoc, NewRev} ->
							try
								NewEntry = #fe{
									oid    = {doc, Store, NewDoc},
									rev    = NewRev,
									title  = Name,
									suffix = peerdrive_util:bin_to_hexstr(NewDoc)
								},
								Update = fun(List) ->
									[NewEntry | List]
								end,
								case folder_update(Store, Doc, NewCache, Update) of
									{ok, AddCache} ->
										{entry, {doc, Store, NewDoc}, AddCache};
									{error, _Reason, _AddCache} = Error ->
										Error
								end
							after
								peerdrive_broker:close(Handle)
							end;

						{error, Reason} ->
							{error, Reason, NewCache}
					end
			end;

		_ ->
			{error, enoent}
	end.


folder_mkdir({doc, Store, Doc}, Name, Cache, S) ->
	case folder_read_entries(Store, Doc, Cache) of
		{ok, Entries, NewCache} ->
			case folder_find_name(Name, Entries) of
				{ok, _} ->
					{error, eexist, NewCache};

				error ->
					case create_empty_directory(Store, Name, S) of
						{ok, Handle, NewDoc, NewRev} ->
							try
								NewEntry = #fe{
									oid    = {doc, Store, NewDoc},
									rev    = NewRev,
									title  = Name,
									suffix = peerdrive_util:bin_to_hexstr(NewDoc)
								},
								Update = fun(List) ->
									[NewEntry | List]
								end,
								case folder_update(Store, Doc, NewCache, Update) of
									{ok, AddCache} ->
										{ok, {doc, Store, NewDoc}, AddCache};
									{error, _Reason, _AddCache} = Error ->
										Error
								end
							after
								peerdrive_broker:close(Handle)
							end;

						{error, Reason} ->
							{error, Reason, NewCache}
					end
			end;

		_ ->
			{error, enoent}
	end.


folder_link({doc, Store, ParentDoc}, {doc, Store, ChildDoc}, Name, Cache) ->
	{Title, _DocId} = folder_split_name(Name),
	ChildRev = case peerdrive_ifc_vfs_broker:lookup(Store, ChildDoc) of
		{ok, Rev} ->
			case folder_read_title(Store, Rev) of
				Title -> Rev;
				_     -> throw({error, eacces})
			end;

		error ->
			throw({error, enoent})
	end,
	NewEntry = #fe{
		oid    = {doc, Store, ChildDoc},
		rev    = ChildRev,
		title  = Title,
		suffix = peerdrive_util:bin_to_hexstr(ChildDoc)
	},
	case folder_read_entries(Store, ParentDoc, Cache) of
		{ok, Entries, NewCache} ->
			Update = case folder_find_name(Name, Entries) of
				{ok, Oid} ->
					fun(List) -> [NewEntry | lists:keydelete(Oid, #fe.oid, List)] end;
				error ->
					fun(List) -> [NewEntry | List] end
			end,
			folder_update(Store, ParentDoc, NewCache, Update);

		_ ->
			{error, enoent}
	end;

folder_link(_, _, _, _) ->
	{error, eacces}.


folder_unlink({doc, Store, Doc}, Name, Cache) ->
	case folder_read_entries(Store, Doc, Cache) of
		{ok, Entries, NewCache} ->
			case folder_find_name(Name, Entries) of
				{ok, Oid} ->
					Update = fun(List) -> lists:keydelete(Oid, #fe.oid, List) end,
					folder_update(Store, Doc, NewCache, Update);

				error ->
					{error, enoent, NewCache}
			end;

		_ ->
			{error, enoent}
	end.


folder_rename({doc, Store, Doc}, OldName, NewName, Cache, S) ->
	case folder_read_entries(Store, Doc, Cache) of
		{ok, Entries, NewCache} ->
			case folder_find_name(OldName, Entries) of
				{ok, {doc, Store, ChildDoc}} ->
					case folder_set_title(Store, ChildDoc, NewName, S) of
						ok ->
							FinalCache = folder_invalidate_cache(NewCache),
							% Did we replace a file?
							case folder_find_name(NewName, Entries) of
								{ok, Oid} ->
									Update = fun(List) -> lists:keydelete(Oid, #fe.oid, List) end,
									folder_update(Store, Doc, FinalCache, Update);

								error ->
									{ok, FinalCache}
							end;

						{error, Reason} ->
							{error, Reason, NewCache}
					end;

				{ok, _} ->
					{error, eacces, NewCache};
				error ->
					{error, enoent, NewCache}
			end;

		_ ->
			{error, enoent}
	end.


folder_find_name(FullName, Entries) ->
	{Name, DocId} = folder_split_name(FullName),
	case find_entry(fun(E) -> folder_lookup_cmp(Name, DocId, E) end, Entries) of
		{value, Oid} -> {ok, Oid};
		none         -> error
	end.


folder_split_name(Name) ->
	% FIXME: precompile
	RegExp = <<"(.*)~([[:xdigit:]]+)(\\.\\w+)?">>,
	case re:run(Name, RegExp, [{capture, all_but_first, binary}, unicode]) of
		{match, [Title, DocId, Extension]} ->
			{<<Title/binary, Extension/binary>>, safe_characters_to_list(DocId)};
		{match, [Title, DocId]} ->
			{Title, safe_characters_to_list(DocId)};
		nomatch ->
			{Name, ""}
	end.


folder_lookup_cmp(Name, DocId, #fe{oid=Oid, title=Title, suffix=Suffix}) ->
	case Name of
		Title ->
			case lists:prefix(DocId, Suffix) of
				true  -> {ok, Oid};
				false -> error
			end;
		_ ->
			error
	end;

folder_lookup_cmp(_, _, _) ->
	error.


folder_readdir_filter(#fe{oid={doc,_,_}=Oid, disp=Name}) ->
	case doc_make_node(Oid) of
		{ok, #vnode{ifc=#ifc{getattr=GetAttr}}} ->
			case catch GetAttr(Oid) of
				{ok, Attr} ->
					{ok, #vfs_direntry{name=Name, attr=Attr}};
				{error, _} ->
					skip
			end;
		error ->
			skip
	end;

folder_readdir_filter(_) ->
	skip.


%% Folders are special. First we have to check if the folder itself has
%% changed. Then we have to lookup every child document if it has changed and
%% re-read the title if so. In any case we have to eliminate duplicates and
%% sanitize the names.
%%
%% Entry list format: [{Oid, Rev, DispTitle, RealTitle, Suffix}]
%% Cache format: {Rev, Entries}
%%
folder_read_entries(Store, Doc, {CacheRev, CacheEntries, CacheTime}=OldCache) ->
	case peerdrive_ifc_vfs_broker:lookup(Store, Doc) of
		{ok, CacheRev} ->
			case folder_need_update(CacheTime) of
				true ->
					NewCacheEntries = folder_update_entries(CacheEntries),
					{ok, NewCacheEntries, {CacheRev, NewCacheEntries, now()}};
				false ->
					{ok, CacheEntries, OldCache}
			end;

		{ok, Rev} ->
			case peerdrive_util:read_rev_struct(Store, Rev, <<"/org.peerdrive.folder">>) of
				{ok, List} when is_list(List) ->
					Entries = folder_read_entries_list(Store, List),
					{ok, Entries, {Rev, Entries, now()}};

				{ok, _} ->
					error;
				{error, _} ->
					error
			end;

		error ->
			error
	end.


folder_need_update({CacheMS, CacheS, CacheUS}) ->
	{NowMS, NowS, NowUS} = now(),
	Delta = (NowMS-CacheMS)*1000000 + (NowS-CacheS) + (NowUS-CacheUS)/1000000,
	Delta > 1.


folder_update_entries(Cache) ->
	case lists:foldl(fun folder_find_update/2, [], Cache) of
		[] ->
			Cache;
		Updates ->
			NewCache = folder_apply_updates(Cache, lists:reverse(Updates), []),
			folder_sanitize_entries(NewCache, 0)
	end.


folder_find_update(#fe{oid={doc, Store, Doc}, rev=CacheRev}=Entry, Acc) ->
	case peerdrive_ifc_vfs_broker:lookup(Store, Doc) of
		{ok, CacheRev} ->
			Acc;
		{ok, NewRev} ->
			[Entry#fe{rev=NewRev, title=folder_read_title(Store, NewRev)} | Acc];
		error ->
			[Entry#fe{rev=undefined, title= <<"">>} | Acc]
	end.


folder_apply_updates([], _, Acc) ->
	Acc;

folder_apply_updates([#fe{oid=Oid} | Cache], [#fe{oid=Oid}=New | Updates], Acc) ->
	folder_apply_updates(Cache, Updates, [New | Acc]);

folder_apply_updates([Entry | Cache], Updates, Acc) ->
	folder_apply_updates(Cache, Updates, [Entry | Acc]).



folder_read_entries_list(Store, List) ->
	RawEntries = map_filter(
		fun(E) -> folder_read_entries_filter(Store, E) end,
		List),
	folder_sanitize_entries(RawEntries, 0).


folder_read_entries_filter(Store, Entry) when ?IS_GB_TREE(Entry) ->
	case gb_trees:lookup(<<"">>, Entry) of
		{value, {dlink, Doc}} ->
			FE = #fe{
				oid    = {doc, Store, Doc},
				suffix = peerdrive_util:bin_to_hexstr(Doc),
				orig   = Entry
			},
			case peerdrive_ifc_vfs_broker:lookup(Store, Doc) of
				{ok, Rev} ->
					Title = folder_read_title(Store, Rev),
					{ok, FE#fe{rev=Rev, title=Title}};

				error ->
					{ok, FE#fe{title= <<"">>}}
			end;
		none ->
			skip
	end;

folder_read_entries_filter(_, _) ->
	skip.


folder_read_title(Store, Rev) ->
	case peerdrive_util:read_rev_struct(Store, Rev, <<"/org.peerdrive.annotation/title">>) of
		{ok, Title} when is_binary(Title) ->
			unicode:characters_to_binary(sanitize(
				safe_characters_to_list(Title)));
		_ ->
			<<"">>
	end.


folder_sanitize_entries(Cache, SuffixLen) ->
	Dict = lists:foldl(
		fun(#fe{title=Title, suffix=Suffix}=Entry, Acc) ->
			dict:append(folder_apply_suffix(Title, Suffix, SuffixLen), Entry, Acc)
		end,
		dict:new(),
		Cache),
	dict:fold(
		fun(Title, Entries, Acc) ->
			case Entries of
				[Entry] ->
					[Entry#fe{disp=Title} | Acc];
				_ ->
					folder_sanitize_entries(Entries, SuffixLen+4) ++ Acc
			end
		end,
		[],
		Dict).


folder_apply_suffix(Title, _Suffix, 0) ->
	Title;

folder_apply_suffix(Title, Suffix, Len) ->
	BinSuffix = unicode:characters_to_binary(lists:sublist(Suffix, Len)),
	Components = re:split(Title, <<"\\.">>),
	folder_join_components(Components, BinSuffix).


folder_join_components([Title], Suffix) ->
	<<Title/binary, "~", Suffix/binary>>;

folder_join_components([Title, Ext], Suffix) ->
	<<Title/binary, "~", Suffix/binary, ".", Ext/binary>>;

folder_join_components([Comp | Rest], Suffix) ->
	Joined = folder_join_components(Rest, Suffix),
	<<Comp/binary, ".", Joined/binary>>.


folder_update(Store, Doc, Cache, Fun) ->
	case peerdrive_ifc_vfs_broker:open_doc(Store, Doc, true) of
		{ok, Rev, Handle} ->
			case folder_update_cache(Store, Handle, Rev, Cache) of
				{ok, Entries, NewCache} ->
					case Fun(Entries) of
						{error, Reason} ->
							peerdrive_ifc_vfs_broker:abort(Handle),
							{error, Reason, NewCache};
						NewEntries ->
							folder_write_entries(Handle, NewEntries, NewCache)
					end;

				Error ->
					peerdrive_ifc_vfs_broker:abort(Handle),
					Error
			end;

		Error ->
			Error
	end.


folder_update_cache(_Store, _Handle, Rev, {Rev, Entries, _LastUpdate}) ->
	NewEntries = folder_update_entries(Entries),
	{ok, NewEntries, {Rev, NewEntries, now()}};

folder_update_cache(Store, Handle, Rev, _Cache) ->
	case peerdrive_ifc_vfs_broker:get_data(Handle, <<"/org.peerdrive.folder">>) of
		List when is_list(List) ->
			Entries = folder_read_entries_list(Store, List),
			{ok, Entries, {Rev, Entries, now()}};
		{error, _} = Error ->
			Error;
		_ ->
			{error, einval}
	end.


folder_write_entries(Handle, Entries, Cache) ->
	List = [gb_trees:enter(<<"">>, {dlink, Doc}, Entry) ||
		#fe{oid={doc, _, Doc}, orig=Entry} <- Entries],
	case peerdrive_ifc_vfs_broker:set_data(Handle, <<"/org.peerdrive.folder">>, List) of
		ok ->
			case peerdrive_ifc_vfs_broker:close(Handle, <<"Changed through VFS">>) of
				{ok, Rev} ->
					{ok, {Rev, folder_sanitize_entries(Entries, 0), now()}};
				{error, Reason} ->
					{error, Reason, Cache}
			end;

		{error, Error} ->
			peerdrive_ifc_vfs_broker:abort(Handle),
			{error, Error, Cache}
	end.


folder_set_title(Store, Doc, NewTitle, S) ->
	case peerdrive_ifc_vfs_broker:open_doc(Store, Doc, true) of
		{ok, _OldRev, Handle} ->
			try
				Comment = case peerdrive_ifc_vfs_broker:get_data(Handle,
					<<"/org.peerdrive.annotation/title">>)
				of
					{ok, OldTitle} when is_binary(OldTitle) ->
						<<"Renamed from \"", OldTitle/binary,  "\" to \"",
							NewTitle/binary, "\"">>;
					_ ->
						<<"Renamed to \"", NewTitle/binary, "\"">>
				end,
				case peerdrive_ifc_vfs_broker:set_data(Handle,
					<<"/org.peerdrive.annotation/title">>, NewTitle)
				of
					ok    -> ok;
					WrErr -> throw(WrErr)
				end,
				case peerdrive_ifc_vfs_broker:fstat(Handle) of
					{ok, #rev{flags=OldFlags, type=OrigUti}} ->
						case peerdrive_registry:conformes(OrigUti, <<"org.peerdrive.folder">>) of
							false ->
								NewUti = peerdrive_registry:get_uti_from_extension(
									filename:extension(NewTitle)),
								peerdrive_ifc_vfs_broker:set_type(Handle, NewUti);
							true ->
								ok
						end,
						NewFlags = case re:run(NewTitle, S#state.ephemeral) of
							{match, _Captured} -> OldFlags bor ?REV_FLAG_EPHEMERAL;
							nomatch -> OldFlags band (bnot ?REV_FLAG_EPHEMERAL)
						end,
						OldFlags =:= NewFlags orelse peerdrive_ifc_vfs_broker:set_flags(
							Handle, NewFlags);
					_ ->
						ok % ignore this error
				end,
				case peerdrive_ifc_vfs_broker:close(Handle, Comment) of
					{ok, _NewRev} -> ok;
					CloseErr      -> CloseErr
				end
			catch
				throw:Error ->
					peerdrive_ifc_vfs_broker:abort(Handle),
					Error
			end;

		Error ->
			Error
	end.


folder_invalidate_cache({Rev, Entries, _LastUpdate}) ->
	{Rev, Entries, {0, 0, 0}}.

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
			setmtime = fun file_setmtime/2,
			open     = fun file_open/3
		}
	}}.


file_getattr({doc, Store, Doc}) ->
	case peerdrive_ifc_vfs_broker:lookup(Store, Doc) of
		{ok, Rev} ->
			file_getattr_rev(Store, Rev);
		error ->
			{error, enoent}
	end.


file_getattr_rev(Store, Rev) ->
	case peerdrive_ifc_vfs_broker:stat(Store, Rev) of
		{ok, #rev{attachments=Attachments, mtime=Mtime, crtime=CrTime}} ->
			{Size, FileMTime} = case find_entry(
				fun
					(#rev_att{name = <<"_">>, size=Size, mtime=MT}) ->
						{ok, {Size, MT}};
					(_) -> error
				end,
				Attachments)
			of
				{value, Ok} -> Ok;
				none        -> {0, Mtime}
			end,
			{ok, #vfs_attr{
				dir    = false,
				atime  = Mtime,
				mtime  = FileMTime,
				ctime  = Mtime,
				crtime = CrTime,
				size   = Size
			}};

		Error ->
			Error
	end.


file_truncate({doc, Store, Doc}, Size) ->
	case peerdrive_ifc_vfs_broker:open_doc(Store, Doc, true) of
		{ok, _Rev, Handle} ->
			case peerdrive_ifc_vfs_broker:truncate(Handle, <<"_">>, Size) of
				ok ->
					case peerdrive_ifc_vfs_broker:close(Handle) of
						{ok, CurRev} ->
							file_getattr_rev(Store, CurRev);
						Error ->
							Error
					end;

				{error, _} = Error ->
					peerdrive_ifc_vfs_broker:close(Handle),
					Error
			end;

		{error, _} = Error ->
			Error
	end.


file_setmtime({doc, Store, Doc}, MTime) ->
	case peerdrive_ifc_vfs_broker:open_doc(Store, Doc, true) of
		{ok, _Rev, Handle} ->
			case peerdrive_ifc_vfs_broker:set_mtime(Handle, <<"_">>, MTime) of
				Ok when Ok == ok; Ok == {error, enoent} ->
					case peerdrive_ifc_vfs_broker:close(Handle) of
						{ok, _CurRev} -> ok;
						Error -> Error
					end;

				{error, _} = Error ->
					peerdrive_ifc_vfs_broker:close(Handle),
					Error
			end;

		{error, _} = Error ->
			Error
	end.


file_open({doc, Store, Doc}, Trunc, Mode) ->
	Write = Mode =/= read,
	case peerdrive_ifc_vfs_broker:open_doc(Store, Doc, Write) of
		{ok, _Rev, Handle} ->
			Res = case Trunc of
				true  -> peerdrive_ifc_vfs_broker:truncate(Handle, <<"_">>, 0);
				false -> ok
			end,
			case Res of
				ok ->
					{ok, #handler{
						read = fun(Size, Offset) ->
							file_read(Handle, Size, Offset)
						end,
						write = fun(Data, Offset) ->
							file_write(Handle, Data, Offset)
						end,
						release = fun(Changed, Rewritten) ->
							file_release(Handle, Changed, Rewritten)
						end
					}};

				{error, _} = Error ->
					Error
			end;

		{error, _} = Error ->
			Error
	end.


file_read(Handle, Size, Offset) ->
	case peerdrive_ifc_vfs_broker:read(Handle, <<"_">>, Offset, Size) of
		{ok, _Data} = R  -> R;
		{error, enoent}  -> {ok, <<>>};
		{error, _}       -> {error, eio}
	end.


file_write(Handle, Data, Offset) ->
	peerdrive_ifc_vfs_broker:write(Handle, <<"_">>, Offset, Data).


file_release(Handle, Changed, Rewritten) ->
	%case Changed of
	%	false -> ok;
	%	true  ->
	%		if
	%			Rewritten ->
	%				case peerdrive_ifc_vfs_broker:read(Handle, <<"_">>, 0, 4096) of
	%					{ok, Data} ->
	%						peerdrive_ifc_vfs_broker:set_type(Handle, registry:guess(Data));
	%					_Else ->
	%						ok
	%				end;
	%			true ->
	%				ok
	%		end
	%end,
	if
		not Changed ->
			peerdrive_ifc_vfs_broker:close(Handle);
		Rewritten  ->
			peerdrive_ifc_vfs_broker:close(Handle, <<"Overwritten through VFS">>);
		true ->
			peerdrive_ifc_vfs_broker:close(Handle, <<"Changed through VFS">>)
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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


sanitize(S) ->
	lists:filter(fun(C) -> (C /= $/) and (C >= 31) end, S).


create_empty_file(Store, Name, S) ->
	MetaData = gb_trees:enter(
		<<"org.peerdrive.annotation">>,
		gb_trees:enter(
			<<"title">>,
			Name,
			gb_trees:empty()),
		gb_trees:empty()),
	Uti = peerdrive_registry:get_uti_from_extension(filename:extension(Name)),
	Flags = case re:run(Name, S#state.ephemeral) of
		{match, _Captured} -> ?REV_FLAG_EPHEMERAL;
		nomatch -> 0
	end,
	case peerdrive_broker:create(Store, Uti, ?VFS_CC) of
		{ok, Doc, Handle} ->
			peerdrive_broker:set_data(Handle, <<"">>, peerdrive_struct:encode(MetaData)),
			peerdrive_broker:set_flags(Handle, Flags),
			peerdrive_broker:write(Handle, <<"_">>, 0, <<>>),
			case peerdrive_broker:commit(Handle, <<"Created by VFS interface">>) of
				{ok, Rev} ->
					% leave handle open, the caller has to close it
					{ok, Handle, Doc, Rev};
				{error, Reason} ->
					peerdrive_broker:close(Handle),
					{error, Reason}
			end;

		{error, Reason} ->
			{error, Reason}
	end.


create_empty_directory(Store, Name, S) ->
	Data1 = gb_trees:enter(
		<<"org.peerdrive.annotation">>,
		gb_trees:enter(<<"title">>, Name, gb_trees:empty()),
		gb_trees:empty()),
	Data2 = gb_trees:enter(<<"org.peerdrive.folder">>, [], Data1),
	TypeCode = <<"org.peerdrive.folder">>,
	Flags = ?REV_FLAG_STICKY bor case re:run(Name, S#state.ephemeral) of
		{match, _Captured} -> ?REV_FLAG_EPHEMERAL;
		nomatch -> 0
	end,
	case peerdrive_broker:create(Store, TypeCode, ?VFS_CC) of
		{ok, Doc, Handle} ->
			peerdrive_broker:set_data(Handle, <<>>, peerdrive_struct:encode(Data2)),
			peerdrive_broker:set_flags(Handle, Flags),
			case peerdrive_broker:commit(Handle, <<"Created by VFS interface">>) of
				{ok, Rev} ->
					% leave handle open, the caller has to close it
					{ok, Handle, Doc, Rev};
				{error, Reason} ->
					peerdrive_broker:close(Handle),
					{error, Reason}
			end;

		{error, Reason} ->
			{error, Reason}
	end.


parse_name_to_uuid(Name) when is_binary(Name) and (size(Name) == 32) ->
	try
		List = binary_to_list(Name),
		{ok, <<(erlang:list_to_integer(List, 16)):128>>}
	catch
		error:_ -> error
	end;

parse_name_to_uuid(_Name) ->
	error.


safe_characters_to_list(Subject) ->
	case unicode:characters_to_list(Subject) of
		Result when is_list(Result) ->
			Result;
		_ ->
			case unicode:characters_to_list(Subject, latin1) of
				Fallback when is_list(Fallback) ->
					Fallback;
				_ ->
					"invalid encoding"
			end
	end.

