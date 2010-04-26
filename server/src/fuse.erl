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

-record(inode, {refcnt, parent, timeout, oid, ifc}).
-record(ifc, {getattr, lookup, getnode, opendir}).

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
		{value, #inode{oid=Oid, timeout=Timeout, ifc=#ifc{getattr=GetAttr}}} ->
			case GetAttr(Oid, Ino) of
				{ok, Attr} ->
					{#fuse_reply_attr{attr=Attr, attr_timeout_ms=Timeout}, S};
				error ->
					{#fuse_reply_err{err=enoent}, S}
			end;
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
							case ((ChildNode#inode.ifc)#ifc.getattr)(ChildOid, ChildIno) of
								{ok, Attr} ->
									{#fuse_reply_entry{fuse_entry_param=
										#fuse_entry_param{
											ino              = ChildIno,
											generation       = 1,
											attr_timeout_ms  = ChildNode#inode.timeout,
											entry_timeout_ms = Timeout,
											attr             = Attr
										}},
										S#state{
											inodes = gb_trees:update(ChildIno,
												ChildNode#inode{refcnt=ChildNode#inode.refcnt+1},
												Inodes)
										}
									};

								error ->
									{#fuse_reply_err{ err = enoent }, S}
							end;

						none ->
							NewCount = Count+1,
							case GetNode(ChildOid) of
								{ok, #inode{ifc=#ifc{getattr=GetAttr}}=ChildNode} ->
									case GetAttr(ChildOid, NewCount) of
										{ok, Attr} ->
											{#fuse_reply_entry{fuse_entry_param=
												#fuse_entry_param{
													ino              = NewCount,
													generation       = 1,
													attr_timeout_ms  = ChildNode#inode.timeout,
													entry_timeout_ms = Timeout,
													attr             = Attr
												}},
												S#state{
													inodes = gb_trees:insert(NewCount,
														ChildNode#inode{refcnt=1, parent=Parent},
														Inodes),
													cache = gb_trees:insert({Parent, ChildOid},
														NewCount, Cache),
													count = NewCount
												}
											};
										error ->
											{#fuse_reply_err{ err = enoent }, S}
									end;

								error ->
									{#fuse_reply_err{ err = enoent }, S}
							end
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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Root directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

root_make_node() ->
	#inode{
		refcnt  = 1,
		parent  = 1,
		timeout = 1000,
		oid     = root,
		ifc     = #ifc{
			getattr = fun(_,_) -> {ok, ?DIRATTR(1)} end,
			lookup  = fun root_lookup/2,
			getnode = fun root_getnode/1,
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

root_getnode(docs) ->
	docs_make_node();
root_getnode(revs) ->
	revs_make_node();
root_getnode(stores) ->
	stores_make_node().

root_opendir(root) ->
	{ok, [
		#direntry{ name = "docs",   stat = ?DIRATTR(0) },
		#direntry{ name = "revs",   stat = ?DIRATTR(0) },
		#direntry{ name = "stores", stat = ?DIRATTR(0) }
	]}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Generic empty directory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

emptydir_make_node(Oid) ->
	{ok, #inode{
		timeout = 1000,
		oid     = Oid,
		ifc     = #ifc{
			getattr = fun(_, Ino) -> {ok, ?DIRATTR(Ino)} end,
			lookup  = fun(_, _) -> error end,
			getnode = fun(_) -> error end,
			opendir = fun(_) -> {ok, []} end
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
		timeout = 5,
		oid     = stores,
		ifc     = #ifc{
			getattr = fun(_,Ino) -> {ok, ?DIRATTR(Ino)} end,
			lookup  = fun stores_lookup/2,
			getnode = fun stores_getnode/1,
			opendir = fun stores_opendir/1
		}
	}}.


stores_lookup(stores, Name) ->
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
		{value, Oid} -> {entry, Oid};
		none         -> error
	end.


stores_getnode(Oid) ->
	doc_make_node(Oid).


stores_opendir(stores) ->
	Stores = lists:map(
		fun({Id, _Descr, _Guid, _Tags}) ->
			#direntry{ name = atom_to_list(Id), stat = ?DIRATTR(0) }
		end,
		lists:filter(
			fun({_Id, _Descr, _Guid, Tags}) ->
				proplists:is_defined(mounted, Tags)
			end,
			volman:enum())),
	{ok, Stores}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Documents
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

doc_make_node({doc, Store, Uuid} = Oid) ->
	case store:lookup(Store, Uuid) of
		{ok, Rev} ->
			case store:stat(Store, Rev) of
				{ok, _Flags, _Parts, _Parents, _Mtime, Uti} ->
					case Uti of
						<<"org.hotchpotch.volume">> ->
							doc_make_node_dict(Oid);
						<<"org.hotchpotch.dict">> ->
							doc_make_node_dict(Oid);
						<<"org.hotchpotch.set">> ->
							doc_make_node_set(Oid);
						_ ->
							doc_make_node_file(Oid)
					end;

				error ->
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
		timeout = 5,
		oid     = Oid,
		ifc     = #ifc{
			getattr = fun dict_getattr/2,
			lookup  = fun dict_lookup/2,
			getnode = fun dict_getnode/1,
			opendir = fun dict_opendir/1
		}
	}}.


dict_getattr({doc, Store, Uuid}, Ino) ->
	case store:lookup(Store, Uuid) of
		{ok, Rev} ->
			case store:stat(Store, Rev) of
				{ok, _Flags, _Parts, _Parents, Mtime, _Uti} ->
					{ok, #stat{
						st_ino   = Ino,
						st_mode  = ?S_IFDIR bor 8#0555,
						st_nlink = 1,
						st_atime = Mtime,
						st_mtime = Mtime,
						st_ctime = Mtime
					}};
				error ->
					error
			end;
		error ->
			error
	end.


dict_lookup({doc, Store, Uuid}, Name) ->
	case dict_read_entries(Store, Uuid) of
		{ok, Entries} ->
			case dict:find(Name, Entries) of
				{ok, {dlink, ChildUuid, _Revs}} ->
					{entry, {doc, Store, ChildUuid}};

				_ ->
					error
			end;

		_ ->
			error
	end.


dict_getnode({doc, _Store, _Uuid} = Oid) ->
	doc_make_node(Oid);
dict_getnode(_) ->
	error.


dict_opendir({doc, Store, Uuid}) ->
	case dict_read_entries(Store, Uuid) of
		{ok, Entries} ->
			Content = map_filter(
				fun(E) -> dict_opendir_filter(Store, E) end,
				dict:to_list(Entries)),
			{ok, Content};

		error ->
			error
	end.


dict_opendir_filter(Store, {Name, {dlink, Child, _Revs}}) ->
	Oid = {doc, Store, Child},
	case doc_make_node(Oid) of
		{ok, #inode{ifc=#ifc{getattr=GetAttr}}} ->
			case GetAttr(Oid, 0) of
				{ok, Attr} ->
					{ok, #direntry{name=binary_to_list(Name), stat=Attr}};
				error ->
					skip
			end;
		error ->
			skip
	end;
dict_opendir_filter(_, _) ->
	skip.


dict_read_entries(Store, Uuid) ->
	case store:lookup(Store, Uuid) of
		{ok, Rev} ->
			case util:read_rev_struct(Rev, <<"HPSD">>) of
				{ok, Entries}=Result when is_record(Entries, dict, 9) ->
					Result;
				{ok, _} ->
					error;
				{error, _} ->
					error
			end;
		error ->
			error
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Set documents
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

doc_make_node_set(Oid) ->
	{ok, #inode{
		timeout = 5,
		oid     = Oid,
		ifc     = #ifc{
			getattr = fun set_getattr/2,
			lookup  = fun set_lookup/2,
			getnode = fun set_getnode/1,
			opendir = fun set_opendir/1
		}
	}}.


set_getattr({doc, Store, Uuid}, Ino) ->
	case store:lookup(Store, Uuid) of
		{ok, Rev} ->
			case store:stat(Store, Rev) of
				{ok, _Flags, _Parts, _Parents, Mtime, _Uti} ->
					{ok, #stat{
						st_ino   = Ino,
						st_mode  = ?S_IFDIR bor 8#0555,
						st_nlink = 1,
						st_atime = Mtime,
						st_mtime = Mtime,
						st_ctime = Mtime
					}};
				error ->
					error
			end;
		error ->
			error
	end.


set_lookup({doc, Store, Uuid}, Name) ->
	case set_read_entries(Store, Uuid) of
		{ok, Entries} ->
			case find_entry(fun(E) -> set_lookup_cmp(Store, Name, E) end, Entries) of
				{value, Oid} -> {entry, Oid};
				none         -> error
			end;

		_ ->
			error
	end.


set_lookup_cmp(Store, Name, {dlink, Child, _Revs}) ->
	case list_to_binary(set_read_title(Store, Child)) of
		Name -> {ok, {doc, Store, Child}};
		_    -> error
	end;

set_lookup_cmp(_, _, _) ->
	error.


set_getnode({doc, _Store, _Uuid} = Oid) ->
	doc_make_node(Oid);
set_getnode(_) ->
	error.


set_opendir({doc, Store, Uuid}) ->
	case set_read_entries(Store, Uuid) of
		{ok, Entries} ->
			Content = map_filter(
				fun(E) -> set_opendir_filter(Store, E) end,
				Entries),
			{ok, Content};

		error ->
			error
	end.


set_opendir_filter(Store, {dlink, Child, _Revs}) ->
	Oid = {doc, Store, Child},
	case doc_make_node(Oid) of
		{ok, #inode{ifc=#ifc{getattr=GetAttr}}} ->
			case GetAttr(Oid, 0) of
				{ok, Attr} ->
					Name = set_read_title(Store, Child),
					{ok, #direntry{name=Name, stat=Attr}};
				error ->
					skip
			end;
		error ->
			skip
	end;
set_opendir_filter(_, _) ->
	skip.


set_read_title(Store, Uuid) ->
	case set_read_title_meta(Store, Uuid) of
		{ok, Title} -> binary_to_list(Title);
		error       -> "." ++ util:bin_to_hexstr(Uuid)
	end.

set_read_title_meta(Store, Uuid) ->
	case store:lookup(Store, Uuid) of
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


set_read_entries(Store, Uuid) ->
	case store:lookup(Store, Uuid) of
		{ok, Rev} ->
			case util:read_rev_struct(Rev, <<"HPSD">>) of
				{ok, Entries}=Result when is_list(Entries) ->
					Result;
				{ok, _} ->
					error;
				{error, _} ->
					error
			end;
		error ->
			error
	end.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% All other documents
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

doc_make_node_file(Oid) ->
	{ok, #inode{
		timeout = 5,
		oid     = Oid,
		ifc     = #ifc{
			getattr = fun(_, Ino) ->
				{ok, #stat{
					st_ino   = Ino,
					st_mode  = ?S_IFREG bor 8#0444,
					st_nlink = 1
				}}
			end,
			lookup  = fun(_, _) -> error end,
			getnode = fun(_, _) -> error end,
			opendir = fun(_) -> {errror, enotdir} end
		}
	}}.


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

