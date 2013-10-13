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

-module(peerdrive_ifc_vfs_dokan).
-export([start_link/1]).

-ifdef(have_dokan).

-export([init/1, handle_info/2, terminate/2, code_change/3]).
-export([close_file/4, create_directory/4, create_file/8, delete_directory/4,
         delete_file/4, find_files/4, get_disk_free_space/3,
         get_file_information/4, get_volume_information/3, move_file/6,
         open_directory/4, read_file/6, set_end_of_file/5,
         set_file_attributes/5, set_file_time/7, write_file/6,
         lock_file/6, unlock_file/6]).


-include_lib("erldokan/include/erldokan.hrl").
-include_lib("erldokan/include/winerror.hrl").
-include("vfs.hrl").
-include("store.hrl").

-record(state, {vfs_state, vnodes, handles, re}).
-record(handle, {parent, name, ino, vfs_handle}).

-record(vnode, {
	ino,      % int(): inode number of vnode
	nlookup,  % int(): number of node lookups
	refcnt,   % int(): number of open handles
	attr,     % #attr{}: node attributes
	attr_tmo, % int(): attribute timeout
	parents,  % [{Ino::int(), Name::binary()}]: parent inodes
	childs    % gb_tree(): child inodes: Name::binary() -> {Ino::int(), Timeout::int()}
}).

% interval of the garbage collector
-define(GC_INTERVAL, 60*1000).

% child entries which have expired longer than this time ago are removed
-define(GC_THRESHOLD, 3*60*1000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(MountOpts) ->
	Path = unicode:characters_to_binary(filename:nativename(filename:absname(
		proplists:get_value(mountpoint, MountOpts, peerdrive_util:cfg_mnt_dir())))),
	DokanOpts = [Opt || Opt <- MountOpts, filter_opt(Opt)],
	erldokan:start_link(?MODULE, {Path, MountOpts}, [{mountpoint, Path} | DokanOpts]).


filter_opt({Tag, _Value}) when (Tag == threads) or
                               (Tag == debug_output) or
                               (Tag == drive_type) ->
	true;

filter_opt(_) ->
	false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Management interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Dir, Opt}) ->
	peerdrive_sys_info:publish(<<"vfs.mountpath">>, Dir),
	case peerdrive_ifc_vfs_common:getattr(1, peerdrive_ifc_vfs_common:init(Opt)) of
		{ok, {Attr, Tmo}, VfsState} ->
			{ok, Re} = re:compile(<<"\\\\"/utf8>>),
			RootNode = #vnode{
				ino      = 1,
				nlookup  = 0,
				refcnt   = 1,
				attr     = Attr,
				attr_tmo = Tmo,
				parents  = [],
				childs   = gb_trees:empty()
			},
			State = #state{
				vfs_state = VfsState,
				vnodes    = gb_trees:from_orddict([{1, RootNode}]),
				handles   = gb_trees:from_orddict([{0, undefined}]),
				re        = Re
			},
			erlang:send_after(?GC_INTERVAL, self(), gc),
			{ok, State};

		{error, Reason, _VfsState} ->
			{stop, Reason}
	end.


handle_info(gc, State) ->
	{noreply, do_gc(State)};

handle_info(_Msg, State) ->
	{noreply, State}.


terminate(_Reason, _State) ->
	ok.


code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

create_file(S, _From, FileName, AccMode, _ShMode, CrDisp, _Flags, _DFI) ->
	case walk(FileName, S) of
		{ok, Parent, Name, Ino, S2} ->
			case get_vnode(Ino, S2) of
				{ok, #vnode{attr=#vfs_attr{dir=IsDir}}, S3} ->
					if
						not IsDir and (CrDisp =/= ?CREATE_NEW) ->
							open(Parent, Name, Ino, AccMode, CrDisp, S3);

						not IsDir ->
							{reply, {error, ?ERROR_FILE_EXISTS}, S3};

						IsDir and (CrDisp == ?OPEN_EXISTING) ->
							opendir(Parent, Name, Ino, true, S3);

						IsDir ->
							{reply, {error, ?ERROR_ACCESS_DENIED}, S3}
					end;

				{error, Reason, S3} ->
					{reply, {error, posix2win(Reason)}, S3}
			end;

		{stop, DirIno, Name, S2} when (CrDisp == ?CREATE_ALWAYS) or
		                              (CrDisp == ?CREATE_NEW) or
		                              (CrDisp == ?OPEN_ALWAYS) ->
			create(DirIno, Name, AccMode, CrDisp, S2);

		{stop, _, _, S2} ->
			{reply, {error, ?ERROR_FILE_NOT_FOUND}, S2};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


create_directory(S, _From, FileName, _DFI) ->
	case walk(FileName, S) of
		{stop, Parent, Name, S2} ->
			case call_vfs(mkdir, [Parent, Name], S2) of
				{ok, Entry, S3} ->
					S4 = inode_child_add(Parent, Name, Entry, S3),
					opendir(Parent, Name, Entry#vfs_entry.ino, false, S4);

				{error, Reason, S3} ->
					{reply, {error, posix2win(Reason)}, S3}
			end;

		{ok, _Parent, _Name, _Ino, S2} ->
			{reply, {error, ?ERROR_ALREADY_EXISTS}, S2};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


open_directory(S, _From, FileName, _DFI) ->
	case walk(FileName, S) of
		{ok, Parent, Name, Ino, S2} ->
			case get_vnode(Ino, S2) of
				{ok, #vnode{attr=#vfs_attr{dir=true}}, S3} ->
					opendir(Parent, Name, Ino, true, S3);

				{ok, _, S3} ->
					{reply, {error, ?ERROR_ACCESS_DENIED}, S3};

				{error, Reason, S3} ->
					{reply, {error, posix2win(Reason)}, S3}
			end;

		{stop, _, _, S2} ->
			{reply, {error, ?ERROR_PATH_NOT_FOUND}, S2};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


close_file(S, _From, _FileName, DFI) ->
	Ctx = DFI#dokan_file_info.context,
	S2 = if
		Ctx =/= 0 -> del_handle(Ctx, S);
		true -> S
	end,
	{reply, ok, S2}.


find_files(S, _From, _Path, DFI) ->
	case call_vfs_handle(readdir, [], DFI#dokan_file_info.context, S) of
		{ok, Entries, S2} ->
			DirEntryList = lists:map(
				fun(Entry) ->
					Attr = Entry#vfs_direntry.attr,
					#dokan_reply_find{
						file_attributes = if
							Attr#vfs_attr.dir -> ?FILE_ATTRIBUTE_DIRECTORY;
							true  -> ?FILE_ATTRIBUTE_NORMAL
						end,
						creation_time    = epoch2win(Attr#vfs_attr.ctime),
						last_access_time = epoch2win(Attr#vfs_attr.atime),
						last_write_time  = epoch2win(Attr#vfs_attr.mtime),
						file_size        = Attr#vfs_attr.size,
						file_name        = Entry#vfs_direntry.name
					}
				end,
				Entries),
			{reply, DirEntryList, S2};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


get_file_information(S, _From, _FileName, DFI) ->
	Ino = (gb_trees:get(DFI#dokan_file_info.context, S#state.handles))#handle.ino,
	case get_vnode(Ino, S) of
		{ok, VNode, S2} ->
			Attr = VNode#vnode.attr,
			Info = #dokan_reply_fi{
				file_attributes = if
					Attr#vfs_attr.dir -> ?FILE_ATTRIBUTE_DIRECTORY;
					true  -> ?FILE_ATTRIBUTE_NORMAL
				end,
				creation_time    = epoch2win(Attr#vfs_attr.crtime),
				last_access_time = epoch2win(Attr#vfs_attr.atime),
				last_write_time  = epoch2win(Attr#vfs_attr.mtime),
				file_size        = Attr#vfs_attr.size
			},
			{reply, Info, S2};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


read_file(S, _From, _FileName, Length, Offset, #dokan_file_info{context=Ctx}) ->
	case call_vfs_handle(read, [Length, Offset], Ctx, S) of
		{ok, Data, S2} ->
			{reply, Data, S2};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


write_file(S, _From, _FileName, Data, Offset, DFI) ->
	Ctx = DFI#dokan_file_info.context,
	RealOffset = case DFI#dokan_file_info.write_to_eof of
		false -> Offset;
		true  -> eof
	end,
	case call_vfs_handle(write, [Data, RealOffset], Ctx, S) of
		{ok, Written, S2} ->
			{reply, {ok, Written}, S2};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


set_end_of_file(S, _From, _FileName, Offset, DFI) ->
	Ino = (gb_trees:get(DFI#dokan_file_info.context, S#state.handles))#handle.ino,
	case call_vfs(setattr, [Ino, [{size, Offset}]], S) of
		{ok, {Attr, Tmo}, S2} ->
			VN = gb_trees:get(Ino, S#state.vnodes),
			NewVN = VN#vnode{attr=Attr, attr_tmo=Tmo+get_wallclock()},
			S3 = S2#state{vnodes=gb_trees:update(Ino, NewVN, S#state.vnodes)},
			{reply, ok, S3};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


set_file_time(S, _From, _FileName, CTime, ATime, MTime, DFI) ->
	Ino = (gb_trees:get(DFI#dokan_file_info.context, S#state.handles))#handle.ino,
	Changes1 = if
		CTime > 0 -> [{crtime, win2epoch(CTime)}];
		true -> []
	end,
	Changes2 = if
		ATime > 0 -> [{atime, win2epoch(ATime)} | Changes1];
		true -> Changes1
	end,
	Changes3 = if
		MTime > 0 -> [{mtime, win2epoch(MTime)} | Changes2];
		true -> Changes2
	end,
	case call_vfs(setattr, [Ino, Changes3], S) of
		{ok, {Attr, Tmo}, S2} ->
			VN = gb_trees:get(Ino, S#state.vnodes),
			NewVN = VN#vnode{attr=Attr, attr_tmo=Tmo+get_wallclock()},
			S3 = S2#state{vnodes=gb_trees:update(Ino, NewVN, S#state.vnodes)},
			{reply, ok, S3};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


set_file_attributes(S, _From, _FileName, _Attr, _DFI) ->
	{reply, ok, S}.


delete_file(S, _From, _FileName, DFI) ->
	delete(S, DFI#dokan_file_info.context, false).


delete_directory(S, _From, _FileName, DFI) ->
	delete(S, DFI#dokan_file_info.context, true).


move_file(S, _From, _OldPath, NewPath, Replace, DFI) ->
	Ctx = DFI#dokan_file_info.context,
	case walk(NewPath, S) of
		{stop, NewParent, NewName, S2} ->
			do_move(Ctx, NewParent, NewName, S2);

		{ok, NewParent, NewName, _Ino, S2} when Replace ->
			do_move(Ctx, NewParent, NewName, S2);

		{ok, _Parent, _Name, _Ino, S2} ->
			{reply, {error, ?ERROR_ALREADY_EXISTS}, S2};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


get_disk_free_space(S, _From, _DFI) ->
	case call_vfs(statfs, [1], S) of
		{ok, Stat, S2} ->
			Reply = #dokan_reply_diskspace{
				available_bytes  = Stat#fs_stat.bsize * Stat#fs_stat.bavail,
				total_bytes      = Stat#fs_stat.bsize * Stat#fs_stat.blocks,
				total_free_bytes = Stat#fs_stat.bsize * Stat#fs_stat.bfree
			},
			{reply, Reply, S2};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


get_volume_information(S, _From, _DFI) ->
	Reply = #dokan_reply_volinfo{
		volume_name              = <<"PeerDrive">>,
		volume_serial_number     = 16#4b6e614a,
		maximum_component_length = 256,
		file_system_flags        = ?FILE_CASE_SENSITIVE_SEARCH bor
		                           ?FILE_CASE_PRESERVED_NAMES bor
		                           ?FILE_UNICODE_ON_DISK,
		file_system_name         = <<"PeerDrive Dokan VFS">>
	},
	{reply, Reply, S}.


lock_file(S, _From, _FileName, _Offset, _Length, _DFI) ->
	{reply, ok, S}.


unlock_file(S, _From, _FileName, _Offset, _Length, _DFI) ->
	{reply, ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


open(Parent, Name, Ino, AccMode, CrDisp, S) ->
	Trunc = case CrDisp of
		?CREATE_ALWAYS     -> true;
		?TRUNCATE_EXISTING -> true;
		_                  -> false
	end,
	Mode = case (AccMode band (?FILE_READ_DATA bor ?FILE_WRITE_DATA)) of
		?FILE_READ_DATA  -> read;
		?FILE_WRITE_DATA -> write;
		?FILE_READ_DATA bor ?FILE_WRITE_DATA -> readwrite;
		_ -> read % Valid, I know... but who cares...
	end,
	case call_vfs(open, [Ino, Trunc, Mode], S) of
		{ok, Handle, S2} ->
			{Ctx, S3} = add_handle(Parent, Name, Ino, Handle, S2),
			{
				reply,
				#dokan_reply_open{context=Ctx, is_directory=false, existed=true},
				S3
			};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


opendir(Parent, Name, Ino, Existed, S) ->
	case call_vfs(opendir, [Ino], S) of
		{ok, Content, S2} ->
			{Ctx, S3} = add_handle(Parent, Name, Ino, Content, S2),
			{
				reply,
				#dokan_reply_open{context=Ctx, is_directory=true, existed=Existed},
				S3
			};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


create(DirIno, Name, AccMode, CrDisp, S) ->
	MustCreate = CrDisp == ?CREATE_NEW,
	Trunc = CrDisp == ?CREATE_ALWAYS,
	Mode = case (AccMode band (?FILE_READ_DATA bor ?FILE_WRITE_DATA)) of
		?FILE_READ_DATA -> read;
		?FILE_WRITE_DATA -> write;
		?FILE_READ_DATA bor ?FILE_WRITE_DATA -> readwrite;
		_ -> read % Valid, I know... but who cares...
	end,
	case call_vfs(create, [DirIno, Name, MustCreate, Trunc, Mode], S) of
		{ok, {Entry, Existed, Handle}, S2} ->
			S3 = inode_child_add(DirIno, Name, Entry, S2),
			{Ctx, S4} = add_handle(DirIno, Name, Entry#vfs_entry.ino, Handle, S3),
			{
				reply,
				#dokan_reply_open{context=Ctx, is_directory=false, existed=Existed},
				S4
			};

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


delete(S, Ctx, IsDir) ->
	#handle{
		parent = ParentIno,
		name   = Name,
		ino    = ChildIno
	} = gb_trees:get(Ctx, S#state.handles),

	% Validate everything again because the world may have changed since the
	% handle was opened.
	try
		case validate_parent_child(ParentIno, Name, ChildIno, IsDir, S) of
			{ok, S2} ->
				case call_vfs(unlink, [ParentIno, Name], S2) of
					{ok, ok, S3} ->
						S4 = inode_child_del(ParentIno, Name, ChildIno, S3),
						{reply, ok, S4};

					{error, _, _S3} = Error ->
						throw(Error)
				end;

			{error, _Reason, _S2} = Error ->
				throw(Error)
		end
	catch
		throw:{error, PosixError, ErrState} ->
			{reply, {error, posix2win(PosixError)}, ErrState}
	end.


validate_parent_child(ParentIno, Name, ChildIno, IsDir, S) ->
	case get_vnode(ParentIno, S) of
		{ok, VNode, S2} ->
			case vnode_lookup(VNode, Name, S2) of
				{ok, ChildIno, S3} ->
					case get_vnode(ChildIno, S3) of
						{ok, #vnode{attr=#vfs_attr{dir=IsDir}}, S4} ->
							{ok, S4};
						{ok, _, S4} ->
							{error, eacces, S4};
						{error, _, _S4} = Error ->
							Error
					end;

				{ok, _OtherIno, S3} ->
					{error, eacces, S3};
				{error, _, _} = Error ->
					Error
			end;

		{error, _, _S2} = Error ->
			Error
	end.


do_move(Ctx, NewParent, NewName, S) ->
	#handle{
		parent = OldParent,
		name   = OldName,
		ino    = Child
	} = OldHandle = gb_trees:get(Ctx, S#state.handles),
	#vnode{attr=#vfs_attr{dir=IsDir}} = gb_trees:get(Child, S#state.vnodes),
	case validate_parent_child(OldParent, OldName, Child, IsDir, S) of
		{ok, S2} ->
			case call_vfs(rename, [OldParent, OldName, NewParent, NewName], S2) of
				{ok, ok, S3} ->
					S4 = inode_child_del(OldParent, OldName, Child, S3),
					NewHandle = OldHandle#handle{
						parent = NewParent,
						name   = NewName
					},
					S5 = S4#state{
						handles=gb_trees:update(Ctx, NewHandle, S4#state.handles)
					},
					{reply, ok, S5};

				{error, Reason, S3} ->
					{reply, {error, posix2win(Reason)}, S3}
			end;

		{error, Reason, S2} ->
			{reply, {error, posix2win(Reason)}, S2}
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Inode management
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% {ok, ParentIno, Name, Ino, S} | {stop, ParentIno, Name, S} | {error, Reason, S}
walk(Name, #state{re=Re} = S) ->
	if
		Name == <<"\\"/utf8>> ->
			{ok, 1, <<"\\"/utf8>>, 1, S};
		true ->
			% drop first empty string (Name always begins with a \)
			[_ | SplitName] = re:split(Name, Re),
			do_walk(SplitName, 1, <<"\\"/utf8>>, 1, S)
	end.


do_walk([], Parent, Name, Ino, S) ->
	{ok, Parent, Name, Ino, S};

do_walk([Name | Rest], _ParentIno, _EntryName, DirIno, S) ->
	case get_vnode(DirIno, S) of
		{ok, VNode, S2} ->
			case vnode_lookup(VNode, Name, S2) of
				{ok, Ino, S3} ->
					do_walk(Rest, DirIno, Name, Ino, S3);

				{error, enoent, S3} ->
					case Rest of
						[] ->
							{stop, DirIno, Name, S3};
						_ ->
							{error, enoent, S3}
					end;

				{error, _, _} = Error ->
					Error
			end;

		{error, _Reason, _S2} = Error ->
			Error
	end.


get_vnode(Ino, S) ->
	VN = gb_trees:get(Ino, S#state.vnodes),
	Now = get_wallclock(),
	if
		VN#vnode.attr_tmo < Now ->
			% The node's attributes have expired. Revalidate them...
			case call_vfs(getattr, [Ino], S) of
				{ok, {Attr, Tmo}, S2} ->
					NewVN = VN#vnode{attr=Attr, attr_tmo=Tmo+get_wallclock()},
					S3 = S2#state{
						vnodes = gb_trees:update(Ino, NewVN, S#state.vnodes)
					},
					{ok, NewVN, S3};

				{error, Reason, S2} ->
					S3 = evict_inode(Ino, S2),
					{error, Reason, S3}
			end;

		true ->
			{ok, VN, S}
	end.


vnode_lookup(#vnode{attr=#vfs_attr{dir=false}}, _Name, S) ->
	{error, enotdir, S};

vnode_lookup(DirVN, Name, S) ->
	case gb_trees:lookup(Name, DirVN#vnode.childs) of
		{value, {Ino, Tmo}} ->
			Now = get_wallclock(),
			if
				Tmo < Now ->
					% The child entries timeout has expired. We try to redo the
					% lookup to re-validate the entry. If the lookup results in
					% a different inode or fails then forget the old entry. If
					% it yields the same inode then update the child entry and
					% target vnode.
					vnode_lookup_validate(DirVN, Name, Ino, S);

				true ->
					{ok, Ino, S}
			end;

		none ->
			% Child not known yet. TODO: cache negative entries...
			DirIno = DirVN#vnode.ino,
			case call_vfs(lookup, [DirIno, Name], S) of
				{ok, Entry, S2} ->
					S3 = inode_child_add(DirIno, Name, Entry, S2),
					{ok, Entry#vfs_entry.ino, S3};

				{error, _Reason, _S2} = Error ->
					Error
			end
	end.


vnode_lookup_validate(#vnode{ino=DirIno} = DirVN, Name, Ino, S) ->
	case call_vfs(lookup, [DirIno, Name], S) of
		{ok, #vfs_entry{ino=Ino} = Entry, S2} ->
			% We got the same inode. Update the child entry and the vnode...
			ChildVN = gb_trees:get(Ino, S2#state.vnodes),
			NewChildVN = ChildVN#vnode{
				nlookup  = ChildVN#vnode.nlookup + 1,
				attr     = Entry#vfs_entry.attr,
				attr_tmo = Entry#vfs_entry.attr_tmo + get_wallclock()
			},
			NewDirVN = DirVN#vnode{
				childs=gb_trees:update(
					Name,
					{Ino, get_wallclock() + Entry#vfs_entry.entry_tmo},
					DirVN#vnode.childs)
			},
			NewVNodes1 = gb_trees:update(DirIno, NewDirVN, S2#state.vnodes),
			NewVNodes2 = gb_trees:update(Ino, NewChildVN, NewVNodes1),
			{ok, Ino, S2#state{vnodes=NewVNodes2}};

		{ok, Entry, S2} ->
			% Other entry. Dump the old entry, and add the new one...
			S3 = inode_child_del(DirIno, Name, Ino, S2),
			S4 = inode_child_add(DirIno, Name, Entry, S3),
			{ok, Entry#vfs_entry.ino, S4};

		{error, Reason, S2} ->
			% something went wrong. Remove the stale entry...
			S3 = inode_child_del(DirIno, Name, Ino, S2),
			{error, Reason, S3}
	end.


inode_child_add(DirIno, Name, #vfs_entry{ino=ChildIno} = Entry, S) ->
	DirVN = gb_trees:get(DirIno, S#state.vnodes),
	NewDirVN = DirVN#vnode{
		childs=gb_trees:enter(
			Name,
			{ChildIno, get_wallclock() + Entry#vfs_entry.entry_tmo},
			DirVN#vnode.childs)
	},
	NewVNodes = gb_trees:update(DirIno, NewDirVN, S#state.vnodes),

	case gb_trees:lookup(ChildIno, NewVNodes) of
		{value, ChildVN} ->
			% Already known inode. Update the vnode accordingly...
			% TODO: what if the type changed?
			NewChildVN = ChildVN#vnode{
				nlookup  = ChildVN#vnode.nlookup + 1,
				attr     = Entry#vfs_entry.attr,
				attr_tmo = Entry#vfs_entry.attr_tmo + get_wallclock(),
				parents  = [{DirIno, Name} | ChildVN#vnode.parents]
			},
			S#state{vnodes=gb_trees:update(ChildIno, NewChildVN, NewVNodes)};

		none ->
			% Yet unknown inode...
			NewChildVN = #vnode{
				ino      = ChildIno,
				nlookup  = 1,
				refcnt   = 0,
				attr     = Entry#vfs_entry.attr,
				attr_tmo = Entry#vfs_entry.attr_tmo + get_wallclock(),
				parents  = [{DirIno, Name}],
				childs   = gb_trees:empty()
			},
			S#state{vnodes=gb_trees:enter(ChildIno, NewChildVN, NewVNodes)}
	end.


inode_child_del(DirIno, ChildName, ChildIno, S) ->
	DirVN = gb_trees:get(DirIno, S#state.vnodes),
	NewDirVN = DirVN#vnode{childs=gb_trees:delete(ChildName, DirVN#vnode.childs)},
	S2 = S#state{vnodes=gb_trees:update(DirIno, NewDirVN, S#state.vnodes)},
	inode_parent_del(ChildIno, DirIno, ChildName, S2).


inode_parent_del(Ino, ParentIno, Name, S) ->
	VN = gb_trees:get(Ino, S#state.vnodes),
	NewParents = lists:delete({ParentIno, Name}, VN#vnode.parents),
	NewVN = VN#vnode{parents=NewParents},
	S2 = S#state{vnodes=gb_trees:update(Ino, NewVN, S#state.vnodes)},
	case NewParents of
		[] -> evict_inode(Ino, S2);
		_  -> S2
	end.


% Clear out all childs and remove from all parents. If refcnt is zero then delete
% immediately, otherwise we're waiting until the last handle is closed...
evict_inode(Ino, S) ->
	VN = gb_trees:get(Ino, S#state.vnodes),
	S2 = lists:foldl(
		fun({ParentIno, Name}, AccS) ->
			ParentVN = gb_trees:get(ParentIno, AccS#state.vnodes),
			NewParentVN = ParentVN#vnode{
				childs=gb_trees:delete(Name, ParentVN#vnode.childs)
			},
			AccS#state{vnodes=gb_trees:update(ParentIno, NewParentVN, AccS#state.vnodes)}
		end,
		S,
		VN#vnode.parents),
	S3 = lists:foldl(
		fun({Name, {ChildIno, _Tmo}}, AccS) ->
			inode_parent_del(ChildIno, Ino, Name, AccS)
		end,
		S2,
		gb_trees:to_list(VN#vnode.childs)),
	if
		VN#vnode.refcnt == 0 ->
			% End of life for this inode. Forget about it...
			{_, _, S4} = call_vfs(forget, [Ino, VN#vnode.nlookup], S3),
			S4#state{vnodes=gb_trees:delete(Ino, S4#state.vnodes)};

		true ->
			% The inode still has open handles. Clear out parents and childs
			% and wait for the last handle to be closed...
			NewVN = VN#vnode{parents=[], childs=gb_trees:empty()},
			S3#state{vnodes=gb_trees:update(Ino, NewVN, S3#state.vnodes)}
	end.


call_vfs(Fun, Args, #state{vfs_state=VfsState} = S) ->
	{OkErr, Reply, NewVfsState} = apply(peerdrive_ifc_vfs_common, Fun,
		Args ++ [VfsState]),
	{OkErr, Reply, S#state{vfs_state=NewVfsState}}.


call_vfs_handle(Fun, Args, Ctx, S) ->
	VfsState = S#state.vfs_state,
	Handle = gb_trees:get(Ctx, S#state.handles),
	VfsHandle = Handle#handle.vfs_handle,
	case catch apply(peerdrive_ifc_vfs_common, Fun, [VfsState, VfsHandle | Args]) of
		{OkErr, Reply} ->
			{OkErr, Reply, S};

		{OkErr, Reply, NewVfsState, NewVfsHandle} ->
			NewHandle = Handle#handle{vfs_handle=NewVfsHandle},
			S2 = S#state{
				vfs_state=NewVfsState,
				handles=gb_trees:update(Ctx, NewHandle, S#state.handles)
			},
			{OkErr, Reply, S2}
	end.


get_wallclock() ->
	{MegaSecs, Secs, MicroSecs} = now(),
	MegaSecs*1000000000 + Secs*1000 + MicroSecs div 1000.


add_handle(Parent, Name, Ino, VfsHandle, S) ->
	Handle = #handle{
		parent     = Parent,
		name       = Name,
		ino        = Ino,
		vfs_handle = VfsHandle
	},
	{Max, _} = gb_trees:largest(S#state.handles), Ctx = Max+1,
	S2 = S#state{ handles=gb_trees:enter(Ctx, Handle, S#state.handles) },
	S3 = inc_refcnt(Parent, S2),
	S4 = inc_refcnt(Ino, S3),
	{Ctx, S4}.


inc_refcnt(Ino, #state{vnodes=VNodes} = S) ->
	VN = gb_trees:get(Ino, VNodes),
	NewVN = VN#vnode{refcnt=VN#vnode.refcnt+1},
	S#state{ vnodes=gb_trees:update(Ino, NewVN, VNodes) }.


del_handle(Ctx, S) ->
	{ok, ok, S2} = call_vfs_handle(close, [], Ctx, S),
	#handle{
		parent = Parent,
		ino    = Ino
	} = gb_trees:get(Ctx, S2#state.handles),
	S3 = S2#state{handles=gb_trees:delete(Ctx, S2#state.handles)},
	S4 = dec_refcnt(Ino, S3),
	dec_refcnt(Parent, S4).


dec_refcnt(Ino, #state{vnodes=VNodes} = S) ->
	VN = gb_trees:get(Ino, VNodes),
	if
		(VN#vnode.refcnt == 1) and (VN#vnode.parents == []) ->
			% End of life for this inode. Forget about it...
			S2 = S#state{vnodes=gb_trees:delete(Ino, VNodes)},
			{_, _, S3} = call_vfs(forget, [Ino, VN#vnode.nlookup], S2),
			S3;

		true ->
			NewVN = VN#vnode{refcnt=VN#vnode.refcnt-1},
			S#state{vnodes=gb_trees:update(Ino, NewVN, VNodes)}
	end.


% scan all inodes and scan for very old, expired entries
do_gc(S) ->
	Clk = get_wallclock() - ?GC_THRESHOLD,
	AllNodes = gb_trees:keys(S#state.vnodes),
	S2 = lists:foldl(fun(Ino, AccS) -> do_gc_vnode(Ino, Clk, AccS) end, S, AllNodes),
	erlang:send_after(?GC_INTERVAL, self(), gc),
	S2.


do_gc_vnode(Ino, Clk, S) ->
	case gb_trees:lookup(Ino, S#state.vnodes) of
		{value, VNode} ->
			Expired = scan_expired_entries(gb_trees:iterator(VNode#vnode.childs),
				Clk, []),
			lists:foldl(
				fun({ChildName, ChildIno}, S2) ->
					inode_child_del(Ino, ChildName, ChildIno, S2)
				end,
				S,
				Expired);

		none ->
			S
	end.


scan_expired_entries(Iter, Clk, Acc) ->
	case gb_trees:next(Iter) of
		{Name, {Ino, Timeout}, Iter2} ->
			if
				Timeout < Clk ->
					scan_expired_entries(Iter2, Clk, [{Name, Ino} | Acc]);
				true ->
					scan_expired_entries(Iter2, Clk, Acc)
			end;

		none ->
			Acc
	end.


% the posix () set of atoms
posix2win(eacces) -> ?ERROR_ACCESS_DENIED;
posix2win(enoent) -> ?ERROR_FILE_NOT_FOUND;
posix2win(enotdir) -> ?ERROR_PATH_NOT_FOUND;
posix2win(ebadf) -> ?ERROR_INVALID_HANDLE;
posix2win(ebadfd) -> ?ERROR_INVALID_HANDLE;
posix2win(ebusy) -> ?ERROR_BUSY;
posix2win(eexist) -> ?ERROR_FILE_EXISTS;
posix2win(efault) -> ?ERROR_READ_FAULT;
posix2win(eintr) -> ?ERROR_OPERATION_ABORTED;
posix2win(einval) -> ?ERROR_BAD_COMMAND;
posix2win(eio) -> ?ERROR_CRC;
posix2win(eisdir) -> ?ERROR_DIRECTORY;
posix2win(emfile) -> ?ERROR_TOO_MANY_OPEN_FILES;
posix2win(enodev) -> ?ERROR_DEV_NOT_EXIST;
posix2win(enomem) -> ?ERROR_OUTOFMEMORY;
posix2win(enosys) -> ?ERROR_INVALID_FUNCTION;
posix2win(enospc) -> ?ERROR_DISK_FULL;
posix2win(enotempty) -> ?ERROR_DIR_NOT_EMPTY;
posix2win(enotsup) -> ?ERROR_NOT_SUPPORTED;
posix2win(eperm) -> ?ERROR_INVALID_ACCESS;

% returned by file: methods, supported for convenience
posix2win(badarg) -> posix2win(einval);
posix2win(terminated) -> posix2win(eio);
posix2win(system_limit) -> posix2win(emfile);

% "no error"
posix2win(ok) -> 0;
posix2win(Error) ->
	error_logger:warning_report([{module, ?MODULE},
		{reason, 'Non-encodable error'}, {error, Error}]),
	?ERROR_GEN_FAILURE.

epoch2win(Epoch) ->
	(Epoch + 134774*24*3600*1000000) * 10.


win2epoch(Win) ->
	Win div 10 - 134774*24*3600*1000000.

-else. % have_dokan

start_link(_) ->
	error_logger:warning_msg("Dokan support was not compiled and is therefore "
		"not available.~n"),
	ignore.

-endif.
