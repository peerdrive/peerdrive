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

-module(peerdrive_ifc_vfs_fuse).
-export([start_link/1]).

-ifdef(have_fuserl).

%-behaviour(fuserl).

%-define(DEBUG(X), X).
-define(DEBUG(X), ok).

-export([code_change/3, handle_info/2, init/1, terminate/2]).
-export([create/7, forget/5, getattr/4, link/6, lookup/5, mkdir/6, open/5,
         opendir/5, read/7, readdir/7, release/5, releasedir/5, rename/7,
         rmdir/5, setattr/7, statfs/4, unlink/5, write/7]).
-export([access/5, flush/5, fsync/6, fsyncdir/6, getlk/6, getxattr/6,
         listxattr/5, mknod/7, readlink/4, removexattr/5, setlk/7, setxattr/7,
         symlink/6]).

-include("store.hrl").
-include("vfs.hrl").
-include_lib("fuserl/include/fuserl.hrl").

-record(state, {
	vfs_state,
	handles,
	uid,
	gid,
	umask
}).

-define(UNKNOWN_INO, 16#ffffffff).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Options) ->
	Dir = filename:nativename(filename:absname(
		proplists:get_value(mountpoint,	Options, peerdrive_util:cfg_mnt_dir()))),
	Server = case application:start(fuserl) of
		ok                               -> ok;
		{error,{already_started,fuserl}} -> ok;
		{error, Reason}                  -> {fuserl, Reason}
	end,
	LinkedIn = proplists:get_value(linkedin, Options, true),
	MountOpts1 = lists:foldl(
		fun
			(default_permissions, Acc) ->
				",default_permissions" ++ Acc;
			(allow_other, Acc) ->
				",allow_other" ++ Acc;
			({max_read, Size}, Acc) ->
				",max_read=" ++ integer_to_list(Size) ++ Acc;
			({user_id, UId}, Acc) ->
				",user_id=" ++ integer_to_list(UId) ++ Acc;
			({group_id, GId}, Acc) ->
				",group_id=" ++ integer_to_list(GId) ++ Acc;
			(_, Acc) ->
				Acc
		end,
		"",
		Options),
	MountOpts2 = if
		MountOpts1 == "" -> MountOpts1;
		true             -> tl(MountOpts1)
	end,
	case Server of
		ok ->
			fuserlsrv:start_link(?MODULE, LinkedIn, MountOpts2, Dir, {Dir, Options}, []);
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

init({Dir, Options}) ->
	BinDir = if
		is_list(Dir)   -> unicode:characters_to_binary(Dir);
		is_binary(Dir) -> Dir
	end,
	peerdrive_sys_info:publish(<<"vfs.mountpath">>, BinDir),
	State = #state{
		vfs_state = peerdrive_ifc_vfs_common:init(Options),
		handles   = gb_trees:from_orddict([{0, undefined}]),
		uid       = proplists:get_value(uid, Options, 0),
		gid       = proplists:get_value(gid, Options, 0),
		umask     = 8#177777 band (bnot proplists:get_value(umask, Options, 0))
	},
	{ok, State}.


getattr(_, Ino, _Cont, S) ->
	Reply = case call_vfs(getattr, [Ino], S) of
		{ok, {VfsAttr, Tmo}, S2} ->
			Attr = make_fuse_attr(Ino, VfsAttr, S2),
			{#fuse_reply_attr{attr=Attr, attr_timeout_ms=Tmo}, S2};

		{error, Reason, S2} ->
			{#fuse_reply_err{err=Reason}, S2}
	end,
	?DEBUG(io:format("getattr(~p) -> ~p~n", [Ino, element(1, Reply)])),
	Reply.


lookup(_, Parent, Name, _Cont, S) ->
	Reply = case call_vfs(lookup, [Parent, Name], S) of
		{ok, Entry, S2} ->
			FuseEntry = make_fuse_entry(Entry, S),
			{#fuse_reply_entry{fuse_entry_param=FuseEntry}, S2};

		{error, Reason, S2} ->
			{#fuse_reply_err{err=Reason}, S2}
	end,
	?DEBUG(io:format("lookup(~p, ~s) -> ~p~n", [Parent, Name, element(1, Reply)])),
	Reply.


forget(_, Ino, N, _Cont, S) ->
	{ok, _, S2} = call_vfs(forget, [Ino, N], S),
	{#fuse_reply_none{}, S2}.


open(_, Ino, Fi, _Cont, S) ->
	#fuse_file_info{flags=Flags} = Fi,
	Trunc = (Flags band ?O_TRUNC) =/= 0,
	Mode = case Flags band ?O_ACCMODE of
		?O_RDONLY -> read;
		?O_WRONLY -> write;
		?O_RDWR   -> readwrite
	end,
	Reply = case call_vfs(open, [Ino, Trunc, Mode], S) of
		{ok, Handle, S2} ->
			{Id, S3} = add_vfs_handle(Handle, S2),
			{
				#fuse_reply_open{fuse_file_info=Fi#fuse_file_info{fh = Id}},
				S3
			};

		{error, Reason, S2} ->
			{#fuse_reply_err{err=Reason}, S2}
	end,
	?DEBUG(io:format("open(~p, ~p) -> ~p~n", [Ino, Fi, element(1, Reply)])),
	Reply.


create(_, Parent, Name, _Mode, Fi, _Cont, S) ->
	Flags = Fi#fuse_file_info.flags,
	MustCreate = (Flags band ?O_EXCL) =/= 0,
	Trunc = (Flags band ?O_TRUNC) =/= 0,
	Mode = case Flags band ?O_ACCMODE of
		?O_RDONLY -> read;
		?O_WRONLY -> write;
		?O_RDWR   -> readwrite
	end,
	Reply = case call_vfs(create, [Parent, Name, MustCreate, Trunc, Mode], S) of
		{ok, {Entry, _Existed, Handle}, S2} ->
			FuseEntry = make_fuse_entry(Entry, S2),
			{Id, S3} = add_vfs_handle(Handle, S2),
			{
				#fuse_reply_create{
					fuse_file_info   = Fi#fuse_file_info{fh=Id},
					fuse_entry_param = FuseEntry
				},
				S3
			};

		{error, Error, S2} ->
			{#fuse_reply_err{ err = Error }, S2}
	end,
	?DEBUG(io:format("create(~s, ~p) ->~n    ~p~n", [Name, Fi, element(1, Reply)])),
	Reply.


read(_, _Ino, Size, Offset, Fi, _Cont, S) ->
	case call_vfs_handle(read, [Size, Offset], Fi#fuse_file_info.fh, S) of
		{ok, Data, S2} ->
			{#fuse_reply_buf{buf=Data, size=erlang:size(Data)}, S2};

		{error, Error, S2} ->
			{#fuse_reply_err{ err = Error }, S2}
	end.


write(_, _Ino, Data, Offset, Fi, _Cont, S) ->
	case call_vfs_handle(write, [Data, Offset], Fi#fuse_file_info.fh, S) of
		{ok, Written, S2} ->
			{#fuse_reply_write{count=Written}, S2};

		{error, Error, S2} ->
			{#fuse_reply_err{ err = Error }, S2}
	end.


release(_, _Ino, #fuse_file_info{fh=Id}, _Cont, S) ->
	{ok, ok, S2} = call_vfs_handle(close, [], Id, S),
	S3 = del_vfs_handle(Id, S2),
	?DEBUG(io:format("release(~p) -> ok~n", [_Ino])),
	{#fuse_reply_err{err=ok}, S3}.


opendir(_, Ino, Fi, _Cont, S) ->
	case call_vfs(opendir, [Ino], S) of
		{ok, Handle, S2} ->
			{Id, S3} = add_vfs_handle(Handle, S2),
			{#fuse_reply_open{fuse_file_info=Fi#fuse_file_info{fh = Id}}, S3};

		{error, Error, S2} ->
			{#fuse_reply_err{err=Error}, S2}
	end.


readdir(_Ctx, _Ino, Size, Offset, Fi, _Cont, S) ->
	{ok, Entries, S2} = call_vfs_handle(readdir, [], Fi#fuse_file_info.fh, S),
	DirEntryList = take_while_map(
		fun (Entry, {Total, Max}, Num) ->
			FuseEntry = make_dir_entry(Entry, Offset+Num, S2),
			Cur = fuserlsrv:dirent_size(FuseEntry),
			if
				Total + Cur =< Max -> {continue, FuseEntry, {Total + Cur, Max}};
				true -> stop
			end
		end,
		{0, Size},
		safe_nthtail(Offset, Entries)),
	?DEBUG(io:format("readdir(~p) -> ~p~n", [_Ino, DirEntryList])),
	{#fuse_reply_direntrylist{direntrylist = DirEntryList}, S2}.


releasedir(_, _Ino, #fuse_file_info{fh=Id}, _Cont, S) ->
	% no need to call peerdrive_ifc_vfs_fuse:close() yet...
	S2  = del_vfs_handle(Id, S),
	{#fuse_reply_err{err=ok}, S2}.


setattr(_, Ino, Attr, ToSet, _Fi, _, S) ->
	Reply = if
		(((ToSet band ?FUSE_SET_ATTR_UID) =/= 0) and
		 (Attr#stat.st_uid =/= S#state.uid)) ->
			{#fuse_reply_err{err=eperm}, S};

		(((ToSet band ?FUSE_SET_ATTR_GID) =/= 0) and
		 (Attr#stat.st_gid =/= S#state.gid)) ->
			{#fuse_reply_err{err=eperm}, S};

		% TODO: maybe not a good idea... seems to break many applications...
		%(ToSet band ?FUSE_SET_ATTR_MODE) =/= 0 ->
		%	{#fuse_reply_err{err=eperm}, S};

		true ->
			Changes1 = if
				(ToSet band ?FUSE_SET_ATTR_SIZE) =/= 0 ->
					[{size, Attr#stat.st_size}];
				true ->
					[]
			end,
			Changes2 = if
				(ToSet band ?FUSE_SET_ATTR_ATIME) =/= 0 ->
					[{atime, fuse2epoch(Attr#stat.st_atime, Attr#stat.st_atimensec)} | Changes1];
				true ->
					Changes1
			end,
			Changes3 = if
				(ToSet band ?FUSE_SET_ATTR_MTIME) =/= 0 ->
					[{mtime, fuse2epoch(Attr#stat.st_mtime, Attr#stat.st_mtimensec)} | Changes2];
				true ->
					Changes2
			end,
			case call_vfs(setattr, [Ino, Changes3], S) of
				{ok, {VfsAttr, Tmo}, S2} ->
					FuseAttr = make_fuse_attr(Ino, VfsAttr, S2),
					{#fuse_reply_attr{attr=FuseAttr, attr_timeout_ms=Tmo}, S2};

				{error, Reason, S2} ->
					{#fuse_reply_err{err=Reason}, S2}
			end
	end,
	?DEBUG(io:format("setattr(~p, ~p, ~p, ~p) ->~n    ~p~n", [Ino, Attr, ToSet, _Fi, element(1, Reply)])),
	Reply.


rename(_, OldParent, OldName, NewParent, NewName, _, S) ->
	{_, Result, S2} = call_vfs(rename, [OldParent, OldName, NewParent, NewName], S),
	?DEBUG(io:format("rename(~p, ~s, ~p, ~s) -> ~p~n", [OldParent, OldName, NewParent, NewName, Result])),
	{#fuse_reply_err{err=Result}, S2}.


unlink(_, Parent, Name, _, S) ->
	{_, Result, S2} = call_vfs(unlink, [Parent, Name], S),
	?DEBUG(io:format("unlink(~p, ~s) -> ~p~n", [Parent, Name, Result])),
	{#fuse_reply_err{err=Result}, S2}.


link(_, Ino, NewParent, NewName, _, S) ->
	Reply = case call_vfs(link, [Ino, NewParent, NewName], S) of
		{ok, VfsEntry, S2} ->
			FuseEntry = make_fuse_entry(VfsEntry, S2),
			{#fuse_reply_entry{fuse_entry_param=FuseEntry}, S2};

		{error, Reason, S2} ->
			{#fuse_reply_err{err=Reason}, S2}
	end,
	?DEBUG(io:format("link(~p, ~p, ~s) -> ~p~n", [Ino, NewParent, NewName, element(1, Reply)])),
	Reply.


mkdir(_, Parent, Name, _Mode, _, S) ->
	Reply = case call_vfs(mkdir, [Parent, Name], S) of
		{ok, VfsEntry, S2} ->
			FuseEntry = make_fuse_entry(VfsEntry, S2),
			{#fuse_reply_entry{fuse_entry_param=FuseEntry}, S2};

		{error, Reason, S2} ->
			{#fuse_reply_err{err=Reason}, S2}
	end,
	?DEBUG(io:format("mkdir(~p, ~s) -> ~p~n", [Parent, Name, element(1, Reply)])),
	Reply.


rmdir(_, Parent, Name, _, S) ->
	% FIXME: unlink only (empty) directory documents, not everything. OTOH...
	% nice feature...
	{_, Result, S2} = call_vfs(unlink, [Parent, Name], S),
	?DEBUG(io:format("rmdir(~p, ~s) -> ~p~n", [Parent, Name, Result])),
	{#fuse_reply_err{err=Result}, S2}.


statfs(_, Ino, _, S) ->
	Reply = case call_vfs(statfs, [Ino], S) of
		{ok, Stat, S2} ->
			{
				#fuse_reply_statfs{statvfs=#statvfs{
					f_bsize   = Stat#fs_stat.bsize,
					f_frsize  = Stat#fs_stat.bsize,
					f_blocks  = Stat#fs_stat.blocks,
					f_bfree   = Stat#fs_stat.bfree,
					f_bavail  = Stat#fs_stat.bavail,
					f_namemax = 255
				}},
				S2
			};
		{error, Reason, S2} ->
			{#fuse_reply_err{err=Reason}, S2}
	end,
	?DEBUG(io:format("statfs(~p) -> ~w~n", [Ino, element(1, Reply)])),
	Reply.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

call_vfs(Fun, Args, #state{vfs_state=VfsState} = S) ->
	{OkErr, Reply, NewVfsState} = (catch apply(peerdrive_ifc_vfs_common,
		Fun, Args ++ [VfsState])),
	{OkErr, Reply, S#state{vfs_state=NewVfsState}}.


add_vfs_handle(VfsHandle, #state{handles=Handles} = S) ->
	{Max, _} = gb_trees:largest(Handles), Id = Max+1,
	{Id, S#state{handles = gb_trees:enter(Id, VfsHandle, Handles)}}.


del_vfs_handle(FuseHandle, #state{handles=Handles} = S) ->
	S#state{handles = gb_trees:delete(FuseHandle, Handles)}.


call_vfs_handle(Fun, Args, FuseHandle, S) ->
	VfsState = S#state.vfs_state,
	VfsHandle = gb_trees:get(FuseHandle, S#state.handles),
	case catch apply(peerdrive_ifc_vfs_common, Fun, [VfsState, VfsHandle | Args]) of
		{OkErr, Reply} ->
			{OkErr, Reply, S};

		{OkErr, Reply, NewVfsState, NewVfsHandle} ->
			S2 = S#state{
				vfs_state=NewVfsState,
				handles=gb_trees:update(FuseHandle, NewVfsHandle, S#state.handles)
			},
			{OkErr, Reply, S2}
	end.


make_fuse_attr(Ino, #vfs_attr{dir=Dir, size=Size, atime=AT, mtime=MT, ctime=CT}, S) ->
	#stat{
		st_ino   = Ino,
		st_mode  = (if
			Dir  -> ?S_IFDIR bor 8#0777;
			true -> ?S_IFREG bor 8#0666
		end) band S#state.umask,
		st_nlink = 1,
		st_uid   = S#state.uid,
		st_gid   = S#state.gid,
		st_size  = Size,
		st_blksize = 512,
		st_blocks  = (Size + 511) div 512,
		st_atime = epoch2fuse_sec(AT),
		st_atimensec = epoch2fuse_nsec(AT),
		st_mtime = epoch2fuse_sec(MT),
		st_mtimensec = epoch2fuse_nsec(MT),
		st_ctime = epoch2fuse_sec(CT),
		st_ctimensec = epoch2fuse_nsec(CT)
	}.


make_fuse_entry(#vfs_entry{ino=Ino} = Entry, S) ->
	#fuse_entry_param{
		ino              = Ino,
		generation       = 1,
		attr_timeout_ms  = Entry#vfs_entry.attr_tmo,
		entry_timeout_ms = Entry#vfs_entry.entry_tmo,
		attr             = make_fuse_attr(Ino, Entry#vfs_entry.attr, S)
	}.


make_dir_entry(#vfs_direntry{name=Name, attr=Attr}, Offset, S) ->
	#direntry{
		name   = Name,
		offset = Offset,
		stat   = make_fuse_attr(?UNKNOWN_INO, Attr, S)
	}.


safe_nthtail (_, []) ->
	[];
safe_nthtail (N, L) when N =< 0 ->
	L;
safe_nthtail (N, L) ->
	safe_nthtail (N - 1, tl (L)).


take_while_map(F, Acc, List) ->
	do_take_while_map(F, Acc, List, 1).


do_take_while_map(_, _, [], _) ->
	[];

do_take_while_map(F, Acc, [ Head | Tail ], Id) ->
	case F(Head, Acc, Id) of
		{continue, NewHead, NewAcc} ->
			[ NewHead | do_take_while_map(F, NewAcc, Tail, Id+1) ];
		stop ->
			[]
	end.


epoch2fuse_sec(Epoch) ->
	Epoch div 1000000.

epoch2fuse_nsec(Epoch) ->
	(Epoch rem 1000000) * 1000.

fuse2epoch(FuseSec, FuseNSec) ->
	FuseSec * 1000000 + FuseNSec div 1000.

-else. % have_fuserl

start_link(_) ->
	error_logger:warning_msg("FUSE support was not compiled and is therefore "
		"not available.~n"),
	ignore.

-endif.
