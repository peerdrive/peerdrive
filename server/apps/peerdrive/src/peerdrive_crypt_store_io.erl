%% PeerDrive
%% Copyright (C) 2012  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
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

-module(peerdrive_crypt_store_io).
-behaviour(gen_server).

-export([start_link/7]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-record(state, {parent, store, key, handle, did, prerid, rev, user,
	readonly, parts, data}).

-record(preprev, {rid, encrid, encrev, encdata, encdoclinks, encrevlinks}).

-record(part, {
	name   :: string(),
	iodev  :: file:io_device(),
	crtime :: integer(),
	mtime  :: undefined | integer()
}).

-include("store.hrl").
-include("cryptstore.hrl").

-define(EMPTY_DATA, <<0,0,0,0,0>>).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Store, Key, Handle, DId, PreRId, Rev, User) ->
	State = #state{
		parent     = self(),
		store      = Store,
		key        = Key,
		handle     = Handle,
		did        = DId,
		prerid     = PreRId,
		rev        = Rev,
		user       = User,
		readonly   = not is_binary(DId),
		parts      = [],
		data       = case Handle of
			undefined -> ?EMPTY_DATA;
			_         -> undefined
		end
	},
	gen_server:start_link(?MODULE, State, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(State) ->
	process_flag(trap_exit, true),
	link(State#state.user),
	{ok, State}.


handle_call({get_data, Selector}, _From, S) ->
	{Reply, S2} = do_get_data(Selector, S),
	{reply, Reply, S2};

handle_call({read, Part, Offset, Length}, _From, S) ->
	Reply = do_read(Part, Offset, Length, S),
	{reply, Reply, S};

handle_call(close, _From, S) ->
	{stop, normal, ok, S};

handle_call(fstat, _From, S) ->
	{Reply, S2} = do_fstat(S),
	{reply, Reply, S2};

% all following calls are only allowed when still writable...
handle_call(_Request, _From, S = #state{readonly=true}) ->
	{reply, {error, ebadf}, S};

handle_call({set_data, Selector, Data}, _From, S) ->
	{Reply, S2} = do_set_data(Selector, Data, S),
	{reply, Reply, S2};

handle_call({write, Part, Offset, Data}, _From, S) ->
	{Reply, S2} = do_write(Part, Offset, Data, S),
	{reply, Reply, S2};

handle_call({truncate, Part, Offset}, _From, S) ->
	{Reply, S2} = do_truncate(Part, Offset, S),
	{reply, Reply, S2};

handle_call(commit, _From, S) ->
	{Reply, S2} = do_commit(undefined, S),
	{reply, Reply, S2};

handle_call({commit, Comment}, _From, S) ->
	{Reply, S2} = do_commit(Comment, S),
	{reply, Reply, S2};

handle_call({suspend, Comment}, _From, S) ->
	{Reply, S2} = do_suspend(Comment, S),
	{reply, Reply, S2};

handle_call({set_flags, Flags}, _From, S) ->
	Rev = S#state.rev,
	NewRev = Rev#rev{flags=Flags},
	{reply, ok, S#state{rev=NewRev}};

handle_call({set_type, Type}, _From, S) ->
	Rev = S#state.rev,
	NewRev = Rev#rev{type=Type},
	{reply, ok, S#state{rev=NewRev}};

handle_call({set_parents, Parents}, _From, S) ->
	Rev = S#state.rev,
	NewRev = Rev#rev{parents=Parents},
	{reply, ok, S#state{rev=NewRev}};

handle_call({set_mtime, Attachment, MTime}, _From, S) ->
	{Reply, S2} = do_set_mtime(Attachment, MTime, S),
	{reply, Reply, S2}.


handle_info({'EXIT', From, Reason}, #state{parent=Parent, user=User} = S) ->
	case From of
		User   -> {stop, normal, S};
		Parent -> {stop, Reason, S};
		_      -> {noreply, S}
	end.


terminate(_Reason, #state{handle=Handle, parts=Parts}) ->
	Handle == undefined orelse peerdrive_store:close(Handle),
	lists:foreach(
		fun({_FCC, #part{name=FileName, iodev=IoDev}}) ->
			file:close(IoDev),
			file:delete(FileName)
		end,
		Parts).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_cast(_, State)    -> {noreply, State}.
code_change(_, State, _) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_fstat(#state{data=Data, rev=Rev, parts=Parts} = S) ->
	Now = peerdrive_util:get_time(),
	try
		% lazy update of mtime
		UpdatedParts = [
			Part#part{mtime=case MTime of undefined -> Now; _ -> MTime end}
			|| #part{mtime=MTime} = Part <- Parts ],
		NewAttachments = [
			case file:position(IoDev, eof) of
				{ok, Size} ->
					#rev_att{
						name=Name, size=Size, hash= <<>>, crtime=CrTime,
						mtime=MTime
					};
				{error, _} = UpdateErr ->
					throw(UpdateErr)
			end
			|| {Name, #part{iodev=IoDev, crtime=CrTime, mtime=MTime}}
				<- UpdatedParts ],
		OldAttachments = [ A#rev_att{hash= <<>>} || A <- Rev#rev.attachments ],
		Attachments = lists:keymerge(#rev_att.name,
			lists:keysort(#rev_att.name, NewAttachments),
			lists:keysort(#rev_att.name, OldAttachments)),
		StatRev = Rev#rev{data=#rev_dat{size=size(Data), hash= <<>>},
			attachments=Attachments, mtime=Now},
		S2 = S#state{parts=UpdatedParts},
		{{ok, StatRev}, S2}
	catch
		throw:Error ->
			{Error, S}
	end.


do_get_data(Selector, S) ->
	case fetch_data(S) of
		{ok, #state{data=Data} = S2} ->
			try
				{{ok, peerdrive_struct:extract(Data, Selector)}, S2}
			catch
				error:badarg -> {{error, einval}, S2};
				error:enoent -> {{error, enoent}, S2}
			end;

		{{error, _}, _S2} = Error ->
			Error
	end.


do_read(Part, Start, Length, S) ->
	case orddict:find(Part, S#state.parts) of
		error ->
			case lists:keymember(Part, #rev_att.name, (S#state.rev)#rev.attachments) of
				true ->
					do_read_remote(Part, Start, Length, S);
				false ->
					{error, enoent}
			end;

		{ok, File} ->
			do_read_local(File, Start, Length)
	end.


do_read_remote(Part, OrigStart, OrigLength, S) ->
	Offset = OrigStart band 15,
	Start  = OrigStart band bnot 15,
	Length = OrigLength + Offset,
	case do_read_remote_aligned(Part, Start, Length, S) of
		{ok, Data} ->
			Size = size(Data),
			PartOffset = if
				Offset >= Size -> Size;
				true -> Offset
			end,
			PartLength = if
				PartOffset+OrigLength > Size -> Size-PartOffset;
				true -> OrigLength
			end,
			{ok, binary:part(Data, PartOffset, PartLength)};

		{error, _} = Error ->
			Error
	end.


do_read_remote_aligned(Part, Start, Length, S) ->
	#state{handle=Handle, key=Key, rev=#rev{attachments=Parts}} = S,
	case peerdrive_store:read(Handle, Part, Start, Length) of
		{ok, EncData} ->
			#rev_att{hash=PId0} = lists:keyfind(Part, #rev_att.name, Parts),
			<<PId:128>> = peerdrive_crypto:make_bin_16(PId0),
			IVec = <<(PId + (Start bsr 4)):128>>,
			Data = peerdrive_crypto:aes_ctr_decrypt(Key, IVec, EncData),
			{ok, Data};

		{error, _} = Error ->
			Error
	end.

do_read_local(#part{iodev=IoDev}, Offset, Length) ->
	case file:pread(IoDev, Offset, Length) of
		eof -> {ok, <<>>};
		Else -> peerdrive_util:fixup_file(Else)
	end.


do_set_data(Selector, Data, S) ->
	case do_get_data(<<>>, S) of
		{{ok, Orig}, S2} ->
			try
				New = peerdrive_struct:update(Orig, Selector, Data),
				{ok, S2#state{data=New}}
			catch
				error:badarg -> {{error, einval}, S2};
				error:enoent -> {{error, enoent}, S2}
			end;

		{{error, _}, _S2} = Error ->
			Error
	end.


do_write(Part, Offset, Data, S) ->
	case get_part_writable(Part, S) of
		{ok, IoDev, S2} ->
			{peerdrive_util:fixup_file(file:pwrite(IoDev, Offset, Data)), S2};
		{error, _} = Error ->
			{Error, S}
	end.


do_truncate(Part, Offset, S) ->
	case get_part_writable(Part, S) of
		{ok, IoDev, S2} ->
			{ok, _} = file:position(IoDev, Offset),
			{peerdrive_util:fixup_file(file:truncate(IoDev)), S2};
		{error, _} = Error ->
			{Error, S}
	end.


do_set_mtime(Attachment, MTime, S) ->
	case get_part_writable(Attachment, S) of
		{ok, _IoDev, #state{parts=Parts} = S2} ->
			Part = orddict:fetch(Attachment, Parts),
			S3 = S2#state{parts=orddict:store(Attachment, Part#part{mtime=MTime}, Parts)},
			{ok, S3};
		{error, _} = Error ->
			{Error, S}
	end.


do_commit(Comment, #state{did=DId, store=Store} = S) ->
	case prepare_rev(Comment, S) of
		{ok, PreparedRev, S2} ->
			case peerdrive_store:lookup(Store, DId) of
				{ok, CurEncRId, _CurPreRevs} ->
					do_commit_forward(CurEncRId, PreparedRev, S2);
				{error, enoent} ->
					do_commit_put(PreparedRev, S2);
				{error, _} = Error ->
					{Error, S2}
			end;

		{{error, _}, _S2} = Error ->
			Error
	end.


do_commit_forward(CurEncRId, PreparedRev, S) ->
	#state{store=Store, did=DId, prerid=PreRId} = S,
	#preprev{rid=NewRId, encrid=NewEncRId} = PreparedRev,
	case peerdrive_store:forward_doc(Store, DId, [CurEncRId, NewEncRId], PreRId) of
		ok ->
			{{ok, NewRId}, S#state{readonly=true}};

		{ok, [NewEncRId], Handle} ->
			try
				case upload_rev(PreparedRev, S) of
					{ok, NewEncRId} ->
						case peerdrive_store:commit(Handle) of
							{ok, NewEncRId} ->
								{{ok, NewRId}, S#state{readonly=true}};
							Error ->
								{Error, S}
						end;
					Error ->
						{Error, S}
				end
			after
				peerdrive_store:close(Handle)
			end;

		{error, _} = Error ->
			{Error, S}
	end.


do_commit_put(PreparedRev, #state{did=DId, store=Store} = S) ->
	#preprev{rid=NewRId, encrid=NewEncRId} = PreparedRev,
	case peerdrive_store:put_doc(Store, DId, NewEncRId) of
		{ok, Handle} ->
			case upload_rev(PreparedRev, S) of
				{ok, NewEncRId} ->
					case peerdrive_store:commit(Handle) of
						{ok, NewEncRId} ->
							% Keep handle intentionally open to protect DId from
							% garbage collection.
							{{ok, NewRId}, S#state{readonly=true}};
						Error ->
							peerdrive_store:close(Handle),
							{Error, S}
					end;
				Error ->
					peerdrive_store:close(Handle),
					{Error, S}
			end;

		{error, _} = Error ->
			{Error, S}
	end.


do_suspend(Comment, #state{did=DId, store=Store, prerid=PreRId} = S) ->
	case prepare_rev(Comment, S) of
		{ok, PreparedRev, S2} ->
			#preprev{rid=NewRId, encrid=NewEncRId} = PreparedRev,
			case peerdrive_store:remember_rev(Store, DId, NewEncRId, PreRId) of
				ok ->
					{{ok, NewRId}, S2#state{readonly=true}};

				{ok, Handle} ->
					try
						case upload_rev(PreparedRev, S2) of
							{ok, NewEncRId} ->
								case peerdrive_store:commit(Handle) of
									{ok, NewEncRId} ->
										{{ok, NewRId}, S2#state{readonly=true}};
									Error ->
										{Error, S2}
								end;
							Error ->
								{Error, S2}
						end
					after
						peerdrive_store:close(Handle)
					end;

				{error, _} = Error ->
					{Error, S2}
			end;

		{{error, _}, _S2} = Error ->
			Error
	end.


get_part_writable(Part, #state{parts=Parts, key=Key} = S) ->
	case orddict:find(Part, Parts) of
		{ok, #part{iodev=IoDev, mtime=undefined}} ->
			{ok, IoDev, S};

		{ok, #part{iodev=IoDev}=P} ->
			S2 = S#state{parts=orddict:store(Part, P#part{mtime=undefined}, Parts)},
			{ok, IoDev, S2};

		error ->
			{Name, IoDev} = tmp_file(),
			try
				P = case lists:keyfind(Part, #rev_att.name, (S#state.rev)#rev.attachments) of
					#rev_att{hash=PId, crtime=CrTime, mtime=MTime} ->
						DecState = peerdrive_crypto:aes_ctr_stream_init(Key,
							peerdrive_crypto:make_bin_16(PId)),
						ok = part_copy_loop(Part, S#state.handle, IoDev, 0, DecState),
						#part{name=Name, iodev=IoDev, crtime=CrTime, mtime=MTime};
					false ->
						Now = peerdrive_util:get_time(),
						#part{name=Name, iodev=IoDev, crtime=Now, mtime=Now}
				end,
				NewParts = orddict:store(Part, P, Parts),
				{ok, IoDev, S#state{parts=NewParts}}
			catch
				throw:Error ->
					file:close(IoDev),
					file:delete(Name),
					Error
			end
	end.


part_copy_loop(Part, Handle, IoDev, Offset, DecState) ->
	case peerdrive_store:read(Handle, Part, Offset, 16#100000) of
		{ok, <<>>} ->
			ok;
		{ok, EncData} ->
			{NewDecState, Data} = peerdrive_crypto:aes_ctr_stream_decrypt(DecState, EncData),
			case file:write(IoDev, Data) of
				ok ->
					part_copy_loop(Part, Handle, IoDev, Offset+16#100000, NewDecState);
				Error ->
					throw(peerdrive_util:fixup_file(Error))
			end;
		Error ->
			throw(Error)
	end.


prepare_rev(Comment, S) ->
	case fetch_data(S) of
		{ok, S2} ->
			try
				do_prepare_rev(Comment, S2)
			catch
				throw:Error -> {Error, S2}
			end;

		{{error, _}, _S2} = Error ->
			Error
	end.

do_prepare_rev(Comment, #state{rev=Rev, parts=Parts, key=Key, data=Data} = S) ->
	{DocLinks, RevLinks} = peerdrive_struct:extract_links(Data),
	DataSize = size(Data),
	DataHash = peerdrive_crypto:merkle(Data),
	LocalParts = [
		case peerdrive_util:hash_file(IoDev) of
			{ok, Size, Sha} ->
				#rev_att{
					name=Name, size=Size, hash=Sha, crtime=CrTime,
					mtime = case MTime of
						undefined -> peerdrive_util:get_time();
						_ -> MTime
					end
				};
			{error, _} = HashErr ->
				throw(HashErr)
		end
		|| {Name, #part{iodev=IoDev, crtime=CrTime, mtime=MTime}}
			<- Parts ],
	NewParts = lists:keymerge(#rev_att.name,
		lists:keysort(#rev_att.name, LocalParts),
		lists:keysort(#rev_att.name, Rev#rev.attachments)),
	Mtime = peerdrive_util:get_time(),
	NewComment = case Comment of
		undefined -> Rev#rev.comment;
		_ -> Comment
	end,
	NewRev = Rev#rev{data=#rev_dat{size=DataSize, hash=DataHash},
		attachments=NewParts, mtime=Mtime, comment=NewComment},
	NewRId = peerdrive_store:hash_revision(NewRev),
	PreparedRev = #preprev{
		rid = NewRId,
		encrid = peerdrive_crypt_store:enc_xid(Key, NewRId),
		encrev = peerdrive_crypt_store:enc_revision(NewRId, NewRev, Key),
		encdata = peerdrive_crypto:aes_ctr_encrypt(Key, peerdrive_crypto:make_bin_16(DataHash), Data),
		encdoclinks = [ peerdrive_crypt_store:enc_xid(Key, DId) || DId <- DocLinks ],
		encrevlinks = [ peerdrive_crypt_store:enc_xid(Key, RId) || RId <- RevLinks ]
	},
	{ok, PreparedRev, S#state{rev=NewRev}}.


upload_rev(PreparedRev, #state{store=Store} = S) ->
	#preprev{
		encrid = RId,
		encrev = Rev,
		encdata = Data,
		encdoclinks = DocLinks,
		encrevlinks = RevLinks
	} = PreparedRev,
	case peerdrive_store:put_rev(Store, RId, Rev, Data, DocLinks, RevLinks) of
		{ok, MissingParts, Handle} ->
			try
				lists:foreach(fun(FCC) -> upload_rev_part(Handle, FCC, S) end,
					MissingParts),
				peerdrive_store:commit(Handle)
			catch
				throw:Error -> Error
			after
				peerdrive_store:close(Handle)
			end;

		{error, _} = Error ->
			Error
	end.


upload_rev_part(Handle, Name, #state{parts=Parts, key=Key, rev=Rev}) ->
	#part{iodev=IoDev} = orddict:fetch(Name, Parts),
	#rev_att{hash=PId} = lists:keyfind(Name, #rev_att.name, Rev#rev.attachments),
	EncState = peerdrive_crypto:aes_ctr_stream_init(Key, peerdrive_crypto:make_bin_16(PId)),
	file:position(IoDev, 0),
	upload_rev_part_loop(Handle, Name, IoDev, EncState).


upload_rev_part_loop(Handle, FCC, IoDev, EncState) ->
	case file:read(IoDev, 16#100000) of
		{ok, Data} ->
			{NewEncState, EncData} = peerdrive_crypto:aes_ctr_stream_encrypt(EncState, Data),
			case peerdrive_store:put_rev_part(Handle, FCC, EncData) of
				ok ->
					upload_rev_part_loop(Handle, FCC, IoDev, NewEncState);
				Else ->
					throw(Else)
			end;
		eof ->
			ok;
		Else ->
			throw(peerdrive_util:fixup_file(Else))
	end.


tmp_file() ->
	{A, B, C} = now(),
	Name = lists:flatten(io_lib:format("~p.~p.~p", [A,B,C])),
	{ok, IoDev} = file:open(Name, [read, write, binary, exclusive, raw]),
	{Name, IoDev}.


fetch_data(#state{data=undefined, handle=Handle, key=Key, rev=Rev} = S) ->
	case peerdrive_store:get_data(Handle, <<>>) of
		{ok, EncData} ->
			Data = peerdrive_crypto:aes_ctr_decrypt(Key,
				peerdrive_crypto:make_bin_16((Rev#rev.data)#rev_dat.hash), EncData),
			{ok, S#state{data=Data}};

		Error ->
			{Error, S}
	end;

fetch_data(S) ->
	{ok, S}.

