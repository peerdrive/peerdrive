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

-module(peerdrive_file_store_io).
-behaviour(gen_server).

-export([start_link/2, start_link/4]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-include("store.hrl").

-define(THRESHOLD, 1024).

-record(state, {
	store    :: pid(),
	did      :: undefined | peerdrive:doc(),
	prerid   :: undefined | peerdrive:rev(),
	rev      :: #rev{},
	parts    :: orddict:orddict(), % [{Name :: data|binary(), Content :: #part{}}]
	readonly :: boolean(),
	locks    :: [peerdrive:hash()],
	user     :: pid()
}).
-record(part, {
	mode   :: rw | ro,
	data   :: binary() | {FileName::string(), IoDev::file:io_device()},
	crtime :: undefined | integer(),
	mtime  :: undefined | integer()
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Rev, User) ->
	gen_server:start_link(?MODULE, {self(), undefined, undefined, Rev, User}, []).

start_link(DId, PreRId, Rev, User) ->
	gen_server:start_link(?MODULE, {self(), DId, PreRId, Rev, User}, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Store, DId, PreRId, Rev, User}) ->
	process_flag(trap_exit, true),
	link(User),
	State = #state{
		store    = Store,
		did      = DId,
		prerid   = PreRId,
		rev      = Rev,
		parts    = orddict:new(),
		readonly = not is_binary(DId),
		locks    = [(Rev#rev.data)#rev_dat.hash] ++
			[PId || #rev_att{hash=PId} <- Rev#rev.attachments],
		user     = User
	},
	{ok, State}.


% returns: {ok, Data} | {error, Reason}
handle_call({get_data, Selector}, _From, S) ->
	{Reply, S2} = do_get_data(Selector, S),
	{reply, Reply, S2};

% returns: {ok, Data} | {error, Reason}
handle_call({read, Part, Offset, Length}, _From, S) ->
	{Reply, S2} = do_read(Part, Offset, Length, S),
	{reply, Reply, S2};

% returns nothing
handle_call(close, _From, S) ->
	{stop, normal, ok, S};

handle_call(fstat, _From, S) ->
	{Reply, S2} = do_fstat(S),
	{reply, Reply, S2};

% the following calls are only allowed when still writable
handle_call(_Request, _From, S = #state{readonly=true}) ->
	{reply, {error, ebadf}, S};

% returns `ok | {error, Reason}'
handle_call({set_data, Selector, Data}, _From, S) ->
	case peerdrive_struct:verify(Data) of
		ok ->
			{Reply, S2} = do_set_data(Selector, Data, S),
			{reply, Reply, S2};
		error ->
			{reply, {error, einval}, S}
	end;

% returns `ok | {error, Reason}'
handle_call({write, Part, Offset, Data}, _From, S) ->
	{Reply, S2} = do_write(Part, Offset, Data, S),
	{reply, Reply, S2};

% returns `ok | {error, Reason}'
handle_call({truncate, Part, Offset}, _From, S) ->
	{Reply, S2} = do_truncate(Part, Offset, S),
	{reply, Reply, S2};

% returns `{ok, Hash} | {error, Reason}'
handle_call(commit, _From, S) ->
	{Reply, S2} = do_commit(fun peerdrive_file_store:commit/6, undefined, S),
	{reply, Reply, S2};

% returns `{ok, Hash} | {error, Reason}'
handle_call({commit, Comment}, _From, S) ->
	{Reply, S2} = do_commit(fun peerdrive_file_store:commit/6, Comment, S),
	{reply, Reply, S2};

% returns `{ok, Hash} | {error, Reason}'
handle_call({suspend, Comment}, _From, S) ->
	{Reply, S2} = do_commit(fun peerdrive_file_store:suspend/6, Comment, S),
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
	S2 = do_set_parents(Parents, S),
	{reply, ok, S2};

handle_call({set_mtime, Attachment, MTime}, _From, S) ->
	{Reply, S2} = do_set_mtime(Attachment, MTime, S),
	{reply, Reply, S2}.


handle_info({'EXIT', From, Reason}, #state{store=Store}=S) ->
	case From of
		Store -> {stop, {orphaned, Reason}, S};
		_User -> {stop, normal, S}
	end.


terminate(_Reason, #state{user=User, did=DId, rev=Rev, store=Store} = S) ->
	% unlink from user to not kill'em
	unlink(User),
	% unlock hashes
	lists:foreach(
		fun(PId) -> peerdrive_file_store:part_unlock(Store, PId) end,
		S#state.locks),
	% unlock parents
	lists:foreach(
		fun(RId) -> peerdrive_file_store:rev_unlock(Store, RId) end,
		Rev#rev.parents),
	% expose document to garbage collector
	DId == undefined orelse peerdrive_file_store:doc_unlock(Store, DId),
	% clean up files
	lists:foreach(
		fun
			({_PId, #part{mode=ro, data={_FileName, IoDev}}}) ->
				file:close(IoDev);
			({_PId, #part{mode=rw, data={FileName, IoDev}}}) ->
				file:close(IoDev),
				file:delete(FileName);
			(_) ->
				ok
		end,
		S#state.parts),
	S#state{locks=[], parts=[]}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


handle_cast(_, State)    -> {noreply, State}.
code_change(_, State, _) -> {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_get_data(Selector, S) ->
	case get_part_readable(data, S) of
		{ok, Data, S2} when is_binary(Data) ->
			try
				{{ok, peerdrive_struct:extract(Data, Selector)}, S2}
			catch
				error:badarg -> {{error, einval}, S2};
				error:enoent -> {{error, enoent}, S2}
			end;

		{ok, {_, IoDev}, S2} ->
			FileData = case file:pread(IoDev, 0, 16#0fffffff) of
				eof -> {ok, <<>>};
				Else -> peerdrive_util:fixup_file(Else)
			end,
			case FileData of
				{ok, Data} ->
					try
						{{ok, peerdrive_struct:extract(Data, Selector)}, S2}
					catch
						error:badarg -> {{error, einval}, S2};
						error:enoent -> {{error, enoent}, S2}
					end;
				Error ->
					{Error, S2}
			end;

		{error, _} = Error ->
			{Error, S}
	end.


do_read(Part, Offset, Length, S) ->
	case get_part_readable(Part, S) of
		{ok, Data, S2} when is_binary(Data) ->
			{{ok, read_from_binary(Data, Offset, Length)}, S2};

		{ok, {_, IoDev}, S2} ->
			case file:pread(IoDev, Offset, Length) of
				eof -> {{ok, <<>>}, S2};
				Else -> {peerdrive_util:fixup_file(Else), S2}
			end;

		{error, _} = Error ->
			{Error, S}
	end.


do_set_data(Selector, Data, S) ->
	case do_get_data(<<>>, S) of
		{{ok, Orig}, S2} ->
			try
				New = peerdrive_struct:update(Orig, Selector, Data),
				case do_truncate(data, size(New), S2) of
					{ok, S3}   -> do_write(data, 0, New, S3);
					TruncError -> TruncError
				end
			catch
				error:badarg -> {{error, einval}, S2};
				error:enoent -> {{error, enoent}, S2}
			end;

		ReadError ->
			ReadError
	end.


do_write(Part, Offset, Data, S) ->
	Conv = case Offset + size(Data) =< ?THRESHOLD of
		true  -> keep;
		false -> forcefile
	end,
	case get_part_writable(Part, Conv, S) of
		{ok, OldData, S2} when is_binary(OldData) ->
			S3 = set_part_data(Part, write_into_binary(OldData, Offset, Data), S2),
			{ok, S3};

		{ok, {_, IoDev}=OldData, S2} ->
			case peerdrive_util:fixup_file(file:pwrite(IoDev, Offset, Data)) of
				ok ->
					{ok, set_part_data(Part, OldData, S2)};
				{error, _} = Error ->
					{Error, S2}
			end;

		{error, _} = Error ->
			{Error, S}
	end.


do_truncate(Part, Offset, S) ->
	Conv = case Offset > ?THRESHOLD of
		true  -> forcefile;
		false -> forcebin
	end,
	case get_part_writable(Part, Conv, S) of
		{ok, OldData, S2} when is_binary(OldData) ->
			S3 = set_part_data(Part, trunc_binary(OldData, Offset), S2),
			{ok, S3};

		{ok, {_, IoDev}=OldData, S2} ->
			{ok, _} = file:position(IoDev, Offset),
			case peerdrive_util:fixup_file(file:truncate(IoDev)) of
				ok ->
					{ok, set_part_data(Part, OldData, S2)};
				{error, _} = Error ->
					{Error, S2}
			end;

		{error, _} = Error ->
			{Error, S}
	end.


do_commit(Fun, Comment, S) ->
	case extract_links(S) of
		{{ok, DocLinks, RevLinks}, S2} ->
			case close_and_writeback(S2) of
				{ok, #state{store=Store, did=DId, prerid=PreRId} = S3} ->
					Rev1 = S3#state.rev,
					Rev2 = Rev1#rev{mtime=peerdrive_util:get_time()},
					Rev3 = case Comment of
						undefined -> Rev2;
						_ -> Rev2#rev{comment=Comment}
					end,
					case Fun(Store, DId, PreRId, Rev3, DocLinks, RevLinks) of
						{ok, _Rev} = Ok ->
							{Ok, S3#state{rev=Rev3, readonly=true}};
						{error, _} = Error ->
							{Error, S3}
					end;

				{{error, _}, _S2} = Error ->
					Error
			end;

		{{error, _}, _S2} = Error ->
			Error
	end.


do_set_parents(Parents, #state{rev=Rev, store=Store} = S) ->
	% lock new parents
	lists:foreach(
		fun(RId) -> peerdrive_file_store:rev_lock(Store, RId) end,
		Parents),
	% unlock old parents
	lists:foreach(
		fun(RId) -> peerdrive_file_store:rev_unlock(Store, RId) end,
		Rev#rev.parents),
	NewRev = Rev#rev{parents=Parents},
	S#state{rev=NewRev}.


do_fstat(#state{rev=Rev, parts=Parts} = S) ->
	Now = peerdrive_util:get_time(),
	try
		% lazy update of mtime
		UpdatedParts = [
			Part#part{mtime=case MTime of undefined -> Now; _ -> MTime end}
			|| #part{mtime=MTime} = Part <- Parts ],
		All = [
			case Data of
				{_TmpName, IoDev} ->
					case file:position(IoDev, eof) of
						{ok, Size} ->
							#rev_att{
								name=Name, size=Size, hash= <<>>, crtime=CrTime,
								mtime=MTime
							};
						{error, _} = UpdateErr ->
							throw(UpdateErr)
					end;
				Bin when is_binary(Bin) ->
					#rev_att{
						name=Name, size=size(Bin), hash= <<>>, crtime=CrTime,
						mtime=MTime
					}
			end
			|| {Name, #part{data=Data, crtime=CrTime, mtime=MTime}}
				<- UpdatedParts ],
		NewAttachments = [ A || #rev_att{name=Name} = A <- All, Name =/= data ],
		OldAttachments = [ A#rev_att{hash= <<>>} || A <- Rev#rev.attachments ],
		Attachments = lists:keymerge(#rev_att.name,
			lists:keysort(#rev_att.name, NewAttachments),
			lists:keysort(#rev_att.name, OldAttachments)),
		DataSize = case lists:keyfind(data, #rev_att.name, All) of
			#rev_att{size=S} -> S;
			false -> (Rev#rev.data)#rev_dat.size
		end,
		StatRev = Rev#rev{ data=#rev_dat{size=DataSize, hash= <<>>},
			attachments=Attachments, mtime=Now},
		S2 = S#state{parts=UpdatedParts},
		{{ok, StatRev}, S2}
	catch
		throw:Error ->
			{Error, S}
	end.


do_set_mtime(Attachment, MTime, #state{rev=Rev, parts=Parts} = S) ->
	case orddict:find(Attachment, Parts) of
		{ok, Part} ->
			NewParts = orddict:store(Attachment, Part#part{mtime=MTime}, Parts),
			{ok, S#state{parts=NewParts}};
		error ->
			Attachments = Rev#rev.attachments,
			case lists:keyfind(Attachment, #rev_att.name, Attachments) of
				#rev_att{} = A ->
					NewAttachments = lists:keystore(Attachment, #rev_att.name,
						Attachments, A#rev_att{mtime=MTime}),
					{ok, S#state{rev=Rev#rev{attachments=NewAttachments}}};
				false ->
					{{error, enoent}, S}
			end
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

extract_links(S) ->
	case do_get_data(<<>>, S) of
		{{ok, BinData}, S2} ->
			{DocLinks, RevLinks} = peerdrive_struct:extract_links(BinData),
			{{ok, DocLinks, RevLinks}, S2};

		{{error, _}, _S2} = Error ->
			Error
	end.


close_and_writeback(#state{parts=[]} = S) ->
	{ok, S};

close_and_writeback(#state{parts=[{Part, Content} | RemParts]} = S) ->
	case Content of
		#part{mode=rw, data={FileName, IoDev}} ->
			case peerdrive_util:hash_file(IoDev) of
				{ok, Size, PId} ->
					ok = file:close(IoDev),
					S2 = lock_part(PId, S),
					case peerdrive_file_store:part_put(S2#state.store, PId, FileName) of
						ok ->
							S3 = commit_part(Part, Content, Size, PId,
								S2#state{parts=RemParts}),
							close_and_writeback(S3);

						{error, _Reason} = Error ->
							% pray that we can re-open the file
							{ok, NewIoDev} = file:open(FileName, [write, read, binary, raw]),
							S3 = S2#state{
								parts=[{Part, {rw, {FileName, NewIoDev}}} | RemParts]
							},
							{Error, S3}
					end;

				{error, _} = Error ->
					{Error, S}
			end;

		#part{mode=rw, data=Data} when is_binary(Data) ->
			PId = peerdrive_crypto:merkle(Data),
			S2 = lock_part(PId, S),
			case peerdrive_file_store:part_put(S#state.store, PId, Data) of
				ok ->
					S3 = commit_part(Part, Content, size(Data), PId,
						S2#state{parts=RemParts}),
					close_and_writeback(S3);
				{error, _Reason} = Error ->
					% keep the part open because there were no side effects yet
					{Error, S2}
			end;

		#part{mode=ro, data={_FileName, IoDev}} ->
			ok = file:close(IoDev),
			close_and_writeback(S#state{parts=RemParts});

		#part{mode=ro, data=Data} when is_binary(Data) ->
			close_and_writeback(S#state{parts=RemParts})
	end.


commit_part(data, _, Size, PId, #state{rev=Rev} = S) ->
	S#state{rev = Rev#rev{data=#rev_dat{size=Size, hash=PId}}};

commit_part(Part, #part{crtime=CrTime, mtime=MTime}, Size, PId, #state{rev=Rev} = S) ->
	Attachment = #rev_att{
		name = Part,
		size = Size,
		hash = PId,
		crtime = CrTime,
		mtime = case MTime of undefined -> peerdrive_util:get_time(); _ -> MTime end
	},
	NewRev = Rev#rev{attachments=lists:keystore(Part, #rev_att.name,
		Rev#rev.attachments, Attachment)},
	S#state{rev=NewRev}.


lock_part(PId, #state{locks=Locks} = S) ->
	peerdrive_file_store:part_lock(S#state.store, PId),
	S#state{locks=[PId | Locks]}.


get_part_readable(Part, S) ->
	case orddict:find(Part, S#state.parts) of
		{ok, #part{data=Data}} ->
			{ok, Data, S};
		error ->
			part_open_read(Part, S)
	end.


part_open_read(Name, #state{rev=Rev} = S) ->
	Search = case Name of
		data ->
			{(Rev#rev.data)#rev_dat.hash, undefined, undefined};
		_ ->
			case lists:keyfind(Name, #rev_att.name, Rev#rev.attachments) of
				#rev_att{hash=Hash, crtime=CrTime0, mtime=MTime0} ->
					{Hash, CrTime0, MTime0};
				false ->
					false
			end
	end,
	case Search of
		{PId, CrTime, MTime} ->
			case peerdrive_file_store:part_get(S#state.store, PId) of
				{ok, Data} when is_binary(Data) ->
					Part = #part{mode=ro, data=Data, crtime=CrTime, mtime=MTime},
					S2 = S#state{parts=orddict:store(Name, Part, S#state.parts)},
					{ok, Data, S2};

				{ok, FileName} when is_list(FileName) ->
					case file:open(FileName, [read, binary, raw]) of
						{ok, IoDev} ->
							Content = {FileName, IoDev},
							Part = #part{mode=ro, data=Content, crtime=CrTime, mtime=MTime},
							S2 = S#state{parts=orddict:store(Name, Part, S#state.parts)},
							{ok, Content, S2};

						{error, _} = Error ->
							peerdrive_util:fixup_file(Error)
					end;

				{error, _} = Error ->
					Error
			end;

		false ->
			{error, enoent}
	end.


get_part_writable(Name, Conv, S) ->
	case orddict:find(Name, S#state.parts) of
		{ok, #part{mode=rw, data=Content}=Part} ->
			if
				is_binary(Content) and (Conv == forcefile) ->
					part_bin_to_file(Name, Part, S);
				not is_binary(Content) and (Conv == forcebin) ->
					part_file_to_bin(Name, Part, S);
				true ->
					{ok, Content, S}
			end;

		{ok, #part{mode=ro, data=Content}=Part} ->
			if
				is_binary(Content) and (Conv == forcefile) ->
					part_bin_to_file(Name, Part, S);
				not is_binary(Content) and (Conv == forcebin) ->
					part_file_to_bin(Name, Part, S);
				true ->
					part_open_write(Name, Conv, part_close_abort(Name, S))
			end;

		error ->
			part_open_write(Name, Conv, S)
	end.


part_bin_to_file(Name, #part{data=Data}=Part, S) ->
	case create_tmp_file(S) of
		{ok, FileName, IoDev} ->
			ok = file:pwrite(IoDev, 0, Data),
			Content = {FileName, IoDev},
			S2 = S#state{parts=orddict:store(Name,
				Part#part{mode=rw, data=Content}, S#state.parts)},
			{ok, Content, S2};

		{error, _} = Error ->
			Error
	end.


part_file_to_bin(Name, #part{data={_FileName, IoDev}} = Part, S) ->
	case file:pread(IoDev, 0, ?THRESHOLD) of
		{ok, Data} ->
			S2 = part_close_abort(Name, S),
			S3 = S2#state{parts=orddict:store(Name,
				Part#part{mode=rw, data=Data}, S2#state.parts)},
			{ok, Data, S3};

		eof ->
			S2 = part_close_abort(Name, S),
			S3 = S2#state{parts=orddict:store(Name,
				Part#part{mode=rw, data = <<>>}, S2#state.parts)},
			{ok, <<>>, S3};

		{error, _} = Error ->
			peerdrive_util:fixup_file(Error)
	end.


part_open_write(Name, Conv, #state{rev=Rev, parts=Parts} = S) ->
	Search = case Name of
		data ->
			{(Rev#rev.data)#rev_dat.hash, #part{mode=rw}};
		_ ->
			case lists:keyfind(Name, #rev_att.name, Rev#rev.attachments) of
				#rev_att{hash=Hash, crtime=CrTime, mtime=MTime} ->
					{Hash, #part{mode=rw, crtime=CrTime, mtime=MTime}};
				false ->
					false
			end
	end,
	case Search of
		{PId, Part} ->
			case peerdrive_file_store:part_get(S#state.store, PId) of
				{ok, Data} when is_binary(Data) ->
					if
						Conv == forcefile ->
							part_bin_to_file(Name, Part#part{data=Data}, S);
						true ->
							S2 = S#state{parts=orddict:store(Name,
								Part#part{data=Data}, Parts)},
							{ok, Data, S2}
					end;

				{ok, FileName} when is_list(FileName) ->
					if
						Conv == forcebin ->
							part_read_bin(Name, FileName, Part, S);
						true ->
							part_copy_and_open(Name, FileName, Part, S)
					end;

				{error, _} = Error ->
					Error
			end;

		false ->
			Now = peerdrive_util:get_time(),
			Part = #part{mode=rw, data = <<>>, crtime=Now, mtime=Now},
			if
				Conv == forcefile ->
					part_bin_to_file(Name, Part, S);
				true ->
					S2 = S#state{parts=orddict:store(Name, Part, S#state.parts)},
					{ok, <<>>, S2}
			end
	end.


part_read_bin(Name, FileName, Part0, S) ->
	case file:open(FileName, [read, binary, raw]) of
		{ok, IoDev} ->
			case file:pread(IoDev, 0, ?THRESHOLD) of
				{ok, Data} ->
					ok = file:close(IoDev),
					Part = Part0#part{data=Data},
					S2 = S#state{parts=orddict:store(Name, Part, S#state.parts)},
					{ok, Data, S2};

				eof ->
					ok = file:close(IoDev),
					Part = Part0#part{data = <<>>},
					S2 = S#state{parts=orddict:store(Name, Part, S#state.parts)},
					{ok, <<>>, S2};

				{error, _} = Error ->
					file:close(IoDev),
					peerdrive_util:fixup_file(Error)
			end;

		{error, _} = Error ->
			peerdrive_util:fixup_file(Error)
	end.


part_copy_and_open(Name, OrigName, Part0, S) ->
	TmpName = tmp_name(S),
	case file:copy(OrigName, TmpName) of
		{ok, _} ->
			case file:open(TmpName, [write, read, binary, raw]) of
				{ok, IoDev} ->
					Content = {TmpName, IoDev},
					Part = Part0#part{data=Content},
					S2 = S#state{parts=orddict:store(Name, Part, S#state.parts)},
					{ok, Content, S2};

				Error ->
					file:delete(TmpName),
					peerdrive_util:fixup_file(Error)
			end;

		Error ->
			peerdrive_util:fixup_file(Error)
	end.


part_close_abort(Name, S) ->
	case orddict:fetch(Name, S#state.parts) of
		#part{mode=rw, data={FileName, IoDev}} ->
			file:close(IoDev),
			file:delete(FileName);

		#part{mode=ro, data={_FileName, IoDev}} ->
			file:close(IoDev);

		_ ->
			ok
	end,
	S#state{parts=orddict:erase(Name, S#state.parts)}.


set_part_data(Part, Data, #state{parts=OldParts} = S) ->
	NewParts = orddict:update(Part,
		fun(Content) -> Content#part{data=Data, mtime=undefined} end,
		OldParts),
	S#state{parts=NewParts}.


read_from_binary(Data, Offset, Length) ->
	Size = size(Data),
	InOffset = if
		Offset >= Size -> Size;
		true -> Offset
	end,
	InLength = if
		InOffset+Length > Size -> Size-InOffset;
		true -> Length
	end,
	binary:part(Data, InOffset, InLength).


write_into_binary(OldData, Offset, Data) ->
	OldSize = size(OldData),
	Length = size(Data),
	Prefix = if
		Offset <  OldSize -> binary:part(OldData, 0, Offset);
		Offset == OldSize -> OldData;
		Offset >  OldSize -> <<OldData/binary, 0:((Offset-OldSize)*8)>>
	end,
	Postfix = if
		Offset+Length < OldSize ->
			binary:part(OldData, Offset+Length, OldSize-Offset-Length);
		true ->
			<<>>
	end,
	<<Prefix/binary, Data/binary, Postfix/binary>>.


trunc_binary(Data, Offset) ->
	Size = size(Data),
	if
		Offset < Size ->
			binary:part(Data, 0, Offset);
		Offset > Size ->
			<<Data/binary, 0:((Offset-Size)*8)>>;
		true ->
			Data
	end.


tmp_name(#state{store=Store}) ->
	peerdrive_file_store:tmp_name(Store).


create_tmp_file(S) ->
	TmpName = tmp_name(S),
	case file:open(TmpName, [read, write, binary, exclusive, raw]) of
		{ok, IoDev} ->
			{ok, TmpName, IoDev};
		Error ->
			peerdrive_util:fixup_file(Error)
	end.

