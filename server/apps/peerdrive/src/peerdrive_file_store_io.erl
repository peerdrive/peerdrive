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

% Part -> {ro, binary() | {FileName, IoDev}} |
%         {rw, binary() | {FileName, IoDev}}
-record(state, {store, did, prerid, rev, parts, readonly, locks}).

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
		rev      = Rev#revision{parts=orddict:from_list(Rev#revision.parts)},
		parts    = [],
		readonly = not is_binary(DId),
		locks    = [PId || {_, PId} <- Rev#revision.parts]
	},
	{ok, State}.


% returns: {ok, Data} | {error, Reason}
handle_call({read, Part, Offset, Length}, _From, S) ->
	{Reply, S2} = do_read(Part, Offset, Length, S),
	{reply, Reply, S2};

% returns nothing
handle_call(close, _From, S) ->
	{stop, normal, ok, S};

handle_call(get_flags, _From, S) ->
	{reply, {ok, (S#state.rev)#revision.flags}, S};

handle_call(get_type, _From, S) ->
	{reply, {ok, (S#state.rev)#revision.type}, S};

handle_call(get_parents, _From, S) ->
	{reply, {ok, (S#state.rev)#revision.parents}, S};

handle_call(get_links, _From, S) ->
	Rev = S#state.rev,
	{reply, {ok, {Rev#revision.doc_links, Rev#revision.rev_links}}, S};

% the following calls are only allowed when still writable
handle_call(_Request, _From, S = #state{readonly=true}) ->
	{reply, {error, ebadf}, S};

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
	{Reply, S2} = do_commit(fun peerdrive_file_store:commit/4, S),
	{reply, Reply, S2};

% returns `{ok, Hash} | {error, Reason}'
handle_call(suspend, _From, S) ->
	{Reply, S2} = do_commit(fun peerdrive_file_store:suspend/4, S),
	{reply, Reply, S2};

handle_call({set_flags, Flags}, _From, S) ->
	Rev = S#state.rev,
	NewRev = Rev#revision{flags=Flags},
	{reply, ok, S#state{rev=NewRev}};

handle_call({set_type, Type}, _From, S) ->
	Rev = S#state.rev,
	NewRev = Rev#revision{type=Type},
	{reply, ok, S#state{rev=NewRev}};

handle_call({set_links, DocLinks, RevLinks}, _From, S) ->
	Rev = S#state.rev,
	NewRev = Rev#revision{doc_links=DocLinks, rev_links=RevLinks},
	{reply, ok, S#state{rev=NewRev}};

handle_call({set_parents, Parents}, _From, S) ->
	S2 = do_set_parents(Parents, S),
	{reply, ok, S2}.


handle_info({'EXIT', From, Reason}, #state{store=Store}=S) ->
	case From of
		Store -> {stop, {orphaned, Reason}, S};
		_User -> {stop, normal, S}
	end.


terminate(_Reason, #state{did=DId, rev=Rev, store=Store} = S) ->
	% unlock hashes
	lists:foreach(
		fun(PId) -> peerdrive_file_store:part_unlock(Store, PId) end,
		S#state.locks),
	% unlock parents
	lists:foreach(
		fun(RId) -> peerdrive_file_store:rev_unlock(Store, RId) end,
		Rev#revision.parents),
	% expose document to garbage collector
	DId == undefined orelse peerdrive_file_store:doc_unlock(Store, DId),
	% clean up files
	lists:foreach(
		fun
			({_PId, {ro, {_FileName, IoDev}}}) ->
				file:close(IoDev);
			({_PId, {rw, {FileName, IoDev}}}) ->
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


do_write(Part, Offset, Data, S) ->
	Conv = case Offset + size(Data) =< ?THRESHOLD of
		true  -> keep;
		false -> forcefile
	end,
	case get_part_writable(Part, Conv, S) of
		{ok, OldData, S2} when is_binary(OldData) ->
			S3 = set_part_data(Part, write_into_binary(OldData, Offset, Data), S2),
			{ok, S3};

		{ok, {_, IoDev}, S2} ->
			{peerdrive_util:fixup_file(file:pwrite(IoDev, Offset, Data)), S2};

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

		{ok, {_, IoDev}, S2} ->
			{ok, _} = file:position(IoDev, Offset),
			{peerdrive_util:fixup_file(file:truncate(IoDev)), S2};

		{error, _} = Error ->
			{Error, S}
	end.


do_commit(Fun, S) ->
	case close_and_writeback(S) of
		{ok, S2} ->
			Rev = S2#state.rev,
			NewRev = Rev#revision{mtime=peerdrive_util:get_time()},
			case Fun(S2#state.store, S2#state.did, S2#state.prerid, NewRev) of
				{ok, _Rev} = Ok ->
					{Ok, S2#state{rev=NewRev, readonly=true}};
				{error, _} = Error ->
					{Error, S2}
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
		Rev#revision.parents),
	NewRev = Rev#revision{parents=Parents},
	S#state{rev=NewRev}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

close_and_writeback(#state{parts=[]} = S) ->
	{ok, S};

close_and_writeback(#state{parts=[{Part, Content} | RemParts]} = S) ->
	case Content of
		{rw, {FileName, IoDev}} ->
			case peerdrive_util:hash_file(IoDev) of
				{ok, PId} ->
					ok = file:close(IoDev),
					S2 = lock_part(PId, S),
					case peerdrive_file_store:part_put(S2#state.store, PId, FileName) of
						ok ->
							S3 = commit_part(Part, PId, S2#state{parts=RemParts}),
							close_and_writeback(S3);

						{error, _Reason} = Error ->
							% pray that we can re-open the file
							{ok, NewIoDev} = file:open(FileName, [write, read, binary]),
							S3 = S2#state{
								parts=[{Part, {rw, {FileName, NewIoDev}}} | RemParts]
							},
							{Error, S3}
					end;

				{error, _} = Error ->
					{Error, S}
			end;

		{rw, Data} when is_binary(Data) ->
			PId = peerdrive_util:merkle(Data),
			S2 = lock_part(PId, S),
			case peerdrive_file_store:part_put(S#state.store, PId, Data) of
				ok ->
					S3 = commit_part(Part, PId, S2#state{parts=RemParts}),
					close_and_writeback(S3);
				{error, _Reason} = Error ->
					% keep the part open because there were no side effects yet
					{Error, S2}
			end;

		{ro, {_FileName, IoDev}} ->
			ok = file:close(IoDev),
			close_and_writeback(S#state{parts=RemParts});

		{ro, Data} when is_binary(Data) ->
			close_and_writeback(S#state{parts=RemParts})
	end.


commit_part(Part, PId, #state{rev=Rev} = S) ->
	NewRev = Rev#revision{parts=orddict:store(Part, PId, Rev#revision.parts)},
	S#state{rev=NewRev}.


lock_part(PId, #state{locks=Locks} = S) ->
	peerdrive_file_store:part_lock(S#state.store, PId),
	S#state{locks=[PId | Locks]}.


get_part_readable(Part, S) ->
	case orddict:find(Part, S#state.parts) of
		{ok, {rw, Content}} ->
			{ok, Content, S};
		{ok, {ro, Content}} ->
			{ok, Content, S};
		error ->
			part_open_read(Part, S)
	end.


part_open_read(Part, #state{rev=Rev} = S) ->
	case lists:keyfind(Part, 1, Rev#revision.parts) of
		{Part, PId} ->
			case peerdrive_file_store:part_get(S#state.store, PId) of
				{ok, Data} when is_binary(Data) ->
					S2 = S#state{parts=orddict:store(Part, {ro, Data}, S#state.parts)},
					{ok, Data, S2};

				{ok, FileName} when is_list(FileName) ->
					case file:open(FileName, [read, binary]) of
						{ok, IoDev} ->
							Content = {FileName, IoDev},
							S2 = S#state{parts=orddict:store(Part, {ro, Content}, S#state.parts)},
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


get_part_writable(Part, Conv, S) ->
	case orddict:find(Part, S#state.parts) of
		{ok, {rw, Content}} ->
			if
				is_binary(Content) and Conv == forcefile ->
					part_bin_to_file(Part, Content, S);
				not is_binary(Content) and Conv == forcebin ->
					part_file_to_bin(Part, Content, S);
				true ->
					{ok, Content, S}
			end;

		{ok, {ro, Content}} ->
			if
				is_binary(Content) and Conv == forcefile ->
					part_bin_to_file(Part, Content, S);
				not is_binary(Content) and Conv == forcebin ->
					part_file_to_bin(Part, Content, S);
				true ->
					part_open_write(Part, Conv, part_close_abort(Part, S))
			end;

		error ->
			part_open_write(Part, Conv, S)
	end.


part_bin_to_file(Part, Data, S) ->
	case create_tmp_file(S) of
		{ok, FileName, IoDev} ->
			ok = file:pwrite(IoDev, 0, Data),
			Content = {FileName, IoDev},
			S2 = S#state{parts=orddict:store(Part, {rw, Content}, S#state.parts)},
			{ok, Content, S2};

		{error, _} = Error ->
			Error
	end.


part_file_to_bin(Part, {_FileName, IoDev}, S) ->
	case file:pread(IoDev, 0, ?THRESHOLD) of
		{ok, Data} ->
			S2 = part_close_abort(Part, S),
			S3 = S2#state{parts=orddict:store(Part, {rw, Data}, S2#state.parts)},
			{ok, Data, S3};

		eof ->
			S2 = part_close_abort(Part, S),
			S3 = S2#state{parts=orddict:store(Part, {rw, <<>>}, S2#state.parts)},
			{ok, <<>>, S3};

		{error, _} = Error ->
			peerdrive_util:fixup_file(Error)
	end.


part_open_write(Part, Conv, #state{rev=Rev, parts=Parts} = S) ->
	case lists:keyfind(Part, 1, Rev#revision.parts) of
		{Part, PId} ->
			case peerdrive_file_store:part_get(S#state.store, PId) of
				{ok, Data} when is_binary(Data) ->
					if
						Conv == forcefile ->
							part_bin_to_file(Part, Data, S);
						true ->
							S2 = S#state{parts=orddict:store(Part, {rw, Data}, Parts)},
							{ok, Data, S2}
					end;

				{ok, FileName} when is_list(FileName) ->
					if
						Conv == forcebin ->
							part_read_bin(Part, FileName, S);
						true ->
							part_copy_and_open(Part, FileName, S)
					end;

				{error, _} = Error ->
					Error
			end;

		false ->
			if
				Conv == forcefile ->
					part_bin_to_file(Part, <<>>, S);
				true ->
					S2 = S#state{parts=orddict:store(Part, {rw, <<>>}, S#state.parts)},
					{ok, <<>>, S2}
			end
	end.


part_read_bin(Part, FileName, S) ->
	case file:open(FileName, [read, binary]) of
		{ok, IoDev} ->
			case file:pread(IoDev, 0, ?THRESHOLD) of
				{ok, Data} ->
					ok = file:close(IoDev),
					S2 = S#state{parts=orddict:store(Part, {rw, Data}, S#state.parts)},
					{ok, Data, S2};

				eof ->
					ok = file:close(IoDev),
					S2 = S#state{parts=orddict:store(Part, {rw, <<>>}, S#state.parts)},
					{ok, <<>>, S2};

				{error, _} = Error ->
					file:close(IoDev),
					peerdrive_util:fixup_file(Error)
			end;

		{error, _} = Error ->
			peerdrive_util:fixup_file(Error)
	end.


part_copy_and_open(Part, OrigName, S) ->
	TmpName = tmp_name(S),
	case file:copy(OrigName, TmpName) of
		{ok, _} ->
			case file:open(TmpName, [write, read, binary]) of
				{ok, IoDev} ->
					Content = {TmpName, IoDev},
					S2 = S#state{parts=orddict:store(Part, {rw, Content}, S#state.parts)},
					{ok, Content, S2};

				Error ->
					file:delete(TmpName),
					peerdrive_util:fixup_file(Error)
			end;

		Error ->
			peerdrive_util:fixup_file(Error)
	end.


part_close_abort(Part, S) ->
	case orddict:fetch(Part, S#state.parts) of
		{rw, {FileName, IoDev}} ->
			file:close(IoDev),
			file:delete(FileName);

		{ro, {_FileName, IoDev}} ->
			file:close(IoDev);

		_ ->
			ok
	end,
	S#state{parts=orddict:erase(Part, S#state.parts)}.


set_part_data(Part, Data, S) ->
	S#state{parts=orddict:store(Part, {rw, Data}, S#state.parts)}.


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
	case file:open(TmpName, [read, write, binary, exclusive]) of
		{ok, IoDev} ->
			{ok, TmpName, IoDev};
		Error ->
			peerdrive_util:fixup_file(Error)
	end.

