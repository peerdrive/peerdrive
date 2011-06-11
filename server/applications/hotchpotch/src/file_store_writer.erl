%% Hotchpotch
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

-module(file_store_writer).
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-include("store.hrl").
-include("file_store.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(State, User) ->
	gen_server:start_link(?MODULE, {State, User}, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({State, User}) ->
	process_flag(trap_exit, true),
	link(User),
	{ok, State}.


% returns: {ok, Data} | {error, Reason}
handle_call({read, Part, Offset, Length}, _From, S) ->
	{S2, File} = get_file_read(S, Part),
	Reply = case File of
		{error, Reason} ->
			{error, Reason};
		IoDevice ->
			case file:pread(IoDevice, Offset, Length) of
				eof -> {ok, <<>>};
				Else -> Else
			end
	end,
	{reply, Reply, S2};

% returns nothing
handle_call(close, _From, S) ->
	{stop, normal, ok, S};

handle_call(get_type, _From, S) ->
	{reply, {ok, S#ws.type}, S};

handle_call(get_parents, _From, S) ->
	{reply, {ok, S#ws.baserevs}, S};

handle_call(get_links, _From, S) ->
	{reply, {ok, S#ws.links}, S};

% the following calls are only allowed when still writable
handle_call(_Request, _From, S = #ws{readonly=true}) ->
	{reply, {error, ebadf}, S};

% returns `ok | {error, Reason}'
handle_call({write, Part, Offset, Data}, _From, S) ->
	{S2, File} = get_file_write(S, Part),
	Reply = case File of
		{error, Reason} ->
			{error, Reason};
		IoDevice ->
			file:pwrite(IoDevice, Offset, Data)
	end,
	{reply, Reply, S2};

% returns `ok | {error, Reason}'
handle_call({truncate, Part, Offset}, _From, S) ->
	{S2, File} = get_file_write(S, Part),
	Reply = case File of
		{error, Reason} ->
			{error, Reason};
		IoDevice ->
			file:position(IoDevice, Offset),
			file:truncate(IoDevice)
	end,
	{reply, Reply, S2};

% returns `{ok, Hash} | {error, Reason}'
handle_call({commit, Mtime}, _From, S) ->
	do_commit(fun file_store:commit/4, S, Mtime);

% returns `{ok, Hash} | {error, Reason}'
handle_call({suspend, Mtime}, _From, S) ->
	do_commit(fun file_store:suspend/4, S, Mtime);

handle_call({set_type, Type}, _From, S) ->
	{reply, ok, S#ws{type=Type}};

handle_call({set_links, Links}, _From, S) ->
	{reply, ok, S#ws{links=Links}};

handle_call({set_parents, Parents}, _From, S) ->
	{reply, ok, S#ws{baserevs=Parents}}.


handle_info({'EXIT', From, Reason}, #ws{server=Server}=S) ->
	case From of
		Server -> {stop, {orphaned, Reason}, S};
		_User  -> {stop, normal, S}
	end.


terminate(_Reason, #ws{doc=Doc, locks=Locks, server=Server} = S) ->
	% unlock hashes
	lists:foreach(fun(Lock) -> file_store:unlock(Server, Lock) end, Locks),
	% expose document to garbage collector
	file_store:unhide(Server, Doc),
	% delete temporary files
	dict:fold(
		fun (_, {FileName, IODevice}, _) ->
			file:close(IODevice),
			file:delete(FileName)
		end,
		ok,
		S#ws.new),
	% close readers
	dict:fold(
		fun(_Hash, IODevice, _) -> file:close(IODevice) end,
		ok,
		S#ws.readers),
	S#ws{locks=[], readers=[]}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


handle_cast(_, State)    -> {noreply, State}.
code_change(_, State, _) -> {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% returns {S, {error, Reason}} | {S2, IoDevice}
get_file_read(S, Part) ->
	case dict:find(Part, S#ws.readers) of
		{ok, ReadFile} ->
			{S, ReadFile};

		error ->
			case dict:find(Part, S#ws.new) of
				{ok, {_, WriteFile}} ->
					{S, WriteFile};

				error ->
					open_file_read(S, Part)
			end
	end.


open_file_read(S, Part) ->
	case dict:find(Part, S#ws.orig) of
		{ok, Hash} ->
			FileName = util:build_path(S#ws.path, Hash),
			case file:open(FileName, [read, binary]) of
				{ok, IoDevice} ->
					{
						S#ws{readers=dict:store(Part, IoDevice, S#ws.readers)},
						IoDevice
					};

				Error ->
					{S, Error}
			end;

		error ->
			{S, {error, enoent}}
	end.


% returns {S, {error, Reason}} | {S2, IoDevice}
get_file_write(S, Part) ->
	case dict:find(Part, S#ws.new) of
		% we've already opened the part for writing
		{ok, {_, File}} ->
			{S, File};

		% not writing at this part yet...
		error ->
			FileName = util:gen_tmp_name(S#ws.path),
			{S2, FileHandle} = case dict:find(Part, S#ws.orig) of
				% part already exists; copy and open...
				{ok, Hash} ->
					case file:copy(util:build_path(S#ws.path, Hash), FileName) of
						{ok, _} ->
							% release original part, we have our own copy now
							{
								S#ws{orig=dict:erase(Part, S#ws.orig)},
								file:open(FileName, [write, read, binary])
							};
						Error ->
							{S, Error}
					end;

				% doesn't exist yet; create new one...
				error ->
					{S, file:open(FileName, [write, read, binary])}
			end,
			case FileHandle of
				{ok, IoDevice} ->
					{
						S2#ws{
							new=dict:store(Part, {FileName, IoDevice}, S2#ws.new),
							readers=close_reader(Part, S2#ws.readers)},
						IoDevice
					};
				Else2 ->
					{S2, Else2}
			end
	end.


close_reader(ClosePart, Readers) ->
	dict:filter(
		fun (OpenPart, IoDevice) ->
			case OpenPart of
				ClosePart -> file:close(IoDevice), false;
				_         -> true
			end
		end,
		Readers).


% calculate hashes, close&move to correct dir, update document
% returns {ok, Hash} | {error, Reason}
do_commit(Fun, S, Mtime) ->
	S2 = close_and_move(S),
	Revision = #revision{
		flags   = S2#ws.flags,
		parts   = lists:usort(dict:to_list(S2#ws.orig)),
		parents = lists:usort(S2#ws.baserevs),
		mtime   = Mtime,
		type    = S2#ws.type,
		creator = S2#ws.creator,
		links   = S2#ws.links},
	case Fun(S2#ws.server, S2#ws.doc, S2#ws.prerev, Revision) of
		{ok, _Rev} = Reply ->
			{reply, Reply, S2#ws{readonly=true}};
		Reply ->
			{reply, Reply, S2}
	end.


close_and_move(S) ->
	NewParts = dict:fold(
		% FIXME: this definitely lacks error handling :(
		fun (Part, {TmpName, IODevice}, Acc) ->
			{ok, Hash} = util:hash_file(IODevice),
			file:close(IODevice),
			file_store:lock(S#ws.server, Hash),
			NewName = util:build_path(S#ws.path, Hash),
			case filelib:is_file(NewName) of
				true ->
					file:delete(TmpName);
				false ->
					ok = filelib:ensure_dir(NewName),
					ok = file:rename(TmpName, NewName)
			end,
			[{Part, Hash} | Acc]
		end,
		[],
		S#ws.new),
	NewOrig = lists:foldl(
		fun({Part, Hash}, Acc) -> dict:store(Part, Hash, Acc) end,
		S#ws.orig,
		NewParts),
	NewLocks = lists:map(fun({_Part, Hash}) -> Hash end, NewParts) ++ S#ws.locks,
	S#ws{
		orig  = NewOrig,
		new   = dict:new(),
		locks = NewLocks}.


