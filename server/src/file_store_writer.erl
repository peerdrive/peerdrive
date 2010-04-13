%% Hotchpotch
%% Copyright (C) 2010  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
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

-export([start/2]).
-export([write_part/4, write_trunc/3, write_commit/2, write_abort/1]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-include("store.hrl").
-include("file_store.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start(State, User) ->
	case gen_server:start(?MODULE, {State, User}, []) of
		{ok, Pid} ->
			{ok, #writer{
				this        = Pid,
				write_part  = fun write_part/4,
				write_trunc = fun write_trunc/3,
				abort       = fun write_abort/1,
				commit      = fun write_commit/2
			}};
		Else ->
			Else
	end.

write_part(Writer, Part, Offset, Data) ->
	gen_server:call(Writer, {write, Part, Offset, Data}).

write_trunc(Writer, Part, Offset) ->
	gen_server:call(Writer, {truncate, Part, Offset}).

write_commit(Writer, Mtime) ->
	gen_server:call(Writer, {commit, Mtime}).

write_abort(Writer) ->
	gen_server:call(Writer, abort).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({State, User}) ->
	process_flag(trap_exit, true),
	link(State#ws.server),
	link(User),
	{ok, State}.


% returns `ok | {error, Reason}'
handle_call({write, Part, Offset, Data}, _From, S) ->
	{S2, File} = writer_get_file(S, Part),
	Reply = case File of
		{error, Reason} ->
			{error, Reason};
		IoDevice ->
			file:pwrite(IoDevice, Offset, Data)
	end,
	{reply, Reply, S2};

% returns `ok | {error, Reason}'
handle_call({truncate, Part, Offset}, _From, S) ->
	{S2, File} = writer_get_file(S, Part),
	Reply = case File of
		{error, Reason} ->
			{error, Reason};
		IoDevice ->
			file:position(IoDevice, Offset),
			file:truncate(IoDevice)
	end,
	{reply, Reply, S2};

% returns `{ok, Hash} | {error, conflict} | {error, Reason}'
handle_call({commit, Mtime}, _From, S) ->
	Reply = do_commit(S, Mtime),
	{stop, normal, Reply, S};

% returns nothing
handle_call(abort, _From, S) ->
	do_abort(S),
	{stop, normal, ok, S}.


handle_info({'EXIT', _From, _Reason}, S) ->
	do_abort(S),
	{stop, orphaned, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


handle_cast(_, State)    -> {stop, enotsup, State}.
code_change(_, State, _) -> {ok, State}.
terminate(_, _)          -> ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% returns {S, {error, Reason}} | {S2, IoDevice}
writer_get_file(S, Part) ->
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
							gen_server:cast(S#ws.server, {part_ref_dec, Hash}),
							{S#ws{orig=dict:erase(Part, S#ws.orig)},
							file:open(FileName, [write, read, binary])};
						Error ->
							{S, Error}
					end;

				% doesn't exit yet; create new one...
				error ->
					{S, file:open(FileName, [write, read, binary])}
			end,
			case FileHandle of
				{ok, IoDevice} ->
					{
						S2#ws{new=dict:store(Part, {FileName, IoDevice}, S2#ws.new)},
						IoDevice
					};
				Else2 ->
					{S2, Else2}
			end
	end.


% calculate hashes, close&move to correct dir, update uuid
% returns {ok, Hash} | {error, conflict} | {error, Reason}
do_commit(S, Mtime) ->
	Parts = dict:fold(
		% FIXME: this definitely lacks error handling :(
		fun (Part, {TmpName, IODevice}, Acc) ->
			{ok, Hash} = hash_file(IODevice),
			file:close(IODevice),
			gen_server:call(S#ws.server, {parts_ref_inc, [Hash]}),
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
		S#ws.new) ++ dict:to_list(S#ws.orig),
	Object = #object{
		flags   = S#ws.flags,
		parts   = lists:sort(Parts),
		parents = lists:sort(S#ws.revs),
		mtime   = Mtime,
		uti     = S#ws.uti},
	gen_server:call(S#ws.server, {commit, S#ws.uuid, Object}).


do_abort(S) ->
	% release parent revs
	lists:foreach(
		fun (Rev) ->
			gen_server:cast(S#ws.server, {object_ref_dec, Rev})
		end,
		S#ws.revs),
	% release original parts
	dict:fold(
		fun (_, Hash, _) ->
			gen_server:cast(S#ws.server, {part_ref_dec, Hash})
		end,
		ok,
		S#ws.orig),
	% delete temporary files
	dict:fold(
		fun (_, {FileName, IODevice}, _) ->
			file:close(IODevice),
			file:delete(FileName)
		end,
		ok,
		S#ws.new).


% returns {ok, Md5} | {error, Reason}
hash_file(File) ->
	file:position(File, 0),
	hash_file_loop(File, crypto:md5_init()).

hash_file_loop(File, Ctx1) ->
	case file:read(File, 16#100000) of
		{ok, Data} ->
			Ctx2 = crypto:md5_update(Ctx1, Data),
			hash_file_loop(File, Ctx2);
		eof ->
			{ok, crypto:md5_final(Ctx1)};
		Else ->
			Else
	end.

