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

-module(file_store_importer).
-behaviour(gen_server).

-export([start/7]).
-export([write/3, commit/1, abort/1]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-include("store.hrl").
-include("file_store.hrl").

% store:   pid of the store
% path:    base directory
% rev:     the revision of the object
% object:  THE object
% done:    list of already present hashes
% needed:  dict: FourCC --> {Hash, FileName, IODevice, Md5Ctx}
-record(state, {storepid, path, rev, object, done, needed}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start(Store, Path, Rev, Object, PartsDone, PartsNeeded, User) ->
	% immediately open temp files for needed parts
	NeededDict = lists:foldl(
		fun({FourCC, Hash}, Acc) ->
			FileName = util:gen_tmp_name(Path),
			{ok, IODevice} = file:open(FileName, [write, read, binary]),
			dict:store(FourCC, {Hash, FileName, IODevice, crypto:md5_init()}, Acc)
		end,
		dict:new(),
		PartsNeeded),
	State = #state{
		storepid = Store,
		path = Path,
		rev = Rev,
		object = Object,
		done = PartsDone,
		needed = NeededDict},
	case gen_server:start(?MODULE, {self(), State, User}, []) of
		{ok, Pid} ->
			{ok, #importer{
				this     = Pid,
				put_part = fun write/3,
				abort    = fun abort/1,
				commit   = fun commit/1
			}};
		Else ->
			Else
	end.

write(Importer, Part, Data) ->
	gen_server:call(Importer, {write, Part, Data}).

commit(Importer) ->
	gen_server:call(Importer, commit).

abort(Importer) ->
	gen_server:call(Importer, abort).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({StorePid, State, User}) ->
	process_flag(trap_exit, true),
	link(StorePid),
	link(User),
	{ok, State}.


% returns `ok | {error, Reason}'
handle_call({write, Part, Data}, _From, S) ->
	{S2, Reply} = do_write(S, Part, Data),
	{reply, Reply, S2};

% returns `ok | {error, Reason}'
handle_call(commit, _From, S) ->
	Reply = do_commit(S),
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

do_write(#state{needed=Needed} = S, Part, Data) ->
	case dict:find(Part, Needed) of
		{ok, {Hash, FileName, IODevice, Ctx1}} ->
			case file:write(IODevice, Data) of
				ok ->
					Ctx2 = crypto:md5_update(Ctx1, Data),
					Needed2 = dict:store(Part, {Hash, FileName, IODevice, Ctx2}, Needed),
					{S#state{needed=Needed2}, ok};

				Else -> % {error, Reason}
					{S, Else}
			end;

		error ->
			{S, {error, notneeded}}
	end.


% calculate hashes, close&move to correct dir, update uuid
% returns ok | {error, Reason}
do_commit(S) ->
	% first check if all needed parts are valid
	Vote = dict:fold(
		fun (_Part, {Hash, _TmpName, _IODevice, Md5Ctx}, Acc) ->
			case crypto:md5_final(Md5Ctx) of
				Hash  -> Acc;
				_Else -> error
			end
		end,
		ok,
		S#state.needed),
	% then either commit or abort
	case Vote of
		ok ->
			dict:fold(
				fun (_Part, {Hash, TmpName, IODevice, _Md5Ctx}, _Acc) ->
					file:close(IODevice),
					NewName = util:build_path(S#state.path, Hash),
					gen_server:call(S#state.storepid, {parts_ref_inc, [Hash]}),
					case filelib:is_file(NewName) of
						true ->
							file:delete(TmpName);
						false ->
							ok = filelib:ensure_dir(NewName),
							ok = file:rename(TmpName, NewName)
					end
				end,
				ok,
				S#state.needed),
			gen_server:call(S#state.storepid, {insert_rev, S#state.rev, S#state.object});

		error ->
			do_abort(S),
			{error, everify}
	end.


do_abort(#state{storepid=StorePid, object=Object, done=Done, needed=Needed}) ->
	% release parent revs
	lists:foreach(
		fun (Rev) ->
			gen_server:cast(StorePid, {object_ref_dec, Rev})
		end,
		Object#object.parents),
	% release already present parts
	dict:fold(
		fun (Hash) ->
			gen_server:cast(StorePid, {part_ref_dec, Hash})
		end,
		ok,
		Done),
	% delete temporary files
	dict:fold(
		fun (_, {_Hash, FileName, IODevice, _Md5Ctx}, _) ->
			file:close(IODevice),
			file:delete(FileName)
		end,
		ok,
		Needed).

