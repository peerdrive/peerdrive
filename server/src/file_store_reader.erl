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

-module(file_store_reader).
-behaviour(gen_server).

-export([start_link/3]).
-export([read/4, done/1]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-include("store.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Path, Revision, User) ->
	#revision{
		parts   = Parts,
		parents = Parents,
		type    = Type,
		links   = Links
	} = Revision,
	case gen_server:start_link(?MODULE, {Path, Parts, User}, []) of
		{ok, Pid} ->
			{ok, #handle{
				this        = Pid,
				read        = fun read/4,
				write       = fun(_, _, _, _) -> {error, ebadf} end,
				truncate    = fun(_, _, _) -> {error, ebadf} end,
				close       = fun done/1,
				commit      = fun(_, _) -> {error, ebadf} end,
				suspend     = fun(_, _) -> {error, ebadf} end,
				get_type    = fun(_) -> {ok, Type} end,
				set_type    = fun(_, _) -> {error, ebadf} end,
				get_parents = fun(_) -> {ok, Parents} end,
				set_parents = fun(_, _) -> {error, ebadf} end,
				get_links   = fun(_) -> {ok, Links} end,
				set_links   = fun(_, _) -> {error, ebadf} end
			}};
		Else ->
			Else
	end.


read(Reader, Part, Offset, Length) ->
	gen_server:call(Reader, {read, Part, Offset, Length}).

done(Reader) ->
	gen_server:cast(Reader, done).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Path, Parts, User}) ->
	case open_part_list(Path, Parts) of
		{error, Reason} ->
			{stop, Reason};
		Handles ->
			process_flag(trap_exit, true),
			link(User),
			{ok, Handles}
	end.

% returns: {ok, Data} | {error, Reason}
handle_call({read, Part, Offset, Length}, _From, Handles) ->
	Reply = case dict:find(Part, Handles) of
		{ok, Handle} ->
			case file:pread(Handle, Offset, Length) of
				eof -> {ok, <<>>};
				Else -> Else
			end;

		error ->
			{error, enoent}
	end,
	{reply, Reply, Handles}.

handle_cast(done, Handles) ->
	{stop, normal, Handles}.

terminate(_Reason, Handles) ->
	close_part_list(Handles).

handle_info({'EXIT', _From, _Reason}, S) ->
	{stop, orphaned, S}.

code_change(_, State, _) -> {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Open all parts.
%
% `Parts' is a list of `{FourCC, Hash}' pairs. The function either returns a dict
% which maps FourCC's to open IODevice's or `{error, Reason}' when a error happens.
% TODO: defer opening the parts until they are accessed
open_part_list(Path, Parts) ->
	open_part_list_loop(Path, Parts, dict:new()).
open_part_list_loop(_, [], Handles) ->
	Handles;
open_part_list_loop(Path, [{Id, Hash} | Parts], Handles1) ->
	case file:open(util:build_path(Path, Hash), [read, binary]) of
		{ok, IoDevice} ->
			Handles2 = dict:store(Id, IoDevice, Handles1),
			open_part_list_loop(Path, Parts, Handles2);
		{error, Reason} ->
			close_part_list(Handles1), % close what was opened so far
			{error, Reason}            % and return the error
	end.

close_part_list(Handles1) ->
	Handles2 = dict:to_list(Handles1),
	lists:foreach(fun({_, File}) -> file:close(File) end, Handles2).

