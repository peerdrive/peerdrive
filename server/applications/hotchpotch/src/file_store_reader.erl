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

-module(file_store_reader).
-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-include("store.hrl").

-record(state, {handles, parts, path, user, type, parents, links}).

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
	State = #state{
		handles = dict:new(),
		parts   = dict:from_list(Parts),
		path    = Path,
		user    = User,
		type    = Type,
		parents = Parents,
		links   = Links
	},
	gen_server:start_link(?MODULE, State, []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(State) ->
	process_flag(trap_exit, true),
	link(State#state.user),
	{ok, State}.


handle_call({read, Part, Offset, Length}, _From, S) ->
	Reply = case open_part(Part, S) of
		{ok, Handle, S2} ->
			case file:pread(Handle, Offset, Length) of
				eof -> {ok, <<>>};
				Else -> Else
			end;

		{error, Reason, S2} ->
			{error, Reason}
	end,
	{reply, Reply, S2};

handle_call(close, _From, S) ->
	{stop, normal, ok, S};

handle_call(get_type, _From, S) ->
	{reply, {ok, S#state.type}, S};

handle_call(get_parents, _From, S) ->
	{reply, {ok, S#state.parents}, S};

handle_call(get_links, _From, S) ->
	{reply, {ok, S#state.links}, S};

% the following calls are only allowed when still writable
handle_call(_Request, _From, S) ->
	{reply, {error, ebadf}, S}.


handle_info({'EXIT', From, Reason}, #state{user=User} = S) ->
	case From of
		User    -> {stop, normal, S};
		_Server -> {stop, {orphaned, Reason}, S}
	end.


terminate(_Reason, S) ->
	close_parts(S).


handle_cast(_, State) -> {noreply, State}.
code_change(_, State, _) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

open_part(Part, #state{handles=Handles, parts=Parts, path=Path} = S) ->
	case dict:find(Part, Handles) of
		{ok, Handle} ->
			{ok, Handle, S};

		error ->
			case dict:find(Part, Parts) of
				{ok, Hash} ->
					case file:open(util:build_path(Path, Hash), [read, binary]) of
						{ok, IoDevice} ->
							NewHandles = dict:store(Part, IoDevice, Handles),
							{ok, IoDevice, S#state{handles=NewHandles}};

						{error, Reason} ->
							{error, Reason, S}
					end;

				error ->
					{error, enoent, S}
			end
	end.


close_parts(#state{handles = Handles1}) ->
	Handles2 = dict:to_list(Handles1),
	lists:foreach(fun({_, File}) -> file:close(File) end, Handles2).

