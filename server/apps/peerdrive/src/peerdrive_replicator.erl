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

-module(peerdrive_replicator).
-behaviour(supervisor).

-export([start_link/0]).
-export([replicate_rev/4, replicate_rev_sync/4, replicate_doc/4,
         replicate_doc_sync/4, close/1]).
-export([init/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	supervisor:start_link({local, peerdrive_replicator}, ?MODULE, []).

replicate_doc(SrcStore, Doc, DstStore, Options) ->
	start_child([{doc, Doc}, SrcStore, DstStore, Options]).

replicate_doc_sync(SrcStore, Doc, DstStore, Options) ->
	start_child_sync([{doc, Doc}, SrcStore, DstStore, [{wait, self()} | Options]]).

replicate_rev(SrcStore, Rev, DstStore, Options) ->
	start_child([{rev, Rev}, SrcStore, DstStore, Options]).

replicate_rev_sync(SrcStore, Rev, DstStore, Options) ->
	start_child_sync([{rev, Rev}, SrcStore, DstStore, [{wait, self()} | Options]]).

close(Handle) ->
	gen_fsm:send_all_state_event(Handle, close).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
	{ok, {
		{simple_one_for_one, 1, 10},
		[{
			peerdrive_replicator,
			{peerdrive_replicator_worker, start_link, []},
			temporary,
			brutal_kill,
			worker,
			[]
		}]
	}}.


start_child(Args) ->
	supervisor:start_child(peerdrive_replicator, Args).

start_child_sync(Args) ->
	Ref = make_ref(),
	case start_child(Args ++ [{self(), Ref}]) of
		{ok, WorkerPid} ->
			MonRef = erlang:monitor(process, WorkerPid),
			receive
				{Ref, Reply} ->
					erlang:demonitor(MonRef, [flush]),
					case Reply of
						ok -> {ok, WorkerPid};
						Error -> Error
					end;

				{'DOWN', MonRef, process, WorkerPid, _Info} ->
					{error, eio}
			end;

		{error, _} ->
			{error, enomem}
	end.

