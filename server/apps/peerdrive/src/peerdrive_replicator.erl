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
         replicate_doc_sync/4]).
-export([init/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
	supervisor:start_link({local, peerdrive_replicator}, ?MODULE, []).

replicate_doc(SrcStore, Doc, DstStore, Depth) ->
	start_child([{replicate_doc, Doc, true}, SrcStore, DstStore, Depth]).

replicate_doc_sync(SrcStore, Doc, DstStore, Depth) ->
	start_child_sync([{replicate_doc, Doc, true}, SrcStore, DstStore, Depth]).

replicate_rev(SrcStore, Rev, DstStore, Depth) ->
	start_child([{replicate_rev, Rev, true}, SrcStore, DstStore, Depth]).

replicate_rev_sync(SrcStore, Rev, DstStore, Depth) ->
	start_child_sync([{replicate_rev, Rev, true}, SrcStore, DstStore, Depth]).

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
			MonRef = monitor(process, WorkerPid),
			receive
				{Ref, Reply} ->
					demonitor(MonRef, [flush]),
					Reply;

				{'DOWN', MonRef, process, WorkerPid, _Info} ->
					{error, eio}
			end;

		{error, _} ->
			{error, enomem}
	end.

