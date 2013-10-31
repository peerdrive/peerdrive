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

-module(peerdrive_file_store_imp).
-behaviour(gen_server).

-export([start_link/7]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-include("store.hrl").

-define(THRESHOLD, 1024).

-record(state, {store, rid, rev, parts, noverify, done, doclinks, revlinks, user}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(RId, Rev, Missing, User, NoVerify, DocLinks, RevLinks) ->
	State = #state{
		store = self(),
		rid = RId,
		rev = Rev,
		parts = orddict:from_list(
			[{FCC, {PId, <<>>, peerdrive_crypto:merkle_init()}} || {FCC, PId} <- Missing]
		),
		noverify = NoVerify,
		done = false,
		doclinks = DocLinks,
		revlinks = RevLinks,
		user = User
	},
	gen_server:start_link(?MODULE, {State, User}, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({State, User}) ->
	process_flag(trap_exit, true),
	link(User),
	{ok, State}.


% returns `ok | {error, Reason}'
handle_call({put_part, Part, Data}, _From, S) ->
	{Reply, S2} = do_write(Part, Data, S),
	{reply, Reply, S2};

% returns `ok | {error, Reason}'
handle_call(commit, _From, #state{done=false} = S) ->
	{Reply, S2} = do_commit(S),
	{reply, Reply, S2};

% returns nothing
handle_call(close, _From, S) ->
	{stop, normal, ok, S};

handle_call(_, _, S) ->
	{reply, {error, ebadf}, S}.


handle_info({'EXIT', From, Reason}, #state{store=Store} = S) ->
	case From of
		Store -> {stop, {orphaned, Reason}, S};
		_User -> {stop, normal, S}
	end.


terminate(_Reason, #state{store=Store, parts=Parts, rev=Rev, rid=RId, user=User}) ->
	unlink(User),
	peerdrive_file_store:part_unlock(Store, (Rev#rev.data)#rev_dat.hash),
	lists:foreach(
		fun(#rev_att{hash=PId}) -> peerdrive_file_store:part_unlock(Store, PId) end,
		Rev#rev.attachments),
	peerdrive_file_store:rev_unlock(Store, RId),
	lists:foreach(
		fun({_, {_PId, Content, _Sha}}) ->
			case Content of
				{FileName, IoDev} ->
					file:close(IoDev),
					file:delete(FileName);
				Data when is_binary(Data) ->
					ok
			end
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

do_write(Part, Data, #state{parts=Parts} = S) ->
	case orddict:find(Part, Parts) of
		{ok, {PId, PartData, Ctx1}} when is_binary(PartData) ->
			Ctx2 = peerdrive_crypto:merkle_update(Ctx1, Data),
			NewData = <<PartData/binary, Data/binary>>,
			if
				size(NewData) > ?THRESHOLD ->
					case create_tmp_file(NewData, S) of
						{ok, FileName, IoDev} ->
							NewParts = orddict:store(Part,
								{PId, {FileName, IoDev}, Ctx2}, Parts),
							{ok, S#state{parts=NewParts}};

						{error, _} = Error ->
							{Error, S}
					end;

				true ->
					NewParts = orddict:store(Part, {PId, NewData, Ctx2}, Parts),
					{ok, S#state{parts=NewParts}}
			end;

		{ok, {PId, {FileName, IoDev}, Ctx1}} ->
			case file:write(IoDev, Data) of
				ok ->
					Ctx2 = peerdrive_crypto:merkle_update(Ctx1, Data),
					NewParts = orddict:store(Part, {PId, {FileName, IoDev}, Ctx2},
						Parts),
					{ok, S#state{parts=NewParts}};

				{error, _Reason} = Error ->
					{peerdrive_util:fixup_file(Error), S}
			end;

		error ->
			{{error, einval}, S}
	end.


do_commit(#state{store=Store, rid=RId, rev=Rev, parts=Parts, noverify=NoVerify} = S) ->
	try
		NoVerify orelse lists:foreach(
			fun({_, {PId, _, ShaCtx}}) ->
				peerdrive_crypto:merkle_final(ShaCtx) == PId orelse throw(einval)
			end,
			Parts),
		case commit_parts(Store, Parts) of
			ok ->
				{
					peerdrive_file_store:put_rev_commit(Store, RId, Rev,
						S#state.doclinks, S#state.revlinks),
					S#state{parts=[], done=true}
				};
			{Error, Remaining} ->
				{Error, S#state{parts=Remaining}}
		end
	catch
		throw:ThrowErr -> {{error, ThrowErr}, S}
	end.


commit_parts(_Store, []) ->
	ok;

commit_parts(Store, [{_FCC, {PId, Data, _}} | Rest]) when is_binary(Data) ->
	case peerdrive_file_store:part_put(Store, PId, Data) of
		ok ->
			commit_parts(Store, Rest);
		{error, _Reason} = Error ->
			{Error, Rest}
	end;

commit_parts(Store, [{_FCC, {PId, {FileName, IoDev}, _}} | Rest]) ->
	ok = file:close(IoDev),
	case peerdrive_file_store:part_put(Store, PId, FileName) of
		ok ->
			commit_parts(Store, Rest);

		{error, _Reason} = Error ->
			file:delete(FileName),
			{Error, Rest}
	end.


create_tmp_file(Data, #state{store=Store}) ->
	TmpName = peerdrive_file_store:tmp_name(Store),
	case file:open(TmpName, [write, binary, exclusive, raw]) of
		{ok, IoDev} ->
			case file:write(IoDev, Data) of
				ok ->
					{ok, TmpName, IoDev};
				Error ->
					file:close(IoDev),
					file:delete(TmpName),
					peerdrive_util:fixup_file(Error)
			end;

		Error ->
			peerdrive_util:fixup_file(Error)
	end.

