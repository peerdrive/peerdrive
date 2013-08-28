%% PeerDrive
%% Copyright (C) 2012  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
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

-module(peerdrive_crypt_store_imp).
-behaviour(gen_server).

-export([start_link/5]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-record(state, {store, handle, parts, key, user}).

-include("store.hrl").
-include("cryptstore.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public interface...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Store, Key, Handle, Parts, User) ->
	State = #state{store=Store, handle=Handle, user=User, key=Key,
		parts=orddict:from_list(
			[{FCC, {PId, peerdrive_crypto:merkle_init(),
				peerdrive_crypto:aes_ctr_stream_init(Key, peerdrive_crypto:make_bin_16(PId))}
				} || {FCC, PId} <- Parts]
		)},
	gen_server:start_link(?MODULE, {State, User}, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({State, User}) ->
	process_flag(trap_exit, true),
	link(User),
	{ok, State}.


handle_call({put_part, Part, Data}, _From, S) ->
	{Reply, S2} = do_put_part(Part, Data, S),
	{reply, Reply, S2};

handle_call(commit, _From, S) ->
	{reply, do_commit(S), S};

handle_call(close, _From, #state{handle=Handle} = S) ->
	peerdrive_store:close(Handle),
	{stop, normal, ok, S}.


handle_info({'EXIT', From, Reason}, #state{store=Store, user=User} = S) ->
	case From of
		Store ->
			peerdrive_store:close(S#state.handle),
			{stop, Reason, S};
		User ->
			peerdrive_store:close(S#state.handle),
			{stop, normal, S}
	end.


terminate(_Reason, #state{user=User}) ->
	unlink(User).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


handle_cast(_, State)    -> {stop, enotsup, State}.
code_change(_, State, _) -> {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_put_part(Part, Data, #state{handle=Handle, parts=Parts} = S) ->
	case orddict:find(Part, Parts) of
		{ok, {PId, ShaCtx1, AesCtx1}} ->
			{AesCtx2, EncData} = peerdrive_crypto:aes_ctr_stream_encrypt(AesCtx1, Data),
			case peerdrive_store:put_rev_part(Handle, Part, EncData) of
				ok ->
					ShaCtx2 = peerdrive_crypto:merkle_update(ShaCtx1, Data),
					NewParts = orddict:store(Part, {PId, ShaCtx2, AesCtx2},
						Parts),
					{ok, S#state{parts=NewParts}};

				{error, _Reason} = Error ->
					{Error, S}
			end;

		error ->
			{{error, einval}, S}
	end.


do_commit(#state{handle=Handle, parts=Parts, key=Key}) ->
	try
		lists:foreach(
			fun({_, {PId, ShaCtx, _AesCtx}}) ->
				peerdrive_crypto:merkle_final(ShaCtx) == PId orelse throw(einval)
			end,
			Parts),
		case peerdrive_store:commit(Handle) of
			{ok, EncRId} -> {ok, peerdrive_crypt_store:dec_xid(Key, EncRId)};
			Error        -> Error
		end
	catch
		throw:ThrowErr ->
			{error, ThrowErr}
	end.

