%% PeerDrive
%% Copyright (C) 2013  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
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

-module(peerdrive_crypto).

% Beginning with R16 all crypto functions used below are deprecated.
% Unfortunately their new counterparts are not available on R14/R15 yet so we
% just ignore the deprecation for the moment.
-compile([nowarn_deprecated_function]).

-export([aes_cbc_128_decrypt/3, aes_cbc_128_encrypt/3, aes_ctr_decrypt/3,
	aes_ctr_encrypt/3, aes_ctr_stream_decrypt/2, aes_ctr_stream_encrypt/2,
	aes_ctr_stream_init/2, sha/1, sha_mac/2]).
-export([merkle/1, merkle_init/0, merkle_update/2, merkle_final/1, make_bin_16/1]).

aes_ctr_stream_init(Key, IVec) ->
	crypto:aes_ctr_stream_init(Key, IVec).

aes_ctr_stream_encrypt(State, Data) ->
	crypto:aes_ctr_stream_encrypt(State, Data).

aes_ctr_stream_decrypt(State, Data) ->
	crypto:aes_ctr_stream_decrypt(State, Data).

aes_ctr_decrypt(Key, IVec, Data) ->
	crypto:aes_ctr_decrypt(Key, IVec, Data).

aes_ctr_encrypt(Key, IVec, Data) ->
	crypto:aes_ctr_encrypt(Key, IVec, Data).

aes_cbc_128_decrypt(Key, IVec, Data) ->
	crypto:aes_cbc_128_decrypt(Key, IVec, Data).

aes_cbc_128_encrypt(Key, IVec, Data) ->
	crypto:aes_cbc_128_encrypt(Key, IVec, Data).

sha_mac(Key, Data) ->
	crypto:sha_mac(Key, Data).

sha(Data) ->
	crypto:sha(Data).


merkle(Data) ->
	Ctx1 = merkle_init(),
	Ctx2 = merkle_update(Ctx1, Data),
	merkle_final(Ctx2).


merkle_init() ->
	{<<>>, []}.


merkle_update({Buffer, HashTree}, Data) when size(Buffer) >= 4096 ->
	<<Block:4096/binary, Rest/binary>> = Buffer,
	% TODO: benchmark if constructing complete binary first is faster
	Ctx1 = crypto:sha_init(),
	Ctx2 = crypto:sha_update(Ctx1, <<0>>),
	Ctx3 = crypto:sha_update(Ctx2, Block),
	NewHashTree = merkle_push(crypto:sha_final(Ctx3), HashTree),
	merkle_update({Rest, NewHashTree}, Data);

merkle_update(State, <<>>) ->
	State;

merkle_update({<<>>, HashTree}, Data) ->
	merkle_update({Data, HashTree}, <<>>);

merkle_update({Buffer, HashTree}, Data) ->
	merkle_update({<<Buffer/binary, Data/binary>>, HashTree}, <<>>).


merkle_push(Block, []) ->
	[Block];

merkle_push(Block, [empty | Root]) ->
	[Block | Root];

merkle_push(Block, [Sibling | Root]) ->
	[empty | merkle_push(crypto:sha(<<1, Sibling/binary, Block/binary>>), Root)].


merkle_final({<<>>, []}) ->
	crypto:sha(<<0>>);

merkle_final({<<>>, HashTree}) ->
	merkle_finalize(HashTree);

merkle_final({Remaining, HashTree}) ->
	FinalTree = merkle_push(crypto:sha(<<0, Remaining/binary>>), HashTree),
	merkle_finalize(FinalTree).


merkle_finalize([Root]) ->
	Root;

merkle_finalize([empty | Root]) ->
	merkle_finalize(Root);

merkle_finalize([Partial, empty | Root]) ->
	merkle_finalize([Partial | Root]);

merkle_finalize([Partial, Sibling | Root]) ->
	merkle_finalize([crypto:sha(<<1, Sibling/binary, Partial/binary>>) | Root]).


make_bin_16(Bin) when size(Bin) == 16 ->
	Bin;

make_bin_16(Bin) when size(Bin) > 16 ->
	binary_part(Bin, 0, 16);

make_bin_16(Bin) ->
	<<Bin/binary, 0:((16-size(Bin))*8)>>.

