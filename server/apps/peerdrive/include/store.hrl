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

-define(REV_FLAG_STICKY,    (1 bsl 0)).
-define(REV_FLAG_EPHEMERAL, (1 bsl 1)).

%% Known hash of the default empty data
-define(REV_DATA_EMPTY_HASH, <<119,34,116,81,5,233,224,46,143,26,175,23,247,
                               179,170,197,197,108,216,5>>).

-record(revision,
	{
		flags = 0        :: non_neg_integer(),
		data = ?REV_DATA_EMPTY_HASH :: peerdrive:hash(),
		attachments = [] :: [{Name::binary(), Hash::peerdrive:hash()}],
		parents = []     :: [peerdrive:rev()],
		mtime = 0        :: integer(), % microseconds since epoch (unix date, UTC)
		type = <<>>      :: binary(),
		creator = <<>>   :: binary(),
		comment = <<>>   :: binary()
	}).


-record(fs_stat,
	{
		bsize  :: integer(), % size of each block (power of two, >=512)
		blocks :: integer(), % overall number of blocks in store
		bfree  :: integer(), % number of free blocks
		bavail :: integer()  % number of blocks available to user
	}).

-record(rev_stat,
	{
		flags            :: non_neg_integer(),
		data             :: {Size::non_neg_integer(), peerdrive:hash()},
		attachments      :: [{Name::binary(), Size::non_neg_integer(), Hash::peerdrive:hash()}],
		parents          :: [peerdrive:rev()],
		mtime            :: integer(), % microseconds since epoch (unix date, UTC)
		type             :: binary(),
		creator          :: binary(),
		comment          :: binary()
	}).

