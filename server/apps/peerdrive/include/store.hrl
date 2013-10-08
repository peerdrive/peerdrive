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

%% Default empty data
-define(REV_DATA_EMPTY_BIN, <<0,0,0,0,0>>).
-define(REV_DATA_EMPTY_SIZE, 5).
-define(REV_DATA_EMPTY_HASH, <<119,34,116,81,5,233,224,46,143,26,175,23,247,
                               179,170,197,197,108,216,5>>).
-define(REV_DATA_EMPTY, #rev_dat{size = ?REV_DATA_EMPTY_SIZE, hash = ?REV_DATA_EMPTY_HASH}).

-record(rev_dat,
	{
		size :: non_neg_integer(),
		hash :: peerdrive:hash()
	}).

-record(rev_att,
	{
		name   :: binary(),
		size   :: non_neg_integer(),
		hash   :: peerdrive:hash(),
		crtime :: integer(),
		mtime  :: integer()
	}).

% The size of the structured data and the attachments is redundant but makes
% things much more convenient.
-record(rev,
	{
		flags = 0        :: non_neg_integer(),
		data = #rev_dat{
			size = ?REV_DATA_EMPTY_SIZE,
			hash = ?REV_DATA_EMPTY_HASH
		}                :: #rev_dat{},
		attachments = [] :: [#rev_att{}],
		parents = []     :: [peerdrive:rev()],
		crtime = 0       :: integer(), % microseconds since epoch (unix date, UTC)
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

