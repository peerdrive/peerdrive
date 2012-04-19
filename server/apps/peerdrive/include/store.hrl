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

-define(REV_FLAG_STICKY, 1).

-record(revision,
	{
		flags = 0,      % integer()
		parts = [],     % [{FourCC::binary(), Hash::guid()}]
		parents = [],   % [Rev::guid()]: Parent revisions
		mtime = 0,      % interger(): Microseconds since epoch (unix date, UTC)
		type = <<>>,    % binary()
		creator = <<>>, % binary()
		comment = <<>>, % binary()
		doc_links = [], % [Doc::guid()]
		rev_links = []  % [Rev::guid()]
	}).


-record(fs_stat,
	{
		bsize,  % size of each block (power of two, >=512)
		blocks, % overall number of blocks in store
		bfree,  % number of free blocks
		bavail  % number of blocks available to user
	}).

-record(rev_stat,
	{
		flags,
		parts,
		parents,
		mtime,
		type,
		creator,
		comment,
		doc_links,
		rev_links
	}).

