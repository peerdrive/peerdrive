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


% parts:   [{PartFourCC, Hash}]
% parents: [Revision]
% mitme:   UTC, Seconds since epoch (unix date)
% type:    Binary
% creator: Binary
-record(revision, {flags=0, parts, parents, mtime, type, creator}).

-define(REV_FLAG_PRELIMINARY, 16#100).

-record(store,
	{
		this,
		guid,
		statfs,
		contains,
		lookup,
		stat,
		peek,
		create,
		fork,
		update,
		resume,
		forget,
		delete_rev,
		delete_doc,
		put_doc,
		put_rev_start,
		sync_get_changes,
		sync_set_anchor
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
		creator
	}).

-record(handle,
	{
		this,
		read,
		write,
		truncate,
		close,
		commit,
		suspend,
		get_type,
		set_type,
		get_parents,
		set_parents
	}).

-record(importer,
	{
		this,
		put_part,
		abort,
		commit
	}).

