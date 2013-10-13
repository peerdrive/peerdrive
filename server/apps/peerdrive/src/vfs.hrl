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

-ifndef(VFS_HRL).
-define(VFS_HRL, true).

-record(vfs_attr, {
	dir,     % bool(): true if node is directory
	size=0,  % int(): size in bytes
	mtime=0, % int(): microseconds since epoch
	atime=0, % int(): microseconds since epoch
	ctime=0, % int(): microseconds since epoch
	crtime=0 % int(): microseconds since epoch
}).

-record(vfs_entry, {
	ino,      % int()
	attr,     % #vfs_attr{}
	attr_tmo, % int(): valid timeout for attributes
	entry_tmo % int(): timeout of entry name in ms
}).

-record(vfs_direntry, {
	name, % binary()
	attr  % #vfs_attr{}
}).

-endif.
