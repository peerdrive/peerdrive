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

-record(revision,
	{
		flags=0,  % integer(): reserved (0)
		parts,    % [{FourCC::binary(), Hash::guid()}]
		parents,  % [Rev::guid()]: Parent revisions
		mtime,    % interger(): Seconds since epoch (unix date, UTC)
		type,     % binary()
		creator,  % binary()
		links     % {SDL, WDL, SRL, WRL, DocMap}
		          %  SDL = [Doc::guid()]: Strong document links
		          %  WDL = [Doc::guid()]: Weak document links
		          %  SRL = [Rev::guid()]: Strong revision links
		          %  WRL = [Rev::guid()]: Weak revision links
		          %  DocMap = orddict(Doc->[Rev]): Document map snapshot
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
		links
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
		set_parents,
		get_links,
		set_links
	}).

-record(importer,
	{
		this,
		put_part,
		abort,
		commit
	}).

