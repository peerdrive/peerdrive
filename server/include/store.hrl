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
% mitme:   Secs (unix date)
% uti:     Binary
-record(object, {flags=0, parts, parents, mtime, uti}).

-record(store,
	{
		this,
		guid,
		contains,
		lookup,
		stat,
		put_doc,
		put_rev_start,
		peek,
		fork,
		update,
		delete_rev,
		delete_doc,
		sync_get_changes,
		sync_set_anchor
	}).

-record(handle,
	{
		this,
		read,
		write,
		truncate,
		abort,
		commit
	}).

-record(importer,
	{
		this,
		put_part,
		abort,
		commit
	}).

