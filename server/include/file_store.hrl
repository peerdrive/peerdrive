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


-record(ws, {
	path,       % base directory of server
	server,     % server process pid
	flags,      % flags of revision
	doc,        % document uuid
	prerev,     % base preliminary revision, might be 'undefined'
	baserevs,   % base revision of revision
	type,       % Type UTI of revision
	creator,    % Creator of revision
	orig,       % dict: FourCC --> Hash
	new,        % dict: FourCC --> {FileName, IODevice}
	readers,    % dict: FourcC --> IODevice
	locks       % list of locked part hashes
	}).

