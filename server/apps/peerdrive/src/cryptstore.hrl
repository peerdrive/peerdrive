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

-define(CS_FLAGS_IVEC(RId),   (crypto:exor(peerdrive_crypto:make_bin_16(RId), <<1:64, 0:64>>))).
-define(CS_MTIME_IVEC(RId),   (crypto:exor(peerdrive_crypto:make_bin_16(RId), <<2:64, 0:64>>))).
-define(CS_TYPE_IVEC(RId),    (crypto:exor(peerdrive_crypto:make_bin_16(RId), <<3:64, 0:64>>))).
-define(CS_CREATOR_IVEC(RId), (crypto:exor(peerdrive_crypto:make_bin_16(RId), <<4:64, 0:64>>))).
-define(CS_COMMENT_IVEC(RId), (crypto:exor(peerdrive_crypto:make_bin_16(RId), <<5:64, 0:64>>))).
-define(CS_CRTIME_IVEC(RId),  (crypto:exor(peerdrive_crypto:make_bin_16(RId), <<6:64, 0:64>>))).

