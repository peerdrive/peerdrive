%% Hotchpotch
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

-define(INIT_REQ,             16#0000).
-define(INIT_CNF,             16#0001).
-define(STATFS_REQ,           16#0010).
-define(STATFS_CNF,           16#0011).
-define(LOOKUP_REQ,           16#0020).
-define(LOOKUP_CNF,           16#0021).
-define(CONTAINS_REQ,         16#0030).
-define(CONTAINS_CNF,         16#0031).
-define(STAT_REQ,             16#0040).
-define(STAT_CNF,             16#0041).
-define(PEEK_REQ,             16#0050).
-define(PEEK_CNF,             16#0051).
-define(CREATE_REQ,           16#0060).
-define(CREATE_CNF,           16#0061).
-define(FORK_REQ,             16#0070).
-define(FORK_CNF,             16#0071).
-define(UPDATE_REQ,           16#0080).
-define(UPDATE_CNF,           16#0081).
-define(RESUME_REQ,           16#0090).
-define(RESUME_CNF,           16#0091).
-define(READ_REQ,             16#00A0).
-define(READ_CNF,             16#00A1).
-define(TRUNC_REQ,            16#00B0).
-define(TRUNC_CNF,            16#00B1).
-define(WRITE_REQ,            16#00C0).
-define(WRITE_CNF,            16#00C1).
-define(GET_TYPE_REQ,         16#00D0).
-define(GET_TYPE_CNF,         16#00D1).
-define(SET_TYPE_REQ,         16#00E0).
-define(SET_TYPE_CNF,         16#00E1).
-define(GET_PARENTS_REQ,      16#00F0).
-define(GET_PARENTS_CNF,      16#00F1).
-define(SET_PARENTS_REQ,      16#0100).
-define(SET_PARENTS_CNF,      16#0101).
-define(GET_LINKS_REQ,        16#0110).
-define(GET_LINKS_CNF,        16#0111).
-define(SET_LINKS_REQ,        16#0120).
-define(SET_LINKS_CNF,        16#0121).
-define(COMMIT_REQ,           16#0130).
-define(COMMIT_CNF,           16#0131).
-define(SUSPEND_REQ,          16#0140).
-define(SUSPEND_CNF,          16#0141).
-define(CLOSE_REQ,            16#0150).
-define(CLOSE_CNF,            16#0151).
-define(FORGET_REQ,           16#0160).
-define(FORGET_CNF,           16#0161).
-define(DELETE_DOC_REQ,       16#0170).
-define(DELETE_DOC_CNF,       16#0171).
-define(DELETE_REV_REQ,       16#0180).
-define(DELETE_REV_CNF,       16#0181).
-define(PUT_DOC_REQ,          16#0190).
-define(PUT_DOC_CNF,          16#0191).
-define(PUT_REV_START_REQ,    16#01A0).
-define(PUT_REV_START_CNF,    16#01A1).
-define(PUT_REV_PART_REQ,     16#01B0).
-define(PUT_REV_PART_CNF,     16#01B1).
-define(PUT_REV_ABORT_REQ,    16#01C0).
-define(PUT_REV_ABORT_CNF,    16#01C1).
-define(PUT_REV_COMMIT_REQ,   16#01D0).
-define(PUT_REV_COMMIT_CNF,   16#01D1).
-define(SYNC_GET_CHANGES_REQ, 16#01E0).
-define(SYNC_GET_CHANGES_CNF, 16#01E1).
-define(SYNC_SET_ANCHOR_REQ,  16#01F0).
-define(SYNC_SET_ANCHOR_CNF,  16#01F1).
-define(SYNC_FINISH_REQ,      16#0200).
-define(SYNC_FINISH_CNF,      16#0201).

-define(ADD_REV_IND,          16#0002).
-define(REM_REV_IND,          16#0012).
-define(ADD_DOC_IND,          16#0022).
-define(REM_DOC_IND,          16#0032).
-define(MOD_DOC_IND,          16#0042).

