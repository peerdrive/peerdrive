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

-define(FLAG_REQ, 0).
-define(FLAG_CNF, 1).
-define(FLAG_IND, 2).
-define(FLAG_RSP, 3).

-define(ERROR_MSG,            16#000).
-define(INIT_MSG,             16#001).
-define(STATFS_MSG,           16#002).
-define(LOOKUP_MSG,           16#003).
-define(CONTAINS_MSG,         16#004).
-define(STAT_MSG,             16#005).
-define(PEEK_MSG,             16#006).
-define(CREATE_MSG,           16#007).
-define(FORK_MSG,             16#008).
-define(UPDATE_MSG,           16#009).
-define(RESUME_MSG,           16#00a).
-define(READ_MSG,             16#00b).
-define(TRUNC_MSG,            16#00c).
-define(WRITE_MSG,            16#00d).
-define(GET_FLAGS_MSG,        16#00e).
-define(SET_FLAGS_MSG,        16#00f).
-define(GET_TYPE_MSG,         16#010).
-define(SET_TYPE_MSG,         16#011).
-define(GET_PARENTS_MSG,      16#012).
-define(SET_PARENTS_MSG,      16#013).
-define(GET_LINKS_MSG,        16#014).
-define(SET_LINKS_MSG,        16#015).
-define(COMMIT_MSG,           16#016).
-define(SUSPEND_MSG,          16#017).
-define(CLOSE_MSG,            16#018).
-define(FORGET_MSG,           16#019).
-define(DELETE_DOC_MSG,       16#01a).
-define(DELETE_REV_MSG,       16#01b).
-define(PUT_DOC_START_MSG,    16#01c).
-define(PUT_DOC_COMMIT_MSG,   16#01d).
-define(PUT_DOC_ABORT_MSG,    16#01e).
-define(FF_DOC_START_MSG,     16#01f).
-define(FF_DOC_COMMIT_MSG,    16#020).
-define(FF_DOC_ABORT_MSG,     16#021).
-define(PUT_REV_START_MSG,    16#022).
-define(PUT_REV_PART_MSG,     16#023).
-define(PUT_REV_ABORT_MSG,    16#024).
-define(PUT_REV_COMMIT_MSG,   16#025).
-define(SYNC_GET_CHANGES_MSG, 16#026).
-define(SYNC_SET_ANCHOR_MSG,  16#027).
-define(SYNC_FINISH_MSG,      16#028).
-define(TRIGGER_MSG,          16#029).


-define(ASSERT_GUID(G), ((is_binary(G) and (size(G) == 16)) orelse
                         begin throw({error, einval}), false end)).
-define(ASSERT_GUID_LIST(L), lists:all(fun(G) -> ?ASSERT_GUID(G) end, L)).
-define(ASSERT_PART(G), ((is_binary(G) and (size(G) == 4)) orelse
                         begin throw({error, einval}), false end)).

