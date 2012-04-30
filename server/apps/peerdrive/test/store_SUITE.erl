-module(store_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").

-include("store.hrl").

suite() ->
	[{timetrap, {seconds, 10}}].


all() ->
	[test_rw_readback_small, test_rw_readback_big, test_rw_mtime,
	test_flags_create, test_flags_update_change, test_flags_update_keep,
	test_flags_fork, test_creator, test_creator_fork,
	test_creator_update_change, test_creator_update_keep].


%% Write and read back some small data
test_rw_readback_small(_Config) ->
	Data = <<"akjdfaqhfkjsalur\naqidahgajsoasoiga\n\nakhsfdlkaf\r\n">>,
	rw_readback(Data).

%% Write and read back a big chunk
test_rw_readback_big(_Config) ->
	Data = iolist_to_binary(lists:duplicate(1024, <<"akjdfaqhfkjsalur">>)),
	io:format("~p", [Data]),
	rw_readback(Data).


rw_readback(Data) ->
	{ok, _Doc, Handle} = peerdrive_store:create(usr_store, <<"public.data">>, <<"test">>),
	ok = peerdrive_store:write(Handle, <<"FILE">>, 0, Data),
	{ok, Rev} = peerdrive_store:commit(Handle),
	% now read back
	{ok, ReadHandle} = peerdrive_store:peek(usr_store, Rev),
	{ok, Data} = peerdrive_store:read(ReadHandle, <<"FILE">>, 0, 16#1000000),
	peerdrive_store:close(ReadHandle),
	peerdrive_store:close(Handle).


%% Test if mtime of created revision is in expected range
test_rw_mtime(_Config) ->
	{ok, _Doc, Handle} = peerdrive_store:create(usr_store, <<"public.data">>, <<"test">>),
	ok = peerdrive_store:write(Handle, <<"FILE">>, 0, <<"foobar">>),
	{ok, Rev} = peerdrive_store:commit(Handle),
	{ok, #rev_stat{mtime=MTime}} = peerdrive_store:stat(usr_store, Rev),
	Now = peerdrive_util:get_time(),
	true = (MTime =< Now) and (MTime >= Now-2).


%% Create a document and make sure flags are empty
test_flags_create(_Config) ->
	{ok, _Doc, Handle} = peerdrive_store:create(usr_store, <<"public.data">>, <<"test">>),
	{ok, Rev} = peerdrive_store:commit(Handle),
	flags_compare(Rev, 0).


%% Test that update of flags works
test_flags_update_change(_Config) ->
	{ok, Doc, Handle} = peerdrive_store:create(usr_store, <<"public.data">>, <<"test">>),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	{ok, UpdateHandle} = peerdrive_store:update(usr_store, Doc, Rev1, undefined),
	Flags = 16#1337,
	ok = peerdrive_store:set_flags(UpdateHandle, Flags),
	{ok, Rev2} = peerdrive_store:commit(UpdateHandle),
	flags_compare(Rev2, Flags).


%% Test that update of a part doesn't change flags
test_flags_update_keep(_Config) ->
	Flags = 16#deadbeef,
	{ok, Doc, Handle} = peerdrive_store:create(usr_store, <<"public.data">>, <<"test">>),
	ok = peerdrive_store:set_flags(Handle, Flags),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	flags_compare(Rev1, Flags),
	{ok, UpdateHandle} = peerdrive_store:update(usr_store, Doc, Rev1, undefined),
	ok = peerdrive_store:write(UpdateHandle, <<"FILE">>, 0, <<"foobar">>),
	{ok, Flags} = peerdrive_store:get_flags(UpdateHandle),
	{ok, Rev2} = peerdrive_store:commit(UpdateHandle),
	flags_compare(Rev2, Flags).


%% Flags should be inherited when forking
test_flags_fork(_Config) ->
	Flags = 16#f00f,
	{ok, _Doc1, Handle} = peerdrive_store:create(usr_store, <<"public.data">>, <<"test">>),
	ok = peerdrive_store:set_flags(Handle, Flags),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	{ok, _Doc2, ForkHandle} = peerdrive_store:fork(usr_store, Rev1, <<"test">>),
	ok = peerdrive_store:write(ForkHandle, <<"FILE">>, 0, <<"foobar">>),
	{ok, Flags} = peerdrive_store:get_flags(ForkHandle),
	{ok, Rev2} = peerdrive_store:commit(ForkHandle),
	flags_compare(Rev1, Flags),
	flags_compare(Rev2, Flags).


flags_compare(Rev, ExpectedFlags) ->
	{ok, #rev_stat{flags=ExpectedFlags}} = peerdrive_store:stat(usr_store, Rev).


%% Creator code must be stored as expected
test_creator(_Config) ->
	Creator = <<"test.foo">>,
	{ok, _Doc1, Handle} = peerdrive_store:create(usr_store, <<"public.data">>, Creator),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	creator_compare(Rev1, Creator).


%% Creator code must be set for forked documents and stay for the old
test_creator_fork(_Config) ->
	{ok, _Doc1, Handle} = peerdrive_store:create(usr_store, <<"public.data">>, <<"test.foo">>),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	{ok, _Doc2, ForkHandle} = peerdrive_store:fork(usr_store, Rev1, <<"test.bar">>),
	{ok, Rev2} = peerdrive_store:commit(ForkHandle),
	creator_compare(Rev1, <<"test.foo">>),
	creator_compare(Rev2, <<"test.bar">>).


%% Creator code can be updated when document is updated
test_creator_update_change(_Config) ->
	{ok, Doc, Handle} = peerdrive_store:create(usr_store, <<"public.data">>, <<"test.foo">>),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	{ok, UpdateHandle} = peerdrive_store:update(usr_store, Doc, Rev1, <<"test.bar">>),
	{ok, Rev2} = peerdrive_store:commit(UpdateHandle),
	creator_compare(Rev1, <<"test.foo">>),
	creator_compare(Rev2, <<"test.bar">>).


%% Creator code must stay the same if not specified on update
test_creator_update_keep(_Config) ->
	{ok, Doc, Handle} = peerdrive_store:create(usr_store, <<"public.data">>, <<"test.foo">>),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	{ok, UpdateHandle} = peerdrive_store:update(usr_store, Doc, Rev1, undefined),
	% write something to create a new revision for sure
	ok = peerdrive_store:write(UpdateHandle, <<"FILE">>, 0, <<"foobar">>),
	{ok, Rev2} = peerdrive_store:commit(UpdateHandle),
	creator_compare(Rev1, <<"test.foo">>),
	creator_compare(Rev2, <<"test.foo">>).


creator_compare(Rev, Expected) ->
	{ok, #rev_stat{creator=Expected}} = peerdrive_store:stat(usr_store, Rev).

