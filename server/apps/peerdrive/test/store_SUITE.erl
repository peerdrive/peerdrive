-module(store_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").

-include("store.hrl").

-define(get(Key, Config), proplists:get_value(Key, Config)).
-define(store(Config), proplists:get_value(store, Config)).

suite() ->
	[{timetrap, {seconds, 10}}].


all() ->
	[test_rw_readback_small, test_rw_readback_big, test_rw_mtime,
	test_flags_create, test_flags_update_change, test_flags_update_keep,
	test_flags_fork, test_creator, test_creator_fork,
	test_creator_update_change, test_creator_update_keep, test_ephemral_gc].

init_per_suite(Config) ->
	SysDir = filename:join(?get(priv_dir, Config), "sys"),
	StoreDir = filename:join(?get(priv_dir, Config), "store"),
	ok = filelib:ensure_dir(filename:join(SysDir, "dummy")),
	ok = filelib:ensure_dir(filename:join(StoreDir, "dummy")),
	ok = application:set_env(peerdrive, sys_store, {SysDir, "", "file"}),
	ok = application:start(peerdrive),
	{ok, SId} = peerdrive_volman:mount(StoreDir, "", "", "file",
		"store_suite"),
	{ok, PId} = peerdrive_volman:store(SId),
	[{store, PId}, {store_sid, SId} | Config].

end_per_suite(Config) ->
	ok = peerdrive_volman:unmount(?get(store_sid, Config)),
	ok = application:stop(peerdrive).


%% Write and read back some small data
test_rw_readback_small(Config) ->
	Data = <<"akjdfaqhfkjsalur\naqidahgajsoasoiga\n\nakhsfdlkaf\r\n">>,
	rw_readback(?store(Config), Data).

%% Write and read back a big chunk
test_rw_readback_big(Config) ->
	Data = iolist_to_binary(lists:duplicate(1024, <<"akjdfaqhfkjsalur">>)),
	io:format("~p", [Data]),
	rw_readback(?store(Config), Data).


rw_readback(Store, Data) ->
	{ok, _Doc, Handle} = peerdrive_store:create(Store, <<"public.data">>, <<"test">>),
	ok = peerdrive_store:write(Handle, <<"FILE">>, 0, Data),
	{ok, Rev} = peerdrive_store:commit(Handle),
	% now read back
	{ok, ReadHandle} = peerdrive_store:peek(Store, Rev),
	{ok, Data} = peerdrive_store:read(ReadHandle, <<"FILE">>, 0, 16#1000000),
	peerdrive_store:close(ReadHandle),
	peerdrive_store:close(Handle).


%% Test if mtime of created revision is in expected range
test_rw_mtime(Config) ->
	{ok, _Doc, Handle} = peerdrive_store:create(?store(Config), <<"public.data">>, <<"test">>),
	ok = peerdrive_store:write(Handle, <<"FILE">>, 0, <<"foobar">>),
	{ok, Rev} = peerdrive_store:commit(Handle),
	{ok, #rev_stat{mtime=MTime}} = peerdrive_store:stat(?store(Config), Rev),
	Now = peerdrive_util:get_time(),
	true = (MTime =< Now) and (MTime >= Now-2).


%% Create a document and make sure flags are empty
test_flags_create(Config) ->
	{ok, _Doc, Handle} = peerdrive_store:create(?store(Config), <<"public.data">>, <<"test">>),
	{ok, Rev} = peerdrive_store:commit(Handle),
	flags_compare(?store(Config), Rev, 0).


%% Test that update of flags works
test_flags_update_change(Config) ->
	Store = ?store(Config),
	{ok, Doc, Handle} = peerdrive_store:create(Store, <<"public.data">>, <<"test">>),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	{ok, UpdateHandle} = peerdrive_store:update(Store, Doc, Rev1, undefined),
	Flags = 16#1337,
	ok = peerdrive_store:set_flags(UpdateHandle, Flags),
	{ok, Rev2} = peerdrive_store:commit(UpdateHandle),
	flags_compare(Store, Rev2, Flags).


%% Test that update of a part doesn't change flags
test_flags_update_keep(Config) ->
	Store = ?store(Config),
	Flags = 16#deadbeef,
	{ok, Doc, Handle} = peerdrive_store:create(Store, <<"public.data">>, <<"test">>),
	ok = peerdrive_store:set_flags(Handle, Flags),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	flags_compare(Store, Rev1, Flags),
	{ok, UpdateHandle} = peerdrive_store:update(Store, Doc, Rev1, undefined),
	ok = peerdrive_store:write(UpdateHandle, <<"FILE">>, 0, <<"foobar">>),
	{ok, Flags} = peerdrive_store:get_flags(UpdateHandle),
	{ok, Rev2} = peerdrive_store:commit(UpdateHandle),
	flags_compare(Store, Rev2, Flags).


%% Flags should be inherited when forking
test_flags_fork(Config) ->
	Store = ?store(Config),
	Flags = 16#f00f,
	{ok, _Doc1, Handle} = peerdrive_store:create(Store, <<"public.data">>, <<"test">>),
	ok = peerdrive_store:set_flags(Handle, Flags),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	{ok, _Doc2, ForkHandle} = peerdrive_store:fork(Store, Rev1, <<"test">>),
	ok = peerdrive_store:write(ForkHandle, <<"FILE">>, 0, <<"foobar">>),
	{ok, Flags} = peerdrive_store:get_flags(ForkHandle),
	{ok, Rev2} = peerdrive_store:commit(ForkHandle),
	flags_compare(Store, Rev1, Flags),
	flags_compare(Store, Rev2, Flags).


flags_compare(Store, Rev, ExpectedFlags) ->
	{ok, #rev_stat{flags=ExpectedFlags}} = peerdrive_store:stat(Store, Rev).


%% Creator code must be stored as expected
test_creator(Config) ->
	Store = ?store(Config),
	Creator = <<"test.foo">>,
	{ok, _Doc1, Handle} = peerdrive_store:create(Store, <<"public.data">>, Creator),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	creator_compare(Store, Rev1, Creator).


%% Creator code must be set for forked documents and stay for the old
test_creator_fork(Config) ->
	Store = ?store(Config),
	{ok, _Doc1, Handle} = peerdrive_store:create(Store, <<"public.data">>, <<"test.foo">>),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	{ok, _Doc2, ForkHandle} = peerdrive_store:fork(Store, Rev1, <<"test.bar">>),
	{ok, Rev2} = peerdrive_store:commit(ForkHandle),
	creator_compare(Store, Rev1, <<"test.foo">>),
	creator_compare(Store, Rev2, <<"test.bar">>).


%% Creator code can be updated when document is updated
test_creator_update_change(Config) ->
	Store = ?store(Config),
	{ok, Doc, Handle} = peerdrive_store:create(Store, <<"public.data">>, <<"test.foo">>),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	{ok, UpdateHandle} = peerdrive_store:update(Store, Doc, Rev1, <<"test.bar">>),
	{ok, Rev2} = peerdrive_store:commit(UpdateHandle),
	creator_compare(Store, Rev1, <<"test.foo">>),
	creator_compare(Store, Rev2, <<"test.bar">>).


%% Creator code must stay the same if not specified on update
test_creator_update_keep(Config) ->
	Store = ?store(Config),
	{ok, Doc, Handle} = peerdrive_store:create(Store, <<"public.data">>, <<"test.foo">>),
	{ok, Rev1} = peerdrive_store:commit(Handle),
	{ok, UpdateHandle} = peerdrive_store:update(Store, Doc, Rev1, undefined),
	% write something to create a new revision for sure
	ok = peerdrive_store:write(UpdateHandle, <<"FILE">>, 0, <<"foobar">>),
	{ok, Rev2} = peerdrive_store:commit(UpdateHandle),
	creator_compare(Store, Rev1, <<"test.foo">>),
	creator_compare(Store, Rev2, <<"test.foo">>).


creator_compare(Store, Rev, Expected) ->
	{ok, #rev_stat{creator=Expected}} = peerdrive_store:stat(Store, Rev).


%% Parents of ephemeral revisions are garbage collected
test_ephemral_gc(Config) ->
	Store = ?store(Config),
	{ok, Doc, Handle} = peerdrive_store:create(Store, <<"public.data">>, <<"test.foo">>),
	{ok, Rev1} = peerdrive_store:commit(Handle),

	% create some other temporary doc to reference first one to prevent it
	% from getting garbage collected
	{ok, _TmpDoc, TmpHndl} = peerdrive_store:create(Store, <<"public.data">>, <<"test.foo">>),
	TmpData = gb_trees:enter(<<"org.peerdrive.folder">>,
		[gb_trees:from_orddict([{<<>>, {dlink, Doc}}])], gb_trees:empty()),
	ok = peerdrive_store:set_data(TmpHndl, <<>>, peerdrive_struct:encode(TmpData)),
	{ok, _} = peerdrive_store:commit(TmpHndl),

	% now we can close the first handle
	ok = peerdrive_store:close(Handle),

	% update Doc and make it ephemeral
	{ok, UpdateHandle} = peerdrive_store:update(Store, Doc, Rev1, undefined),
	ok = peerdrive_store:set_flags(UpdateHandle, ?REV_FLAG_EPHEMERAL),
	ok = peerdrive_store:write(UpdateHandle, <<"FILE">>, 0, <<"foobar">>),
	{ok, Rev2} = peerdrive_store:commit(UpdateHandle),
	ok = peerdrive_store:close(UpdateHandle),

	% force garbage collection
	ok = peerdrive_file_store:gc(Store),

	% now check that original revision was garbage collected
	{ok, _} = peerdrive_store:stat(Store, Rev2),
	{error, enoent} = peerdrive_store:stat(Store, Rev1).

