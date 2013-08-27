-module(sync_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("store.hrl").

-define(get(Key, Config), proplists:get_value(Key, Config)).
-define(store1(Config), proplists:get_value(store1, Config)).
-define(store2(Config), proplists:get_value(store2, Config)).
-define(store1_sid(Config), proplists:get_value(store1_sid, Config)).
-define(store2_sid(Config), proplists:get_value(store2_sid, Config)).

suite() ->
	[{timetrap, {seconds, 10}}].

all() ->
	[test_ff_ok, test_ff_err, test_latest, test_latest_fallback,
		test_merge, test_merge_fallback].


init_per_suite(Config) ->
	SysDir = filename:join(?get(priv_dir, Config), "sys"),
	ok = filelib:ensure_dir(filename:join(SysDir, "dummy")),
	ok = application:set_env(peerdrive, sys_store, {SysDir, "", "file"}),
	ok = application:start(peerdrive),
	Config.

end_per_suite(_Config) ->
	ok = application:stop(peerdrive).


init_per_testcase(_Test, Config) ->
	StoreDir1 = filename:join(?get(priv_dir, Config), "store1"),
	StoreDir2 = filename:join(?get(priv_dir, Config), "store2"),
	ok = rm_dir(StoreDir1),
	ok = rm_dir(StoreDir2),
	ok = filelib:ensure_dir(filename:join(StoreDir1, "dummy")),
	ok = filelib:ensure_dir(filename:join(StoreDir2, "dummy")),
	case application:start(peerdrive) of
		ok -> ok;
		{error, {already_started, peerdrive}} -> ok
	end,
	{ok, SId1} = peerdrive_volman:mount(StoreDir1, "", "", "file",
		"store_suite_1"),
	{ok, PId1} = peerdrive_volman:store(SId1),
	{ok, SId2} = peerdrive_volman:mount(StoreDir2, "", "", "file",
		"store_suite_2"),
	{ok, PId2} = peerdrive_volman:store(SId2),
	[{store1, PId1}, {store1_sid, SId1}, {store2, PId2}, {store2_sid, SId2} | Config].

end_per_testcase(_Test, Config) ->
	ok = peerdrive_volman:unmount(?get(store2_sid, Config)),
	ok = peerdrive_volman:unmount(?get(store1_sid, Config)).


% Helpers

rm_dir(Target) ->
	Filelist = filelib:wildcard(filename:join(Target, "*")),
	Dirs = [F || F <- Filelist, filelib:is_dir(F)],
	Files = Filelist -- Dirs,
	lists:foreach(fun(File) -> ok = file:delete(File) end, Files),
	lists:foreach(fun(Dir) -> ok = rm_dir(filename:join(Target, Dir)) end, Dirs),
	case file:del_dir(Target) of
		ok -> ok;
		{error, enoent} -> ok
	end.


create_doc(Store, Uti) ->
	{ok, Doc, Handle} = peerdrive_store:create(Store, Uti, <<"test">>),
	{Doc, Handle}.


create_common(Config) ->
	create_common(Config, <<"public.data">>, undefined).

create_common(Config, Uti, Data) ->
	Store1 = ?store1(Config),
	Store2 = ?store2(Config),

	% create a doc
	{Doc, Handle} = create_doc(Store1, Uti),
	Data == undefined orelse (ok = peerdrive_store:set_data(Handle, <<>>,
		peerdrive_struct:encode(Data))),
	{ok, Rev} = peerdrive_store:commit(Handle),

	% replicate doc
	{ok, _} = peerdrive_broker:replicate_doc(Store1, Doc, Store2, []),

	% verify the common document
	{[{Rev, [_, _]}], []} = peerdrive_broker:lookup_doc(Doc, [Store1, Store2]),
	{Doc, Rev}.


create_fast_forward(Config) ->
	{Doc, Rev} = create_common(Config),
	{ok, Handle} = peerdrive_broker:update(?store1(Config), Doc, Rev, undefined),
	ok = peerdrive_broker:write(Handle, <<"_">>, 0, <<"update">>),
	{ok, NewRev} = peerdrive_broker:commit(Handle),
	{Doc, NewRev}.


%% Create a merge condition. The revision on store 1 will be newer than on
%% store 2.
create_merge(Config, Data1, Data2) ->
	create_merge(Config, undefined, Data1, Data2, <<"public.data">>).

create_merge(Config, Base, Data1, Data2, Uti) ->
	Store1 = ?store1(Config),
	Store1SId = ?store1_sid(Config),
	Store2 = ?store2(Config),
	Store2SId = ?store2_sid(Config),

	{Doc, Rev} = create_common(Config, Uti, Base),
	{ok, Handle2} = peerdrive_broker:update(Store2, Doc, Rev, undefined),
	ok = peerdrive_broker:set_data(Handle2, <<>>, peerdrive_struct:encode(Data2)),
	{ok, NewRev2} = peerdrive_broker:commit(Handle2),
	{ok, Handle1} = peerdrive_broker:update(Store1, Doc, Rev, undefined),
	ok = peerdrive_broker:set_data(Handle1, <<>>, peerdrive_struct:encode(Data1)),
	{ok, NewRev1} = peerdrive_broker:commit(Handle1),

	% verify the merge condition
	{[{NewRev1, [Store1SId]}], []} = peerdrive_broker:lookup_doc(Doc, [Store1]),
	{ok, #rev_stat{parents=[Rev]}} = peerdrive_broker:stat(NewRev1, [Store1, Store2]),
	{[{NewRev2, [Store2SId]}], []} = peerdrive_broker:lookup_doc(Doc, [Store2]),
	{ok, #rev_stat{parents=[Rev]}} = peerdrive_broker:stat(NewRev2, [Store1, Store2]),
	{Doc, NewRev1, NewRev2}.


%% We might get watch events from the past. Add some 100ms slack and remove all
%% watch messages that we got/get so far...
remove_watch_msg() ->
	receive
		{watch, _, _, _, _} -> remove_watch_msg()
	after
		100 -> ok
	end.


sync(Config, Doc, Mode) ->
	sync(Config, Doc, Mode, 3000).

sync(Config, Doc, Mode, Timeout) ->
	Store1SId = ?store1_sid(Config),
	Store2SId = ?store2_sid(Config),
	remove_watch_msg(),
	ok = peerdrive_change_monitor:watch(doc, Doc),
	ok = peerdrive_sync_sup:start_sync(Mode, Store1SId, Store2SId),
	Result = receive
		{watch, modified, doc, Store2SId, Doc} -> ok
	after
		Timeout -> error
	end,
	peerdrive_change_monitor:unwatch(doc, Doc),
	ok = peerdrive_sync_sup:stop_sync(Store1SId, Store2SId),
	Result.


assert_rev_content(Store, Rev, Expect) ->
	{ok, Handle} = peerdrive_broker:peek(Store, Rev),
	{ok, RevContent} = peerdrive_broker:get_data(Handle, <<>>),
	io:format("got: ~p~nexpect: ~p~n", [peerdrive_struct:decode(RevContent), Expect]),
	?assert(peerdrive_struct:cmp(peerdrive_struct:decode(RevContent), Expect)),
	ok = peerdrive_broker:close(Handle).


%% Tests

%% Checks if a document is synchronized via fast-forward
test_ff_ok(Config) ->
	{Doc, Rev} = create_fast_forward(Config),
	ok = sync(Config, Doc, ff),
	Store1 = ?store1(Config),
	Store2 = ?store2(Config),
	{[{Rev, [_, _]}], []} = peerdrive_broker:lookup_doc(Doc, [Store1, Store2]).


%% Checks that the fast-forward sync does not synchronize documents which need
%% a real merge
test_ff_err(Config) ->
	{Doc, Rev1, Rev2} = create_merge(Config, [1], [2]),
	error = sync(Config, Doc, ff, 1000),
	Store1 = ?store1(Config),
	Store2 = ?store2(Config),
	{[{Rev1, [_]}], []} = peerdrive_broker:lookup_doc(Doc, [Store1]),
	{[{Rev2, [_]}], []} = peerdrive_broker:lookup_doc(Doc, [Store2]).

%% Checks if the 'latest' strategy makes an 'ours' merge and replicates that to
%% both stores
test_latest(Config) ->
	{Doc, Rev1, Rev2} = create_merge(Config, [1], [2]),
	ok = sync(Config, Doc, latest),
	Store1 = ?store1(Config),
	Store2 = ?store2(Config),
	% has it been merged?
	{[{NewRev, [_, _]}], []} = peerdrive_broker:lookup_doc(Doc, [Store1, Store2]),
	% are the parents correct?
	{ok, #rev_stat{parents=Parents}} = peerdrive_broker:stat(NewRev, [Store1, Store2]),
	?assertEqual(lists:sort(Parents), lists:sort([Rev1, Rev2])),
	% correct content
	assert_rev_content(Store1, NewRev, [1]).


%% Check that 'latest' recognizes a fast-forward contition correctly
test_latest_fallback(Config) ->
	{Doc, Rev} = create_fast_forward(Config),
	ok = sync(Config, Doc, latest),
	Store1 = ?store1(Config),
	Store2 = ?store2(Config),
	{[{Rev, [_, _]}], []} = peerdrive_broker:lookup_doc(Doc, [Store1, Store2]).


%% Checks that the 'merge' strategy falls back to 'latest' in case of an
%% unknown document
test_merge_fallback(Config) ->
	{Doc, Rev1, Rev2} = create_merge(Config, [1], [2]),
	ok = sync(Config, Doc, merge),
	Store1 = ?store1(Config),
	Store2 = ?store2(Config),
	% has it been merged?
	{[{NewRev, [_, _]}], []} = peerdrive_broker:lookup_doc(Doc, [Store1, Store2]),
	% are the parents correct?
	{ok, #rev_stat{parents=Parents}} = peerdrive_broker:stat(NewRev, [Store1, Store2]),
	?assertEqual(lists:sort(Parents), lists:sort([Rev1, Rev2])),
	% correct content
	assert_rev_content(Store1, NewRev, [1]).


%% Checks that the 'merge' strategy really merges
test_merge(Config) ->
	Data1 = gb_trees:enter(<<"org.peerdrive.folder">>,
		[gb_trees:from_orddict([{<<>>, {dlink, <<1>>}}])], gb_trees:empty()),
	Data2 = gb_trees:enter(<<"org.peerdrive.folder">>,
		[gb_trees:from_orddict([{<<>>, {dlink, <<2>>}}])], gb_trees:empty()),
	{Doc, Rev1, Rev2} = create_merge(Config, gb_trees:empty(), Data1, Data2,
		<<"org.peerdrive.folder">>),

	% sync it
	ok = sync(Config, Doc, merge),
	Store1 = ?store1(Config),
	Store2 = ?store2(Config),

	% has it been merged?
	{[{NewRev, [_, _]}], []} = peerdrive_broker:lookup_doc(Doc, [Store1, Store2]),
	% are the parents correct?
	{ok, #rev_stat{parents=Parents}} = peerdrive_broker:stat(NewRev, [Store1, Store2]),
	?assertEqual(lists:sort(Parents), lists:sort([Rev1, Rev2])),
	% correct content
	Data = gb_trees:enter(<<"org.peerdrive.folder">>,
		[gb_trees:from_orddict([{<<>>, {dlink, <<1>>}}]),
		 gb_trees:from_orddict([{<<>>, {dlink, <<2>>}}]) ], gb_trees:empty()),
	assert_rev_content(Store1, NewRev, Data).
