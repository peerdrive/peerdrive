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

-module(file_store).
-behaviour(gen_server).

-export([start_link/2, stop/1, dump/1]).
-export([init/1, handle_call/3, handle_cast/2, code_change/3, handle_info/2, terminate/2]).

-export([guid/1, contains/2, stat/2, lookup/2, put_uuid/4, put_rev_start/3,
	read_start/3, write_start_fork/3, write_start_update/3, write_start_merge/4,
	delete_rev/2, delete_uuid/2, sync_get_changes/2, sync_set_anchor/3]).

-include("store.hrl").
-include("file_store.hrl").
-include_lib("kernel/include/file.hrl").

% path:    string,  base directory
% guid:    binary,  GUID of the store
% gen:     integer, Generation of the store
% uuids:   dict: Uuid --> {Revision, Generation}
% objects: dict: Revision --> {refcount, #object | stub}
% parts:   dict: PartHash --> refcount
% peers:   dict: GUID --> Generation
% changed: Bool
-record(state, {path, guid, gen, uuids, objects, parts, peers, changed=false}).

-define(SYNC_INTERVAL, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Server state management...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Id, {Path, Name}) ->
	case gen_server:start_link({local, Id}, ?MODULE, {Id, Path, Name}, []) of
		{ok, Pid} ->
			{ok, Pid, make_interface(Pid)};
		Else ->
			Else
	end.

init({Id, Path, Name}) ->
	case filelib:is_dir(Path) of
		true ->
			Guid    = load_guid(Path),
			Gen     = load_gen(Path),
			Uuids   = load_uuids(Path),
			Objects = load_objects(Path),
			Parts   = load_parts(Path),
			Peers   = load_peers(Path),
			State   = #state{
				path    = Path,
				guid    = Guid,
				gen     = Gen,
				uuids   = Uuids,
				objects = Objects,
				parts   = Parts,
				peers   = Peers
			},
			volman:reg_store(Id, Guid, make_interface(self())),
			process_flag(trap_exit, true),
			{ok, check_root_doc(State, Name)};
			
		false ->
			{stop, {enoent, Path}}
	end.

stop(Store) ->
	gen_server:cast(Store, stop).

dump(Store) ->
	gen_server:cast(Store, dump).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Operations on object store...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Get GUID of a store
%% @see store:guid/1
guid(Store) ->
	gen_server:call(Store, guid).

%% @doc Lookup a UUID.
%% @see store:lookup/2
lookup(Store, Uuid) ->
	gen_server:call(Store, {lookup, Uuid}).

%% @doc Check if a revision exists in the store
%% @see store:contains/2
contains(Store, Rev) ->
	gen_server:call(Store, {contains, Rev}).

%% @doc Stat a revision.
%% @see store:stat/2
stat(Store, Rev) ->
	gen_server:call(Store, {stat, Rev}).

%% @doc Start reading a document revision.
%% @see store:read_start/3
read_start(Store, Rev, User) ->
	gen_server:call(Store, {read, Rev, User}).

%% @doc Create a new document
%% @see store:write_start_fork/3
write_start_fork(Store, StartRev, Uti) ->
	gen_server:call(Store, {fork, StartRev, Uti}).

%% @doc Write to an existing document
%% @see store:write_start_update/3
write_start_update(Store, Uuid, StartRev) ->
	gen_server:call(Store, {update, Uuid, StartRev}).

%% @doc Merge a document
%% @see store:write_start_merge/4
write_start_merge(Store, Uuid, StartRevs, Uti) ->
	gen_server:call(Store, {merge, Uuid, StartRevs, Uti}).

%% @doc Delete a UUID
%% @see store:delete_uuid/2
delete_uuid(Store, Uuid) ->
	gen_server:call(Store, {delete_uuid, Uuid}).

%% @doc Delete a revision
%% @see store:delete_rev/2
delete_rev(Store, Rev) ->
	gen_server:call(Store, {delete_rev, Rev}).

%% @doc Put/update a UUID in the store
%% @see store:put_uuid/4
put_uuid(Store, Uuid, OldRev, NewRev) ->
	gen_server:call(Store, {put_uuid, Uuid, OldRev, NewRev}).

%% @doc Put/import a revision into the store.
%% @see store:put_rev_start/3
put_rev_start(Store, Rev, Object) ->
	gen_server:call(Store, {put_rev, Rev, Object}).

%% @doc Get changes since the last sync point of peer store
%% @see store:sync_get_changes/2
sync_get_changes(Store, PeerGuid) ->
	gen_server:call(Store, {sync_get_changes, PeerGuid}).

%% @doc Set sync point of peer store to new generation
%% @see store:sync_set_anchor/3
sync_set_anchor(Store, PeerGuid, SeqNum) ->
	gen_server:call(Store, {sync_set_anchor, PeerGuid, SeqNum}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Gen_server callbacks...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_call(Request, From, State) ->
	case handle_call_internal(Request, From, State) of
		{reply, Reply, NewState} = Result ->
			case NewState#state.changed of
				true  -> {reply, Reply, NewState, ?SYNC_INTERVAL};
				false -> Result
			end;

		{noreply, NewState} = Result ->
			case NewState#state.changed of
				true  -> {noreply, NewState, ?SYNC_INTERVAL};
				false -> Result
			end;

		Result ->
			Result
	end.

handle_cast(Request, State) ->
	case handle_cast_internal(Request, State) of
		{noreply, NewState} = Result ->
			case NewState#state.changed of
				true  -> {noreply, NewState, ?SYNC_INTERVAL};
				false -> Result
			end;

		Result ->
			Result
	end.


handle_info(Info, State) ->
	case handle_info_internal(Info, State) of
		{noreply, NewState} = Result ->
			case NewState#state.changed of
				true  -> {noreply, NewState, ?SYNC_INTERVAL};
				false -> Result
			end;

		Result ->
			Result
	end.


handle_call_internal(guid, _From, S) ->
	{reply, S#state.guid, S};

% returns `{ok, Rev} | error'
handle_call_internal({lookup, Uuid}, _From, S) ->
	case dict:find(Uuid, S#state.uuids) of
		{ok, {Rev, _Gen}} -> {reply, {ok, Rev}, S};
		error             -> {reply, error, S}
	end;

handle_call_internal({contains, Rev}, _From, S) ->
	Reply = case dict:find(Rev, S#state.objects) of
		{ok, {_, stub}} -> false;
		{ok, {_, _}}    -> true;
		error           -> false
	end,
	{reply, Reply, S};

handle_call_internal({stat, Rev}, _From, S) ->
	Reply = do_stat(Rev, S),
	{reply, Reply, S};

% returns `{ok, Reader} | {error, Reason}'
handle_call_internal({read, Rev, User}, From, S) ->
	case dict:find(Rev, S#state.objects) of
		{ok, {_, stub}} ->
			{reply, {error, enoent}, S};

		{ok, {_, Object}} ->
			Pid = self(),
			spawn_link(fun() ->
				Reply = file_store_reader:start(Pid, S#state.path, Object#object.parts, User),
				gen_server:reply(From, Reply)
			end),
			{noreply, S};

		error ->
			{reply, {error, enoent}, S}
	end;

handle_call_internal({fork, StartRev, Uti}, From, S) ->
	{User, _} = From,
	{S2, Reply} = do_write_start_fork(S, StartRev, Uti, User),
	{reply, Reply, S2};

handle_call_internal({update, Uuid, StartRev}, From, S) ->
	{User, _} = From,
	{S2, Reply} = do_write_start_update(S, Uuid, StartRev, User),
	{reply, Reply, S2};

handle_call_internal({merge, Uuid, StartRevs, Uti}, From, S) ->
	{User, _} = From,
	{S2, Reply} = do_write_start_merge(S, Uuid, StartRevs, Uti, User),
	{reply, Reply, S2};

handle_call_internal({delete_rev, Rev}, _From, S) ->
	{S2, Reply} = do_delete_rev(S, Rev),
	{reply, Reply, S2};

handle_call_internal({delete_uuid, Uuid}, _From, S) ->
	{S2, Reply} = do_delete_uuid(S, Uuid),
	{reply, Reply, S2};

handle_call_internal({put_uuid, Uuid, OldRev, NewRev}, _From, S) ->
	{S2, Reply} = do_put_uuid(S, Uuid, OldRev, NewRev),
	{reply, Reply, S2};

handle_call_internal({put_rev, Rev, Object}, From, S) ->
	{User, _} = From,
	{S2, Reply} = do_put_rev(S, Rev, Object, User),
	{reply, Reply, S2};

handle_call_internal({sync_get_changes, PeerGuid}, _From, S) ->
	Reply = do_sync_get_changes(S, PeerGuid),
	{reply, Reply, S};

handle_call_internal({sync_set_anchor, PeerGuid, SeqNum}, _From, S) ->
	S2 = do_sync_set_anchor(S, PeerGuid, SeqNum),
	{reply, ok, S2};

% internal: ok | {error, Reason}
handle_call_internal({insert_rev, Rev, Object}, _From, S) ->
	{S2, Reply} = do_put_rev_commit(S, Rev, Object),
	{reply, Reply, S2};

% internal: commit a new object
handle_call_internal({commit, Uuid, Object}, _From, S) ->
	{S2, Reply} = do_commit(S, Uuid, Object),
	{reply, Reply, S2};

% internal: add part refcount
handle_call_internal({parts_ref_inc, PartHashes}, _From, S) ->
	S2 = do_parts_ref_inc(S, PartHashes),
	{reply, ok, S2}.


% internal: decrease part refcount
handle_cast_internal({part_ref_dec, PartHash}, S) ->
	RefCount = dict:fetch(PartHash, S#state.parts)-1,
	Parts = if
		RefCount == 0 ->
			file:delete(util:build_path(S#state.path, PartHash)),
			dict:erase(PartHash, S#state.parts);
		true ->
			dict:store(PartHash, RefCount, S#state.parts)
	end,
	{noreply, S#state{parts=Parts, changed=true}};

% internal: decrease refcount of a object
handle_cast_internal({object_ref_dec, Rev}, S) ->
	{RefCount, Object} = dict:fetch(Rev, S#state.objects),
	Objects = if
		RefCount == 1 ->
			dispose_object(Object),
			case Object of
				stub  -> ok;
				_Else -> vol_monitor:trigger_rm_rev(S#state.guid, Rev)
			end,
			dict:erase(Rev, S#state.objects);
		true ->
			dict:store(Rev, {RefCount-1, Object}, S#state.objects)
	end,
	{noreply, S#state{objects=Objects, changed=true}};

handle_cast_internal(dump, S) ->
	do_dump(S),
	{noreply, S};

handle_cast_internal(stop, S) ->
	{stop, normal, S}.


handle_info_internal(timeout, S) ->
	do_checkpoint(S),
	{noreply, S#state{changed=false}};

handle_info_internal({'EXIT', _From, Reason}, S) ->
	case Reason of
		normal   -> {noreply, S};
		shutdown -> {noreply, S};
		% special reason when reader/writer/importer user died
		orphaned -> {noreply, S};
		_ ->        {stop, {eunexpected, Reason}, S}
	end.


terminate(Reason, State) ->
	case Reason of
		shutdown -> do_checkpoint(State);
		normal   -> do_checkpoint(State);
		Error    -> io:format("store: Unexpected terminate: ~p~n", [Error])
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Stubs...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


code_change(_, State, _) -> {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local functions...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

make_interface(Pid) ->
	#store{
		this               = Pid,
		guid               = fun guid/1,
		contains           = fun contains/2,
		lookup             = fun lookup/2,
		stat               = fun stat/2,
		put_uuid           = fun put_uuid/4,
		put_rev_start      = fun put_rev_start/3,
		read_start         = fun read_start/3,
		write_start_fork   = fun write_start_fork/3,
		write_start_update = fun write_start_update/3,
		write_start_merge  = fun write_start_merge/4,
		delete_rev         = fun delete_rev/2,
		delete_uuid        = fun delete_uuid/2,
		sync_get_changes   = fun sync_get_changes/2,
		sync_set_anchor    = fun sync_set_anchor/3
	}.

check_root_doc(S, Name) ->
	case dict:is_key(S#state.guid, S#state.uuids) of
		true ->
			S;
		false ->
			RootContent = dict:new(),
			{S2, ContentHash} = crd_write_part(S, RootContent),
			Annotation1 = dict:new(),
			Annotation2 = dict:store(<<"title">>, list_to_binary(Name), Annotation1),
			Annotation3 = dict:store(<<"comment">>, list_to_binary("<<Initial store creation>>"), Annotation2),
			Sync = dict:store(<<"sticky">>, true, dict:new()),
			RootMeta1 = dict:new(),
			RootMeta2 = dict:store(<<"org.hotchpotch.annotation">>, Annotation3, RootMeta1),
			RootMeta3 = dict:store(<<"org.hotchpotch.sync">>, Sync, RootMeta2),
			{S3, MetaHash} = crd_write_part(S2, RootMeta3),
			Object = #object{
				parts   = [{<<"HPSD">>, ContentHash}, {<<"META">>, MetaHash}],
				parents = [],
				mtime   = util:get_time(),
				uti     = <<"org.hotchpotch.volume">>},
			Rev = store:hash_object(Object),
			S4 = S3#state{objects=dict:store(Rev, {1, Object}, S3#state.objects)},
			Gen = S4#state.gen,
			S4#state{
				gen     = Gen+1,
				uuids   = dict:store(S4#state.guid, {Rev, Gen}, S4#state.uuids),
				changed = true
			}
	end.

crd_write_part(S, RawContent) ->
	Content = struct:encode(RawContent),
	Hash = crypto:md5(Content),
	Name = util:build_path(S#state.path, Hash),
	filelib:ensure_dir(Name),
	file:write_file(Name, Content),
	S2 = do_parts_ref_inc(S, [Hash]),
	{S2, Hash}.


% returns `{ok, Parts, Parents, Mtime, Uti} | error'
do_stat(Rev, #state{objects=Objects, path=Path}) ->
	case dict:find(Rev, Objects) of
		% object doesn't exist anymore on this store
		{ok, {_, stub}} ->
			error;

		% got'ya
		{ok, {_, Object}} ->
			Parts = lists:foldl(
				fun ({FourCC, Hash}, AccIn) ->
					case file:read_file_info(util:build_path(Path, Hash)) of
						{ok, FileInfo} ->
							[{FourCC, FileInfo#file_info.size, Hash} | AccIn];
						{error, _} ->
							AccIn
					end
				end,
				[],
				Object#object.parts),
			{ok, Parts, Object#object.parents, Object#object.mtime, Object#object.uti};

		% completely unknown...
		error ->
			error
	end.

%%% Delete %%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_delete_rev(#state{guid=Guid, objects=Objects} = S, Rev) ->
	case dict:find(Rev, Objects) of
		{ok, {_, stub}} ->
			{S, {error, enoent}};

		{ok, {RefCount, Object}} ->
			vol_monitor:trigger_rm_rev(Guid, Rev),
			dispose_object(Object),
			{
				S#state{
					objects=dict:store(Rev, {RefCount, stub}, Objects),
					changed=true
				},
				ok
			};

		error ->
			{S, {error, enoent}}
	end.


do_delete_uuid(#state{guid=Guid, uuids=Uuids} = S, Uuid) ->
	case Uuid of
		Guid ->
			{S, {error, eaccess}};

		_ ->
			case dict:find(Uuid, Uuids) of
				{ok, {Rev, _Gen}} ->
					vol_monitor:trigger_rm_uuid(Guid, Uuid),
					gen_server:cast(self(), {object_ref_dec, Rev}),
					{S#state{ uuids=dict:erase(Uuid, Uuids), changed=true}, ok};

				error ->
					{S, {error, enoent}}
			end
	end.


dispose_object(Object) ->
	case Object of
		stub ->
			ok;

		#object{parts=Parts, parents=Parents} ->
			lists:foreach(
				fun({_, Hash}) -> gen_server:cast(self(), {part_ref_dec, Hash}) end,
				Parts),
			lists:foreach(
				fun (Rev) -> gen_server:cast(self(), {object_ref_dec, Rev}) end,
				Parents)
	end.


%%% Dump %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% #state{
%   path:    base directory
%   gen:     integer
%   uuids:   dict: Uuid --> {Revision, Generation}
%   objects: dict: Revision --> {refcount, #object | stub}
%   parts:   dict: PartHash --> refcount
%   peers:   dict: GUID --> Generation
%   changed: Bool
% }
do_dump(S) ->
	io:format("Guid: ~s~n", [util:bin_to_hexstr(S#state.guid)]),
	io:format("Gen:  ~p~n", [S#state.gen]),
	io:format("Path: ~s~n", [S#state.path]),
	io:format("Uuids:~n"),
	dict:fold(
		fun (Uuid, {Rev, Gen}, _) ->
			io:format("    ~s -> ~s @ ~p~n", [util:bin_to_hexstr(Uuid), util:bin_to_hexstr(Rev), Gen])
		end,
		ok, S#state.uuids),
	io:format("Objects:~n"),
	dict:fold(
		fun (Rev, {RefCount, Object}, _) ->
			io:format("    ~s #~w~n", [util:bin_to_hexstr(Rev), RefCount]),
			do_dump_object(Object)
		end,
		ok, S#state.objects),
	io:format("Parts:~n"),
	dict:fold(
		fun (Hash, RefCount, _) ->
			io:format("    ~s #~w~n", [util:bin_to_hexstr(Hash), RefCount])
		end,
		ok, S#state.parts),
	io:format("Peers:~n"),
	dict:fold(
		fun (Guid, Gen, _) ->
			io:format("    ~s @ ~w~n", [util:bin_to_hexstr(Guid), Gen])
		end,
		ok, S#state.peers).


% #object{
%   parts:   [{PartFourCC, Hash}]*
%   parents: [Revision]*
%   mitme:   {MegaSecs, Secs, MicroSecs}
%   uti:     Binary
% }
do_dump_object(Object) ->
	if
		Object == stub ->
			ok;
		true ->
			io:format("        Parts:~n"),
			lists:foreach(
				fun ({FourCC, Hash}) ->
					io:format("            ~w -> ~s~n", [FourCC, util:bin_to_hexstr(Hash)])
				end,
				Object#object.parts),
			io:format("        Parents:~n"),
			lists:foreach(
				fun (Rev) ->
					io:format("            ~s~n", [util:bin_to_hexstr(Rev)])
				end,
				Object#object.parents)
	end.


%%% Creating & Writing %%%%%%%%%%%%%%%%

% returns `{S2, {ok, Uuid, Writer} | {error, Reason}}'
do_write_start_fork(S, StartRev, Uti, User) ->
	Uuid = crypto:rand_bytes(16),
	case StartRev of
		% create new document
		<<0:128>> ->
			State = #ws{
				path   = S#state.path,
				server = self(),
				uuid   = Uuid,
				revs   = [],
				uti    = Uti,
				orig   = dict:new(),
				new    = dict:new()},
			{S2, Writer} = start_writer(S, State, User),
			{S2, {ok, Uuid, Writer}};

		% start from existing document
		Rev ->
			case dict:find(Rev, S#state.objects) of
				% err, don't known that one...
				error ->
					{S, {error, enoent}};
				{ok, {_, stub}} ->
					{S, {error, enoent}};

				% load old values and start writer process
				{ok, {_, Object}} ->
					Parts = dict:from_list(Object#object.parts),
					State = #ws{
						path   = S#state.path,
						server = self(),
						uuid   = Uuid,
						revs   = [Rev],
						uti    = Uti,
						orig   = Parts,
						new    = dict:new()},
					{S2, Writer} = start_writer(S, State, User),
					{S2, {ok, Uuid, Writer}}
			end
	end.


% returns `{S2, {ok, Writer} | {error, Reason}}' where `Reason = conflict | enoent | ...'
do_write_start_update(S, Uuid, StartRev, User) ->
	case dict:find(Uuid, S#state.uuids) of
		{ok, {StartRev, _Gen}} ->
			case dict:fetch(StartRev, S#state.objects) of
				{_, stub} ->
					{S, {error, orphaned}};

				{_, Object} ->
					Parts = dict:from_list(Object#object.parts),
					State = #ws{
						path   = S#state.path,
						server = self(),
						uuid   = Uuid,
						revs   = [StartRev],
						uti    = Object#object.uti,
						orig   = Parts,
						new    = dict:new()},
					{S2, Writer} = start_writer(S, State, User),
					{S2, {ok, Writer}}
			end;

		{ok, {_OtherRev, _Gen}} ->
			% wrong revision
			{S, {error, conflict}};

		error ->
			% unknown UUID
			{S, {error, enoent}}
	end.


% returns `{S2, {ok, Writer} | {error, Reason}}' where `Reason = conflict | enoent | ...'
do_write_start_merge(S, Uuid, StartRevs, Uti, User) ->
	case dict:find(Uuid, S#state.uuids) of
		{ok, {CurrentRev, _Gen}} ->
			case lists:member(CurrentRev, StartRevs) of
				true ->
					State = #ws{
						path   = S#state.path,
						server = self(),
						uuid   = Uuid,
						revs   = StartRevs,
						uti    = Uti,
						orig   = dict:new(),
						new    = dict:new()},
					{S2, Writer} = start_writer(S, State, User),
					{S2, {ok, Writer}};
				false ->
					{S, {error, conflict}}
			end;
		error ->
			% unknown UUID
			{S, {error, enoent}}
	end.


% adjust reference count, then start writer process
start_writer(S1, State, User) ->
	S2 = do_objects_ref_inc(S1, State#ws.revs),
	OrigParts = dict:fold(fun(_,H,Acc) -> [H|Acc] end, [], State#ws.orig),
	S3 = do_parts_ref_inc(S2, OrigParts),
	{ok, Writer} = file_store_writer:start(State, User),
	{S3, Writer}.


% ok | {error, Reason}
do_put_uuid(#state{uuids=Uuids} = S, Uuid, OldRev, NewRev) ->
	case dict:find(Uuid, Uuids) of
		% already pointing to requested rev
		{ok, {NewRev, _}} ->
			{S, ok};

		% free old version, store new one
		{ok, {OldRev, _}} ->
			gen_server:cast(self(), {object_ref_dec, OldRev}),
			vol_monitor:trigger_mod_uuid(S#state.guid, Uuid),
			S21 = do_objects_ref_inc(S, [NewRev]),
			Gen = S21#state.gen,
			{
				S21#state{
					gen     = Gen+1,
					uuids   = dict:store(Uuid, {NewRev, Gen}, S21#state.uuids),
					changed = true
				},
				ok
			};

		% completely other rev
		{ok, _} ->
			{S, {error, conflict}};

		% Uuid does not exist (yet)...
		error ->
			vol_monitor:trigger_add_uuid(S#state.guid, Uuid),
			S21 = do_objects_ref_inc(S, [NewRev]),
			Gen = S21#state.gen,
			{
				S21#state{
					gen     = Gen+1,
					uuids   = dict:store(Uuid, {NewRev, Gen}, S21#state.uuids),
					changed = true
				},
				ok
			}
	end.


% ok | {ok, MissingParts, Importer} | {error, Reason}
do_put_rev(S, Rev, Object, User) ->
	case dict:find(Rev, S#state.objects) of
		error ->
			{S, {error, enotref}};

		{ok, {_, stub}} ->
			AllParts = S#state.parts,
			{PartsDone, PartsNeeded} = lists:foldl(
				fun({FourCC, Hash}, {AccDone, AccNeed}) ->
					case dict:is_key(Hash, AllParts) of
						true  -> {[Hash|AccDone], AccNeed};
						false -> {AccDone, [{FourCC, Hash}|AccNeed]}
					end
				end,
				{[], []},
				Object#object.parts),
			S2 = do_parts_ref_inc(S, PartsDone),
			S3 = do_objects_ref_inc(S2, Object#object.parents),
			case PartsNeeded of
				[] ->
					do_put_rev_commit(S3, Rev, Object);

				_ ->
					{ok, Importer} = file_store_importer:start(self(),
						S#state.path, Rev, Object, PartsDone, PartsNeeded,
						User),
					NeededFourCCs = lists:map(fun({FCC, _Hash}) -> FCC end, PartsNeeded),
					{S3, {ok, NeededFourCCs, Importer}}
			end;

		{ok, _} ->
			{S, ok}
	end.

do_put_rev_commit(#state{objects=Objects} = S, Rev, Object) ->
	case dict:find(Rev, Objects) of
		error ->
			dispose_object(Object),
			{S, {error, enotref}};

		{ok, {RefCount, stub}} ->
			S2 = S#state{
				objects=dict:store(Rev, {RefCount, Object}, Objects),
				changed=true},
			vol_monitor:trigger_add_rev(S#state.guid, Rev),
			{S2, ok};

		{ok, _} ->
			dispose_object(Object),
			{S, ok}
	end.

%%% Commit %%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_commit(S, Uuid, Object) ->
	NewRev = store:hash_object(Object),
	Reply = case dict:find(Uuid, S#state.uuids) of
		{ok, {CurrentRev, _Gen}} ->
			case lists:member(CurrentRev, Object#object.parents) of
				true  ->
					gen_server:cast(self(), {object_ref_dec, CurrentRev}),
					{ok, NewRev};
				false ->
					{error, conflict}
			end;
		error ->
			{ok, NewRev}
	end,
	S2 = case Reply of
		{ok, _} ->
			vol_monitor:trigger_mod_uuid(S#state.guid, Uuid),
			Gen = S#state.gen,
			S#state{
				gen     = Gen+1,
				uuids   = dict:store(Uuid, {NewRev, Gen}, S#state.uuids),
				objects = dict:store(NewRev, {1, Object}, S#state.objects),
				changed = true
			};

		_ ->
			dispose_object(Object),
			S
	end,
	{S2, Reply}.


%%% Reference counting %%%%%%%%%%%%%%%%

% do_objects_ref_inc(S, [Rev]) -> S2
do_objects_ref_inc(S, Revs) ->
	Objects1 = S#state.objects,
	Objects2 = lists:foldl(
		fun (Rev, Objects) ->
			case dict:find(Rev, Objects) of
				{ok, {RefCount, Value}} ->
					dict:store(Rev, {RefCount+1, Value}, Objects);
				error ->
					dict:store(Rev, {1, stub}, Objects)
			end
		end,
		Objects1,
		Revs),
	S#state{objects=Objects2, changed=true}.

% do_parts_ref_inc(S, [Hash]) -> S2
do_parts_ref_inc(#state{parts=Parts1} = S, Hashes) ->
	Parts2 = lists:foldl(
		fun(PartHash, PartsAcc) ->
			RefCount = case dict:find(PartHash, PartsAcc) of
				{ok, Count} -> Count+1;
				error       -> 1
			end,
			dict:store(PartHash, RefCount, PartsAcc)
		end,
		Parts1,
		Hashes),
	S#state{parts=Parts2, changed=true}.


%%% Reference counting %%%%%%%%%%%%%%%%

% returns [{Uuid, SeqNum}]
do_sync_get_changes(#state{uuids=Uuids, peers=Peers}, PeerGuid) ->
	Anchor = case dict:find(PeerGuid, Peers) of
		{ok, Value} -> Value;
		error       -> 0
	end,
	Changes = dict:fold(
		fun(Uuid, {_Rev, SeqNum}, Acc) ->
			if
				SeqNum > Anchor -> [{Uuid, SeqNum} | Acc];
				true            -> Acc
			end
		end,
		[],
		Uuids),
	lists:sort(
		fun({_Uuid1, Seq1}, {_Uuid2, Seq2}) -> Seq1 =< Seq2 end,
		Changes).


do_sync_set_anchor(#state{peers=Peers} = S, PeerGuid, SeqNum) ->
	NewPeers = dict:store(PeerGuid, SeqNum, Peers),
	S#state{peers=NewPeers, changed=true}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Low level file management...
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load_uuids(Path) ->
	load_dict(Path, "uuids.dict").

load_objects(Path) ->
	load_dict(Path, "objects.dict").

load_parts(Path) ->
	load_dict(Path, "parts.dict").

load_guid(Path) ->
	case file:read_file(Path ++ "/guid") of
		{ok, Data}      -> binary_to_term(Data);
		{error, enoent} -> crypto:rand_bytes(16)
	end.

load_gen(Path) ->
	case file:read_file(Path ++ "/generation") of
		{ok, Data}      -> binary_to_term(Data);
		{error, enoent} -> 0
	end.

load_peers(Path) ->
	load_dict(Path, "peers.dict").

do_checkpoint(S) ->
	case S#state.changed of
		true ->
			file:write_file(S#state.path ++ "/guid", term_to_binary(S#state.guid)),
			file:write_file(S#state.path ++ "/generation", term_to_binary(S#state.gen)),
			save_dict(S#state.path, "uuids.dict",   S#state.uuids),
			save_dict(S#state.path, "objects.dict", S#state.objects),
			save_dict(S#state.path, "parts.dict",   S#state.parts),
			save_dict(S#state.path, "peers.dict",   S#state.peers);

		false ->
			ok
	end.

load_dict(Path, Name) ->
	case file:read_file(Path ++ "/" ++ Name) of
		{ok, Data} ->
			binary_to_term(Data);
		{error, _} ->
			dict:new()
	end.

save_dict(Path, Name, Dict) ->
	case file:write_file(Path ++ "/" ++ Name, term_to_binary(Dict)) of
		ok ->
			ok;
		{error, Reason} ->
			io:format("Failed to save ~w: ~s~n", [Name, Reason]),
			ok
	end.

