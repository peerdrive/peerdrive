{
	application,
	hotchpotch,
	[
		{description, "Hotchpotch server"},
		{vsn, "0.0.1"},
		{modules, [
			broker,
			broker_merger,
			broker_replicator,
			broker_writer,
			change_monitor,
			client_servlet,
			dispatcher,
			file_store,
			file_store_importer,
			file_store_reader,
			file_store_writer,
			gen_servlet,
			hotchpotch,
			hotchpotch_app,
			hysteresis,
			interfaces_sup,
			listener,
			main_sup,
			pool_sup,
			replicator,
			replicator_worker,
			revcache,
			server_sup,
			servlet_sup,
			store,
			store_sup,
			struct,
			sync_worker,
			synchronizer,
			util,
			vol_monitor,
			volman,
			work_monitor,
			worker_sup
		]},
		{applications, [kernel, stdlib, crypto, sasl]},
		{mod, {hotchpotch_app, []}},
		{env, [
			{stores, [
				{
					sys,
					"System mountpoint",
					[system],
					file_store,
					{"priv/stores/sys",  "System store"}
				},
				{
					usr,
					"Default user mount",
					[],
					file_store,
					{"priv/stores/user", "User store"}
				},
				{
					rem1,
					"Rem1 mountpoint",
					[removable],
					file_store,
					{"priv/stores/rem1", "1st removable store"}
				},
				{
					rem2,
					"Rem2 mountpoint",
					[removable],
					file_store,
					{"priv/stores/rem2", "2nd removable store"}
				}
			]},
			{interfaces, [
				{"client", client_servlet, 4567}
			]}
		]}
	]
}.
