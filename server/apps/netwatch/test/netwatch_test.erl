-module(netwatch_test).

-include_lib("eunit/include/eunit.hrl").

start_stop_test() ->
	Watch = netwatch:start(),
	netwatch:stop(Watch).
