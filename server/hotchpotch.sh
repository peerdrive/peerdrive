#!/bin/bash

set -e

# some setup
if [ "$OS" = "Windows_NT" ]
then
	CUR=`cygpath -wa .`
	ERL="werl -pa $CUR\\ebin"
	[ -f ebin/hotchpotch.app ] || cp priv/hotchpotch.app.windows ebin/hotchpotch.app
else
	export ERL_LIBS=/usr/local/lib/erlang/lib
	ERL="erl -pa $PWD/ebin"
	[ -f ebin/hotchpotch.app ] || cp priv/hotchpotch.app.unix ebin/hotchpotch.app
fi

STORES="priv/stores/user/ priv/stores/rem1/ priv/stores/rem2/ priv/stores/sys/"

# some special options
if [ $# -gt 0 ]; then
	case "$1" in
		clear )
			rm -rf $STORES
			;;

		save )
			mkdir -p priv/snapshots
			tar zcf "priv/snapshots/$2.tgz" $STORES
			exit
			;;

		restore )
			rm -rf $STORES
			tar zxf "priv/snapshots/$2.tgz"
			;;

		* )
			echo "hp.sh [clear | save <name> | restore <name>]"
			exit
			;;
	esac
fi

# Compile everything. Unfortuately a simple "erl -make" doesn't work as it
# always exits with 0 even if the build fails. :(
erl -noshell -eval "case make:all() of up_to_date -> erlang:halt(); error -> erlang:halt(1) end"

# make sure store directories exist
mkdir -p $STORES
mkdir -p /tmp/hotchpotch

# start erlang
$ERL +A 4 +W w -boot start_sasl -s crypto -s hotchpotch -sname hotchpotch

