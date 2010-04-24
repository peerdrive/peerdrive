#!/bin/bash

set -e

# some setup
if [ "$OS" = "Windows_NT" ]
then
	CUR=`cygpath -wa .`
	ERL="werl -pa $CUR\\ebin -config windows"
else
	export ERL_LIBS=/usr/local/lib/erlang/lib
	ERL="erl -pa $PWD/ebin -config linux"
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

# comile everything
erl -make

# make sure store directories exist
mkdir -p $STORES

# start erlang
$ERL +A 4 -boot start_sasl -s crypto -s hotchpotch

