#!/bin/bash

set -e

ERL="erl -pa $PWD/ebin"
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
			echo `basename $0` "[clear | save <name> | restore <name>]"
			exit
			;;
	esac
fi

# make sure store directories exist
mkdir -p $STORES
mkdir -p /tmp/hotchpotch

# start erlang
$ERL +A 4 +W w -config hotchpotch -boot start_sasl -s crypto -s hotchpotch \
	-sname hotchpotch

