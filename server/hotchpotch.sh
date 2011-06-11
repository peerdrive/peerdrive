#!/bin/bash

set -e

ERL="erl -pa $PWD/applications/hotchpotch/ebin"
STORES="stores/user/ stores/sys/"

# some special options
if [ $# -gt 0 ]; then
	case "$1" in
		clear )
			rm -rf $STORES
			;;

		save )
			mkdir -p snapshots
			tar zcf "snapshots/$2.tgz" $STORES
			exit
			;;

		restore )
			rm -rf $STORES
			tar zxf "snapshots/$2.tgz"
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

# copy standard configuration if there is none
[ -f hotchpotch.config ] || cp templates/hotchpotch.config.unix hotchpotch.config

# start erlang
$ERL +A 4 +W w -config hotchpotch -boot start_sasl -s crypto -s hotchpotch \
	-sname hotchpotch

