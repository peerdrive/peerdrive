#!/bin/bash

set -e

cd `dirname "$0"`
ERL="erl -pa apps/peerdrive/ebin"
STORES="stores/user/ stores/sys/"

# add fetched dependencies
for i in deps/*; do
	ERL="$ERL -pa $i/ebin"
done

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
mkdir -p vfs

# copy standard configuration if there is none
[ -f peerdrive.config ] || cp templates/peerdrive.config peerdrive.config

# start erlang
$ERL +A 4 +W w -config peerdrive -boot start_sasl -s crypto -s ssl -s peerdrive \
	-sname peerdrive

