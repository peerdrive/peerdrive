#!/bin/bash

set -e

cd `dirname "$0"`

export ERL_LIBS=apps/:deps/
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
mkdir -p vfs

# copy standard configuration if there is none
[ -f peerdrive.config ] || cp templates/peerdrive.config peerdrive.config

# start erlang
erl +A 4 +W w -config peerdrive -boot start_sasl -s crypto -s ssl -s peerdrive \
	-sname peerdrive_test

