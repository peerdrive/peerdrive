#!/bin/bash

set -e

LIBDIR=`erl -noshell -eval 'io:format("~s", [code:lib_dir()]), erlang:halt(0).'`
NAME=`basename $0`

# copy config template
[ -f hotchpotch.config ] || cp priv/hotchpotch.config.unix hotchpotch.config

echo "==> lib_dir:" $LIBDIR

OPTIONS="debug_info"

# check if we have fuserl available
echo -n "==> fuserl: "
if [ -d "$LIBDIR"/fuserl* ]; then
	echo "enabled"
	OPTIONS="$OPTIONS, {d, have_fuserl}"
else
	echo "disabled"
fi

# Compile everything. Unfortuately a simple "erl -make" doesn't work as it
# always exits with 0 even if the build fails. :(
erl -noshell -eval "case make:all([$OPTIONS]) of up_to_date -> erlang:halt(); error -> erlang:halt(1) end"

