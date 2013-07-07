#!/bin/bash -ex

prefix=/tmp/peerdrive-1

while getopts ":p:" opt; do
	case $opt in
		p)
			prefix=$OPTARG
			;;
		\?)
			echo "Invalid option: -$OPTARG" >&2
			exit 1
			;;
		:)
			echo "Option -$OPTARG requires argument" >&2
			exit 1
			;;
	esac
done

cd `dirname $0`

copy_template() {
	sed -e "s|%homedir%|${PEERDRIVE_HOME}|g" \
	    -e "s|%etcdir%|${PEERDRIVE_ETC}|g" \
	    -e "s|%logdir%|${PEERDRIVE_LOG}|g" \
	    -e "s|%libsdir%|${PEERDRIVE_LIBS}|g" \
	    -e "s|%bindir%|${PEERDRIVE_BIN}|g" \
	    -e "s|%rundir%|${PEERDRIVE_RUN}|g" \
	< templates/$1 > $2
}

# setup default directories
PREFIX=$prefix
PEERDRIVE_LOG=$PREFIX/var/log/peerdrive
PEERDRIVE_ETC=$PREFIX/etc/peerdrive
PEERDRIVE_HOME=$PREFIX/var/lib/peerdrive
PEERDRIVE_LIBS=$PREFIX/lib/peerdrive/lib
PEERDRIVE_BIN=$PREFIX/lib/peerdrive/bin
PEERDRIVE_RUN=$PREFIX/var/run/peerdrive

echo "Installing PeerDrive (prefix: $PREFIX)..."

# make sure log, home dir and system store directories exit
echo -n "  * Creating directories..."
mkdir -p $PEERDRIVE_LOG
mkdir -p $PEERDRIVE_HOME/sys
echo "ok"

# don't overwrite config file
echo -n "  * Install default configuration files..."
mkdir -p $PEERDRIVE_ETC
if [ ! -e $PEERDRIVE_ETC/peerdrive.config ]; then
	copy_template peerdrive.init.config $PEERDRIVE_ETC/peerdrive.config
	echo "ok"
else
	echo "no (already exist)"
fi

# (re-)install all erlang libs
echo -n "  * Install runtime files..."
mkdir -p $PEERDRIVE_BIN $PEERDRIVE_LIBS
copy_template peerdrive $PEERDRIVE_BIN/peerdrive
rebar install target=$PEERDRIVE_LIBS force=1 > /dev/null
chmod a+x $PEERDRIVE_BIN/peerdrive
mkdir -p $PEERDRIVE_RUN
chown peerdrive:peerdrive $PEERDRIVE_RUN
echo "ok"

# install service
echo -n "  * Install service..."
echo "todo"
