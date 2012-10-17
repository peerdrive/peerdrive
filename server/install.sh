#!/bin/sh -e

cd `dirname $0`

copy_template() {
	sed -e "s|%homedir%|${PEERDRIVE_HOME}|g" \
	    -e "s|%etcdir%|${PEERDRIVE_ETC}|g" \
	    -e "s|%logdir%|${PEERDRIVE_LOG}|g" \
	    -e "s|%libsdir%|${PEERDRIVE_LIBS}|g" \
	    -e "s|%bindir%|${PEERDRIVE_BIN}|g" \
	< templates/$1 > $2
}

# setup default directories
PREFIX=/usr/local
PEERDRIVE_LOG=/var/log/peerdrive
PEERDRIVE_ETC=/etc/peerdrive
PEERDRIVE_HOME=/var/lib/peerdrive
PEERDRIVE_LIBS=$PREFIX/lib/peerdrive/lib
PEERDRIVE_BIN=$PREFIX/lib/peerdrive/bin

echo "Installing PeerDrive (prefix: $PREFIX)..."

# check is user and group exists
echo -n "  * Checking peerdrive user..."
if ! getent passwd peerdrive > /dev/null; then
	adduser --system --quiet \
		--home $PEERDRIVE_HOME --no-create-home \
		--shell /bin/bash --group --gecos "PeerDrive server" peerdrive
	echo "created"
else
	PEERDRIVE_HOME=`getent passwd peerdrive | awk -F: '{print $6}'`
	echo "ok"
fi

# make sure log, home dir and system store directories exit
echo -n "  * Creating directories..."
mkdir -p $PEERDRIVE_LOG
chown peerdrive:peerdrive $PEERDRIVE_LOG
mkdir -p $PEERDRIVE_HOME/sys
chown -R peerdrive:peerdrive $PEERDRIVE_HOME
echo "ok"

# don't overwrite config file
echo -n "  * Install default configuration files..."
mkdir -p $PEERDRIVE_ETC
if [ ! -e $PEERDRIVE_ETC/peerdrive.config ]; then
	copy_template peerdrive.init.config $PEERDRIVE_ETC/peerdrive.config
	copy_template vm.args $PEERDRIVE_ETC/vm.args
	echo "ok"
else
	echo "no (already exist)"
fi

# (re-)install all erlang libs
echo -n "  * Install runtime files..."
mkdir -p $PEERDRIVE_BIN $PEERDRIVE_LIBS
copy_template peerdrive $PEERDRIVE_BIN/peerdrive
copy_template nodetool $PEERDRIVE_BIN/nodetool
rebar install target=$PEERDRIVE_LIBS force=1 > /dev/null
chown -R root:root $PEERDRIVE_LIBS
chmod a+x $PEERDRIVE_BIN/peerdrive
echo "ok"

# install service
echo -n "  * Install service..."
copy_template peerdrive.init.d /etc/init.d/peerdrive
chmod a+x /etc/init.d/peerdrive
if [ `lsb_release -si` = "Ubuntu" ]; then
	# Ubuntu uses upstart and insserv will mess up the init.d links
	update-rc.d peerdrive defaults 75 15
elif [ -x /usr/lib/lsb/install_initd ]; then
	/usr/lib/lsb/install_initd /etc/init.d/peerdrive
elif [ -x /sbin/chkconfig ]; then
	/sbin/chkconfig --add peerdrive
elif [ -x /sbin/insserv ]; then
	/sbin/insserv peerdrive
else
	echo "don't know how to activate the service. :("
fi
echo "ok"
