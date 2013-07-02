#! /bin/sh

### BEGIN INIT INFO
# Provides:          peerdrive
# Required-Start:    $local_fs $remote_fs $network
# Required-Stop:     $local_fs $remote_fs $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: PeerDrive
# Description:       Starts PeerDrive
### END INIT INFO

set -e

PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
DAEMON="%bindir%/peerdrive"
NAME=peerdrive
DESC="peerdrive server"

test -x $DAEMON || exit 0

daemon() {
	su -l -c "$DAEMON --system $1" peerdrive
}

case "$1" in
    start)
        echo -n "Starting $DESC: "
        daemon --daemon
        echo "$NAME."
        ;;
    stop)
        echo -n "Stopping $DESC: "
        daemon --stop
        echo "$NAME."
        ;;
    status)
		$DAEMON --system --check
        ;;
    restart)
        echo -n "Restarting $DESC: "
        daemon --stop
        daemon --daemon
        echo "$NAME."
        ;;
    *)
        N=/etc/init.d/$NAME
        echo "Usage: $N {start|stop|status|restart|reload|force-reload}" >&2
        exit 1
        ;;
esac

exit 0
