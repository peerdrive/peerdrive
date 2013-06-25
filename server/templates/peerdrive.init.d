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
DAEMON=%bindir%/peerdrive
NAME=peerdrive
DESC="peerdrive server"

test -x $DAEMON || exit 0

daemon() {
	su -l -c "$DAEMON $1" peerdrive
}

case "$1" in
    start)
        echo -n "Starting $DESC: "
        daemon start
        echo "$NAME."
        ;;
    stop)
        echo -n "Stopping $DESC: "
        daemon stop
        echo "$NAME."
        ;;
    status)
        if `daemon ping 2>&1 >/dev/null`; then
            echo "$NAME is running."
        else
            echo "$NAME is not running."
        fi
        ;;
    reload|force-reload)
        echo "Reloading $DESC configuration files."
        ;;
    restart)
        echo -n "Restarting $DESC: "
        daemon restart
        echo "$NAME."
        ;;
    *)
        N=/etc/init.d/$NAME
        echo "Usage: $N {start|stop|status|restart|reload|force-reload}" >&2
        exit 1
        ;;
esac

exit 0
