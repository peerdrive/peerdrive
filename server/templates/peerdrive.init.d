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

PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
DAEMON="%bindir%/peerdrive"
NAME=peerdrive

test -x $DAEMON || exit 0

. /lib/lsb/init-functions

PEERDRIVE_SYSTEM_START=0
test -f /etc/default/peerdrive && . /etc/default/peerdrive
if [ "$PEERDRIVE_SYSTEM_START" != "1" ]; then
	log_warning_msg "PeerDrive configured for per-user sessions"
	exit 0
fi

daemon() {
	su -l -c "$DAEMON --system $1" peerdrive
}

case "$1" in
    start)
		log_daemon_msg "Starting system PeerDrive Daemon"
		if [ ! -d "%rundir%" ]; then
			mkdir -m 755 -p "%rundir%"
			chown peerdrive:peerdrive "%rundir%"
		fi
        daemon --daemon
		log_end_msg $?
        ;;
    stop)
		log_daemon_msg "Stopping system PeerDrive Daemon"
        daemon --stop
		log_end_msg $?
        ;;
    status)
		$DAEMON --system --check
        ;;
    restart)
		log_daemon_msg "Restarting system PeerDrive Daemon"
        daemon --stop && daemon --daemon
		log_end_msg $?
        ;;
    *)
        N=/etc/init.d/$NAME
        echo "Usage: $N {start|stop|status|restart|reload|force-reload}" >&2
        exit 1
        ;;
esac

exit 0
