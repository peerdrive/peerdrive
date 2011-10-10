
PeerDrive is a document oriented file system with revision control and
database concepts. It is a open source solution to securely access, distribute,
share and sync your data from everywhere. It is built on a purely distributed
data model, will allow optional encryption and should be a perfect solution
when you need your data on different devices in different locations without
relying on a permanent internet connection.

For an introduction into PeerDrive's features see the Wiki on the project home
and the doc/ directory.

Project home: http://github.com/jkloetzke/peerdrive

Mailing list: hotchpotch@freelists.org (the projects former name)
              http://www.freelists.org/list/hotchpotch

Current status
==============

Currently PeerDrive is in the early alpha stage. The basic design is settled
and there is a prototype implementation of the file system in Erlang. Mounting
via FUSE/Dokan, network transparency and automatic synchronization/replication
are working.  Additionally some small GUI applications exist to demonstrate the
full feature set.

Requirements
============

* Python 2.6
    * PyQt >=4.6.x
    * protobuf
    * magic (optional)
* Erlang >= R14A
    * rebar (http://github.com/basho/rebar)
    * fuserl (optional, http://code.google.com/p/fuserl/)
    * erldokan (optional, http://github.com/jkloetzke/erldokan)

Try it out
==========

Currently there is no installation needed. Do the following steps to try it
out. It is assumed that Erlang, Rebar and Python are all in your path.

1. Clone the git repository: `git clone git://github.com/jkloetzke/peerdrive.git`
2. Open two terminals
3. Type in the 1st terminal:
    * `cd server`
    * `./configure`
    * `rebar compile`
    * `./peerdrive.sh` or on Windows: `peerdrive.bat`
    * This will start the PeerDrive server in the foreground. To gracefully shut down the server type `q().`.
    * The stores will be mounted to ./vfs by default
4. In the 2nd terminal type:
    * `cd client`
    * `./boot.py`
    * `./launchbox.py`

The stores place their files under "stores/". Initially all stores start empty.
When executing "boot.py" some necessary documents are created in the system
store. This has to be done only once after the system store was created.

There are some convenient options when starting the server:

    peerdrive.sh clear

This will clear out all stores. Call boot.py before any other client after this
command.

    peerdrive.sh save <name>

This will save the state of all stores to a tar+gzip archive under
"snapshots/<name>.tgz".

    peerdrive.sh restore <name>

Restore the state of all stores from the given checkpoint.

