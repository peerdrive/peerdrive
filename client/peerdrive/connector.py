# vim: set fileencoding=utf-8 :
#
# PeerDrive
# Copyright (C) 2011  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import

from PyQt4 import QtCore, QtNetwork
from datetime import datetime
import sys, struct, atexit, weakref, traceback, os, os.path, json, time
from . import peerdrive_client_pb2 as pb

if sys.platform == "win32":
	import _winreg

_errorCodes = {
	0 : 'ECONFLICT',
	1 : 'E2BIG',
	2 : 'EACCES',
	3 : 'EADDRINUSE',
	4 : 'EADDRNOTAVAIL',
	5 : 'EADV',
	6 : 'EAFNOSUPPORT',
	7 : 'EAGAIN',
	8 : 'EALIGN',
	9 : 'EALREADY',
	10 : 'EBADE',
	11 : 'EBADF',
	12 : 'EBADFD',
	13 : 'EBADMSG',
	14 : 'EBADR',
	15 : 'EBADRPC',
	16 : 'EBADRQC',
	17 : 'EBADSLT',
	18 : 'EBFONT',
	19 : 'EBUSY',
	20 : 'ECHILD',
	21 : 'ECHRNG',
	22 : 'ECOMM',
	23 : 'ECONNABORTED',
	24 : 'ECONNREFUSED',
	25 : 'ECONNRESET',
	26 : 'EDEADLK',
	27 : 'EDEADLOCK',
	28 : 'EDESTADDRREQ',
	29 : 'EDIRTY',
	30 : 'EDOM',
	31 : 'EDOTDOT',
	32 : 'EDQUOT',
	33 : 'EDUPPKG',
	34 : 'EEXIST',
	35 : 'EFAULT',
	36 : 'EFBIG',
	37 : 'EHOSTDOWN',
	38 : 'EHOSTUNREACH',
	39 : 'EIDRM',
	40 : 'EINIT',
	41 : 'EINPROGRESS',
	42 : 'EINTR',
	43 : 'EINVAL',
	44 : 'EIO',
	45 : 'EISCONN',
	46 : 'EISDIR',
	47 : 'EISNAM',
	48 : 'ELBIN',
	49 : 'EL2HLT',
	50 : 'EL2NSYNC',
	51 : 'EL3HLT',
	52 : 'EL3RST',
	53 : 'ELIBACC',
	54 : 'ELIBBAD',
	55 : 'ELIBEXEC',
	56 : 'ELIBMAX',
	57 : 'ELIBSCN',
	58 : 'ELNRNG',
	59 : 'ELOOP',
	60 : 'EMFILE',
	61 : 'EMLINK',
	62 : 'EMSGSIZE',
	63 : 'EMULTIHOP',
	64 : 'ENAMETOOLONG',
	65 : 'ENAVAIL',
	66 : 'ENET',
	67 : 'ENETDOWN',
	68 : 'ENETRESET',
	69 : 'ENETUNREACH',
	70 : 'ENFILE',
	71 : 'ENOANO',
	72 : 'ENOBUFS',
	73 : 'ENOCSI',
	74 : 'ENODATA',
	75 : 'ENODEV',
	76 : 'ENOENT',
	77 : 'ENOEXEC',
	78 : 'ENOLCK',
	79 : 'ENOLINK',
	80 : 'ENOMEM',
	81 : 'ENOMSG',
	82 : 'ENONET',
	83 : 'ENOPKG',
	84 : 'ENOPROTOOPT',
	85 : 'ENOSPC',
	86 : 'ENOSR',
	87 : 'ENOSYM',
	88 : 'ENOSYS',
	89 : 'ENOTBLK',
	90 : 'ENOTCONN',
	91 : 'ENOTDIR',
	92 : 'ENOTEMPTY',
	93 : 'ENOTNAM',
	94 : 'ENOTSOCK',
	95 : 'ENOTSUP',
	96 : 'ENOTTY',
	97 : 'ENOTUNIQ',
	98 : 'ENXIO',
	99 : 'EOPNOTSUPP',
	100 : 'EPERM',
	101 : 'EPFNOSUPPORT',
	102 : 'EPIPE',
	103 : 'EPROCLIM',
	104 : 'EPROCUNAVAIL',
	105 : 'EPROGMISMATCH',
	106 : 'EPROGUNAVAIL',
	107 : 'EPROTO',
	108 : 'EPROTONOSUPPORT',
	109 : 'EPROTOTYPE',
	110 : 'ERANGE',
	111 : 'EREFUSED',
	112 : 'EREMCHG',
	113 : 'EREMDEV',
	114 : 'EREMOTE',
	115 : 'EREMOTEIO',
	116 : 'EREMOTERELEASE',
	117 : 'EROFS',
	118 : 'ERPCMISMATCH',
	119 : 'ERREMOTE',
	120 : 'ESHUTDOWN',
	121 : 'ESOCKTNOSUPPORT',
	122 : 'ESPIPE',
	123 : 'ESRCH',
	124 : 'ESRMNT',
	125 : 'ESTALE',
	126 : 'ESUCCESS',
	127 : 'ETIME',
	128 : 'ETIMEDOUT',
	129 : 'ETOOMANYREFS',
	130 : 'ETXTBSY',
	131 : 'EUCLEAN',
	132 : 'EUNATCH',
	133 : 'EUSERS',
	134 : 'EVERSION',
	135 : 'EWOULDBLOCK',
	136 : 'EXDEV',
	137 : 'EXFULL',
	138 : 'NXDOMAIN'
}

_requestNames = {
	0x0000 : "ERROR_MSG",
	0x0001 : "INIT_MSG",
	0x0002 : "ENUM_MSG",
	0x0003 : "LOOKUP_DOC_MSG",
	0x0004 : "LOOKUP_REV_MSG",
	0x0005 : "STAT_MSG",
	0x0006 : "PEEK_MSG",
	0x0007 : "CREATE_MSG",
	0x0008 : "FORK_MSG",
	0x0009 : "UPDATE_MSG",
	0x000a : "RESUME_MSG",
	0x000b : "READ_MSG",
	0x000c : "TRUNC_MSG",
	0x000d : "WRITE_BUFFER_MSG",
	0x000e : "WRITE_COMMIT_MSG",
	0x000f : "FSTAT_MSG",
	0x0010 : "SET_FLAGS_MSG",
	0x0011 : "SET_TYPE_MSG",
	0x0012 : "SET_MTIME_MSG",
	0x0013 : "<unused>",
	0x0014 : "MERGE_MSG",
	0x0015 : "REBASE_MSG",
	0x0016 : "COMMIT_MSG",
	0x0017 : "SUSPEND_MSG",
	0x0018 : "CLOSE_MSG",
	0x0019 : "WATCH_ADD_MSG",
	0x001a : "WATCH_REM_MSG",
	0x001b : "WATCH_PROGRESS_MSG",
	0x001c : "FORGET_MSG",
	0x001d : "DELETE_DOC_MSG",
	0x001e : "DELETE_REV_MSG",
	0x001f : "FORWARD_DOC_MSG",
	0x0020 : "REPLICATE_DOC_MSG",
	0x0021 : "REPLICATE_REV_MSG",
	0x0022 : "MOUNT_MSG",
	0x0023 : "UNMOUNT_MSG",
	0x0024 : "GET_PATH_MSG",
	0x0025 : "WATCH_MSG",
	0x0026 : "PROGRESS_START_MSG",
	0x0027 : "PROGRESS_MSG",
	0x0028 : "PROGRESS_END_MSG",
	0x0029 : "PROGRESS_QUERY_MSG",
	0x002a : "WALK_PATH_MSG",
	0x002b : "GET_DATA_MSG",
	0x002c : "SET_DATA_MSG",
	0x002d : "GET_LINKS_MSG"
}


def _checkUuid(uuid):
	if not (uuid.__class__ == str):
		raise IOError('Invalid UUID: '+str(uuid.__class__)+' '+str(uuid))
	return uuid


def _raiseError(error):
	if error in _errorCodes:
		raise IOError(_errorCodes[error])
	else:
		raise IOError('Unknown error')


class _Connector(QtCore.QObject):
	ERROR_MSG           = 0x0000
	INIT_MSG            = 0x0001
	ENUM_MSG            = 0x0002
	LOOKUP_DOC_MSG      = 0x0003
	LOOKUP_REV_MSG      = 0x0004
	STAT_MSG            = 0x0005
	PEEK_MSG            = 0x0006
	CREATE_MSG          = 0x0007
	FORK_MSG            = 0x0008
	UPDATE_MSG          = 0x0009
	RESUME_MSG          = 0x000a
	READ_MSG            = 0x000b
	TRUNC_MSG           = 0x000c
	WRITE_BUFFER_MSG    = 0x000d
	WRITE_COMMIT_MSG    = 0x000e
	FSTAT_MSG           = 0x000f
	SET_FLAGS_MSG       = 0x0010
	SET_TYPE_MSG        = 0x0011
	SET_MTIME_MSG       = 0x0012
	#_MSG     = 0x0013
	MERGE_MSG           = 0x0014
	REBASE_MSG          = 0x0015
	COMMIT_MSG          = 0x0016
	SUSPEND_MSG         = 0x0017
	CLOSE_MSG           = 0x0018
	WATCH_ADD_MSG       = 0x0019
	WATCH_REM_MSG       = 0x001a
	WATCH_PROGRESS_MSG  = 0x001b
	FORGET_MSG          = 0x001c
	DELETE_DOC_MSG      = 0x001d
	DELETE_REV_MSG      = 0x001e
	FORWARD_DOC_MSG     = 0x001f
	REPLICATE_DOC_MSG   = 0x0020
	REPLICATE_REV_MSG   = 0x0021
	MOUNT_MSG           = 0x0022
	UNMOUNT_MSG         = 0x0023
	GET_PATH_MSG        = 0x0024
	WATCH_MSG           = 0x0025
	PROGRESS_START_MSG  = 0x0026
	PROGRESS_MSG        = 0x0027
	PROGRESS_END_MSG    = 0x0028
	PROGRESS_QUERY_MSG  = 0x0029
	WALK_PATH_MSG       = 0x002a
	GET_DATA_MSG        = 0x002b
	SET_DATA_MSG        = 0x002c
	GET_LINKS_MSG       = 0x002d

	FLAG_REQ = 0
	FLAG_CNF = 1
	FLAG_IND = 2
	FLAG_RSP = 3

	PROGRESS_RUNNING = pb.ProgressInd.running
	PROGRESS_PAUSED = pb.ProgressInd.paused
	PROGRESS_ERROR = pb.ProgressInd.error
	PROGRESS_SYNC = pb.ProgressStartInd.sync
	PROGRESS_REP_DOC = pb.ProgressStartInd.rep_doc
	PROGRESS_REP_REV = pb.ProgressStartInd.rep_rev

	watchReady = QtCore.pyqtSignal()

	def __init__(self, address=None):
		super(_Connector, self).__init__()

		if not address:
			# look into environment
			address = os.getenv('PEERDRIVE')
		if not address:
			# look for per-user daemon
			try:
				if sys.platform == "win32":
					with _winreg.OpenKey(_winreg.HKEY_CURRENT_USER, "Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\Shell Folders") as key:
						path = _winreg.QueryValueEx(key, "Local AppData")[0]
					path = os.path.join(path, "PeerDrive", "server.info")
				else:
					path = "/tmp/peerdrive-" + os.getenv('USER') + "/server.info"
				with open(path, 'r') as f:
					address = f.readline()
			except IOError:
				pass
		if not address:
			# look for system daemon
			try:
				if sys.platform == "win32":
					with _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE, "SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Explorer\\Shell Folders") as key:
						path = _winreg.QueryValueEx(key, "Common AppData")[0]
					path = os.path.join(path, "PeerDrive", "server.info")
				else:
					path = "/var/run/peerdrive/server.info"
				with open(path, 'r') as f:
					address = f.readline()
			except IOError:
				pass

		if not address:
			raise IOError("Cannot find server!")
		if not address.startswith("tcp://"):
			raise IOError("Unknown address scheme: " + address)

		(address, cookie) = address[6:].split("/")
		(host, port) = address.split(':')
		port = int(port)
		cookie = cookie.strip().decode('hex')

		self.socket = QtNetwork.QTcpSocket()
		self.socket.readyRead.connect(self.__readReady)
		self.socket.connectToHost(host, port)
		if not self.socket.waitForConnected(1000):
			raise IOError("Could not connect to server!")
		self.socket.setSocketOption(QtNetwork.QAbstractSocket.LowDelayOption, 1)
		self.next = 0
		self.buf = ''
		self.confirmations = {}
		self.indications = []
		self.watchHandlers = {}
		self.progressHandlers = []
		self.recursion = 0

		self.watchReady.connect(self.__dispatchIndications, QtCore.Qt.QueuedConnection)

		try:
			req = pb.InitReq()
			req.major = 2
			req.minor = 0
			req.cookie = cookie
			reply = self._rpc(_Connector.INIT_MSG, req.SerializeToString())
			cnf = pb.InitCnf.FromString(reply)
			if cnf.major != 2 or cnf.minor != 0:
				raise IOError("Unsupported protocol version!")
			self.maxPacketSize = cnf.max_packet_size
		except:
			self.socket.disconnectFromHost()
			raise

	def enum(self):
		reply = pb.EnumCnf.FromString(self._rpc(_Connector.ENUM_MSG))
		return Enum(reply)

	def lookupDoc(self, doc, stores=[]):
		req = pb.LookupDocReq()
		req.doc = _checkUuid(doc)
		for store in stores:
			req.stores.append(_checkUuid(store))
		reply = self._rpc(_Connector.LOOKUP_DOC_MSG, req.SerializeToString())
		return Lookup(pb.LookupDocCnf.FromString(reply))

	def lookupRev(self, rev, stores=[]):
		req = pb.LookupRevReq()
		req.rev = _checkUuid(rev)
		for store in stores:
			req.stores.append(_checkUuid(store))
		reply = self._rpc(_Connector.LOOKUP_REV_MSG, req.SerializeToString())
		return pb.LookupRevCnf.FromString(reply).stores

	def stat(self, rev, stores=[]):
		req = pb.StatReq()
		req.rev = _checkUuid(rev)
		for store in stores:
			req.stores.append(_checkUuid(store))
		reply = self._rpc(_Connector.STAT_MSG, req.SerializeToString())
		return Stat(pb.StatCnf.FromString(reply))

	def getLinks(self, rev, stores=[]):
		req = pb.GetLinksReq()
		req.rev = _checkUuid(rev)
		for store in stores:
			req.stores.append(_checkUuid(store))
		reply = self._rpc(_Connector.GET_LINKS_MSG, req.SerializeToString())
		cnf = pb.GetLinksCnf.FromString(reply)
		return (cnf.doc_links, cnf.rev_links)

	def peek(self, store, rev):
		req = pb.PeekReq()
		req.store = _checkUuid(store)
		req.rev = _checkUuid(rev)
		reply = self._rpc(_Connector.PEEK_MSG, req.SerializeToString())
		cnf = pb.PeekCnf.FromString(reply)
		return Handle(self, store, cnf.handle, None, rev)

	def create(self, store, typ, creator):
		req = pb.CreateReq()
		req.store = _checkUuid(store)
		req.type_code = typ
		req.creator_code = creator
		reply = self._rpc(_Connector.CREATE_MSG, req.SerializeToString())
		cnf = pb.CreateCnf.FromString(reply)
		return Handle(self, store, cnf.handle, cnf.doc, None)

	def fork(self, store, rev, creator):
		req = pb.ForkReq()
		req.store = _checkUuid(store)
		req.rev = _checkUuid(rev)
		req.creator_code = creator
		reply = self._rpc(_Connector.FORK_MSG, req.SerializeToString())
		cnf = pb.ForkCnf.FromString(reply)
		return Handle(self, store, cnf.handle, cnf.doc, rev)

	def update(self, store, doc, rev, creator=None):
		req = pb.UpdateReq()
		req.store = _checkUuid(store)
		req.doc = _checkUuid(doc)
		req.rev = _checkUuid(rev)
		if creator is not None:
			req.creator_code = creator
		reply = self._rpc(_Connector.UPDATE_MSG, req.SerializeToString())
		cnf = pb.UpdateCnf.FromString(reply)
		return Handle(self, store, cnf.handle, doc, rev)

	def resume(self, store, doc, rev, creator=None):
		req = pb.ResumeReq()
		req.store = _checkUuid(store)
		req.doc = _checkUuid(doc)
		req.rev = _checkUuid(rev)
		if creator is not None:
			req.creator_code = creator
		reply = self._rpc(_Connector.RESUME_MSG, req.SerializeToString())
		cnf = pb.ResumeCnf.FromString(reply)
		return Handle(self, store, cnf.handle, doc, rev)

	def watch(self, w):
		if w._incWatchRef() == 1:
			(typ, h) = ref = w._getRef()
			if ref not in self.watchHandlers:
				req = pb.WatchAddReq()
				req.type = typ
				req.element = _checkUuid(h)
				self._rpc(_Connector.WATCH_ADD_MSG, req.SerializeToString())
				self.watchHandlers[ref] = []
			tb = None #traceback.extract_stack()
			self.watchHandlers[ref].append(weakref.ref(w,
				lambda r, ref=ref, tb=tb: self.__delWatch(r, ref, tb)))

	def __delWatch(self, watchObjRef, watchSpec, tb):
		if tb:
			print >>sys.stderr, "Warning: watch object has been deleted while being armed!"
			for line in traceback.format_list(tb)[:-1]:
				print >>sys.stderr, line,
		self.watchHandlers[watchSpec].remove(watchObjRef)
		if self.watchHandlers[watchSpec] == []:
			(typ, h) = watchSpec
			req = pb.WatchRemReq()
			req.type = typ
			req.element = h
			self._rpc(_Connector.WATCH_REM_MSG, req.SerializeToString())
			del self.watchHandlers[watchSpec]

	def unwatch(self, w):
		if w._decWatchRef() == 0:
			(typ, h) = ref = w._getRef()
			oldHandlers = self.watchHandlers[ref]
			newHandlers = [x for x in oldHandlers if x() != w]
			if newHandlers == []:
				req = pb.WatchRemReq()
				req.type = typ
				req.element = h
				self._rpc(_Connector.WATCH_REM_MSG, req.SerializeToString())
				del self.watchHandlers[ref]
			else:
				self.watchHandlers[ref] = newHandlers

	def forget(self, store, doc, rev):
		req = pb.ForgetReq()
		req.store = _checkUuid(store)
		req.doc = _checkUuid(doc)
		req.rev = _checkUuid(rev)
		self._rpc(_Connector.FORGET_MSG, req.SerializeToString())

	def deleteDoc(self, store, doc, rev):
		req = pb.DeleteDocReq()
		req.store = _checkUuid(store)
		req.doc = _checkUuid(doc)
		req.rev = _checkUuid(rev)
		self._rpc(_Connector.DELETE_DOC_MSG, req.SerializeToString())

	def deleteRev(self, store, rev):
		req = pb.DeleteRevReq()
		req.store = _checkUuid(store)
		req.rev = _checkUuid(rev)
		self._rpc(_Connector.DELETE_REV_MSG, req.SerializeToString())

	def forwardDoc(self, store, doc, fromRev, toRev, srcStore, depth=None, verbose=False):
		req = pb.ForwardDocReq()
		req.store = _checkUuid(store)
		req.doc = _checkUuid(doc)
		req.from_rev = _checkUuid(fromRev)
		req.to_rev = _checkUuid(toRev)
		req.src_store = _checkUuid(srcStore)
		if depth is not None:
			req.depth = depth
		if verbose: req.verbose = verbose
		self._rpc(_Connector.FORWARD_DOC_MSG, req.SerializeToString())

	def replicateDoc(self, srcStore, doc, dstStore, depth=None, verbose=False, async=None):
		req = pb.ReplicateDocReq()
		req.src_store = _checkUuid(srcStore)
		req.doc = _checkUuid(doc)
		req.dst_store = _checkUuid(dstStore)
		if depth is not None:
			req.depth = depth
		if verbose: req.verbose = verbose
		return self._rpc(_Connector.REPLICATE_DOC_MSG, req.SerializeToString(),
			async, self.__replicateDocDone)

	def __replicateDocDone(self, reply):
		cnf = pb.ReplicateDocCnf.FromString(reply)
		return ReplicateHandle(self, cnf.handle)

	def replicateRev(self, srcStore, rev, dstStore, depth=None, verbose=False, async=None):
		req = pb.ReplicateRevReq()
		req.src_store = _checkUuid(srcStore)
		req.rev = _checkUuid(rev)
		req.dst_store = _checkUuid(dstStore)
		if depth is not None:
			req.depth = depth
		if verbose: req.verbose = verbose
		return self._rpc(_Connector.REPLICATE_REV_MSG, req.SerializeToString(),
			async, self.__replicateRevDone)

	def __replicateRevDone(self, reply):
		cnf = pb.ReplicateRevCnf.FromString(reply)
		return ReplicateHandle(self, cnf.handle)

	def mount(self, src, label, type, options=None, credentials=None):
		req = pb.MountReq()
		req.src = src
		req.label = label
		req.type = type
		if options is not None: req.options = options
		if credentials is not None: req.credentials = credentials
		reply = self._rpc(_Connector.MOUNT_MSG, req.SerializeToString())
		cnf = pb.MountCnf.FromString(reply)
		return cnf.sid

	def unmount(self, sid):
		req = pb.UnmountReq()
		req.sid = sid
		self._rpc(_Connector.UNMOUNT_MSG, req.SerializeToString())

	def getDocPath(self, store, doc):
		req = pb.GetPathReq()
		req.store = _checkUuid(store)
		req.object = _checkUuid(doc)
		req.is_rev = False
		reply = self._rpc(_Connector.GET_PATH_MSG, req.SerializeToString())
		cnf = pb.GetPathCnf.FromString(reply)
		return cnf.path

	def getRevPath(self, store, rev):
		req = pb.GetPathReq()
		req.store = _checkUuid(store)
		req.object = _checkUuid(rev)
		req.is_rev = True
		reply = self._rpc(_Connector.GET_PATH_MSG, req.SerializeToString())
		cnf = pb.GetPathCnf.FromString(reply)
		return cnf.path

	def walkPath(self, path):
		req = pb.WalkPathReq()
		req.path = path
		reply = self._rpc(_Connector.WALK_PATH_MSG, req.SerializeToString())
		cnf = pb.WalkPathCnf.FromString(reply)
		return [ (item.store, item.doc) for item in cnf.items ]

	def flush(self):
		while self.socket.flush():
			self.socket.waitForBytesWritten(10000)

	def process(self, timeout=1):
		if self.socket.waitForReadyRead(timeout):
			self.__readReady()
		self.__dispatchIndications()

	def regProgressHandler(self, start=None, progress=None, stop=None):
		if start or progress:
			cnf = pb.ProgressQueryCnf.FromString(self._rpc(_Connector.PROGRESS_QUERY_MSG, ''))
		if start:
			self.__regProgressHandler(_Connector.PROGRESS_START_MSG, start)
			for item in cnf.items:
				self.__dispatchProgressStart(item.item, [start])
		if progress:
			self.__regProgressHandler(_Connector.PROGRESS_MSG, progress)
			for item in cnf.items:
				self.__dispatchProgress(item.state, [progress])
		if stop:
			self.__regProgressHandler(_Connector.PROGRESS_END_MSG, stop)

	def __regProgressHandler(self, event, handler):
		if len(self.progressHandlers) == 0:
			req = pb.WatchProgressReq()
			req.enable = True
			self._rpc(_Connector.WATCH_PROGRESS_MSG, req.SerializeToString())
		self.progressHandlers.append((event, handler))

	def unregProgressHandler(self, start=None, progress=None, stop=None):
		if start:
			self.__unregProgressHandler(_Connector.PROGRESS_START_MSG, start)
		if progress:
			self.__unregProgressHandler(_Connector.PROGRESS_MSG, progress)
		if stop:
			self.__unregProgressHandler(_Connector.PROGRESS_END_MSG, stop)

	def __unregProgressHandler(self, event, handler):
		self.progressHandlers.remove((event, handler))
		if len(self.progressHandlers) == 0:
			req = pb.WatchProgressReq()
			req.enable = False
			self._rpc(_Connector.WATCH_PROGRESS_MSG, req.SerializeToString())

	def progressPause(self, tag):
		req = pb.ProgressEndReq()
		req.tag = tag
		req.pause = True
		self._rpc(_Connector.PROGRESS_END_MSG, req.SerializeToString())

	def progressStop(self, tag):
		req = pb.ProgressEndReq()
		req.tag = tag
		req.pause = False
		self._rpc(_Connector.PROGRESS_END_MSG, req.SerializeToString())

	def progressResume(self, tag, skip=None):
		req = pb.ProgressStartReq()
		req.tag = tag
		if skip is not None:
			req.skip = skip
		self._rpc(_Connector.PROGRESS_START_MSG, req.SerializeToString())

	# protected functions

	class _PollCompletion(object):
		__slots__ = ['pending', 'cnf', 'reply']
		def __init__(self):
			self.pending = True
			self.cnf = None
			self.reply = None

		def setResult(self, cnf, reply):
			self.cnf = cnf
			self.reply = reply
			self.pending = False

	class _AsyncCompletion(object):
		__slots__ = ['__callback', '__msg', '__done']
		def __init__(self, msg, callback, done):
			self.__msg = msg
			self.__callback = callback
			self.__done = done

		def setResult(self, cnf, reply):
			if cnf == self.__msg:
				self.__callback(self.__done(reply))
			elif cnf == _Connector.ERROR_MSG:
				error_cnf = pb.ErrorCnf.FromString(reply)
				self.__callback(IOError(_errorCodes[error_cnf.error]))

	def _rpc(self, msg, request = '', async=None, done=lambda x: x):
		ref = self.__make_ref()
		req_msg = (msg << 4) | _Connector.FLAG_REQ
		if async:
			completion = _Connector._AsyncCompletion(msg, async, done)
		else:
			completion = _Connector._PollCompletion()
		self.confirmations[ref] = completion
		self.__send(struct.pack('>LH', ref, req_msg) + request)
		if not async:
			start = time.time()
			self.__poll(completion)
			end = time.time()
			#print "RPC sync:", _requestNames[msg], int((end-start)*1000000)
			if completion.cnf == msg:
				return done(completion.reply)
			elif completion.cnf == _Connector.ERROR_MSG:
				error_cnf = pb.ErrorCnf.FromString(completion.reply)
				_raiseError(error_cnf.error)
			else:
				raise IOError("Invalid server reply!")

	# private functions

	def __send(self, packet):
		raw = struct.pack('>H', len(packet)) + packet
		if self.socket.write(raw) == -1:
			raise IOError("Could not send request to server: "
				+ str(self.socket.errorString()))

	def __readReady(self):
		# unpack incoming packets
		indications = False
		self.buf = self.buf + str(self.socket.readAll())
		while len(self.buf) > 2:
			expect = struct.unpack_from('>H', self.buf, 0)[0] + 2
			if expect <= len(self.buf):
				packet = self.buf[2:expect]
				self.buf = self.buf[expect:]

				# immediately remove indications
				(ref, msg) = struct.unpack_from('>LH', packet, 0)
				typ = msg & 3
				msg = msg >> 4
				if typ == _Connector.FLAG_IND:
					indications = True
					self.indications.append((msg, packet[6:]))
				elif typ == _Connector.FLAG_CNF:
					self.confirmations[ref].setResult(msg, packet[6:])
					del self.confirmations[ref]
			else:
				break

		if indications:
			self.__dispatchIndications()

	def __dispatchIndications(self):
		# dispatch received indications if not in recursion
		if self.recursion == 0:
			dispatched = True
			while dispatched:
				dispatched = False
				indications = self.indications[:]
				self.indications = []
				for (msg, packet) in indications:
					dispatched = True
					if msg == _Connector.WATCH_MSG:
						ind = pb.WatchInd.FromString(packet)
						# make explicit copy as watches may get modified by callouts!
						matches = self.watchHandlers.get((ind.type, ind.element), [])[:]
						for i in matches:
							i = i() # dereference weakref
							if i is not None:
								i.triggered(ind.event, ind.store)
					elif msg == _Connector.PROGRESS_START_MSG:
						ind = pb.ProgressStartInd.FromString(packet)
						handlers = [h for (e,h) in self.progressHandlers if e == msg]
						self.__dispatchProgressStart(ind, handlers)
					elif msg == _Connector.PROGRESS_MSG:
						ind = pb.ProgressInd.FromString(packet)
						handlers = [h for (e,h) in self.progressHandlers if e == msg]
						self.__dispatchProgress(ind, handlers)
					elif msg == _Connector.PROGRESS_END_MSG:
						ind = pb.ProgressEndInd.FromString(packet)
						handlers = self.progressHandlers[:]
						for (event, handler) in handlers:
							if event == msg:
								handler(ind.tag)
		else:
			self.watchReady.emit()

	def __dispatchProgressStart(self, ind, handlers):
		for handler in handlers:
			if ind.HasField('item'):
				handler(ind.tag, ind.type, ind.source, ind.dest, ind.item)
			else:
				handler(ind.tag, ind.type, ind.source, ind.dest)

	def __dispatchProgress(self, ind, handlers):
		kwargs = {}
		if ind.HasField('err_code'): kwargs['err_code'] = (ind.err_code, _errorCodes.get(ind.err_code, 'unknown'))
		if ind.HasField('err_doc'): kwargs['err_doc'] = ind.err_doc
		if ind.HasField('err_rev'): kwargs['err_rev'] = ind.err_rev
		for handler in handlers:
			handler(ind.tag, ind.state, ind.progress, **kwargs)

	def __poll(self, completion):
		self.recursion += 1
		try:
			# loop until we've received the answer
			while completion.pending:
				if not self.socket.waitForReadyRead(-1):
					raise IOError("Error while waiting for data from server: "
						+ str(self.socket.errorString()))
				self.__readReady()
		finally:
			self.recursion -= 1

	def __make_ref(self):
		ref = self.next
		self.next += 1
		return ref


class Watch(object):
	EVENT_MODIFIED    = pb.WatchInd.modified
	EVENT_APPEARED    = pb.WatchInd.appeared
	EVENT_REPLICATED  = pb.WatchInd.replicated
	EVENT_DIMINISHED  = pb.WatchInd.diminished
	EVENT_DISAPPEARED = pb.WatchInd.disappeared

	TYPE_DOC = pb.WatchInd.doc
	TYPE_REV = pb.WatchInd.rev

	ROOT_DOC = '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0'

	def __init__(self, typ, h):
		self.__typ = typ
		self.__h = _checkUuid(h)
		self.__refcount = 0

	def _incWatchRef(self):
		self.__refcount += 1
		return self.__refcount

	def _decWatchRef(self):
		self.__refcount -= 1
		return self.__refcount

	def _getRef(self):
		return (self.__typ, self.__h)

	def getHash(self):
		return self.__h

	def triggered(self, cause, store):
		pass


class Enum(object):

	class Store(object):
		def __init__(self, store, isSystem):
			self.sid = store.sid
			self.src = store.src
			self.type = store.type
			self.label = store.label
			self.options = store.options
			self.isSystem = isSystem

	def __init__(self, reply):
		self.__sysStore = Enum.Store(reply.sys_store, True)
		self.__stores = [ Enum.Store(s, False) for s in reply.stores ]

	def sysStore(self):
		return self.__sysStore

	def regularStores(self):
		return self.__stores

	def allStores(self):
		stores = self.__stores[:]
		stores.append(self.__sysStore)
		return stores

	def fromLabel(self, label):
		if label == self.__sysStore.label:
			return self.__sysStore

		for store in self.__stores:
			if store.label == label:
				return store

		return None

	def fromSId(self, sid):
		if sid == self.__sysStore.sid:
			return self.__sysStore

		for store in self.__stores:
			if store.sid == sid:
				return store

		return None


class Lookup(object):
	def __init__(self, reply):
		self.__revs = {}
		self.__preRevs = {}
		self.__stores = {}

		# first get the current revisions
		for rev in reply.revs:
			self.__revs[rev.rid] = rev.stores[:]
			for store in rev.stores:
				self.__stores[store] = (rev.rid, [])

		# now get all the preliminary revisions
		for rev in reply.pre_revs:
			self.__preRevs[rev.rid] = rev.stores[:]
			for store in rev.stores:
				self.__stores[store][1].append(rev.rid)

	def revs(self):
		return self.__revs.keys()

	def rev(self, store):
		return self.__stores[store][0]

	def preRevs(self, store=None):
		if store:
			return self.__stores[store][1]
		else:
			return self.__preRevs.keys()

	def stores(self, rev = None):
		if rev:
			stores = []
			if rev in self.__revs:
				stores.extend(self.__revs[rev])
			if rev in self.__preRevs:
				stores.extend(self.__preRevs[rev])
			return stores
		else:
			return self.__stores.keys()


class Stat(object):
	__slots__ = ['__flags', '__data', '__attachments', '__parents', '__mtime',
		'__type', '__creator', '__comment', '__crtime']

	FLAG_STICKY = 0

	def __init__(self, reply):
		self.__flags = reply.flags
		self.__data = (reply.data.size, reply.data.hash)
		self.__attachments = {}
		for a in reply.attachments:
			self.__attachments[a.name] = (a.size, a.hash,
				datetime.fromtimestamp(a.crtime / 1000000.0),
				datetime.fromtimestamp(a.mtime / 1000000.0))
		self.__parents = reply.parents
		self.__crtime = datetime.fromtimestamp(reply.crtime / 1000000.0)
		self.__mtime = datetime.fromtimestamp(reply.mtime / 1000000.0)
		self.__type = reply.type_code
		self.__creator = reply.creator_code
		self.__comment = reply.comment

	def flags(self):
		# lazy flags decomposition
		if not isinstance(self.__flags, set):
			flags = self.__flags
			flagSet = set()
			i = 0
			while flags:
				if flags & 1:
					flagSet.add(i)
				flags = flags >> 1
				i += 1
			self.__flags = flagSet
		return self.__flags

	def dataSize(self):
		return self.__data[0]

	def dataHash(self):
		return self.__data[1]

	def size(self, attachment):
		return self.__attachments[attachment][0]

	def hash(self, attachment):
		return self.__attachments[attachment][1]

	def attachments(self):
		return self.__attachments.keys()

	def parents(self):
		return self.__parents

	def crtime(self, attachment=None):
		if attachment:
			return self.__attachments[attachment][2]
		else:
			return self.__crtime

	def mtime(self, attachment=None):
		if attachment:
			return self.__attachments[attachment][3]
		else:
			return self.__mtime

	def type(self):
		return self.__type

	def creator(self):
		return self.__creator

	def comment(self):
		return self.__comment


class Handle(object):
	def __init__(self, connector, store, handle, doc, rev):
		self.__pos = { }
		self.connector = connector
		self.__store = store
		self.handle = handle
		self.doc = doc
		self.rev = rev
		self.active = True

	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		if self.active:
			self.close()
		return False

	def __del__(self):
		if self.active:
			self.close()

	def _getPos(self, part):
		if part in self.__pos:
			return self.__pos[part]
		else:
			return 0

	def _setPos(self, part, pos):
		self.__pos[part] = pos

	def getData(self, selector):
		req = pb.GetDataReq()
		req.handle = self.handle
		req.selector = selector
		reply = self.connector._rpc(_Connector.GET_DATA_MSG, req.SerializeToString())
		data = pb.GetDataCnf.FromString(reply).data
		return loadPDSD(self.__store, data)

	def setData(self, selector, data):
		req = pb.SetDataReq()
		req.handle = self.handle
		req.selector = selector
		req.data = dumpPDSD(data)
		reply = self.connector._rpc(_Connector.SET_DATA_MSG, req.SerializeToString())

	def seek(self, part, offset, whence = 0):
		if whence == 0:
			pos = offset
		elif whence == 1:
			pos = self._getPos(part) + offset
		else:
			raise IOError('Not implemented')
		self._setPos(part, pos)

	def tell(self, part):
		return self._getPos(part)

	def read(self, part, length):
		if not self.active:
			raise IOError('Handle expired')
		result = ''
		pos = self._getPos(part)
		packetSize = self.connector.maxPacketSize
		while length > 0:
			if length > packetSize:
				chunk = packetSize
			else:
				chunk = length
			req = pb.ReadReq()
			req.handle = self.handle
			req.part = part
			req.offset = pos
			req.length = chunk
			reply = self.connector._rpc(_Connector.READ_MSG, req.SerializeToString())
			data = pb.ReadCnf.FromString(reply).data
			size = len(data)
			result = result + data
			pos = pos + size
			if size < chunk:
				break
		self._setPos(part, pos)
		return result

	def readAll(self, part):
		result = ''
		oldPos = self._getPos(part)
		try:
			self._setPos(part, 0)
			while True:
				chunk = self.read(part, 0x20000)
				result = result + chunk
				if len(chunk) < 0x20000:
					break
			return result
		finally:
			self._setPos(part, oldPos)

	def write(self, part, data):
		if not self.active:
			raise IOError('Handle expired')

		pos = self._getPos(part)
		i = 0
		length = len(data)
		packetSize = self.connector.maxPacketSize
		while length > i+packetSize:
			req = pb.WriteBufferReq()
			req.handle = self.handle
			req.part = part
			req.data = data[i:i+packetSize]
			self.connector._rpc(_Connector.WRITE_BUFFER_MSG, req.SerializeToString())
			i += packetSize

		req = pb.WriteCommitReq()
		req.handle = self.handle
		req.part = part
		req.offset = pos
		req.data = data[i:i+packetSize]
		self.connector._rpc(_Connector.WRITE_COMMIT_MSG, req.SerializeToString())

		self._setPos(part, pos+length)

	def writeAll(self, part, data):
		self._setPos(part, 0)
		self.truncate(part)
		self.write(part, data)

	def truncate(self, part):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.TruncReq()
		req.handle = self.handle
		req.part = part
		req.offset = self._getPos(part)
		self.connector._rpc(_Connector.TRUNC_MSG, req.SerializeToString())

	def commit(self, comment=None):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.CommitReq()
		req.handle = self.handle
		if comment is not None: req.comment = comment
		reply = self.connector._rpc(_Connector.COMMIT_MSG, req.SerializeToString())
		cnf = pb.CommitCnf.FromString(reply)
		self.rev = cnf.rev

	def suspend(self, comment=None):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.SuspendReq()
		req.handle = self.handle
		if comment is not None: req.comment = comment
		reply = self.connector._rpc(_Connector.SUSPEND_MSG, req.SerializeToString())
		cnf = pb.SuspendCnf.FromString(reply)
		self.rev = cnf.rev

	def close(self):
		if self.active:
			self.active = False
			req = pb.CloseReq()
			req.handle = self.handle
			self.connector._rpc(_Connector.CLOSE_MSG, req.SerializeToString())
		else:
			raise IOError('Handle expired')

	def stat(self):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.FStatReq()
		req.handle = self.handle
		reply = self.connector._rpc(_Connector.FSTAT_MSG, req.SerializeToString())
		return Stat(pb.StatCnf.FromString(reply))

	def setFlags(self, flags):
		if not self.active:
			raise IOError('Handle expired')
		flags = reduce(lambda x,y: x|y, [ 1 << f for f in flags ], 0)
		req = pb.SetFlagsReq()
		req.handle = self.handle
		req.flags = flags
		self.connector._rpc(_Connector.SET_FLAGS_MSG, req.SerializeToString())

	def setType(self, uti):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.SetTypeReq()
		req.handle = self.handle
		req.type_code = uti
		self.connector._rpc(_Connector.SET_TYPE_MSG, req.SerializeToString())

	def setMTime(self, attachment, mtime):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.SetMTimeReq()
		req.handle = self.handle
		req.attachment = attachment
		req.mtime = mtime
		self.connector._rpc(_Connector.SET_MTIME_MSG, req.SerializeToString())

	def merge(self, store, rev, depth=None, verbose=False):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.MergeReq()
		req.handle = self.handle
		req.store = _checkUuid(store)
		req.rev = _checkUuid(rev)
		if depth is not None:
			req.depth = depth
		if verbose: req.verbose = verbose
		self.connector._rpc(_Connector.MERGE_MSG, req.SerializeToString())

	def rebase(self, parent):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.RebaseReq()
		req.handle = self.handle
		req.rev = _checkUuid(parent)
		self.connector._rpc(_Connector.REBASE_MSG, req.SerializeToString())

	def getDoc(self):
		return self.doc

	def getRev(self):
		return self.rev

	def getStore(self):
		return self.__store

class ReplicateHandle(object):
	def __init__(self, connector, handle):
		self.connector = connector
		self.handle = handle
		self.active = True

	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		if self.active:
			self.close()
		return False

	def __del__(self):
		if self.active:
			self.close()

	def close(self):
		if self.active:
			self.active = False
			req = pb.CloseReq()
			req.handle = self.handle
			self.connector._rpc(_Connector.CLOSE_MSG, req.SerializeToString())
		else:
			raise IOError('Handle expired')

_connection = None

def __FlushConnection():
	global _connection
	if _connection:
		_connection.flush()

def Connector(address=None):
	global _connection
	if not _connection:
		_connection = _Connector(address)
		atexit.register(__FlushConnection)
	return _connection


###############################################################################
# PDSD data structures, encoders and decoders
###############################################################################

LINK_MIME_TYPE = 'application/x-peerdrive-links'

def Link(spec):
	if spec.startswith('doc:'):
		link = DocLink()
		link._fromString(spec)
		return link
	elif spec.startswith('rev:'):
		link = RevLink()
		link._fromString(spec)
		return link
	else:
		# FIXME: this assumes that we will only ever get single doc links o_O
		[(store, doc)] = Connector().walkPath(spec)
		return DocLink(store, doc, False)

class RevLink(object):
	MIME_TYPE = 'application/x-peerdrive-revlink'
	__slots__ = ['__rev', '__store']

	def __init__(self, store=None, rev=None):
		self.__store = store
		self.__rev = rev

	def __str__(self):
		return 'rev:'+self.__store.encode('hex')+':'+self.__rev.encode('hex')

	def __eq__(self, link):
		if isinstance(link, RevLink):
			if link.__rev == self.__rev:
				return True
		return False

	def __ne__(self, link):
		if isinstance(link, RevLink):
			if link.__rev == self.__rev:
				return False
		return True

	def __hash__(self):
		return hash(self.__rev)

	def _fromStruct(self, decoder):
		self.__store = decoder._getStore()
		length = decoder._getInt('B')
		self.__rev = decoder._getStr(length)

	def _toStruct(self):
		return struct.pack('<BB', 0x40, len(self.__rev)) + self.__rev

	def _fromDict(self, dct):
		self.__store = None
		self.__rev = dct['rev'].decode('hex')

	def _toDict(self):
		return { "__rlink__" : True, "rev" : self.__rev.encode('hex') }

	def _fromString(self, spec):
		(doc, store, rev) = spec.split(':')
		self.__store = store.decode("hex")
		self.__rev = rev.decode("hex")

	def update(self, newStore=None):
		if newStore:
			self.__store = newStore
		return self

	def doc(self):
		return None

	def rev(self):
		return self.__rev

	def store(self):
		return self.__store

class DocLink(object):
	MIME_TYPE = 'application/x-peerdrive-doclink'
	__slots__ = ['__store', '__doc', '__rev', '__updated']

	def __init__(self, store=None, doc=None, autoUpdate=True):
		self.__store = store
		self.__doc = doc
		self.__rev = None
		self.__updated = False
		if doc and autoUpdate:
			self.update()

	def __str__(self):
		return 'doc:'+self.__store.encode('hex')+':'+self.__doc.encode('hex')

	def __eq__(self, link):
		if isinstance(link, DocLink):
			if link.__doc == self.__doc:
				return True
		return False

	def __ne__(self, link):
		if isinstance(link, DocLink):
			if link.__doc == self.__doc:
				return False
		return True

	def __hash__(self):
		return hash(self.__doc)

	def _fromStruct(self, decoder):
		self.__store = decoder._getStore()
		length = decoder._getInt('B')
		self.__doc = decoder._getStr(length)
		self.__rev = None

	def _toStruct(self):
		return struct.pack('<BB', 0x41, len(self.__doc)) + self.__doc

	def _fromDict(self, dct):
		self.__store = None
		self.__doc = dct['doc'].decode('hex')
		self.__rev = None

	def _toDict(self):
		return {
			"__dlink__" : True,
			"doc" : self.__doc.encode('hex') }

	def _fromString(self, spec):
		(doc, store, doc) = spec.split(':')
		self.__store = store.decode("hex")
		self.__doc = doc.decode("hex")
		self.__rev = None

	def update(self, newStore=None):
		if newStore:
			self.__store = newStore
		l = Connector().lookupDoc(self.__doc, [self.__store])
		if self.__store in l.stores():
			self.__rev = l.rev(self.__store)
		else:
			self.__rev = None
		self.__updated = True
		return self

	def doc(self):
		return self.__doc

	def rev(self):
		if not self.__updated:
			self.update()
		return self.__rev

	def store(self):
		return self.__store


class Decoder(object):
	def __init__(self, store):
		self.__store = store
		pass

	def decode(self, s):
		self._s = s
		self._i = 0
		return self._decodeDoc()

	def _getStore(self):
		return self.__store

	def _getInt(self, code):
		length = struct.calcsize('<'+code)
		value = struct.unpack_from('<'+code, self._s, self._i)[0]
		self._i += length
		return value

	def _getStr(self, length):
		res = self._s[self._i:self._i+length]
		self._i += length
		return res

	def _decodeDoc(self):
		tag = self._getInt('B')
		if tag == 0x00:
			res = self._decodeDict()
		elif tag == 0x10:
			res = self._decodeList()
		elif tag == 0x20:
			res = self._decodeString()
		elif tag == 0x30:
			res = (self._getInt('B') != 0)
		elif tag == 0x40:
			res = RevLink()
			res._fromStruct(self)
		elif tag == 0x41:
			res = DocLink()
			res._fromStruct(self)
		elif tag == 0x50:
			res = self._getInt('f')
		elif tag == 0x51:
			res = self._getInt('d')
		elif tag == 0x60:
			res = self._getInt('B')
		elif tag ==	0x61:
			res = self._getInt('b')
		elif tag ==	0x62:
			res = self._getInt('H')
		elif tag ==	0x63:
			res = self._getInt('h')
		elif tag ==	0x64:
			res = self._getInt('L')
		elif tag ==	0x65:
			res = self._getInt('l')
		elif tag ==	0x66:
			res = self._getInt('Q')
		elif tag ==	0x67:
			res = self._getInt('q')
		else:
			raise TypeError("Invalid tag")

		return res

	def _decodeDict(self):
		elements = self._getInt('L')
		d = { }
		for i in range(elements):
			key = self._decodeString()
			value = self._decodeDoc()
			d[key] = value
		return d

	def _decodeList(self):
		elements = self._getInt('L')
		l = []
		for i in range(elements):
			l.append(self._decodeDoc())
		return l

	def _decodeString(self):
		length = self._getInt('L')
		value = self._getStr(length).decode('utf-8')
		return value


class Encoder(object):
	def __init__(self):
		pass

	def encode(self, o):
		if isinstance(o, dict):
			res = self._encodeDict(o)
		elif isinstance(o, (list, tuple)):
			res = self._encodeList(o)
		elif isinstance(o, basestring):
			if isinstance(o, str):
				res = struct.pack('<BL', 0x20, len(o)) + o
			else:
				encStr = o.encode('utf-8')
				res = struct.pack('<BL', 0x20, len(encStr)) + encStr
		elif o is True:
			res = struct.pack('BB', 0x30, 1)
		elif o is False:
			res = struct.pack('BB', 0x30, 0)
		elif isinstance(o, RevLink):
			res = o._toStruct()
		elif isinstance(o, DocLink):
			res = o._toStruct()
		elif isinstance(o, float):
			res = struct.pack('<Bd', 0x51, o)
		elif isinstance(o, (int, long)):
			res = self._encodeInt(o)
		else:
			raise TypeError("Invalid object: " + repr(o))

		return res

	def _encodeDict(self, d):
		data = struct.pack('<BL', 0x00, len(d))
		for key, value in d.iteritems():
			if isinstance(key, basestring):
				if isinstance(key, unicode):
					key = key.encode('utf-8')
			else:
				raise TypeError("Invalid dict key: " + repr(key))
			data += struct.pack('<L', len(key)) + key + self.encode(value)

		return data

	def _encodeList(self, l):
		data = struct.pack('<BL', 0x10, len(l))
		for i in l:
			data += self.encode(i)
		return data

	def _encodeInt(self, i):
		if i < 0:
			if i >= -128:
				return struct.pack('<Bb', 0x61, i)
			elif i >= -32768:
				return struct.pack('<Bh', 0x63, i)
			elif i >= -2147483648:
				return struct.pack('<Bl', 0x65, i)
			else:
				return struct.pack('<Bq', 0x67, i)
		else:
			if i <= 0xff:
				return struct.pack('<BB', 0x60, i)
			elif i <= 0xffff:
				return struct.pack('<BH', 0x62, i)
			elif i <= 0xffffffff:
				return struct.pack('<BL', 0x64, i)
			else:
				return struct.pack('<BQ', 0x66, i)


def __decode_link(dct):
	if '__rlink__' in dct:
		link = RevLink()
		link._fromDict(dct)
		return link
	elif '__dlink__' in dct:
		link = DocLink()
		link._fromDict(dct)
		return link
	else:
		return dct

def __encode_link(obj):
	if isinstance(obj, RevLink):
		return obj._toDict()
	elif isinstance(obj, DocLink):
		return obj._toDict()
	else:
		raise TypeError(repr(obj) + " is not serializable")


def loadPDSD(store, s):
	dec = Decoder(store)
	return dec.decode(s)


def dumpPDSD(o):
	enc = Encoder()
	return enc.encode(o)


def loadJSON(s):
	return json.loads(s, object_hook=__decode_link)


def dumpJSON(o):
	return json.dumps(o, default=__encode_link)


def loadMimeData(mimeData):
	links = []
	if mimeData.hasFormat(LINK_MIME_TYPE):
		data = str(mimeData.data(LINK_MIME_TYPE))
		for spec in data.splitlines():
			link = Link(spec)
			if link:
				links.append(link)
	return links

def dumpMimeData(mimeData, links):
	if len(links) >= 1:
		data = reduce(lambda x,y: x+'\n'+y, [str(link) for link in links])
		mimeData.setData(LINK_MIME_TYPE, data)


