# vim: set fileencoding=utf-8 :
#
# Hotchpotch
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
import sys, struct, atexit, weakref, traceback
from . import hotchpotch_client_pb2 as pb

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


def _checkUuid(uuid):
	valid = (uuid.__class__ == str) and (len(uuid) == 16)
	if not valid:
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
	WRITE_MSG           = 0x000d
	GET_FLAGS_MSG       = 0x000e
	SET_FLAGS_MSG       = 0x000f
	GET_TYPE_MSG        = 0x0010
	SET_TYPE_MSG        = 0x0011
	GET_PARENTS_MSG     = 0x0012
	MERGE_MSG           = 0x0013
	REBASE_MSG          = 0x0014
	COMMIT_MSG          = 0x0015
	SUSPEND_MSG         = 0x0016
	CLOSE_MSG           = 0x0017
	WATCH_ADD_MSG       = 0x0018
	WATCH_REM_MSG       = 0x0019
	WATCH_PROGRESS_MSG  = 0x001a
	FORGET_MSG          = 0x001b
	DELETE_DOC_MSG      = 0x001c
	DELETE_REV_MSG      = 0x001d
	FORWARD_DOC_MSG     = 0x001e
	REPLICATE_DOC_MSG   = 0x001f
	REPLICATE_REV_MSG   = 0x0020
	MOUNT_MSG           = 0x0021
	UNMOUNT_MSG         = 0x0022
	SYS_INFO_MSG        = 0x0023
	WATCH_MSG           = 0x0024
	PROGRESS_START_MSG  = 0x0025
	PROGRESS_MSG        = 0x0026
	PROGRESS_END_MSG    = 0x0027

	FLAG_REQ = 0
	FLAG_CNF = 1
	FLAG_IND = 2
	FLAG_RSP = 3

	watchReady = QtCore.pyqtSignal()

	def __init__(self, host = '127.0.0.1', port = 4567):
		super(_Connector, self).__init__()
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
			req.major = 0
			req.minor = 0
			reply = self._rpc(_Connector.INIT_MSG, req.SerializeToString())
			cnf = pb.InitCnf.FromString(reply)
			if cnf.major != 0 or cnf.minor != 0:
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

	def forwardDoc(self, store, doc, fromRev, toRev, srcStore, depth=None):
		req = pb.ForwardDocReq()
		req.store = _checkUuid(store)
		req.doc = _checkUuid(doc)
		req.from_rev = _checkUuid(fromRev)
		req.to_rev = _checkUuid(toRev)
		req.src_store = _checkUuid(srcStore)
		if depth is not None:
			req.depth = depth
		self._rpc(_Connector.FORWARD_DOC_MSG, req.SerializeToString())

	def replicateDoc(self, srcStore, doc, dstStore, depth=None):
		req = pb.ReplicateDocReq()
		req.src_store = _checkUuid(srcStore)
		req.doc = _checkUuid(doc)
		req.dst_store = _checkUuid(dstStore)
		if depth is not None:
			req.depth = depth
		self._rpc(_Connector.REPLICATE_DOC_MSG, req.SerializeToString())

	def replicateRev(self, srcStore, rev, dstStore, depth=None):
		req = pb.ReplicateRevReq()
		req.src_store = _checkUuid(srcStore)
		req.rev = _checkUuid(rev)
		req.dst_store = _checkUuid(dstStore)
		if depth is not None:
			req.depth = depth
		self._rpc(_Connector.REPLICATE_REV_MSG, req.SerializeToString())

	def mount(self, store):
		req = pb.MountReq()
		req.id = store
		self._rpc(_Connector.MOUNT_MSG, req.SerializeToString())

	def unmount(self, store):
		req = pb.UnmountReq()
		req.id = store
		self._rpc(_Connector.UNMOUNT_MSG, req.SerializeToString())

	def sysInfo(self, param):
		req = pb.SysInfoReq()
		req.param = param
		reply = self._rpc(_Connector.SYS_INFO_MSG, req.SerializeToString())
		cnf = pb.SysInfoCnf.FromString(reply)
		if cnf.HasField("as_string"):
			return cnf.as_string
		elif cnf.HasField("as_int"):
			return cnf.as_int
		else:
			return None

	def flush(self):
		while self.socket.flush():
			self.socket.waitForBytesWritten(10000)

	def process(self, timeout=1):
		if self.socket.waitForReadyRead(timeout):
			self.__readReady()
		self.__dispatchIndications()

	def regProgressHandler(self, start=None, progress=None, stop=None):
		if start:
			self.__regProgressHandler(_Connector.PROGRESS_START_MSG, start)
		if progress:
			self.__regProgressHandler(_Connector.PROGRESS_MSG, progress)
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

	# protected functions

	def _rpc(self, msg, request = ''):
		ref = self.__make_ref()
		req_msg = (msg << 4) | _Connector.FLAG_REQ
		self.__send(struct.pack('>LH', ref, req_msg) + request)
		(cnf, reply) = self.__poll(ref)
		if cnf == msg:
			return reply
		elif cnf == _Connector.ERROR_MSG:
			error_cnf = pb.ErrorCnf.FromString(reply)
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
				# TODO: discard invalid packets

				# immediately remove indications
				(ref, msg) = struct.unpack_from('>LH', packet, 0)
				typ = msg & 3
				msg = msg >> 4
				if typ == _Connector.FLAG_IND:
					indications = True
					self.indications.append((msg, packet[6:]))
				elif typ == _Connector.FLAG_CNF:
					self.confirmations[ref] = (msg, packet[6:])
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
								i.triggered(ind.event)
					elif msg == _Connector.PROGRESS_START_MSG:
						ind = pb.ProgressStartInd.FromString(packet)
						handlers = self.progressHandlers[:]
						for (event, handler) in handlers:
							if event == msg:
								handler(ind.tag, ind.type, ind.source, ind.dest)
					elif msg == _Connector.PROGRESS_MSG:
						ind = pb.ProgressInd.FromString(packet)
						handlers = self.progressHandlers[:]
						for (event, handler) in handlers:
							if event == msg:
								handler(ind.tag, ind.progress)
					elif msg == _Connector.PROGRESS_END_MSG:
						ind = pb.ProgressEndInd.FromString(packet)
						handlers = self.progressHandlers[:]
						for (event, handler) in handlers:
							if event == msg:
								handler(ind.tag)
		else:
			self.watchReady.emit()

	def __poll(self, ref):
		self.recursion += 1
		try:
			# loop until we've received the answer
			while True:
				if ref in self.confirmations:
					cnf = self.confirmations[ref]
					del self.confirmations[ref]
					return cnf

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

	def triggered(self, cause):
		pass


class Enum(object):

	class Store(object):
		def __init__(self, store):
			self.doc = store.guid
			self.name = store.name
			self.isMounted = store.is_mounted
			self.isRemovable = store.is_removable
			self.isSystem = store.is_system_store
			self.isNet = store.is_network_store

	def __init__(self, reply):
		self.__stores = { }
		self.__sysStore = None
		for s in reply.stores:
			self.__stores[s.id] = Enum.Store(s)
			if s.is_system_store:
				self.__sysStore = s.guid

	def sysStore(self):
		return self.__sysStore

	def allStores(self):
		return self.__stores.keys()

	def isMounted(self, store):
		return self.__stores[store].isMounted

	def isSystem(self, store):
		return self.__stores[store].isSystem

	def isRemovable(self, store):
		return self.__stores[store].isRemovable

	def isNet(self, store):
		return self.__stores[store].isNet

	def store(self, doc):
		for (store, info) in self.__stores.items():
			if info.doc == doc:
				return store
		return None

	def doc(self, store):
		return self.__stores[store].doc

	def name(self, store):
		return self.__stores[store].name


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
	__slots__ = ['__flags', '__parts', '__parents', '__mtime', '__type',
		'__creator', '__docLinks', '__revLinks']

	FLAG_STICKY = 0

	def __init__(self, reply):
		self.__flags = reply.flags
		self.__parts = {}
		for part in reply.parts:
			self.__parts[part.fourcc] = (part.size, part.pid)
		self.__parents = reply.parents
		self.__mtime = datetime.fromtimestamp(reply.mtime / 1000000.0)
		self.__type = reply.type_code
		self.__creator = reply.creator_code

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

	def size(self, part):
		return self.__parts[part][0]

	def hash(self, part):
		return self.__parts[part][1]

	def parts(self):
		return self.__parts.keys()

	def parents(self):
		return self.__parents

	def mtime(self):
		return self.__mtime

	def type(self):
		return self.__type

	def creator(self):
		return self.__creator


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
		while length > i:
			reqData = data[i:i+packetSize]
			reqSize = len(reqData)
			req = pb.WriteReq()
			req.handle = self.handle
			req.part = part
			req.offset = pos + i
			req.data = reqData
			self.connector._rpc(_Connector.WRITE_MSG, req.SerializeToString())
			i += reqSize
			self._setPos(part, pos+i)


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

	def commit(self):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.CommitReq()
		req.handle = self.handle
		reply = self.connector._rpc(_Connector.COMMIT_MSG, req.SerializeToString())
		cnf = pb.CommitCnf.FromString(reply)
		self.rev = cnf.rev

	def suspend(self):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.SuspendReq()
		req.handle = self.handle
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

	def getFlags(self):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.GetFlagsReq()
		req.handle = self.handle
		reply = self.connector._rpc(_Connector.GET_FLAGS_MSG, req.SerializeToString())
		flags = pb.GetFlagsCnf.FromString(reply).flags
		flagSet = set()
		i = 0
		while flags:
			if flags & 1:
				flagSet.add(i)
			flags = flags >> 1
			i += 1
		return flagSet

	def setFlags(self, flags):
		if not self.active:
			raise IOError('Handle expired')
		flags = reduce(lambda x,y: x|y, [ 1 << f for f in flags ], 0)
		req = pb.SetFlagsReq()
		req.handle = self.handle
		req.flags = flags
		self.connector._rpc(_Connector.SET_FLAGS_MSG, req.SerializeToString())

	def getType(self):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.GetTypeReq()
		req.handle = self.handle
		reply = self.connector._rpc(_Connector.GET_TYPE_MSG, req.SerializeToString())
		return pb.GetTypeCnf.FromString(reply).type_code

	def setType(self, uti):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.SetTypeReq()
		req.handle = self.handle
		req.type_code = uti
		self.connector._rpc(_Connector.SET_TYPE_MSG, req.SerializeToString())

	def getParents(self):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.GetParentsReq()
		req.handle = self.handle
		reply = self.connector._rpc(_Connector.GET_PARENTS_MSG, req.SerializeToString())
		return pb.GetParentsCnf.FromString(reply).parents

	def merge(self, store, rev, depth=None):
		if not self.active:
			raise IOError('Handle expired')
		req = pb.MergeReq()
		req.handle = self.handle
		req.store = _checkUuid(store)
		req.rev = _checkUuid(rev)
		if depth is not None:
			req.depth = depth
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

_connection = None

def __FlushConnection():
	global _connection
	if _connection:
		_connection.flush()

def Connector():
	global _connection
	if not _connection:
		_connection = _Connector()
		atexit.register(__FlushConnection)
	return _connection

