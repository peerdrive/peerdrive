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

_errorCodes = {
	1 : 'ECONFLICT',
	2 : 'EAMBIG',
	256 : 'E2BIG',
	257 : 'EACCES',
	258 : 'EADDRINUSE',
	259 : 'EADDRNOTAVAIL',
	260 : 'EADV',
	261 : 'EAFNOSUPPORT',
	262 : 'EAGAIN',
	263 : 'EALIGN',
	264 : 'EALREADY',
	265 : 'EBADE',
	266 : 'EBADF',
	267 : 'EBADFD',
	268 : 'EBADMSG',
	269 : 'EBADR',
	270 : 'EBADRPC',
	271 : 'EBADRQC',
	272 : 'EBADSLT',
	273 : 'EBFONT',
	274 : 'EBUSY',
	275 : 'ECHILD',
	276 : 'ECHRNG',
	277 : 'ECOMM',
	278 : 'ECONNABORTED',
	279 : 'ECONNREFUSED',
	280 : 'ECONNRESET',
	281 : 'EDEADLK',
	282 : 'EDEADLOCK',
	283 : 'EDESTADDRREQ',
	284 : 'EDIRTY',
	285 : 'EDOM',
	286 : 'EDOTDOT',
	287 : 'EDQUOT',
	288 : 'EDUPPKG',
	289 : 'EEXIST',
	290 : 'EFAULT',
	291 : 'EFBIG',
	292 : 'EHOSTDOWN',
	293 : 'EHOSTUNREACH',
	294 : 'EIDRM',
	295 : 'EINIT',
	296 : 'EINPROGRESS',
	297 : 'EINTR',
	298 : 'EINVAL',
	299 : 'EIO',
	300 : 'EISCONN',
	301 : 'EISDIR',
	302 : 'EISNAM',
	303 : 'ELBIN',
	304 : 'EL2HLT',
	305 : 'EL2NSYNC',
	306 : 'EL3HLT',
	307 : 'EL3RST',
	308 : 'ELIBACC',
	309 : 'ELIBBAD',
	310 : 'ELIBEXEC',
	311 : 'ELIBMAX',
	312 : 'ELIBSCN',
	313 : 'ELNRNG',
	314 : 'ELOOP',
	315 : 'EMFILE',
	316 : 'EMLINK',
	317 : 'EMSGSIZE',
	318 : 'EMULTIHOP',
	319 : 'ENAMETOOLONG',
	320 : 'ENAVAIL',
	321 : 'ENET',
	322 : 'ENETDOWN',
	323 : 'ENETRESET',
	324 : 'ENETUNREACH',
	325 : 'ENFILE',
	326 : 'ENOANO',
	327 : 'ENOBUFS',
	328 : 'ENOCSI',
	329 : 'ENODATA',
	330 : 'ENODEV',
	331 : 'ENOENT',
	332 : 'ENOEXEC',
	333 : 'ENOLCK',
	334 : 'ENOLINK',
	335 : 'ENOMEM',
	336 : 'ENOMSG',
	337 : 'ENONET',
	338 : 'ENOPKG',
	339 : 'ENOPROTOOPT',
	340 : 'ENOSPC',
	341 : 'ENOSR',
	342 : 'ENOSYM',
	343 : 'ENOSYS',
	344 : 'ENOTBLK',
	345 : 'ENOTCONN',
	346 : 'ENOTDIR',
	347 : 'ENOTEMPTY',
	348 : 'ENOTNAM',
	349 : 'ENOTSOCK',
	350 : 'ENOTSUP',
	351 : 'ENOTTY',
	352 : 'ENOTUNIQ',
	353 : 'ENXIO',
	354 : 'EOPNOTSUPP',
	355 : 'EPERM',
	356 : 'EPFNOSUPPORT',
	357 : 'EPIPE',
	358 : 'EPROCLIM',
	359 : 'EPROCUNAVAIL',
	360 : 'EPROGMISMATCH',
	361 : 'EPROGUNAVAIL',
	362 : 'EPROTO',
	363 : 'EPROTONOSUPPORT',
	364 : 'EPROTOTYPE',
	365 : 'ERANGE',
	366 : 'EREFUSED',
	367 : 'EREMCHG',
	368 : 'EREMDEV',
	369 : 'EREMOTE',
	370 : 'EREMOTEIO',
	371 : 'EREMOTERELEASE',
	372 : 'EROFS',
	373 : 'ERPCMISMATCH',
	374 : 'ERREMOTE',
	375 : 'ESHUTDOWN',
	376 : 'ESOCKTNOSUPPORT',
	377 : 'ESPIPE',
	378 : 'ESRCH',
	379 : 'ESRMNT',
	380 : 'ESTALE',
	381 : 'ESUCCESS',
	382 : 'ETIME',
	383 : 'ETIMEDOUT',
	384 : 'ETOOMANYREFS',
	385 : 'ETXTBSY',
	386 : 'EUCLEAN',
	387 : 'EUNATCH',
	388 : 'EUSERS',
	389 : 'EVERSION',
	390 : 'EWOULDBLOCK',
	391 : 'EXDEV',
	392 : 'EXFULL',
	393 : 'NXDOMAIN'
}


def _checkUuid(uuid):
	valid = (uuid.__class__ == str) and (len(uuid) == 16)
	if not valid:
		raise IOError('Invalid UUID: '+str(uuid.__class__)+' '+str(uuid))
	return True


def _encodeUuidList(uuids):
	packet = struct.pack('>B', len(uuids))
	for rev in uuids:
		_checkUuid(rev)
		packet += rev
	return packet


def _encodeString(string):
	if isinstance(string, str):
		return struct.pack('>H', len(string)) + string
	elif isinstance(string, unicode):
		encStr = string.encode('utf-8')
		return struct.pack('>H', len(encStr)) + encStr

def _raiseError(error):
	if error == 0:
		return True
	elif error in _errorCodes:
		raise IOError(_errorCodes[error])
	else:
		raise IOError('Unknown error')


class _Connector(QtCore.QObject):
	INIT_REQ            = 0x0000
	INIT_CNF            = 0x0001
	ENUM_REQ            = 0x0010
	ENUM_CNF            = 0x0011
	LOOKUP_DOC_REQ      = 0x0020
	LOOKUP_DOC_CNF      = 0x0021
	LOOKUP_REV_REQ      = 0x0030
	LOOKUP_REV_CNF      = 0x0031
	STAT_REQ            = 0x0040
	STAT_CNF            = 0x0041
	PEEK_REQ            = 0x0050
	PEEK_CNF            = 0x0051
	CREATE_REQ          = 0x0060
	CREATE_CNF          = 0x0061
	FORK_REQ            = 0x0070
	FORK_CNF            = 0x0071
	UPDATE_REQ          = 0x0080
	UPDATE_CNF          = 0x0081
	RESUME_REQ          = 0x0090
	RESUME_CNF          = 0x0091
	READ_REQ            = 0x00A0
	READ_CNF            = 0x00A1
	TRUNC_REQ           = 0x00B0
	TRUNC_CNF           = 0x00B1
	WRITE_REQ           = 0x00C0
	WRITE_CNF           = 0x00C1
	GET_FLAGS_REQ       = 0x0210
	GET_FLAGS_CNF       = 0x0211
	SET_FLAGS_REQ       = 0x0220
	SET_FLAGS_CNF       = 0x0221
	GET_TYPE_REQ        = 0x00D0
	GET_TYPE_CNF        = 0x00D1
	SET_TYPE_REQ        = 0x00E0
	SET_TYPE_CNF        = 0x00E1
	GET_PARENTS_REQ     = 0x00F0
	GET_PARENTS_CNF     = 0x00F1
	MERGE_REQ           = 0x0100
	MERGE_CNF           = 0x0101
	REBASE_REQ          = 0x0110
	REBASE_CNF          = 0x0111
	COMMIT_REQ          = 0x0120
	COMMIT_CNF          = 0x0121
	SUSPEND_REQ         = 0x0130
	SUSPEND_CNF         = 0x0131
	CLOSE_REQ           = 0x0140
	CLOSE_CNF           = 0x0141
	WATCH_ADD_REQ       = 0x0150
	WATCH_ADD_CNF       = 0x0151
	WATCH_REM_REQ       = 0x0160
	WATCH_REM_CNF       = 0x0161
	WATCH_PROGRESS_REQ  = 0x0170
	WATCH_PROGRESS_CNF  = 0x0171
	FORGET_REQ          = 0x0180
	FORGET_CNF          = 0x0181
	DELETE_DOC_REQ      = 0x0190
	DELETE_DOC_CNF      = 0x0191
	DELETE_REV_REQ      = 0x01A0
	DELETE_REV_CNF      = 0x01A1
	FORWARD_DOC_REQ     = 0x01B0
	FORWARD_DOC_CNF     = 0x01B1
	REPLICATE_DOC_REQ   = 0x01C0
	REPLICATE_DOC_CNF   = 0x01C1
	REPLICATE_REV_REQ   = 0x01D0
	REPLICATE_REV_CNF   = 0x01D1
	MOUNT_REQ           = 0x01E0
	MOUNT_CNF           = 0x01E1
	UNMOUNT_REQ         = 0x01F0
	UNMOUNT_CNF         = 0x01F1
	SYS_INFO_REQ        = 0x0200
	SYS_INFO_CNF        = 0x0201
	WATCH_IND           = 0x0002
	PROGRESS_START_IND  = 0x0012
	PROGRESS_IND        = 0x0022
	PROGRESS_END_IND    = 0x0032

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
		self.queue = []
		self.buf = ''
		self.watchHandlers = {}
		self.watchIndications = []
		self.progressHandlers = []
		self.progressIndications = []
		self.recursion = 0

		self.watchReady.connect(self.__dispatchIndications, QtCore.Qt.QueuedConnection)

		try:
			reply = self._rpc(_Connector.INIT_REQ, _Connector.INIT_CNF,
				struct.pack('>L', 0))
			(version, self.maxPacketSize) = struct.unpack_from('>LL',
				self._parseResult(reply), 0)
			if version != 0:
				raise IOError("Unsupported protocol version!")
		except:
			self.socket.disconnectFromHost()
			raise

	def enum(self):
		reply = self._rpc(_Connector.ENUM_REQ, _Connector.ENUM_CNF)
		return Enum(reply)

	def lookup_doc(self, doc, stores=[]):
		_checkUuid(doc)
		request = doc + _encodeUuidList(stores)
		reply = self._rpc(_Connector.LOOKUP_DOC_REQ, _Connector.LOOKUP_DOC_CNF, request)
		return Lookup(reply)

	def lookup_rev(self, rev, stores=[]):
		_checkUuid(rev)
		request = rev + _encodeUuidList(stores)
		reply = self._rpc(_Connector.LOOKUP_REV_REQ, _Connector.LOOKUP_REV_CNF, request)
		(storeCount,) = struct.unpack_from('>B', reply, 0)
		pos = 1
		found = []
		for i in xrange(storeCount):
			(store,) = struct.unpack_from('>16s', reply, pos)
			found.append(store)
			pos += 16
		return found

	def stat(self, rev, stores=[]):
		_checkUuid(rev)
		request = rev + _encodeUuidList(stores)
		reply = self._rpc(_Connector.STAT_REQ, _Connector.STAT_CNF, request)
		stat = self._parseResult(reply)
		return Stat(stat)

	def peek(self, store, rev):
		_checkUuid(store)
		_checkUuid(rev)
		request = store + rev
		reply = self._rpc(_Connector.PEEK_REQ, _Connector.PEEK_CNF, request)
		handle = self._parseResult(reply)
		return Handle(self, store, handle, None, rev)

	def create(self, store, typ, creator):
		_checkUuid(store)
		request = store + _encodeString(typ) + _encodeString(creator)
		reply = self._rpc(_Connector.CREATE_REQ, _Connector.CREATE_CNF, request)
		reply = self._parseResult(reply)
		(handle, doc) = struct.unpack('>4s16s', reply)
		return Handle(self, store, handle, doc, None)

	def fork(self, store, rev, creator):
		_checkUuid(store)
		_checkUuid(rev)
		request = store + rev + _encodeString(creator)
		reply = self._rpc(_Connector.FORK_REQ, _Connector.FORK_CNF, request)
		reply = self._parseResult(reply)
		(handle, doc) = struct.unpack('>4s16s', reply)
		return Handle(self, store, handle, doc, rev)

	def update(self, store, doc, rev, creator=''):
		_checkUuid(store)
		_checkUuid(doc)
		_checkUuid(rev)
		request = store + doc + rev + _encodeString(creator)
		reply = self._rpc(_Connector.UPDATE_REQ, _Connector.UPDATE_CNF, request)
		handle = self._parseResult(reply)
		return Handle(self, store, handle, doc, rev)

	def resume(self, store, doc, rev, creator=''):
		_checkUuid(store)
		_checkUuid(doc)
		_checkUuid(rev)
		request = store + doc + rev + _encodeString(creator)
		reply = self._rpc(_Connector.RESUME_REQ, _Connector.RESUME_CNF, request)
		handle = self._parseResult(reply)
		return Handle(self, store, handle, doc, rev)

	def watch(self, w):
		if w._incWatchRef() == 1:
			(typ, h) = ref = w._getRef()
			if ref not in self.watchHandlers:
				reply = self._rpc(_Connector.WATCH_ADD_REQ,
					_Connector.WATCH_ADD_CNF, struct.pack('>B16s', typ, h))
				self._parseResult(reply)
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
			self._rpc(_Connector.WATCH_REM_REQ,
				_Connector.WATCH_REM_CNF, struct.pack('>B16s', typ, h))
			del self.watchHandlers[watchSpec]

	def unwatch(self, w):
		if w._decWatchRef() == 0:
			(typ, h) = ref = w._getRef()
			oldHandlers = self.watchHandlers[ref]
			newHandlers = [x for x in oldHandlers if x() != w]
			if newHandlers == []:
				reply = self._rpc(_Connector.WATCH_REM_REQ,
					_Connector.WATCH_REM_CNF, struct.pack('>B16s', typ, h))
				self._parseResult(reply)
				del self.watchHandlers[ref]
			else:
				self.watchHandlers[ref] = newHandlers

	def forget(self, store, doc, rev):
		_checkUuid(store)
		_checkUuid(doc)
		_checkUuid(rev)
		request = store + doc + rev
		reply = self._rpc(_Connector.FORGET_REQ, _Connector.FORGET_CNF,
			request)
		self._parseResult(reply)
		return True

	def deleteDoc(self, store, doc, rev):
		_checkUuid(store)
		_checkUuid(doc)
		_checkUuid(rev)
		request = store + doc + rev
		reply = self._rpc(_Connector.DELETE_DOC_REQ,
			_Connector.DELETE_DOC_CNF, request)
		self._parseResult(reply)
		return True

	def deleteRev(self, store, rev):
		_checkUuid(store)
		_checkUuid(rev)
		request = store + rev
		reply = self._rpc(_Connector.DELETE_REV_REQ,
			_Connector.DELETE_REV_CNF, request)
		self._parseResult(reply)
		return True

	def forwardDoc(self, store, doc, fromRev, toRev, srcStore, depth=0):
		_checkUuid(store)
		_checkUuid(doc)
		_checkUuid(fromRev)
		_checkUuid(toRev)
		_checkUuid(srcStore)
		request = store + doc + fromRev + toRev + srcStore + struct.pack('>Q',
			depth)
		reply = self._rpc(_Connector.FORWARD_DOC_REQ, _Connector.FORWARD_DOC_CNF,
			request)
		rev = self._parseResult(reply)
		return rev

	def replicateDoc(self, srcStore, doc, dstStore, depth=0):
		_checkUuid(srcStore)
		_checkUuid(doc)
		_checkUuid(dstStore)
		request = srcStore + doc + dstStore + struct.pack('>Q', depth)
		reply = self._rpc(_Connector.REPLICATE_DOC_REQ,
			_Connector.REPLICATE_DOC_CNF, request)
		self._parseResult(reply)
		return True

	def replicateRev(self, srcStore, rev, dstStore, depth=0):
		_checkUuid(srcStore)
		_checkUuid(rev)
		_checkUuid(dstStore)
		request = srcStore + rev + dstStore + struct.pack('>Q', depth)
		reply = self._rpc(_Connector.REPLICATE_REV_REQ,
			_Connector.REPLICATE_REV_CNF, request)
		self._parseResult(reply)
		return True

	def mount(self, store):
		reply = self._rpc(_Connector.MOUNT_REQ, _Connector.MOUNT_CNF,
			_encodeString(store))
		self._parseResult(reply)
		return True

	def unmount(self, store):
		reply = self._rpc(_Connector.UNMOUNT_REQ, _Connector.UNMOUNT_CNF,
			_encodeString(store))
		self._parseResult(reply)
		return True

	def sysInfo(self, param):
		reply = self._rpc(_Connector.SYS_INFO_REQ, _Connector.SYS_INFO_CNF,
			_encodeString(param))
		res = self._parseResult(reply)
		(valueLen,) = struct.unpack_from('>H', res, 2)
		return res[2:2+valueLen]

	def flush(self):
		while self.socket.flush():
			self.socket.waitForBytesWritten(10000)

	def process(self, timeout=1):
		if self.socket.waitForReadyRead(timeout):
			self.__readReady()
		self.__dispatchIndications()

	def regProgressHandler(self, start=None, progress=None, stop=None):
		if start:
			self.__regProgressHandler(_Connector.PROGRESS_START_IND, start)
		if progress:
			self.__regProgressHandler(_Connector.PROGRESS_IND, progress)
		if stop:
			self.__regProgressHandler(_Connector.PROGRESS_END_IND, stop)

	def __regProgressHandler(self, event, handler):
		if len(self.progressHandlers) == 0:
			reply = self._rpc(_Connector.WATCH_PROGRESS_REQ,
				_Connector.WATCH_PROGRESS_CNF, '\x01')
			self._parseResult(reply)
		self.progressHandlers.append((event, handler))

	def unregProgressHandler(self, start=None, progress=None, stop=None):
		if start:
			self.__unregProgressHandler(_Connector.PROGRESS_START_IND, start)
		if progress:
			self.__unregProgressHandler(_Connector.PROGRESS_IND, progress)
		if stop:
			self.__unregProgressHandler(_Connector.PROGRESS_END_IND, stop)

	def __unregProgressHandler(self, event, handler):
		self.progressHandlers.remove((event, handler))
		if len(self.progressHandlers) == 0:
			reply = self._rpc(_Connector.WATCH_PROGRESS_REQ,
				_Connector.WATCH_PROGRESS_CNF, '\x00')
			self._parseResult(reply)

	# protected functions

	def _rpc(self, req_op, cnf_op, request = ''):
		ref = self.__make_ref()
		raw_request = struct.pack('>LH', ref, req_op) + request
		self.__send(raw_request)
		reply = self.__poll(ref, cnf_op)
		return reply

	def _ind(self, req_op, body = ''):
		ref = self.__make_ref()
		raw_ind = struct.pack('>LL', ref, req_op) + body
		self.__send(raw_ind)

	def _parseResult(self, reply):
		(error,) = struct.unpack_from('>L', reply, 0)
		_raiseError(error)
		return reply[4:]

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
				(ref, ind) = struct.unpack_from('>LH', packet, 0)
				if ind == _Connector.WATCH_IND:
					self.watchIndications.append(struct.unpack('>BB16s', packet[6:]))
					indications = True
				elif ind in [_Connector.PROGRESS_START_IND, _Connector.PROGRESS_IND, _Connector.PROGRESS_END_IND]:
					self.progressIndications.append((ind, packet[6:]))
					indications = True
				else:
					self.queue.append(packet)
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
				watchIndications = self.watchIndications[:]
				self.watchIndications = []
				for (event, typ, h) in watchIndications:
					# make explicit copy as watches may get modified by callouts!
					dispatched = True
					matches = self.watchHandlers.get((typ, h), [])[:]
					for i in matches:
						i = i() # dereference weakref
						if i is not None:
							i.triggered(event)

				progressIndications = self.progressIndications[:]
				self.progressIndications = []
				for (ind, body) in progressIndications:
					dispatched = True
					handlers = self.progressHandlers[:]
					if ind == _Connector.PROGRESS_START_IND:
						(tag,typ) = struct.unpack_from('>HB', body, 0)
						info = body[3:]
						args = (tag, typ, info)
					elif ind == _Connector.PROGRESS_IND:
						args = struct.unpack_from('>HB', body, 0)
					else:
						args = struct.unpack_from('>H', body, 0)
					for (event, handler) in handlers:
						if event == ind:
							handler(*args)
		else:
			self.watchReady.emit()

	def __poll(self, ref, cnf_op):
		self.recursion += 1
		try:
			# loop until we've received the answer
			while True:
				for packet in self.queue:
					if self.__match(ref, cnf_op, packet):
						self.queue.remove(packet)
						return packet[6:]

				if not self.socket.waitForReadyRead(-1):
					raise IOError("Error while waiting for data from server: "
						+ str(self.socket.errorString()))
				self.__readReady()
		finally:
			self.recursion -= 1

	def __match(self, cnf_ref, cnf_op, packet):
		(ref, op) = struct.unpack_from('>LH', packet, 0)
		return (ref == cnf_ref) and (op == cnf_op)

	def __make_ref(self):
		ref = self.next
		self.next += 1
		return ref


class Watch(object):
	EVENT_MODIFIED    = 0
	EVENT_APPEARED    = 1
	EVENT_REPLICATED  = 2
	EVENT_DIMINISHED  = 3
	EVENT_DISAPPEARED = 4

	TYPE_DOC = 0
	TYPE_REV = 1

	def __init__(self, typ, h):
		_checkUuid(h)
		self.__typ = typ
		self.__h = h
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
	FLAG_MOUNTED   = 1
	FLAG_REMOVABLE = 2
	FLAG_SYSTEM    = 4
	FLAG_NET       = 8

	def __init__(self, packet):
		self.__stores = { }
		self.__sysStore = None
		(storeCount,) = struct.unpack_from('>B', packet, 0)
		pos = 1
		for i in range(storeCount):
			(doc, flags, idLen) = struct.unpack_from('>16sLH', packet, pos)
			pos += 22
			id = packet[pos:pos+idLen]
			pos += idLen
			(nameLen,) = struct.unpack_from('>H', packet, pos)
			pos += 2
			name = packet[pos:pos+nameLen]
			pos += nameLen
			self.__stores[id] = (doc, flags, name)
			if flags & Enum.FLAG_SYSTEM:
				self.__sysStore = doc

	def sysStore(self):
		return self.__sysStore

	def allStores(self):
		return self.__stores.keys()

	def isMounted(self, store):
		return self.__testFlag(store, Enum.FLAG_MOUNTED)

	def isSystem(self, store):
		return self.__testFlag(store, Enum.FLAG_SYSTEM)

	def isRemovable(self, store):
		return self.__testFlag(store, Enum.FLAG_REMOVABLE)

	def isNet(self, store):
		return self.__testFlag(store, Enum.FLAG_NET)

	def store(self, doc):
		for (store, info) in self.__stores.items():
			if info[0] == doc:
				return store
		return None

	def doc(self, store):
		return self.__stores[store][0]

	def name(self, store):
		return self.__stores[store][2]

	def __testFlag(self, store, flag):
		if store in self.__stores:
			return (self.__stores[store][1] & flag) != 0
		else:
			return False


class Lookup(object):
	def __init__(self, packet):
		self.__revs = {}
		self.__preRevs = {}
		self.__stores = {}

		# first get the current revisions
		(revCount,) = struct.unpack_from('>B', packet, 0)
		pos = 1
		for i in range(revCount):
			(rev,) = struct.unpack_from('>16s', packet, pos)
			pos += 16
			(storeCount,) = struct.unpack_from('>B', packet, pos)
			pos += 1
			stores = []
			for j in range(storeCount):
				(store,) = struct.unpack_from('>16s', packet, pos)
				stores.append(store)
				self.__stores[store] = (rev, [])
				pos += 16
			self.__revs[rev] = stores

		# now get all the preliminary revisions
		(revCount,) = struct.unpack_from('>B', packet, pos)
		pos += 1
		for i in range(revCount):
			(rev,) = struct.unpack_from('>16s', packet, pos)
			pos += 16
			(storeCount,) = struct.unpack_from('>B', packet, pos)
			pos += 1
			stores = []
			for j in range(storeCount):
				(store,) = struct.unpack_from('>16s', packet, pos)
				stores.append(store)
				self.__stores[store][1].append(rev)
				pos += 16
			self.__preRevs[rev] = stores

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

	def __init__(self, packet):
		# Packet format:
		#   Flags:32
		#   PartsCount:8,  [FourCC:32, Size:64, Hash:128],
		#   ParentCount:8, [Parent:128],
		#   TypeLen:16,    [Type],
		#   CreatorLen:16, [Creator]
		#   DocLinksCount:32, [DocLink:128]
		#   RevLinksCount:32, [RevLink:128]
		(flags, count) = struct.unpack_from('>LB', packet, 0)
		pos = 5
		self.__flags = flags
		self.__parts = {}
		for i in xrange(count):
			(fourcc, size, hash) = struct.unpack_from('>4sQ16s', packet, pos)
			pos += 28
			self.__parts[fourcc] = (size, hash)

		count = struct.unpack_from('>B', packet, pos)[0]
		pos += 1
		self.__parents = []
		for i in xrange(count):
			self.__parents.append(packet[pos:pos+16])
			pos += 16

		self.__mtime = datetime.fromtimestamp(
			struct.unpack_from('>Q', packet, pos)[0] / 1000000.0)
		pos += 8
		(count,) = struct.unpack_from('>H', packet, pos)
		pos += 2
		self.__type = packet[pos:pos+count]
		pos += count
		(count,) = struct.unpack_from('>H', packet, pos)
		pos += 2
		self.__creator = packet[pos:pos+count]
		pos += count

		(count,) = struct.unpack_from('>L', packet, pos)
		pos += 4
		self.__docLinks = []
		for i in xrange(count):
			self.__docLinks.append(packet[pos:pos+16])
			pos += 16
		(count,) = struct.unpack_from('>L', packet, pos)
		pos += 4
		self.__revLinks = []
		for i in xrange(count):
			self.__revLinks.append(packet[pos:pos+16])
			pos += 16

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

	def docLinks(self):
		return self.__docLinks

	def revLinks(self):
		return self.__revLinks


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
			request = struct.pack('>4s4sQL', self.handle, part, pos, chunk)
			reply = self.connector._rpc(_Connector.READ_REQ,
				_Connector.READ_CNF, request)
			data = self.connector._parseResult(reply)
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
			request = struct.pack('>4s4sQ', self.handle, part, pos+i) + reqData
			reply = self.connector._rpc(_Connector.WRITE_REQ,
				_Connector.WRITE_CNF, request)
			self.connector._parseResult(reply)
			i += reqSize
			self._setPos(part, pos+i)


	def writeAll(self, part, data):
		self._setPos(part, 0)
		self.truncate(part)
		self.write(part, data)

	def truncate(self, part):
		if not self.active:
			raise IOError('Handle expired')
		request = struct.pack('>4s4sQ', self.handle, part, self._getPos(part))
		reply = self.connector._rpc(_Connector.TRUNC_REQ,
			_Connector.TRUNC_CNF, request)
		self.connector._parseResult(reply)

	def commit(self):
		if not self.active:
			raise IOError('Handle expired')
		reply = self.connector._rpc(_Connector.COMMIT_REQ,
			_Connector.COMMIT_CNF, self.handle)
		rev = self.connector._parseResult(reply)
		self.rev = rev

	def suspend(self):
		if not self.active:
			raise IOError('Handle expired')
		reply = self.connector._rpc(_Connector.SUSPEND_REQ,
			_Connector.SUSPEND_CNF, self.handle)
		rev = self.connector._parseResult(reply)
		self.rev = rev

	def close(self):
		if self.active:
			self.active = False
			reply = self.connector._rpc(_Connector.CLOSE_REQ,
				_Connector.CLOSE_CNF, self.handle)
			self.connector._parseResult(reply)
		else:
			raise IOError('Handle expired')

	def getFlags(self):
		if not self.active:
			raise IOError('Handle expired')
		reply = self.connector._rpc(_Connector.GET_FLAGS_REQ,
			_Connector.GET_FLAGS_CNF, self.handle)
		result = self.connector._parseResult(reply)
		(flags,) = struct.unpack_from('>L', result, 0)
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
		request = self.handle + struct.pack('>L', flags)
		reply = self.connector._rpc(_Connector.SET_FLAGS_REQ,
			_Connector.SET_FLAGS_CNF, request)
		self.connector._parseResult(reply)

	def getType(self):
		if not self.active:
			raise IOError('Handle expired')
		reply = self.connector._rpc(_Connector.GET_TYPE_REQ,
			_Connector.GET_TYPE_CNF, self.handle)
		result = self.connector._parseResult(reply)
		(length,) = struct.unpack_from('>H', result, 0)
		return result[2:2+length]

	def setType(self, uti):
		if not self.active:
			raise IOError('Handle expired')
		request = self.handle + _encodeString(uti)
		reply = self.connector._rpc(_Connector.SET_TYPE_REQ,
			_Connector.SET_TYPE_CNF, request)
		self.connector._parseResult(reply)

	def getParents(self):
		if not self.active:
			raise IOError('Handle expired')
		reply = self.connector._rpc(_Connector.GET_PARENTS_REQ,
			_Connector.GET_PARENTS_CNF, self.handle)
		result = self.connector._parseResult(reply)
		parents = []
		(count,) = struct.unpack_from('>B', result, 0)
		for i in range(count):
			parents.append(result[i*16+1:i*16+17])
		return parents

	def merge(self, store, rev, depth=0):
		_checkUuid(store)
		_checkUuid(rev)
		if not self.active:
			raise IOError('Handle expired')
		request = self.handle + store + rev + struct.pack('>Q', depth)
		reply = self.connector._rpc(_Connector.MERGE_REQ, _Connector.MERGE_CNF,
			request)
		self.connector._parseResult(reply)

	def rebase(self, parent):
		_checkUuid(parent)
		if not self.active:
			raise IOError('Handle expired')
		request = self.handle + parent
		reply = self.connector._rpc(_Connector.REBASE_REQ,
			_Connector.REBASE_CNF, request)
		self.connector._parseResult(reply)

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

