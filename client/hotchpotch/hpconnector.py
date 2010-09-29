# vim: set fileencoding=utf-8 :
#
# Hotchpotch
# Copyright (C) 2010  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
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

from PyQt4 import QtCore, QtNetwork
from datetime import datetime
import struct
import atexit

_errorCodes = {
	1 : 'ECONFLICT',
	2 : 'ENOENT',
	3 : 'EINVAL',
	4 : 'EBADF',
	5 : 'EAMBIG',
	6 : 'ENOSYS'
}


def checkUuid(uuid):
	valid = (uuid.__class__ == str) and (len(uuid) == 16)
	if not valid:
		raise IOError('Invalid UUID: '+str(uuid.__class__)+' '+str(uuid))
	return True


def encodeUuidList(uuids):
	packet = struct.pack('>B', len(uuids))
	for rev in uuids:
		checkUuid(rev)
		packet += rev
	return packet


def encodeString(string):
	if isinstance(string, str):
		return struct.pack('>H', len(string)) + string
	elif isinstance(string, unicode):
		encStr = string.encode('utf-8')
		return struct.pack('>H', len(encStr)) + encStr

def raiseError(error):
	if error == 0:
		return True
	elif error in _errorCodes:
		raise IOError(_errorCodes[error])
	else:
		raise IOError('Unknown error')


class ConnError(Exception):
    """Base class for exceptions in this module."""
    pass


class _HpConnector(object):
	INIT_REQ            = 0x0000
	INIT_CNF            = 0x0001
	ENUM_REQ            = 0x0010
	ENUM_CNF            = 0x0011
	LOOKUP_REQ          = 0x0020
	LOOKUP_CNF          = 0x0021
	STAT_REQ            = 0x0030
	STAT_CNF            = 0x0031
	PEEK_REQ            = 0x0040
	PEEK_CNF            = 0x0041
	CREATE_REQ          = 0x0050
	CREATE_CNF          = 0x0051
	FORK_REQ            = 0x0060
	FORK_CNF            = 0x0061
	UPDATE_REQ          = 0x0070
	UPDATE_CNF          = 0x0071
	RESUME_REQ          = 0x0080
	RESUME_CNF          = 0x0081
	READ_REQ            = 0x0090
	READ_CNF            = 0x0091
	TRUNC_REQ           = 0x00A0
	TRUNC_CNF           = 0x00A1
	WRITE_REQ           = 0x00B0
	WRITE_CNF           = 0x00B1
	GET_TYPE_REQ        = 0x00C0
	GET_TYPE_CNF        = 0x00C1
	SET_TYPE_REQ        = 0x00D0
	SET_TYPE_CNF        = 0x00D1
	GET_PARENTS_REQ     = 0x00E0
	GET_PARENTS_CNF     = 0x00E1
	SET_PARENTS_REQ     = 0x00F0
	SET_PARENTS_CNF     = 0x00F1
	COMMIT_REQ          = 0x0100
	COMMIT_CNF          = 0x0101
	SUSPEND_REQ         = 0x0110
	SUSPEND_CNF         = 0x0111
	ABORT_REQ           = 0x0120
	ABORT_CNF           = 0x0121
	WATCH_ADD_REQ       = 0x0130
	WATCH_ADD_CNF       = 0x0131
	WATCH_REM_REQ       = 0x0140
	WATCH_REM_CNF       = 0x0141
	FORGET_REQ          = 0x0150
	FORGET_CNF          = 0x0151
	DELETE_DOC_REQ      = 0x0160
	DELETE_DOC_CNF      = 0x0161
	DELETE_REV_REQ      = 0x0170
	DELETE_REV_CNF      = 0x0171
	SYNC_DOC_REQ        = 0x0180
	SYNC_DOC_CNF        = 0x0181
	REPLICATE_DOC_REQ   = 0x0190
	REPLICATE_DOC_CNF   = 0x0191
	REPLICATE_REV_REQ   = 0x01A0
	REPLICATE_REV_CNF   = 0x01A1
	MOUNT_REQ           = 0x01B0
	MOUNT_CNF           = 0x01B1
	UNMOUNT_REQ         = 0x01C0
	UNMOUNT_CNF         = 0x01C1
	WATCH_IND           = 0x0002
	PROGRESS_IND        = 0x0012


	def __init__(self, host = '127.0.0.1', port = 4567):
		self.socket = QtNetwork.QTcpSocket()
		QtCore.QObject.connect(self.socket, QtCore.SIGNAL("readyRead()"),
				self.__readReady)
		self.socket.connectToHost(host, port)
		if not self.socket.waitForConnected(1000):
			raise IOError("Could not connect to server!")
		self.next = 0
		self.queue = []
		self.buf = ''
		self.watchHandlers = {}
		self.watchIndications = []
		self.progressHandlers = []
		self.progressIndications = []
		self.recursion = 0

		try:
			reply = self._rpc(_HpConnector.INIT_REQ, _HpConnector.INIT_CNF,
				struct.pack('>L', 0))
			(version, self.maxPacketSize) = struct.unpack_from('>LL',
				self._parseDirectResult(reply), 0)
			if version != 0:
				raise IOError("Unsupported protocol version!")
		except:
			self.socket.disconnectFromHost()
			raise

	def enum(self):
		reply = self._rpc(_HpConnector.ENUM_REQ, _HpConnector.ENUM_CNF)
		return HpEnum(reply)

	def lookup(self, doc, stores=[]):
		checkUuid(doc)
		request = doc + encodeUuidList(stores)
		reply = self._rpc(_HpConnector.LOOKUP_REQ, _HpConnector.LOOKUP_CNF, request)
		return HpLookup(reply)

	def stat(self, rev, stores=[]):
		checkUuid(rev)
		request = rev + encodeUuidList(stores)
		reply = self._rpc(_HpConnector.STAT_REQ, _HpConnector.STAT_CNF, request)
		(True, stat) = self._parseBrokerResult(reply)
		return HpStat(stat)

	def peek(self, rev, stores=[]):
		checkUuid(rev)
		request = rev + encodeUuidList(stores)
		reply = self._rpc(_HpConnector.PEEK_REQ, _HpConnector.PEEK_CNF, request)
		(True, handle) = self._parseBrokerResult(reply)
		return HpHandle(self, handle, None, rev)

	def create(self, typ, creator, stores=[]):
		request = encodeString(typ) + encodeString(creator) + encodeUuidList(stores)
		reply = self._rpc(_HpConnector.CREATE_REQ, _HpConnector.CREATE_CNF, request)
		(True, reply) = self._parseBrokerResult(reply)
		(handle, doc) = struct.unpack('>4s16s', reply)
		return HpHandle(self, handle, doc, None)

	def fork(self, rev, creator, stores=[]):
		checkUuid(rev)
		request = rev + encodeString(creator) + encodeUuidList(stores)
		reply = self._rpc(_HpConnector.FORK_REQ, _HpConnector.FORK_CNF, request)
		(True, reply) = self._parseBrokerResult(reply)
		(handle, doc) = struct.unpack('>4s16s', reply)
		return HpHandle(self, handle, doc, rev)

	def update(self, doc, rev, creator='', stores=[]):
		checkUuid(doc)
		checkUuid(rev)
		request = doc + rev + encodeString(creator) + encodeUuidList(stores)
		reply = self._rpc(_HpConnector.UPDATE_REQ, _HpConnector.UPDATE_CNF, request)
		(True, handle) = self._parseBrokerResult(reply)
		return HpHandle(self, handle, doc, rev)

	def resume(self, doc, rev, creator='', stores=[]):
		checkUuid(doc)
		checkUuid(rev)
		request = doc + rev + encodeString(creator) + encodeUuidList(stores)
		reply = self._rpc(_HpConnector.RESUME_REQ, _HpConnector.RESUME_CNF, request)
		(True, handle) = self._parseBrokerResult(reply)
		return HpHandle(self, handle, doc, rev)

	def watch(self, w):
		if w._incWatchRef() == 1:
			(typ, h) = ref = w._getRef()
			if ref in self.watchHandlers:
				self.watchHandlers[ref].append(w)
			else:
				reply = self._rpc(_HpConnector.WATCH_ADD_REQ,
					_HpConnector.WATCH_ADD_CNF, struct.pack('>B16s', typ, h))
				self._parseDirectResult(reply)
				self.watchHandlers[ref] = [w]

	def unwatch(self, w):
		if w._decWatchRef() == 0:
			(typ, h) = ref = w._getRef()
			self.watchHandlers[ref].remove(w)
			if self.watchHandlers[ref] == []:
				reply = self._rpc(_HpConnector.WATCH_REM_REQ,
					_HpConnector.WATCH_REM_CNF, struct.pack('>B16s', typ, h))
				self._parseDirectResult(reply)
				del self.watchHandlers[ref]

	def forget(self, doc, rev, stores=[]):
		checkUuid(doc)
		checkUuid(rev)
		request = doc + rev + encodeUuidList(stores)
		reply = self._rpc(_HpConnector.FORGET_REQ, _HpConnector.FORGET_CNF,
			request)
		(result, empty) = self._parseBrokerResult(reply)
		return result

	def delete_doc(self, doc, rev, stores=[]):
		checkUuid(doc)
		checkUuid(rev)
		request = doc + rev + encodeUuidList(stores)
		reply = self._rpc(_HpConnector.DELETE_DOC_REQ,
			_HpConnector.DELETE_DOC_CNF, request)
		(result, empty) = self._parseBrokerResult(reply)
		return result

	def delete_rev(self, rev, stores=[]):
		checkUuid(rev)
		request = rev + encodeUuidList(stores)
		reply = self._rpc(_HpConnector.DELETE_REV_REQ,
			_HpConnector.DELETE_REV_CNF, request)
		(result, empty) = self._parseBrokerResult(reply)
		return result

	def sync(self, doc, stores=[]):
		checkUuid(doc)
		request = doc + encodeUuidList(stores)
		reply = self._rpc(_HpConnector.SYNC_DOC_REQ, _HpConnector.SYNC_DOC_CNF,
			request)
		(result, empty) = self._parseBrokerResult(reply)
		return result

	def replicate_doc(self, doc, depth, stores):
		checkUuid(doc)
		request = struct.pack('>16sQ', doc, depth) + encodeUuidList(stores)
		reply = self._rpc(_HpConnector.REPLICATE_DOC_REQ,
			_HpConnector.REPLICATE_DOC_CNF, request)
		(result, empty) = self._parseBrokerResult(reply)
		return result

	def replicate_rev(self, rev, depth, stores):
		checkUuid(rev)
		request = struct.pack('>16sQ', rev, depth) + encodeUuidList(stores)
		reply = self._rpc(_HpConnector.REPLICATE_REV_REQ,
			_HpConnector.REPLICATE_REV_CNF, request)
		(result, empty) = self._parseBrokerResult(reply)
		return result

	def mount(self, store):
		reply = self._rpc(_HpConnector.MOUNT_REQ, _HpConnector.MOUNT_CNF,
			encodeString(store))
		self._parseDirectResult(reply)
		return True

	def unmount(self, store):
		reply = self._rpc(_HpConnector.UNMOUNT_REQ, _HpConnector.UNMOUNT_CNF,
			encodeString(store))
		self._parseDirectResult(reply)
		return True

	def flush(self):
		while self.socket.flush():
			self.socket.waitForBytesWritten(10000)

	def regProgressHandler(self, handler):
		self.progressHandlers.append(handler)

	def unregProgressHandler(self, handler):
		self.progressHandlers.remove(handler)

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

	def _parseBrokerResult(self, reply):
		(result,) = struct.unpack_from('>B', reply, 0)
		if result == 0:
			return (True, reply[1:])
		else:
			pos = 1
			if result != 1:
				(error,) = struct.unpack_from('>L', reply, pos)
				pos += 4
			(count,) = struct.unpack_from('>B', reply, pos)
			errors = []
			pos += 1
			for i in range(count):
				(store, error) = struct.unpack_from('>16sL', reply, pos)
				pos += 20
				errors.append({'store': store, 'error': error})
			if result == 1:
				return (True, reply[pos:])
			elif result == 2:
				return (False, reply[pos:])
			else:
				raiseError(error)

	def _parseDirectResult(self, reply):
		(error,) = struct.unpack_from('>L', reply, 0)
		raiseError(error)
		return reply[4:]

	# private functions

	def __send(self, packet):
		raw = struct.pack('>H', len(packet)) + packet
		if self.socket.write(raw) == -1:
			raise IOError("Could not send request to server!")

	def __readReady(self):
		# increase recursion depth
		self.recursion += 1

		# unpack incoming packets
		self.buf = self.buf + str(self.socket.readAll())
		while len(self.buf) > 2:
			expect = struct.unpack_from('>H', self.buf, 0)[0] + 2
			if expect <= len(self.buf):
				packet = self.buf[2:expect]
				self.buf = self.buf[expect:]
				# TODO: discard invalid packets

				# immediately remove indications
				(ref, ind) = struct.unpack_from('>LH', packet, 0)
				if ind == _HpConnector.WATCH_IND:
					self.watchIndications.append(struct.unpack('>BB16s', packet[6:]))
				elif ind == _HpConnector.PROGRESS_IND:
					self.progressIndications.append(packet[6:])
				else:
					self.queue.append(packet)
			else:
				break

		# dispatch received indications if not in recursion
		if self.recursion <= 1:
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
						i.triggered(event)

				progressIndications = self.progressIndications[:]
				self.progressIndications = []
				for ind in progressIndications:
					dispatched = True
					handlers = self.progressHandlers[:]
					(typ, prog) = struct.unpack_from('>BH', ind, 0)
					for handler in handlers:
						handler(typ, prog, ind[3:])

		# decrease nesting level
		self.recursion -= 1

	def __poll(self, ref, cnf_op):
		# loop until we've received the answer
		while True:
			for packet in self.queue:
				if self.__match(ref, cnf_op, packet):
					self.queue.remove(packet)
					return packet[6:]

			if not self.socket.waitForReadyRead(-1):
				raise IOError("Error while waiting for data from server!")
			self.__readReady()

	def __match(self, cnf_ref, cnf_op, packet):
		(ref, op) = struct.unpack_from('>LH', packet, 0)
		return (ref == cnf_ref) and (op == cnf_op)

	def __make_ref(self):
		ref = self.next
		self.next += 1
		return ref


class HpWatch(object):
	CAUSE_MODIFIED    = 0
	CAUSE_APPEARED    = 1
	CAUSE_REPLICATED  = 2
	CAUSE_DIMINISHED  = 3
	CAUSE_DISAPPEARED = 4

	TYPE_DOC = 0
	TYPE_REV = 1

	def __init__(self, typ, h):
		checkUuid(h)
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


class HpEnum(object):
	FLAG_MOUNTED   = 1
	FLAG_REMOVABLE = 2
	FLAG_SYSTEM    = 4

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
			if flags & HpEnum.FLAG_SYSTEM:
				self.__sysStore = doc

	def sysStore(self):
		return self.__sysStore

	def allStores(self):
		return self.__stores.keys()

	def isMounted(self, store):
		if store in self.__stores:
			return (self.__stores[store][1] & HpEnum.FLAG_MOUNTED) != 0
		else:
			return False

	def isSystem(self, store):
		if store in self.__stores:
			return (self.__stores[store][1] & HpEnum.FLAG_SYSTEM) != 0
		else:
			return False

	def isRemovable(self, store):
		if store in self.__stores:
			return (self.__stores[store][1] & HpEnum.FLAG_REMOVABLE) != 0
		else:
			return False

	def store(self, doc):
		for (store, info) in self.__stores.items():
			if info[0] == doc:
				return store
		return None

	def doc(self, store):
		return self.__stores[store][0]

	def name(self, store):
		return self.__stores[store][2]


class HpLookup(object):
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


class HpStat(object):
	def __init__(self, packet):
		# Packet format:
		#   Flags:32
		#   PartsCount:8,  [FourCC:32, Size:64, Hash:128],
		#   ParentCount:8, [Parent:128],
		#   VolumeCount:8, [Volume:128],
		#   TypeLen:16,    [Type],
		#   CreatorLen:16, [Creator]
		(flags, count) = struct.unpack_from('>LB', packet, 0)
		pos = 5
		self.__flags = flags
		self.__parts = {}
		for i in range(count):
			(fourcc, size, hash) = struct.unpack_from('>4sQ16s', packet, pos)
			pos += 28
			self.__parts[fourcc] = (size, hash)

		count = struct.unpack_from('>B', packet, pos)[0]
		pos += 1
		self.__parents = []
		for i in range(count):
			self.__parents.append(packet[pos:pos+16])
			pos += 16

		count = struct.unpack_from('>B', packet, pos)[0]
		pos += 1
		self.__volumes = []
		for i in range(count):
			self.__volumes.append(packet[pos:pos+16])
			pos += 16

		self.__mtime = datetime.fromtimestamp(struct.unpack_from('>Q', packet, pos)[0])
		pos += 8
		(count,) = struct.unpack_from('>H', packet, pos)
		pos += 2
		self.__type = packet[pos:pos+count]
		pos += count
		(count,) = struct.unpack_from('>H', packet, pos)
		pos += 2
		self.__creator = packet[pos:pos+count]

	def size(self, part):
		return self.__parts[part][0]

	def hash(self, part):
		return self.__parts[part][1]

	def parts(self):
		return self.__parts.keys()

	def parents(self):
		return self.__parents

	def volumes(self):
		return self.__volumes

	def mtime(self):
		return self.__mtime

	def type(self):
		return self.__type

	def creator(self):
		return self.__creator

	def preliminary(self):
		return (self.__flags & 0x10) != 0


class HpHandle(object):
	def __init__(self, connector, handle, doc, rev):
		self.__pos = { }
		self.connector = connector
		self.handle = handle
		self.doc = doc
		self.rev = rev
		self.active = True

	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		if self.active:
			self.abort()
		return False

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
			raise ConnError('Not implemented')
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
			reply = self.connector._rpc(_HpConnector.READ_REQ,
				_HpConnector.READ_CNF, request)
			(True, data) = self.connector._parseBrokerResult(reply)
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
			reply = self.connector._rpc(_HpConnector.WRITE_REQ,
				_HpConnector.WRITE_CNF, request)
			(True, empty) = self.connector._parseBrokerResult(reply)
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
		reply = self.connector._rpc(_HpConnector.TRUNC_REQ,
			_HpConnector.TRUNC_CNF, request)
		(True, empty) = self.connector._parseBrokerResult(reply)

	def commit(self, retry=False):
		if not self.active:
			raise IOError('Handle expired')
		self.active = False
		reply = self.connector._rpc(_HpConnector.COMMIT_REQ,
			_HpConnector.COMMIT_CNF, self.handle)
		(result, rev) = self.connector._parseBrokerResult(reply)
		if result:
			self.rev = rev
			return True
		else:
			self.active = True
			if retry:
				return False
			else:
				self.abort()
				raise IOError('Revision conflict')

	def suspend(self):
		if not self.active:
			raise IOError('Handle expired')
		self.active = False
		reply = self.connector._rpc(_HpConnector.SUSPEND_REQ,
			_HpConnector.SUSPEND_CNF, self.handle)
		(result, rev) = self.connector._parseBrokerResult(reply)
		self.rev = rev

	def abort(self):
		if self.active:
			self.active = False
			reply = self.connector._rpc(_HpConnector.ABORT_REQ,
				_HpConnector.ABORT_CNF, self.handle)
			self.connector._parseBrokerResult(reply)
		else:
			raise IOError('Handle expired')

	def getType(self):
		if not self.active:
			raise IOError('Handle expired')
		reply = self.connector._rpc(_HpConnector.GET_TYPE_REQ,
			_HpConnector.GET_TYPE_CNF, self.handle)
		(True, result) = self.connector._parseBrokerResult(reply)
		(length,) = struct.unpack_from('>H', result, 0)
		return result[2:2+length]

	def setType(self, uti):
		if not self.active:
			raise IOError('Handle expired')
		request = self.handle + encodeString(uti)
		reply = self.connector._rpc(_HpConnector.SET_TYPE_REQ,
			_HpConnector.SET_TYPE_CNF, request)
		(True, empty) = self.connector._parseBrokerResult(reply)

	def getParents(self):
		if not self.active:
			raise IOError('Handle expired')
		reply = self.connector._rpc(_HpConnector.GET_PARENTS_REQ,
			_HpConnector.GET_PARENTS_CNF, self.handle)
		(True, result) = self.connector._parseBrokerResult(reply)
		parents = []
		(count,) = struct.unpack_from('>B', result, 0)
		for i in range(count):
			parents.append(result[i*16+1:i*16+17])
		return parents

	def setParents(self, parents):
		if not self.active:
			raise IOError('Handle expired')
		request = self.handle + encodeUuidList(parents)
		reply = self.connector._rpc(_HpConnector.SET_PARENTS_REQ,
			_HpConnector.SET_PARENTS_CNF, request)
		(True, empty) = self.connector._parseBrokerResult(reply)

	def getDoc(self):
		return self.doc

	def getRev(self):
		return self.rev

_connector = None

def __FlushConnection():
	global _connector
	if _connector:
		_connector.flush()

def HpConnector():
	global _connector
	if not _connector:
		_connector = _HpConnector()
		atexit.register(__FlushConnection)
	return _connector

