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
from struct import *
from datetime import datetime
import sys
import atexit


def checkGuid(guid):
	valid = (guid.__class__ == str) and (len(guid) == 16)
	if not valid:
		raise IOError('Invalid GUID: '+str(guid.__class__)+' '+str(guid))
	return True


def genericReply(reply, body):
	if reply == _HpConnector.GENERIC_CNF:
		(code,) = unpack('<L', body)
		if code == 0:
			return True
		elif code == 1:
			return False # Recoverable conflict (Handle still valid)
		elif code == 2:
			raise IOError('ENOENT')
		elif code == 3:
			raise IOError('EINVAL')
		elif code == 4:
			raise IOError('EMULTIPLE')
		elif code == 5:
			raise IOError('EBADF')
		else:
			raise IOError('Unknown error')
	else:
		raise ConnError('Invalid server reply')


def encodeUuidList(uuids):
	packet = pack('B', len(uuids))
	for rev in uuids:
		checkGuid(rev)
		packet += rev
	return packet


class ConnError(Exception):
    """Base class for exceptions in this module."""
    pass


class _HpConnector(object):
	GENERIC_CNF         = 0x0001
	ENUM_REQ            = 0x0010
	ENUM_CNF            = 0x0011
	LOOKUP_REQ          = 0x0020
	LOOKUP_CNF          = 0x0021
	STAT_REQ            = 0x0030
	STAT_CNF            = 0x0031
	PEEK_REQ            = 0x0040
	PEEK_CNF            = 0x0041
	FORK_REQ            = 0x0050
	FORK_CNF            = 0x0051
	UPDATE_REQ          = 0x0060
	UPDATE_CNF          = 0x0061
	READ_REQ            = 0x0070
	READ_CNF            = 0x0071
	WRITE_REQ           = 0x0080
	TRUNC_REQ           = 0x0090
	COMMIT_REQ          = 0x00A0
	COMMIT_CNF          = 0x00A1
	ABORT_REQ           = 0x00B0
	WATCH_ADD_REQ       = 0x00C0
	WATCH_REM_REQ       = 0x00D0
	WATCH_IND           = 0x00E2
	DELETE_DOC_REQ      = 0x00F0
	DELETE_REV_REQ      = 0x0100
	SYNC_DOC_REQ        = 0x0110
	REPLICATE_DOC_REQ   = 0x0120
	REPLICATE_REV_REQ   = 0x0130
	MOUNT_REQ           = 0x0140
	UNMOUNT_REQ         = 0x0150
	PROGRESS_IND        = 0x0162


	def __init__(self, host = '127.0.0.1', port = 4567):
		self.socket = QtNetwork.QTcpSocket()
		QtCore.QObject.connect(self.socket, QtCore.SIGNAL("readyRead()"),
				self.__readReady)
		self.socket.connectToHost(host, port)
		if not self.socket.waitForConnected(1000):
			print "Could not connect to server!"
			sys.exit(2)
		self.next = 0
		self.queue = []
		self.buf = ''
		self.watchHandlers = {}
		self.watchIndications = []
		self.progressHandlers = []
		self.progressIndications = []
		self.recursion = 0

	def enum(self):
		(reply, body) = self._rpc(_HpConnector.ENUM_REQ)
		if reply == _HpConnector.ENUM_CNF:
			return HpEnum(body)
		else:
			raise ConnError('Invalid server reply')

	def lookup(self, doc):
		checkGuid(doc)
		(reply, body) = self._rpc(_HpConnector.LOOKUP_REQ, doc)
		if reply == _HpConnector.LOOKUP_CNF:
			return HpLookup(body)
		else:
			raise ConnError('Invalid server reply')

	def stat(self, rev):
		checkGuid(rev)
		(reply, body) = self._rpc(_HpConnector.STAT_REQ, rev)
		if reply == _HpConnector.STAT_CNF:
			return HpStat(body)
		else:
			return genericReply(reply, body)

	def peek(self, rev, stores=[]):
		checkGuid(rev)
		body = rev + encodeUuidList(stores)
		(reply, cookie) = self._rpc(_HpConnector.PEEK_REQ, body)
		if reply == _HpConnector.PEEK_CNF:
			return HpHandle(self, cookie, None, rev)
		else:
			return genericReply(reply, cookie)

	def fork(self, rev=None, uti='', stores=[]):
		if rev:
			checkGuid(rev)
		else:
			rev = '\x00'*16
		body = rev + encodeUuidList(stores) + uti
		(reply, raw) = self._rpc(_HpConnector.FORK_REQ, body)
		if reply == _HpConnector.FORK_CNF:
			(cookie, doc) = unpack('<4s16s', raw)
			return HpHandle(self, cookie, doc, rev)
		else:
			return genericReply(reply, raw)

	def update(self, doc, rev, uti='', stores=[]):
		checkGuid(doc)
		checkGuid(rev)
		body = doc + rev + encodeUuidList(stores) + uti
		(reply, raw) = self._rpc(_HpConnector.UPDATE_REQ, body)
		if reply == _HpConnector.UPDATE_CNF:
			return HpHandle(self, raw, doc, rev)
		else:
			return genericReply(reply, raw)

	def watch(self, w):
		if w._incWatchRef() == 1:
			(typ, h) = ref = w._getRef()
			if ref in self.watchHandlers:
				self.watchHandlers[ref].append(w)
			else:
				self.watchHandlers[ref] = [w]
				self._rpc(_HpConnector.WATCH_ADD_REQ, pack('B16s', typ, h))

	def unwatch(self, w):
		if w._decWatchRef() == 0:
			(typ, h) = ref = w._getRef()
			self.watchHandlers[ref].remove(w)
			if self.watchHandlers[ref] == []:
				del self.watchHandlers[ref]
				self._rpc(_HpConnector.WATCH_REM_REQ, pack('B16s', typ, h))

	def delete_doc(self, doc, stores=[]):
		checkGuid(doc)
		body = doc + encodeUuidList(stores)
		(reply, code) = self._rpc(_HpConnector.DELETE_DOC_REQ, body)
		return genericReply(reply, code)

	def delete_rev(self, rev, stores=[]):
		checkGuid(rev)
		body = rev + encodeUuidList(stores)
		(reply, code) = self._rpc(_HpConnector.DELETE_REV_REQ, body)
		return genericReply(reply, code)

	def sync(self, doc, stores=[]):
		checkGuid(doc)
		body = doc + encodeUuidList(stores)
		(reply, code) = self._rpc(_HpConnector.SYNC_DOC_REQ, body)
		return genericReply(reply, code)

	def replicate_doc(self, doc, stores, history=True):
		checkGuid(doc)
		body = pack('16sB', doc, int(history)) + encodeUuidList(stores)
		(reply, code) = self._rpc(_HpConnector.REPLICATE_DOC_REQ, body)
		return genericReply(reply, code)

	def replicate_rev(self, rev, stores, history=True):
		checkGuid(rev)
		body = pack('16sB', rev, int(history)) + encodeUuidList(stores)
		(reply, code) = self._rpc(_HpConnector.REPLICATE_REV_REQ, body)
		return genericReply(reply, code)

	def mount(self, store):
		(reply, code) = self._rpc(_HpConnector.MOUNT_REQ, store)
		return genericReply(reply, code)

	def unmount(self, store):
		(reply, code) = self._rpc(_HpConnector.UNMOUNT_REQ, store)
		return genericReply(reply, code)

	def flush(self):
		while self.socket.flush():
			self.socket.waitForBytesWritten(10000)

	def regProgressHandler(self, handler):
		self.progressHandlers.append(handler)

	def unregProgressHandler(self, handler):
		self.progressHandlers.remove(handler)

	# protected functions

	def _rpc(self, op, request = ''):
		ref = self.__make_ref()
		raw_request = pack('<HL', op, ref) + request
		self.__send(raw_request)
		raw_reply = self.__poll(ref)
		reply = unpack_from('<H', raw_reply)[0]
		return (reply, raw_reply[6:])

	def _ind(self, op, body = ''):
		ref = self.__make_ref()
		raw_ind = pack('<HL', op, ref) + body
		self.__send(raw_ind)

	# private functions

	def __send(self, packet):
		raw = pack('>H', len(packet)) + packet
		if self.socket.write(raw) == -1:
			print "Could not send request to server!"
			sys.exit(2)

	def __readReady(self):
		# increase recursion depth
		self.recursion += 1

		# unpack incoming packets
		self.buf = self.buf + str(self.socket.readAll())
		while len(self.buf) > 2:
			expect = unpack_from('>H', self.buf, 0)[0] + 2
			if expect <= len(self.buf):
				packet = self.buf[2:expect]
				self.buf = self.buf[expect:]
				# TODO: discard invalid packets

				# immediately remove indications
				(ind, ref) = unpack_from('<HL', packet, 0)
				if ind == _HpConnector.WATCH_IND:
					self.watchIndications.append(unpack_from('BB16s', packet, 6))
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
				for (cause, typ, h) in watchIndications:
					# make explicit copy as watches may get modified by callouts!
					dispatched = True
					matches = self.watchHandlers.get((typ, h), [])[:]
					for i in matches:
						i.triggered(cause)

				progressIndications = self.progressIndications[:]
				self.progressIndications = []
				for ind in progressIndications:
					dispatched = True
					handlers = self.progressHandlers[:]
					(typ, prog) = unpack_from('<BH', ind, 0)
					for handler in handlers:
						handler(typ, prog, ind[3:])

		# decrease nesting level
		self.recursion -= 1

	def __poll(self, ref):
		# loop until we've received the answer
		while True:
			for packet in self.queue:
				if self.__match(ref, packet):
					self.queue.remove(packet)
					return packet

			if not self.socket.waitForReadyRead(-1):
				print "Error while waiting for data from server!"
				sys.exit(2)
			self.__readReady()

	def __match(self, ref, packet):
		return unpack_from('<L', packet, 2)[0] == ref

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

	TYPE_UUID = 0
	TYPE_REV  = 1

	def __init__(self, typ, h):
		checkGuid(h)
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
		self.__sysStoreGuid = None
		(storeCount,) = unpack_from('B', packet, 0)
		pos = 1
		for i in range(storeCount):
			(guid, flags, idLen) = unpack_from('<16sLH', packet, pos)
			pos += 22
			id = packet[pos:pos+idLen]
			pos += idLen
			(nameLen,) = unpack_from('<H', packet, pos)
			pos += 2
			name = packet[pos:pos+nameLen]
			pos += nameLen
			self.__stores[id] = (guid, flags, name)
			if flags & HpEnum.FLAG_SYSTEM:
				self.__sysStoreGuid = guid

	def sysStoreGuid(self):
		return self.__sysStoreGuid

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

	def store(self, guid):
		for (store, info) in self.__stores.items():
			if info[0] == guid:
				return store
		return None

	def guid(self, store):
		return self.__stores[store][0]

	def name(self, store):
		return self.__stores[store][2]


class HpLookup(object):
	def __init__(self, packet):
		self.__revs = {}
		self.__stores = {}
		(revCount,) = unpack_from('B', packet, 0)
		pos = 1
		for i in range(revCount):
			(rev,) = unpack_from('<16s', packet, pos)
			pos += 16
			(storeCount,) = unpack_from('B', packet, pos)
			pos += 1
			stores = []
			for j in range(storeCount):
				(store,) = unpack_from('<16s', packet, pos)
				stores.append(store)
				self.__stores[store] = rev
				pos += 16
			self.__revs[rev] = stores

	def revs(self):
		return self.__revs.keys()

	def rev(self, store):
		return self.__stores[store]

	def stores(self, rev = None):
		if rev:
			return self.__revs[rev]
		else:
			return self.__stores.keys()


class HpStat(object):
	def __init__(self, packet):
		# Packet format:
		#   Flags:32
		#   PartsCount:8,  [FourCC:32, Size:64, Hash:128],
		#   ParentCount:8, [Parent:128],
		#   VolumeCount:8, [Volume:128],
		#   Mtime:64, Uti...
		(flags, count) = unpack_from('<LB', packet, 0)
		pos = 5
		self.__flags = flags
		self.__parts = {}
		for i in range(count):
			(fourcc, size, hash) = unpack_from('<4sQ16s', packet, pos)
			pos += 28
			self.__parts[fourcc] = (size, hash)

		count = unpack_from('B', packet, pos)[0]
		pos += 1
		self.__parents = []
		for i in range(count):
			self.__parents.append(packet[pos:pos+16])
			pos += 16

		count = unpack_from('B', packet, pos)[0]
		pos += 1
		self.__volumes = []
		for i in range(count):
			self.__volumes.append(packet[pos:pos+16])
			pos += 16

		self.__mtime = datetime.fromtimestamp(unpack_from('<Q', packet, pos)[0])
		pos += 8
		self.__uti = packet[pos:]

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

	def uti(self):
		return self.__uti

	def preliminary(self):
		return (self.__flags & 0x10) != 0


class HpHandle(object):
	def __init__(self, connector, cookie, doc, rev):
		self.__pos = { }
		self.connector = connector
		self.cookie = cookie
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
			raise IOError('Document already closed!')
		result = ''
		pos = self._getPos(part)
		while length > 0:
			if length > 2048:
				chunk = 2048
			else:
				chunk = length
			request = pack('<4s4sQL', self.cookie, part, pos, chunk)
			(reply, data) = self.connector._rpc(_HpConnector.READ_REQ, request)
			if reply == _HpConnector.READ_CNF:
				size = len(data)
				result = result + data
				pos = pos + size
				if size < chunk:
					break
			else:
				genericReply(reply, data)
		self._setPos(part, pos)
		return result

	def readAll(self, part):
		result = ''
		oldPos = self._getPos(part)
		self._setPos(part, 0)
		while True:
			chunk = self.read(part, 0x20000)
			result = result + chunk
			if len(chunk) < 0x20000:
				break
		self._setPos(part, oldPos)
		return result

	def write(self, part, data):
		if not self.active:
			raise IOError('Document already immutable!')

		pos = self._getPos(part)
		i = 0
		length = len(data)
		while length >= i:
			request = pack('<4s4sQ', self.cookie, part, pos+i) + data[i:i+2048]
			(reply, empty) = self.connector._rpc(_HpConnector.WRITE_REQ, request)
			genericReply(reply, empty)
			i += 2048

		self._setPos(part, pos+length)

	def writeAll(self, part, data):
		self._setPos(part, 0)
		self.write(part, data)
		self.truncate(part)

	def truncate(self, part):
		if not self.active:
			raise IOError('Document already immutable!')
		request = pack('4s4sQ', self.cookie, part, self._getPos(part))
		(reply, empty) = self.connector._rpc(_HpConnector.TRUNC_REQ, request)
		return genericReply(reply, empty)

	def commit(self, mergeRevs=[]):
		if not self.active:
			raise IOError('Document already immutable!')
		self.active = False
		body = self.cookie + encodeUuidList(mergeRevs)
		(reply, raw) = self.connector._rpc(_HpConnector.COMMIT_REQ, body)
		if reply == _HpConnector.COMMIT_CNF:
			self.rev = raw
			return True
		else:
			genericReply(reply, raw)
			self.active = True
			return False

	def abort(self):
		if self.active:
			(reply, code) = self.connector._rpc(_HpConnector.ABORT_REQ, self.cookie)
			self.active = False
			return genericReply(reply, code)
		else:
			raise IOError('Document already immutable!')

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

