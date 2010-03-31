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


class ConnError(Exception):
    """Base class for exceptions in this module."""
    pass


class _HpConnector(object):
	ENUM_REQ            = 0x0000
	ENUM_CNF            = 0x0001
	LOOKUP_REQ          = 0x0010
	LOOKUP_CNF          = 0x0011
	STAT_REQ            = 0x0020
	STAT_CNF            = 0x0021
	STAT_REJ            = 0x0022
	READ_START_REQ      = 0x0030
	READ_START_CNF      = 0x0031
	READ_START_REJ      = 0x0032
	READ_PART_REQ       = 0x0040
	READ_PART_CNF       = 0x0041
	READ_PART_REJ       = 0x0042
	READ_DONE_IND       = 0x0053
	FORK_REQ            = 0x0060
	FORK_CNF            = 0x0061
	FORK_REJ            = 0x0062
	MERGE_REQ           = 0x0070
	MERGE_CNF           = 0x0071
	MERGE_REJ           = 0x0072
	UPDATE_REQ          = 0x0080
	UPDATE_CNF          = 0x0081
	UPDATE_REJ          = 0x0082
	MERGE_TRIVIAL_REQ   = 0x0120
	MERGE_TRIVIAL_CNF   = 0x0121
	MERGE_TRIVIAL_REJ   = 0x0122
	WRITE_TRUNC_REQ     = 0x0090
	WRITE_TRUNC_CNF     = 0x0091
	WRITE_TRUNC_REJ     = 0x0092
	WRITE_PART_REQ      = 0x00A0
	WRITE_PART_CNF      = 0x00A1
	WRITE_PART_REJ      = 0x00A2
	WRITE_COMMIT_REQ    = 0x00B0
	WRITE_COMMIT_CNF    = 0x00B1
	WRITE_COMMIT_REJ    = 0x00B2
	WRITE_ABORT_IND     = 0x00C3
	WATCH_ADD_REQ       = 0x00D0
	WATCH_ADD_CNF       = 0x00D1
	WATCH_ADD_REJ       = 0x00D2
	WATCH_REM_REQ       = 0x00D3
	WATCH_REM_CNF       = 0x00D4
	WATCH_REM_REJ       = 0x00D5
	WATCH_IND           = 0x00D6
	DELETE_UUID_REQ     = 0x00E0
	DELETE_REV_REQ      = 0x00E1
	DELETE_CNF          = 0x00E2
	DELETE_REJ          = 0x00E3
	REPLICATE_UUID_IND  = 0x00F0
	REPLICATE_REV_IND   = 0x00F1
	OPEN_UUID_IND       = 0x0103
	OPEN_REV_IND        = 0x0113
	MOUNT_REQ           = 0x0130
	MOUNT_CNF           = 0x0131
	MOUNT_REJ           = 0x0132
	UNMOUNT_IND         = 0x0123
	PROGRESS_IND        = 0x0146


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

	def lookup(self, uuid):
		checkGuid(uuid)
		(reply, body) = self._rpc(_HpConnector.LOOKUP_REQ, uuid)
		if reply == _HpConnector.LOOKUP_CNF:
			return HpLookup(body)
		else:
			raise ConnError('Invalid server reply')

	def stat(self, rev):
		checkGuid(rev)
		(reply, body) = self._rpc(_HpConnector.STAT_REQ, rev)
		if reply == _HpConnector.STAT_CNF:
			return HpStat(body)
		elif reply == _HpConnector.STAT_REJ:
			raise IOError('Invalid Revision')
		else:
			raise ConnError('Invalid server reply')

	def read(self, rev):
		checkGuid(rev)
		(reply, cookie) = self._rpc(_HpConnector.READ_START_REQ, rev)
		if reply == _HpConnector.READ_START_CNF:
			return HpReader(self, cookie, rev)
		elif reply == _HpConnector.READ_START_REJ:
			raise IOError('Unknown revision')
		else:
			raise ConnError('Invalid server reply')

	def fork(self, store, uti, startRev = None):
		checkGuid(store)
		if startRev:
			checkGuid(startRev)
			rev = startRev
		else:
			rev = '\x00'*16
		(reply, raw) = self._rpc(_HpConnector.FORK_REQ, store+rev+uti)
		if reply == _HpConnector.FORK_CNF:
			(uuid, cookie) = unpack('<16s4s', raw)
			return HpWriter(self, cookie, uuid)
		elif reply == _HpConnector.FORK_REJ:
			raise IOError('Cannot create document')
		else:
			raise ConnError('Invalid server reply: %d : %s' % (reply, raw))

	def merge(self, uuid, startRevs, uti):
		checkGuid(uuid)
		revs = pack('B', len(startRevs))
		for rev in startRevs:
			checkGuid(rev)
			revs += rev
		(reply, raw) = self._rpc(_HpConnector.MERGE_REQ, uuid+revs+uti)
		if reply == _HpConnector.MERGE_CNF:
			(cookie,) = unpack('<4s', raw)
			return HpWriter(self, cookie, uuid)
		elif reply == _HpConnector.MERGE_REJ:
			cause = unpack('<B', raw)[0]
			if cause == 0:
				raise IOError('Conflicting revision')
			elif cause == 1:
				raise IOError('Invalid UUID')
			else:
				raise IOError('Unknown error')
		else:
			raise ConnError('Invalid server reply: %d : %s' % (reply, raw))

	def mergeTrivial(self, uuid, destRev, otherRevs):
		checkGuid(uuid)
		checkGuid(destRev)
		revs = pack('B', len(otherRevs))
		for rev in otherRevs:
			checkGuid(rev)
			revs += rev
		(reply, newRev) = self._rpc(_HpConnector.MERGE_TRIVIAL_REQ, uuid+destRev+revs)
		if reply == _HpConnector.MERGE_TRIVIAL_CNF:
			return newRev
		elif reply == _HpConnector.MERGE_TRIVIAL_REJ:
			raise IOError('Trivial merge failed')
		else:
			raise ConnError('Invalid server reply: %d : %s' % (reply, newRev.encode('hex')))

	def update(self, Uuid, Rev):
		checkGuid(Uuid)
		checkGuid(Rev)
		(reply, raw) = self._rpc(_HpConnector.UPDATE_REQ, Uuid + Rev)
		if reply == _HpConnector.UPDATE_CNF:
			return HpWriter(self, raw, Uuid)
		elif reply == _HpConnector.UPDATE_REJ:
			cause = unpack('<B', raw)[0]
			if cause == 0:
				raise IOError('Conflicting revision')
			elif cause == 1:
				raise IOError('Invalid UUID')
			else:
				raise IOError('Unknown error')
		else:
			raise ConnError('Invalid server reply')

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

	def delete_uuid(self, store, uuid):
		checkGuid(store)
		checkGuid(uuid)
		reply = self._rpc(_HpConnector.DELETE_UUID_REQ, store+uuid)[0]
		if reply == _HpConnector.DELETE_CNF:
			return True
		elif reply == _HpConnector.DELETE_REJ:
			raise IOError('Unknown UUID')
		else:
			raise ConnError('Invalid server reply')

	def delete_rev(self, store, rev):
		checkGuid(store)
		checkGuid(rev)
		reply = self._rpc(_HpConnector.DELETE_REV_REQ, store+rev)[0]
		if reply == _HpConnector.DELETE_CNF:
			return True
		elif reply == _HpConnector.DELETE_REJ:
			raise IOError('Unknown revision')
		else:
			raise ConnError('Invalid server reply')

	def replicate_uuid(self, uuid, stores, history=True):
		checkGuid(uuid)
		body = pack('16sBB', uuid, int(history), len(stores))
		for store in stores:
			checkGuid(store)
			body += store
		self._ind(_HpConnector.REPLICATE_UUID_IND, body)

	def replicate_rev(self, rev, stores, history=True):
		checkGuid(rev)
		body = pack('16sBB', rev, int(history), len(stores))
		for store in stores:
			checkGuid(store)
			body += store
		self._ind(_HpConnector.REPLICATE_REV_IND, body)

	def mount(self, store):
		reply = self._rpc(_HpConnector.MOUNT_REQ, store)[0]
		if reply == _HpConnector.MOUNT_CNF:
			return True
		elif reply == _HpConnector.MOUNT_REJ:
			return False
		else:
			raise ConnError('Invalid server reply')

	def unmount(self, store):
		self._ind(_HpConnector.UNMOUNT_IND, store)

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
		#   PartsCount:8,  [FourCC:32, Size:64, Hash:128],
		#   ParentCount:8, [Parent:128],
		#   VolumeCount:8, [Volume:128],
		#   Mtime:64, Uti...
		count = unpack_from('B', packet, 0)[0]
		pos = 1
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



class HpSeekable(object):
	def __init__(self):
		self.__pos = { }

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


class HpReader(HpSeekable):
	def __init__(self, connector, cookie, rev):
		HpSeekable.__init__(self)
		self.connector = connector
		self.cookie = cookie
		self.rev = rev
		self.active = True

	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		self.done()
		return False

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
			(reply, data) = self.connector._rpc(_HpConnector.READ_PART_REQ, request)
			if reply == _HpConnector.READ_PART_CNF:
				size = len(data)
				result = result + data
				pos = pos + size
				if size < chunk:
					break
			elif reply == _HpConnector.READ_PART_REJ:
				raise IOError('Error while reading')
			else:
				raise ConnError('Unexpected server response')
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

	def done(self):
		if self.active:
			self.active = False
			self.connector._ind(_HpConnector.READ_DONE_IND, self.cookie)

	def getRevision(self):
		return self.rev


class HpWriter(HpSeekable):
	def __init__(self, connector, cookie, uuid):
		HpSeekable.__init__(self)
		self.connector = connector
		self.cookie = cookie
		self.uuid = uuid
		self.rev = None
		self.active = True

	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		if self.active:
			self.abort()
		return False

	def write(self, part, data):
		if not self.active:
			raise IOError('Document already immutable!')

		pos = self._getPos(part)
		i = 0
		length = len(data)
		while length >= i:
			request = pack('<4s4sQ', self.cookie, part, pos+i) + data[i:i+2048]
			(reply, empty) = self.connector._rpc(_HpConnector.WRITE_PART_REQ, request)
			if reply == _HpConnector.WRITE_PART_CNF:
				i += 2048
			elif reply == _HpConnector.WRITE_PART_REJ:
				raise IOError('Error while writing')
			else:
				raise ConnError('Unexpected server response')

		self._setPos(part, pos+length)

	def writeAll(self, part, data):
		self._setPos(part, 0)
		self.write(part, data)
		self.truncate(part)

	def truncate(self, part):
		if not self.active:
			raise IOError('Document already immutable!')
		request = pack('4s4sQ', self.cookie, part, self._getPos(part))
		(reply, empty) = self.connector._rpc(_HpConnector.WRITE_TRUNC_REQ, request)
		if reply == _HpConnector.WRITE_TRUNC_CNF:
			pass
		elif reply == _HpConnector.WRITE_TRUNC_REJ:
			raise IOError('Write error')
		else:
			raise ConnError('Unexpected server response')

	def commit(self):
		if not self.active:
			raise IOError('Document already immutable!')
		self.active = False
		(reply, raw) = self.connector._rpc(_HpConnector.WRITE_COMMIT_REQ, self.cookie)
		if reply == _HpConnector.WRITE_COMMIT_CNF:
			self.rev = raw
			return raw
		elif reply == _HpConnector.WRITE_COMMIT_REJ:
			cause = unpack('<B', raw)[0]
			if cause == 0:
				raise IOError('Conflicting revision')
			elif cause == 1:
				raise IOError('Write error')
			else:
				raise IOError('Unknown error')
		else:
			raise ConnError('Unexpected server response')

	def abort(self):
		if self.active:
			self.connector._ind(_HpConnector.WRITE_ABORT_IND, self.cookie)
			self.active = False
		else:
			raise IOError('Document already immutable!')

	def getUUID(self):
		return self.uuid

	def getRevision(self):
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

