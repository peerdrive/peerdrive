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

from __future__ import absolute_import

import struct, json

from . import connector


###############################################################################
# HPSD data structures, encoders and decoders
###############################################################################

def Link(spec):
	if spec.startswith('doc:'):
		return DocLink(spec[4:].decode("hex"), autoUpdate=False)
	elif spec.startswith('rev:'):
		return RevLink(spec[4:].decode("hex"))
	else:
		return resolvePath(spec)

class RevLink(object):
	MIME_TYPE = 'application/x-hotchpotch-revlink'
	__slots__ = ['__rev']

	def __init__(self, rev=None):
		self.__rev = rev

	def __str__(self):
		return 'rev:'+self.__rev.encode('hex')

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
		self.__rev = decoder._getStr(16)

	def _toStruct(self):
		return '\x40' + self.__rev

	def _fromDict(self, dct):
		self.__rev = dct['rev'].decode('hex')

	def _toDict(self):
		return { "__rlink__" : True, "rev" : self.__rev.encode('hex') }

	def _fromMime(self, mimeData):
		self.__rev = str(mimeData.data(RevLink.MIME_TYPE)).decode('hex')

	def mimeData(self, mimeData):
		mimeData.setData(RevLink.MIME_TYPE, self.__rev.encode('hex'))

	def rev(self):
		return self.__rev

	def update(self):
		pass

	def doc(self):
		return None

	def revs(self):
		return [ self.__rev ]


class DocLink(object):
	MIME_TYPE = 'application/x-hotchpotch-doclink'
	__slots__ = ['__doc', '__lookup', '__revs', '__updated']

	def __init__(self, doc=None, autoUpdate=True, lookup=None):
		self.__doc = doc
		self.__lookup = lookup
		self.__revs = []
		self.__updated = False
		if doc and autoUpdate:
			self.update()
		else:
			self.__revs = None

	def __str__(self):
		return 'doc:'+self.__doc.encode('hex')

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
		self.__doc = decoder._getStr(16)
		self.__revs = []

	def _toStruct(self):
		return struct.pack('<B16s', 0x41, self.__doc)

	def _fromDict(self, dct):
		self.__doc = dct['doc'].decode('hex')
		self.__revs = []

	def _toDict(self):
		return {
			"__dlink__" : True,
			"doc" : self.__doc.encode('hex') }

	def _fromMime(self, mimeData):
		self.__doc = str(mimeData.data(DocLink.MIME_TYPE)).decode('hex')
		self.update()

	def mimeData(self, mimeData):
		mimeData.setData(DocLink.MIME_TYPE, self.__doc.encode('hex'))

	def update(self):
		if self.__lookup:
			self.__revs = self.__lookup(self.__doc)
		else:
			self.__revs = connector.Connector().lookup_doc(self.__doc).revs()
		self.__updated = True

	def doc(self):
		return self.__doc

	def rev(self):
		if not self.__updated:
			self.update()
		if len(self.__revs) == 1:
			return self.__revs[0]
		else:
			return None

	def revs(self):
		if not self.__updated:
			self.update()
		return self.__revs


class Decoder(object):
	def __init__(self, lookup=None):
		self._lookup = lookup

	def decode(self, s):
		self._s = s
		self._i = 0
		return self._decodeDoc()

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
			res = DocLink(lookup=self._lookup)
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


def loads(s, lookup=None):
	dec = Decoder(lookup)
	return dec.decode(s)


def dumps(o):
	enc = Encoder()
	return enc.encode(o)


def loadJSON(s):
	return json.loads(s, object_hook=__decode_link)


def dumpJSON(o):
	return json.dumps(o, default=__encode_link)


def loadMimeData(mimeData):
	if mimeData.hasFormat(DocLink.MIME_TYPE):
		link = DocLink()
		link._fromMime(mimeData)
		return link
	elif mimeData.hasFormat(RevLink.MIME_TYPE):
		link = RevLink()
		link._fromMime(mimeData)
		return link
	else:
		return None


# returns (result, conflicts)
def merge(base, versions):
	baseType = type(base)
	if any([not isinstance(o, baseType) for o in versions]):
		# the type changed -> unmergable
		return (versions[0], True)

	if isinstance(base, dict):
		return __mergeDict(base, versions)
	elif isinstance(base, (list, tuple)):
		return __mergeList(base, versions)
	elif (isinstance(base, basestring)
			or isinstance(base, bool)
			or isinstance(base, RevLink)
			or isinstance(base, DocLink)
			or isinstance(base, float)
			or isinstance(base, (int, long))):
		changes = [o for o in versions if o != base]
		count = len(set(changes))
		if count > 1:
			# take the latest change
			return (changes[0], True)
		elif count == 1:
			return (changes[0], False)
		else:
			return (base, False)
	else:
		raise TypeError("Invalid object: " + repr(o))


def __mergeDict(base, versions):
	baseKeys = set(base.keys())
	conflict = False
	added = { }
	removed = set()

	# get differences
	for ver in versions:
		verKeys = set(ver.keys())
		# removed keys
		removed.update(baseKeys - verKeys)
		# added keys
		newKeys = verKeys - baseKeys
		for key in newKeys:
			if key in added:
				# already added; the same?
				if ver[key] != added[key]:
					conflict = True
			else:
				added[key] = ver[key]

	# construct new dict
	newDict = {}
	for (key, oldValue) in base.items():
		if key in removed:
			# modify/delete conflict?
			remainingValues = [d[key] for d in versions if (key in d)]
			if any([other != oldValue for other in remainingValues]):
				conflict = True
				# yes :( -> take the latest version
				if key in versions[0]:
					# the latest version still has it.. retain
					(newValue, newConflict) = merge(oldValue, remainingValues)
					newDict[key] = newValue
				else:
					# the latest version deleted it.. bye bye
					pass
		else:
			# descend
			(newValue, newConflict) = merge(oldValue, [d[key] for d in versions])
			conflict = conflict or newConflict
			newDict[key] = newValue
	for (key, newValue) in added.items():
		newDict[key] = newValue

	# return new dict
	return (newDict, conflict)


def __mergeList(base, versions):
	added   = []
	removed = []
	for ver in versions:
		# check for removed items
		for item in base:
			if item not in ver:
				if item not in removed:
					removed.append(item)
		# check for added items
		for item in ver:
			if item not in base:
				if item not in added:
					added.append(item)

	# apply diff
	newList = base[:]
	for item in removed:
		newList.remove(item)
	for item in added:
		newList.append(item)
	return (newList, False)


###############################################################################
# HPSD container objects
###############################################################################

def Container(link):
	link.update()
	rev = link.rev()
	if not rev:
		return None
	uti = connector.Connector().stat(rev).type()
	if uti in Dict.UTIs:
		return Dict(link)
	elif uti in Set.UTIs:
		return Set(link)
	else:
		return None


class Dict(object):
	UTIs = ["org.hotchpotch.dict", "org.hotchpotch.store"]

	def __init__(self, link = None):
		self.__conn = connector.Connector()
		if link:
			self.__rev = link.rev()
			self.__doc = link.doc()
			self.__load()
		else:
			self.__content = { }
			self.__rev = None
			self.__doc = None

	def __load(self):
		uti = self.__conn.stat(self.__rev).type()
		if uti not in Dict.UTIs:
			raise IOError("Not a dict: "+uti)
		with self.__conn.peek(self.__rev) as r:
			self.__meta    = loads(r.readAll('META'))
			self.__content = loads(r.readAll('HPSD'))

	def create(self, name, stores):
		if self.__rev or self.__doc:
			raise IOError("Not new")

		if not name:
			name = "New dict"
		self.__meta = {
			"org.hotchpotch.annotation" : {
				"title" : name,
				"comment" : "<<Created by import>>"
			},
			"org.hotchpotch.sync" : {
				"sticky" : True
			}
		}
		w = connector.Connector().create("org.hotchpotch.dict", "", stores)
		try:
			w.writeAll('META', dumps(self.__meta))
			w.writeAll('HPSD', dumps(self.__content))
			w.commit()
			self.__rev = w.getRev()
			self.__doc = w.getDoc()
			return w
		except:
			w.close()
			raise

	def save(self):
		self.__meta["org.hotchpotch.annotation"]["comment"] = "<<Changed by import>>"
		if self.__rev and self.__doc:
			with connector.Connector().update(self.__doc, self.__rev) as w:
				w.writeAll('META', dumps(self.__meta))
				w.writeAll('HPSD', dumps(self.__content))
				self.__rev = w.commit()
		else:
			raise IOError('Not writable')

	def title(self):
		if "org.hotchpotch.annotation" in self.__meta:
			a = self.__meta["org.hotchpotch.annotation"]
			if "title" in a:
				return a["title"]
		return "Unnamed dictionary"

	def __len__(self):
		return len(self.__content)

	def __getitem__(self, name):
		key = name.split(':')[0]
		return self.__content[key]

	def __setitem__(self, name, link):
		key = name.split(':')[0]
		self.__content[key] = link

	def __delitem__(self, name):
		key = name.split(':')[0]
		del self.__content[key]

	def __contains__(self, name):
		key = name.split(':')[0]
		return key in self.__content

	def get(self, name):
		key = name.split(':')[0]
		return self.__content.get(key)

	def items(self):
		return self.__content.items()

	def remove(self, name, link):
		if self.__content.get(name) == link:
			del self.__content[name]
		else:
			raise KeyError(name)


class Set(object):
	UTIs = ["org.hotchpotch.set"]

	def __init__(self, link = None):
		self.__didCache = False
		self.__conn = connector.Connector()
		if link:
			self.__rev = link.rev()
			self.__doc = link.doc()
			self.__load()
		else:
			self.__content = []
			self.__rev = None
			self.__doc = None

	def __load(self):
		uti = self.__conn.stat(self.__rev).type()
		if uti not in Set.UTIs:
			raise IOError("Not a dict: "+uti)
		with self.__conn.peek(self.__rev) as r:
			self.__meta = loads(r.readAll('META'))
			content = loads(r.readAll('HPSD'))
		self.__content = [ (None, l) for l in content ]

	def __doCache(self):
		if not self.__didCache:
			self.__content = [ (readTitle(l), l) for (t, l) in self.__content ]
			self.__didCache = True

	def create(self, name, stores):
		if self.__rev or self.__doc:
			raise IOError("Not new")

		if not name:
			name = "New set"
		self.__meta = {
			"org.hotchpotch.annotation" : {
				"title" : name,
				"comment" : "<<Created by import>>"
			},
			"org.hotchpotch.sync" : {
				"sticky" : True
			}
		}
		content = [ link for (descr, link) in self.__content ]
		for link in content:
			link.update()
		w = connector.Connector().create("org.hotchpotch.set", "", stores)
		try:
			w.writeAll('META', dumps(self.__meta))
			w.writeAll('HPSD', dumps(content))
			w.commit()
			self.__rev = w.getRev()
			self.__doc = w.getDoc()
			return w
		except:
			w.close()
			raise

	def save(self):
		if self.__rev and self.__doc:
			self.__meta["org.hotchpotch.annotation"]["comment"] = "<<Changed by import>>"
			content = [ link for (descr, link) in self.__content ]
			for link in content:
				link.update()
			with connector.Connector().update(self.__doc, self.__rev) as w:
				w.writeAll('META', dumps(self.__meta))
				w.writeAll('HPSD', dumps(content))
				self.__rev = w.commit()
		else:
			raise IOError('Not writable')

	def title(self):
		if "org.hotchpotch.annotation" in self.__meta:
			a = self.__meta["org.hotchpotch.annotation"]
			if "title" in a:
				return a["title"]
		return "Unnamed set"

	def __index(self, title, fail=True):
		i = 0
		for (key, link) in self.__content:
			if key == title:
				return i
			else:
				i += 1
		if fail:
			raise IndexError(title)
		else:
			return None

	def __len__(self):
		return len(self.__content)

	def __getitem__(self, name):
		self.__doCache()
		i = self.__index(name)
		return self.__content[i][1]

	def __setitem__(self, name, link):
		self.__content.append( (readTitle(link), link) )

	def __delitem__(self, name):
		self.__doCache()
		i = self.__index(name)
		del self.__content[i]

	def __contains__(self, name):
		self.__doCache()
		i = self.__index(name, False)
		return i is not None

	def get(self, name):
		self.__doCache()
		i = self.__index(name, False)
		if i is None:
			return None
		else:
			return self.__content[i][1]

	def items(self):
		self.__doCache()
		return self.__content

	def remove(self, name, link):
		self.__doCache()
		self.__content.remove((name, link))

# tiny helper function
def readTitle(link):
	rev = link.rev()
	if rev:
		try:
			with connector.Connector().peek(rev) as r:
				meta = loads(r.readAll('META'))
			if "org.hotchpotch.annotation" in meta:
				annotation = meta["org.hotchpotch.annotation"]
				if "title" in annotation:
					return annotation["title"]
		except IOError:
			pass

	return None


###############################################################################
# Path resolving
###############################################################################

# return (store:uuid, container:Container, docName:str)
def walkPath(path, create=False):
	steps = path.split('/')
	storeName = steps[0]
	docName  = steps[-1]
	for i in xrange(1, len(steps)-1):
		steps[i] = (steps[i], steps[i+1])
	steps = steps[1:-1]

	# search for store
	enum = connector.Connector().enum()
	storeDoc = None
	for mount in enum.allStores():
		if not enum.isMounted(mount):
			continue
		mountDoc = enum.doc(mount)
		if (mount == storeName) or (mountDoc.encode("hex").startswith(storeName)):
			storeDoc = mountDoc
			break
	if not storeDoc:
		raise IOError("Store not found")

	# walk the path
	curContainer = Dict(DocLink(storeDoc, False))
	for (step, nextStep) in steps:
		next = curContainer.get(step)
		if next:
			curContainer = Container(next)
		elif create:
			name = step.split(':')[-1]
			if ':' in nextStep:
				handle = Dict().create(name, [storeDoc])
			else:
				handle = Set().create(name, [storeDoc])

			try:
				next = DocLink(handle.getDoc())
				curContainer[step] = next
				curContainer.save()
			finally:
				handle.close()
			curContainer = Container(next)
		else:
			raise IOError("Path not found")

	# return result
	return (storeDoc, curContainer, docName)


def resolvePath(path):
	if '/' in path:
		(storeDoc, container, docName) = walkPath(path)
		if docName not in container:
			raise IOError("Path not found")
		return container[docName]
	else:
		enum = connector.Connector().enum()
		if path not in enum.allStores():
			raise IOError("Path not found")
		return DocLink(enum.doc(path), False)

