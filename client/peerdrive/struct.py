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

import struct, copy

from . import connector

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
			or isinstance(base, connector.RevLink)
			or isinstance(base, connector.DocLink)
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
# PeerDrive folder object
###############################################################################

class Folder(object):
	UTIs = ["org.peerdrive.folder", "org.peerdrive.store"]

	def __init__(self, link = None):
		self.__didCache = False
		if link:
			link.update()
			self.__rev = link.rev()
			self.__doc = link.doc()
			self.__store = link.store()
			self.__load()
		else:
			self.__content = []
			self.__rev = None
			self.__doc = None
			self.__store = None

	def __load(self):
		if not self.__rev:
			raise IOError("Folder not found")
		uti = connector.Connector().stat(self.__rev, [self.__store]).type()
		if uti not in Folder.UTIs:
			raise IOError("Not a folder: "+uti)
		with connector.Connector().peek(self.__store, self.__rev) as r:
			self.__meta = r.getData('/org.peerdrive.annotation')
			content = r.getData('/org.peerdrive.folder')
		self.__content = [ (None, l) for l in content ]

	def __doCache(self):
		if not self.__didCache:
			self.__content = [ (readTitle(i['']), i) for (t, i) in
				self.__content ]
			self.__didCache = True

	def create(self, store, name=None):
		if self.__rev or self.__doc:
			raise IOError("Not new")

		self.__store = store
		if not name:
			name = "New folder"
		self.__meta = { "title" : name }
		for (descr, item) in self.__content:
			item[''].update(self.__store)
		content = [ item for (descr, item) in self.__content ]
		w = connector.Connector().create(store, "org.peerdrive.folder", "")
		try:
			w.setData('', {
				"org.peerdrive.folder" : content,
				"org.peerdrive.annotation" : self.__meta
			})
			w.setFlags([connector.Stat.FLAG_STICKY])
			w.commit()
			self.__rev = w.getRev()
			self.__doc = w.getDoc()
			return w
		except:
			w.close()
			raise

	def save(self):
		if self.__rev and self.__doc and self.__store:
			content = [ item for (descr, item) in self.__content ]
			with connector.Connector().update(self.__store, self.__doc, self.__rev) as w:
				w.setData('', {
					"org.peerdrive.folder" : content,
					"org.peerdrive.annotation" : self.__meta
				})
				self.__rev = w.commit()
		else:
			raise IOError('Not writable')

	def title(self):
		if "title" in self.__meta:
			return self.__meta["title"]
		return "Unnamed folder"

	def __index(self, title, fail=True):
		i = 0
		for (key, item) in self.__content:
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

	def __getitem__(self, i):
		if isinstance(i, basestring):
			self.__doCache()
			i = self.__index(i)
		return self.__content[i][1]['']

	def __delitem__(self, select):
		if isinstance(i, basestring):
			self.__doCache()
			i = self.__index(i)
		del self.__content[i]

	def __contains__(self, name):
		self.__doCache()
		i = self.__index(name, False)
		return i is not None

	def append(self, link):
		if self.__store:
			link.update(self.__store)
		self.__content.append( (readTitle(link), { '' : link }) )

	def get(self, name):
		self.__doCache()
		i = self.__index(name, False)
		if i is None:
			return None
		else:
			return self.__content[i][1]['']

	def items(self):
		self.__doCache()
		return [ (name, item['']) for (name, item) in self.__content ]

	def remove(self, name, link):
		self.__doCache()
		self.__content.remove((name, {'' : link}))

	def getDoc(self):
		return self.__doc

	def getRev(self):
		return self.__rev

# tiny helper function
def readTitle(link, default=None):
	rev = link.rev()
	if rev:
		try:
			with connector.Connector().peek(link.store(), rev) as r:
				return r.getData("/org.peerdrive.annotation/title")
		except IOError:
			pass

	return default


class FSTab(object):
	def __init__(self):
		self.__changed = False
		self.__store = connector.Connector().enum().sysStore().sid
		self.__doc = Folder(connector.DocLink(self.__store, self.__store))["fstab"].doc()
		self.__rev = connector.Connector().lookupDoc(self.__doc).rev(self.__store)
		with connector.Connector().peek(self.__store, self.__rev) as r:
			self.__fstab = r.getData('/org.peerdrive.fstab')

	def save(self):
		if not self.__changed:
			return

		with connector.Connector().update(self.__store, self.__doc, self.__rev) as w:
			w.setData('/org.peerdrive.fstab', self.__fstab)
			w.commit()
			self.__rev = w.getRev()

		self.__changed = False

	def knownLabels(self):
		return self.__fstab.keys()

	def mount(self, label):
		src = self.__fstab[label]["src"]
		type = self.__fstab[label].get("type", "file")
		options = self.__fstab[label].get("options", "")
		credentials = self.__fstab[label].get("credentials", "")
		return connector.Connector().mount(src, label, type, options, credentials)

	def get(self, label):
		return copy.deepcopy(self.__fstab[label])

	def set(self, label, mount):
		self.__fstab[label] = mount
		self.__changed = True

	def remove(self, label):
		del self.__fstab[label]
		self.__changed = True


###############################################################################
# Path resolving
###############################################################################

# return (store:uuid, folder:Folder, docName:str)
def walkPath(path, create=False):
	steps = path.split('/')
	storeName = steps[0]
	docName = steps[-1]
	steps = steps[1:-1]

	# search for store
	enum = connector.Connector().enum()
	storeDoc = None
	for mount in enum.allStores():
		mountDoc = mount.sid
		if (mount.label == storeName) or (mountDoc.encode("hex").startswith(storeName)):
			storeDoc = mountDoc
			break
	if not storeDoc:
		raise IOError("Store not found")

	# walk the path
	curFolder = Folder(connector.DocLink(storeDoc, storeDoc, False))
	for step in steps:
		next = curFolder.get(step)
		if next:
			curFolder = Folder(next)
		elif create:
			handle = Folder().create(storeDoc, step)
			try:
				next = DocLink(connector.storeDoc, handle.getDoc())
				curFolder.append(next)
				curFolder.save()
			finally:
				handle.close()
			curFolder = Folder(next)
		else:
			raise IOError("Path not found")

	# return result
	return (storeDoc, curFolder, docName)


def copyDoc(src, dstStore):
	src.update()
	if not src.rev():
		raise IOError('Source not found!')

	repHandle = connector.Connector().replicateRev(src.store(), src.rev(), dstStore)
	try:
		handle = connector.Connector().fork(dstStore, src.rev(), 'org.peerdrive.cp')
		try:
			try:
				title = handle.getData('/org.peerdrive.annotation/title')
			except IOError:
				title = 'unnamed document'

			handle.setData('/org.peerdrive.annotation/title', 'Copy of ' + title)
			handle.commit("<<Copy document>>")
			return handle
		except:
			handle.close()
			raise
	finally:
		repHandle.close()

