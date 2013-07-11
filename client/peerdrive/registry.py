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

from . import struct
from . import connector

class _Registry(connector.Watch):

	def __init__(self):
		self.connection = connector.Connector()

		sysDoc = self.connection.enum().sysStore().sid
		root = struct.Folder(connector.DocLink(sysDoc, sysDoc))
		self.__regLink = root["registry"]

		connector.Watch.__init__(self, connector.Watch.TYPE_DOC, self.__regLink.doc())
		self.connection.watch(self)
		self.loadRegistry()

	def loadRegistry(self):
		self.__regLink.update()
		with self.connection.peek(self.__regLink.store(), self.__regLink.rev()) as r:
			self.registry = r.getData('/org.peerdrive.registry')

	def triggered(self, event, store):
		if event == connector.Watch.EVENT_MODIFIED:
			self.loadRegistry()
		else:
			print "hpregistry: Unexpected watch trigger: %s -> %d" % (self.getHash().encode("hex"), cause)

	def getDisplayString(self, uti):
		return self.search(uti, "display", default=uti)

	def getIcon(self, uti):
		icon = self.search(uti, "icon", default="uti/unknown.png")
		return "icons/" + icon

	def getMeta(self, uti):
		return reduce(lambda x,y: x+y, self.searchAll(uti, "meta").values(), [])

	def getUtiFromExtension(self, ext, default = "public.data"):
		for (uti, spec) in self.registry.items():
			if "extensions" in spec:
				if ext in spec["extensions"]:
					return uti
		return default

	def getUtiFromMime(self, mime, default = "public.data"):
		mime = mime.split(';')[0].strip()
		# maybe support mime parameters too?
		for (uti, spec) in self.registry.items():
			if "mimetypes" in spec:
				if mime in spec["mimetypes"]:
					return uti
		return default

	def getExtractor(self, uti):
		return self.search(uti, "extractor")

	def getExecutables(self, uti):
		preliminary = self.__getExecutables(uti)
		# remove duplicate items
		result = []
		for i in preliminary:
			if i not in result:
				result.append(i)
		return result

	def conformes(self, uti, superClass):
		if uti == superClass:
			return True
		result = False
		item = self.registry.get(uti, {})
		for i in item.get("conforming", []):
			result = result or self.conformes(i, superClass)
		return result

	def __getExecutables(self, uti):
		item = self.registry.get(uti, {})
		data = item.get("exec", [])
		for i in item.get("conforming", []):
			data.extend(self.__getExecutables(i))
		return data

	def search(self, uti, key, recursive=True, default=None):
		if uti not in self.registry:
			return default

		item = self.registry[uti]
		if key in item:
			return item[key]
		elif not recursive:
			return default
		elif "conforming" not in item:
			return default
		else:
			for i in item["conforming"]:
				data = self.search(i, key)
				if not (data is None):
					return data
			return default

	def searchAll(self, uti, key):
		if uti not in self.registry:
			return {}

		item = self.registry[uti]
		if key in item:
			data = { uti : item[key] }
		else:
			data = {}
		if "conforming" in item:
			for i in item["conforming"]:
				data.update(self.searchAll(i, key))
		return data


_instance = None

def Registry():
	global _instance
	if not _instance:
		_instance = _Registry()
	return _instance

