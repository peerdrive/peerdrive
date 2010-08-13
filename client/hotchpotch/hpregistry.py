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

from __future__ import with_statement
import hpstruct, hpconnector

class _HpRegistry(hpconnector.HpWatch):

	def __init__(self):
		self.connection = hpconnector.HpConnector()

		sysUuid = self.connection.enum().sysStoreGuid()
		sysRev = self.connection.lookup(sysUuid).rev(sysUuid)
		with self.connection.peek(sysRev) as r:
			root = hpstruct.loads(r.readAll('HPSD'))
			self.regUuid = root["registry"].uuid()

		hpconnector.HpWatch.__init__(self, hpconnector.HpWatch.TYPE_UUID, self.regUuid)
		self.loadRegistry()

	def loadRegistry(self):
		regRev = self.connection.lookup(self.regUuid).revs()[0]
		with self.connection.peek(regRev) as r:
			self.registry = hpstruct.loads(r.readAll('HPSD'))

	def triggered(self, cause):
		if cause == hpconnector.HpWatch.CAUSE_MODIFIED:
			self.loadRegistry()
		else:
			print "hpregistry: Unexpected watch trigger: %s -> %d" % (self.getHash().encode("hex"), cause)

	def getDisplayString(self, uti):
		name = self.search(uti, "display")
		if name is None:
			name = uti
		return name

	def getIcon(self, uti):
		icon = self.search(uti, "icon")
		if icon is None:
			icon = "uti/unknown.png"
		return "icons/" + icon

	def getMeta(self, uti):
		return reduce(lambda x,y: x+y, self._searchAll(uti, "meta"), [])

	def getUtiFromExtension(self, ext, default = "public.data"):
		for (uti, spec) in self.registry.items():
			if "extensions" in spec:
				if ext in spec["extensions"]:
					return uti
		return default

	def getUtiFromMime(self, mime, default = "public.data"):
		for (uti, spec) in self.registry.items():
			if "mimetypes" in spec:
				if mime in spec["mimetypes"]:
					return uti
		return default

	def getExtractor(self, uti):
		return self.search(uti, "extractor")

	def getExecutable(self, uti):
		return self.search(uti, "exec")

	def search(self, uti, key):
		if uti not in self.registry:
			return None

		item = self.registry[uti]
		if key in item:
			return item[key]
		elif "conforming" not in item:
			return None
		else:
			for i in item["conforming"]:
				data = self.search(i, key)
				if not (data is None):
					return data
			return None
	
	def _searchAll(self, uti, key):
		if uti not in self.registry:
			return []

		item = self.registry[uti]
		if key in item:
			data = [ item[key] ]
		else:
			data = []
		if "conforming" in item:
			for i in item["conforming"]:
				data = data + self._searchAll(i, key)
		return data


_registry = None

def HpRegistry():
	global _registry
	if not _registry:
		_registry = _HpRegistry()
	return _registry

