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

import os, sys, subprocess

from . import struct
from .connector import Connector
from .registry import Registry

try:
	import magic
	mimeGuess = magic.open(magic.MAGIC_MIME)
	mimeGuess.load()
except ImportError:
	mimeGuess = None


class ImporterError(Exception):
    """Base class for exceptions in this module."""
    pass


def __runExtractor(extractor, path):
	if sys.platform == "win32":
		proc = subprocess.Popen([extractor, path], shell=True,
			stdout=subprocess.PIPE, creationflags=0x08000000)
	else:
		proc = subprocess.Popen(['./'+extractor, path], stdout=subprocess.PIPE)
	data = proc.stdout.read()
	proc.wait()
	return struct.loadJSON(data)


def __merge(old, new):
	for (key, newValue) in new.items():
		if key in old:
			oldValue = old[key]
			if isinstance(newValue, dict) and isinstance(oldValue, dict):
				__merge(oldValue, newValue)
			elif isinstance(newValue, list) and isinstance(oldValue, list):
				for i in newValue:
					if i not in oldValue:
						oldValue.append(i)
			else:
				old[key] = newValue
		else:
			old[key] = newValue


# returns a commited writer or None
def importFile(store, path, name=""):
	if os.path.isfile(path):
		# determine file type
		uti = None
		if mimeGuess:
			mime = mimeGuess.file(path)
			uti = Registry().getUtiFromMime(mime, None)
		if not uti:
			ext  = os.path.splitext(path)[1].lower()
			uti  = Registry().getUtiFromExtension(ext)
		if not name:
			name = os.path.basename(path)
		meta = {
			"org.hotchpotch.annotation" : {
				"title"   : name,
				"origin"  : path,
				"comment" : "Import from external file system"
			}
		}

		extractor = Registry().getExtractor(uti)
		if extractor:
			additionalMeta = __runExtractor(extractor, path)
			if additionalMeta:
				__merge(meta, additionalMeta)

		#print 'META: ', repr(meta)
		with open(path, "rb") as file:
			writer = Connector().create(uti, "", [store])
			try:
				writer.write('FILE', file.read())
				writer.write('META', struct.dumps(meta))
				writer.commit()
				return writer
			except:
				writer.close()
				raise
	else:
		return None


# returns a commited writer or None
def importObject(store, uti, spec):
	try:
		writer = Connector().create(uti, "", [store])
		try:
			for (fourcc, data) in spec:
				writer.writeAll(fourcc, data)
			writer.commit()
			return writer
		except:
			writer.close()
	except IOError:
		pass
	return None


def overwriteObject(store, link, uti, spec):
	doc = link.doc()
	rev = Connector().lookup_doc(doc, [store]).rev(store)
	with Connector().update(doc, rev, stores=[store]) as writer:
		for (fourcc, data) in spec:
			writer.writeAll(fourcc, data)
		writer.setType(uti)
		writer.commit()


def importObjectByPath(path, uti, spec, overwrite=False):
	try:
		# resolve the path
		(store, container, name) = struct.walkPath(path, True)
		if name in container:
			if not overwrite:
				return False
			try:
				print "overwrite"
				overwriteObject(store, container[name], uti, spec)
				print "overwritten"
				return True
			except IOError:
				pass

		# create the object and add to dict
		handle = importObject(store, uti, spec)
		if not handle:
			return False
		try:
			container[name] = struct.DocLink(handle.getDoc())
			container.save()
			return True
		finally:
			handle.close()

	except IOError:
		return False


def importFileByPath(impPath, impFile, overwrite=False, progress=None, error=None):
	# resolve the path
	(store, container, name) = struct.walkPath(impPath, True)

	# create the object and add to dict
	if isinstance(impFile, list):
		counter = 0
		handles = []
		nn = "%s%d" % (name, counter)
		try:
			for f in impFile:
				while nn in container:
					counter += 1
					nn = "%s%d" % (name, counter)
				if progress:
					progress(f, nn)
				handle = importFile(store, f)
				if handle:
					handles.append(handle)
					container[nn] = struct.DocLink(handle.getDoc())
				elif error:
					error(f, nn)
			container.save()
		finally:
			for handle in handles:
				handle.close()
	else:
		if (name in container) and (not overwrite):
			raise ImporterError("Duplicate item name")

		handle = importFile(store, impFile, name)
		try:
			if handle:
				container[name] = struct.DocLink(handle.getDoc())
				container.save()
			else:
				raise ImporterError("Invalid file")
		finally:
			handle.close()

