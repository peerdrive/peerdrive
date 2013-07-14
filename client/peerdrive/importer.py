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

import os, sys, subprocess

from . import struct, connector
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
	return connector.loadJSON(data)


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


# returns a commited writer, None or throws an IOError
def importFile(store, path, name="", progress=None):
	if not name:
		name = os.path.basename(path)

	if os.path.isfile(path):
		if progress:
			progress(path)

		# determine file type
		uti = None
		if mimeGuess:
			mime = mimeGuess.file(path)
			uti = Registry().getUtiFromMime(mime, None)
		if not uti:
			ext  = os.path.splitext(path)[1].lower()
			uti  = Registry().getUtiFromExtension(ext)
		meta = {
			"org.peerdrive.annotation" : {
				"title"   : name,
				"origin"  : path
			}
		}

		extractor = Registry().getExtractor(uti)
		if extractor:
			additionalMeta = __runExtractor(extractor, path)
			if additionalMeta:
				__merge(meta, additionalMeta)

		with open(path, "rb") as file:
			writer = Connector().create(store, uti, "")
			try:
				writer.setData('', meta)
				writer.write('_', file.read())
				writer.commit("Import from external file system")
				return writer
			except:
				writer.close()
				raise
	elif os.path.isdir(path):
		handles = []
		try:
			for entry in os.listdir(path):
				handle = importFile(store, os.path.join(path, entry), entry, progress)
				handles.append(handle)

			folder = struct.Folder()
			for handle in handles:
				folder.append(connector.DocLink(store, handle.getDoc()))

			return folder.create(store, name)
		finally:
			for handle in handles:
				handle.close()
	else:
		return None


def overwriteFile(link, path):
	if not os.path.isfile(path):
		return False

	# determine file type
	uti = None
	if mimeGuess:
		mime = mimeGuess.file(path)
		uti = Registry().getUtiFromMime(mime, None)
	if not uti:
		ext  = os.path.splitext(path)[1].lower()
		uti  = Registry().getUtiFromExtension(ext)

	link.update()
	store = link.store()
	doc = link.doc()
	rev = link.rev()
	if not (doc and rev):
		return False
	with Connector().update(store, doc, rev) as writer:
		meta = writer.getData('')
		meta["org.peerdrive.annotation"]["origin"] = path

		extractor = Registry().getExtractor(uti)
		if extractor:
			additionalMeta = __runExtractor(extractor, path)
			if additionalMeta:
				__merge(meta, additionalMeta)

		with open(path, "rb") as file:
			writer.writeAll('_', file.read())
		writer.setData('', meta)
		writer.setType(uti)
		writer.commit("Overwritten from external file system")

	return True


# returns a commited writer or None
def importObject(store, uti, data, spec, flags):
	try:
		writer = Connector().create(store, uti, "")
		try:
			writer.setData('', data)
			for (fourcc, data) in spec:
				writer.writeAll(fourcc, data)
			writer.setFlags(flags)
			writer.commit()
			return writer
		except:
			writer.close()
	except IOError:
		pass
	return None


def overwriteObject(link, uti, data, spec, flags):
	link.update()
	store = link.store()
	doc = link.doc()
	rev = link.rev()
	if not (doc and rev):
		return False

	with Connector().update(store, doc, rev) as writer:
		writer.setData('', data)
		for (fourcc, data) in spec:
			writer.writeAll(fourcc, data)
		writer.setType(uti)
		writer.setFlags(flags)
		writer.commit()

	return True


def importObjectByPath(path, uti, data, spec, overwrite=False, flags=[]):
	try:
		# resolve the path
		(store, folder, name) = struct.walkPath(path, True)
		if name in folder:
			if not overwrite:
				return False
			try:
				return overwriteObject(folder[name], uti, data, spec, flags)
			except IOError:
				pass

		# create the object and add to dict
		handle = importObject(store, uti, data, spec, flags)
		if not handle:
			return False
		try:
			folder.append(connector.DocLink(store, handle.getDoc()))
			folder.save()
			return True
		finally:
			handle.close()

	except IOError:
		return False


def importFileByPath(impPath, impFile, overwrite=False, progress=None, error=None):
	# resolve the path
	(store, folder, name) = struct.walkPath(impPath, True)

	# create the object and add to dict
	if isinstance(impFile, list):
		counter = 0
		handles = []
		nn = "%s%d" % (name, counter)
		try:
			for f in impFile:
				while nn in folder:
					counter += 1
					nn = "%s%d" % (name, counter)
				if progress:
					progress(f, nn)
				handle = importFile(store, f)
				if handle:
					handles.append(handle)
					folder[nn] = connector.DocLink(store, handle.getDoc())
				elif error:
					error(f, nn)
			folder.save()
		finally:
			for handle in handles:
				handle.close()
	else:
		if (name in folder) and (not overwrite):
			raise ImporterError("Duplicate item name")

		handle = importFile(store, impFile, name)
		try:
			if handle:
				folder[name] = connector.DocLink(store, handle.getDoc())
				folder.save()
			else:
				raise ImporterError("Invalid file")
		finally:
			handle.close()

