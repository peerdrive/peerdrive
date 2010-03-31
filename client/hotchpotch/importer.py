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

import os, sys
import subprocess

import hpstruct
from hpconnector import HpConnector
from hpregistry import HpRegistry

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
	return hpstruct.loadJSON(data)


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


# returns a DocLink or None
def importFile(store, path, name=""):
	if os.path.isfile(path):
		# determine file type
		uti = None
		if mimeGuess:
			mime = mimeGuess.file(path).split(' ')[0]
			uti = HpRegistry().getUtiFromMime(mime, None)
		if not uti:
			ext  = os.path.splitext(path)[1][1:].lower()
			uti  = HpRegistry().getUtiFromExtension(ext)
		if not name:
			name = os.path.basename(path)
		meta = {
			"org.hotchpotch.annotation" : {
				"title"   : name,
				"origin"  : path,
				"comment" : "Import from external file system"
			}
		}

		extractor = HpRegistry().getExtractor(uti)
		if extractor:
			additionalMeta = __runExtractor(extractor, path)
			if additionalMeta:
				__merge(meta, additionalMeta)

		#print 'META: ', repr(meta)
		with open(path, "rb") as file:
			with HpConnector().fork(store, uti) as writer:
				writer.write('FILE', file.read())
				writer.write('META', hpstruct.dumps(meta))
				writer.commit()
				link = hpstruct.DocLink(writer.getUUID())
				return link
	else:
		return None


# returns a DocLink or None
def importObject(store, uti, spec):
	link = None
	try:
		with HpConnector().fork(store, uti) as writer:
			for (fourcc, data) in spec:
				writer.writeAll(fourcc, data)
			writer.commit()
			link = hpstruct.DocLink(writer.getUUID())
	except IOError:
		pass
	return link


# return (store:guid, container:HpContainer, docName:str)
def walkPath(path, create=False):
	steps = path.split('/')
	storeName = steps[0]
	docName  = steps[-1]
	for i in xrange(1, len(steps)-1):
		steps[i] = (steps[i], steps[i+1])
	steps = steps[1:-1]

	# search for store
	enum = HpConnector().enum()
	storeGuid = None
	for mount in enum.allStores():
		if not enum.isMounted(mount):
			continue
		mountGuid = enum.guid(mount)
		if (mount == storeName) or (mountGuid.encode("hex").startswith(storeName)):
			storeGuid = mountGuid
			break
	if not storeGuid:
		raise ImporterError("Store not found")

	# walk the path
	curContainer = hpstruct.HpDict(hpstruct.DocLink(storeGuid, False))
	for (step, nextStep) in steps:
		next = curContainer.get(step)
		if next:
			curContainer = hpstruct.HpContainer(next)
		elif create:
			name = step.split(':')[-1]
			if ':' in nextStep:
				next = hpstruct.HpDict().create(name, storeGuid)
			else:
				next = hpstruct.HpSet().create(name, storeGuid)
			curContainer[step] = next
			curContainer.save()
			curContainer = hpstruct.HpContainer(next)
		else:
			raise ImporterError("Invalid path")

	# return result
	return (storeGuid, curContainer, docName)


def importObjectByPath(path, uti, spec, overwrite=False):
	try:
		# resolve the path
		(store, container, name) = walkPath(path, True)
		if (name in container) and (not overwrite):
			return False

		# create the object and add to dict
		container[name] = importObject(store, uti, spec)
		container.save()
		return True

	except IOError:
		return False


def importFileByPath(impPath, impFile, overwrite=False):
	# resolve the path
	(store, container, name) = walkPath(impPath, True)
	if (name in container) and (not overwrite):
		return ImporterError("Duplicate item name")

	# create the object and add to dict
	link = importFile(store, impFile, name)
	if link:
		container[name] = link
		container.save()
	else:
		raise ImporterError("Invalid file")

