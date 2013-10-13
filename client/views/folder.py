#!/usr/bin/env python
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

import os, os.path, copy
from PyQt4 import QtCore, QtGui
from datetime import datetime
import struct as pystruct

from peerdrive import Connector, Registry
from peerdrive import struct, importer, fuse, connector
from peerdrive.connector import Watch, Stat
from peerdrive.gui import widgets, utils

class AbortException(Exception):
	def __init__(self):
		pass

def countFilesRecursive(path):
	QtCore.QCoreApplication.processEvents()
	if os.path.isfile(path):
		return 1
	else:
		return sum([countFilesRecursive(os.path.join(path, f)) for f in
			os.listdir(path)])

def makeProgressHelper(p):
	i = [0]

	def progressHelper(path):
		QtCore.QCoreApplication.processEvents()
		p.setValue(i[0])
		if len(path) > 50:
			path = '...' + path[-50:]
		p.setLabelText(path)
		i[0] += 1
		if p.wasCanceled():
			raise AbortException

	return progressHelper

def getFileNamesFromMime(mime):
	if mime.hasFormat("FileGroupDescriptorW"):
		data = str(mime.data("FileGroupDescriptorW"))
		(count,) = pystruct.unpack_from("I", data)
		pos = pystruct.calcsize("I")
		result = []
		for i in xrange(count):
			(name,) = pystruct.unpack_from("72x520s", data, pos)
			result.append(name.decode('utf16').rstrip('\x00'))
			pos += pystruct.calcsize("72x520s")
		return result
	if mime.hasFormat("FileGroupDescriptor"):
		data = str(mime.data("FileGroupDescriptor"))
		(count,) = pystruct.unpack_from("I", data)
		pos = pystruct.calcsize("I")
		result = []
		for i in xrange(count):
			(name,) = pystruct.unpack_from("72x260s", data, pos)
			result.append(name.rstrip('\x00').decode('latin-1'))
			pos += pystruct.calcsize("72x260s")
		return result
	return []

class MetaColumnInfo(object):
	def __init__(self, key, spec):
		self.__key = key
		self.__name = spec["display"]
		self.__path = spec["key"]
		typ = spec["type"]
		if typ.startswith("list of"):
			self.__convert = MetaColumnInfo.__convertList
		elif typ == "datetime":
			self.__convert = MetaColumnInfo.__convertDateTime
		else:
			self.__convert = MetaColumnInfo.__convertNone
		self.__editable = typ == "string"
		self.__default = ""

	def removable(self):
		return True

	def editable(self):
		return self.__editable

	def derived(self):
		return True

	def default(self):
		return self.__default

	def name(self):
		return self.__name

	def key(self):
		return self.__key

	def extract(self, stat, metaData):
		item = metaData
		for step in self.__path:
			if step in item:
				item = item[step]
			else:
				return self.__default
		return self.__convert(item)

	def update(self, metaData, data):
		for step in self.__path[:-1]:
			if step not in metaData:
				metaData[step] = {}
			metaData = metaData[step]
		[lastStep] = self.__path[-1:]
		metaData[lastStep] = data

	@staticmethod
	def __convertNone(item):
		return unicode(item)

	@staticmethod
	def __convertList(item):
		if isinstance(item, list):
			if item == []:
				return ""
			else:
				return reduce(lambda x,y: unicode(x) + u', ' + unicode(y), item)
		else:
			return "#VALUE"

	@staticmethod
	def __convertDateTime(item):
		if isinstance(item, int) or isinstance(item, long):
			mtime = datetime.fromtimestamp(item)
			return str(mtime)
		else:
			return "#VALUE"


class StatColumnInfo(object):
	def __init__(self, key):
		self.__key = key
		self.__default = ""
		if key == ":size":
			self.__name = "Size"
			self.__extractor = StatColumnInfo.__extractSize
		elif key == ":mtime":
			self.__name = "Modification time"
			self.__extractor = lambda s: str(s.mtime())
		elif key == ":type":
			self.__name = "Type code"
			self.__extractor = lambda s: s.type()
		elif key == ":creator":
			self.__name = "Creator code"
			self.__extractor = lambda s: s.creator()
		elif key == ":comment":
			self.__name = "Comment"
			self.__extractor = lambda s: s.comment()
		else:
			raise KeyError("Invalid StatColumnInfo key")

	def removable(self):
		return True

	def editable(self):
		return False

	def derived(self):
		return True

	def default(self):
		return self.__default

	def name(self):
		return self.__name

	def key(self):
		return self.__key

	def extract(self, stat, metaData):
		return self.__extractor(stat)

	@staticmethod
	def __extractSize(stat):
		size = stat.dataSize()
		for att in stat.attachments():
			size += stat.size(att)
		for unit in ['Bytes', 'KiB', 'MiB', 'GiB']:
			if size < (1 << 10):
				break
			else:
				size = size >> 10
		return "%d %s" % (size, unit)


def _columnFactory(key):
	(uti, path) = key.split(':')
	if uti == "":
		return StatColumnInfo(key)
	else:
		for meta in Registry().search(uti, "meta", recursive=False, default=[]):
			if path == reduce(lambda x,y: x+"/"+y, meta["key"]):
				return MetaColumnInfo(key, meta)
	return None


class FolderEntry(Watch):
	def __init__(self, item, model, columns):
		self.__model = model
		self.__item  = copy.deepcopy(item)
		self.__valid = False
		self.__isFolder = False
		self.__icon  = None
		self.__uti   = None
		self.__replacable = False
		self.__columnValues = [ column.default() for column in columns ]
		self.__columnDefs = columns[:]
		self.__metaData = None

		link = self.__item[''].update(self.__model.getStore())
		self.__store = model.getStore()
		self.__doc = link.doc()

		if isinstance(link, connector.DocLink):
			super(FolderEntry, self).__init__(Watch.TYPE_DOC, link.doc())
		else:
			super(FolderEntry, self).__init__(Watch.TYPE_REV, link.rev())

		self.update(False)

	def isValid(self):
		return self.__valid

	def isFolder(self):
		return self.__isFolder

	def editable(self):
		return self.__doc is not None

	def overwritable(self):
		return self.__valid and self.__replacable

	def getColumnData(self, index):
		return self.__columnValues[index]

	def setColumnData(self, index, data):
		if self.__doc and (self.__metaData is not None):
			meta = copy.deepcopy(self.__metaData) # make sure we can revert!
			self.__columnDefs[index].update(meta, data)
			try:
				with Connector().update(self.__store, self.__doc, self.__rev) as w:
					w.setData('/org.peerdrive.annotation', meta)
					w.commit("Changed " + self.__columnDefs[index].name())
				self.__rev = w.getRev()
				self.__metaData = meta
				self.__columnValues[index] = data
				return True
			except IOError:
				pass

		return False

	def insColumn(self, index, column):
		self.__columnValues.insert(index, column.default())
		self.__columnDefs.insert(index, column)
		self.__updateColumns()

	def remColumn(self, index):
		del self.__columnValues[index]
		del self.__columnDefs[index]

	def setColumns(self, columns):
		values = []
		for column in columns:
			if column.derived():
				values.append(column.default())
			else:
				old = column.default()
				for (oldVal, oldDef) in zip(self.__columnValues, self.__columnDefs):
					if oldDef.key() == column.key():
						old = oldVal
						break
				values.append(old)
		self.__columnValues = values
		self.__columnDefs = columns[:]
		self.__updateColumns()

	def getLink(self):
		return self.__item['']

	def getItem(self):
		return self.__item

	def getIcon(self):
		if (not self.__valid) and (not self.__icon):
			self.__icon = QtGui.QIcon("icons/uti/file_broken.png")
		return self.__icon

	def getTypeCode(self):
		return self.__uti

	def update(self, updateItem = True):
		# reset everything
		self.__valid = False
		self.__icon = None
		for i in xrange(len(self.__columnDefs)):
			column = self.__columnDefs[i]
			if column.derived():
				self.__columnValues[i] = column.default()

		# determine revision
		needMerge = False
		isReplicated = False
		if self.__doc:
			l = Connector().lookupDoc(self.__doc)
			isReplicated = len(l.stores()) > 1
			revisions = l.revs()
			if len(revisions) == 0:
				return
			elif len(revisions) > 1:
				needMerge = True
			if updateItem:
				self.__item[''].update()

		self.__rev = self.__item[''].rev()

		# stat
		try:
			s = Connector().stat(self.__rev)
		except IOError:
			return
		self.__uti = s.type()
		if needMerge or isReplicated:
			image = QtGui.QImage(Registry().getIcon(s.type()))
			painter = QtGui.QPainter()
			painter.begin(image)
			if needMerge:
				painter.drawImage(0, 16, QtGui.QImage("icons/emblems/split.png"))
			elif isReplicated:
				painter.drawImage(0, 16, QtGui.QImage("icons/emblems/distributed.png"))
			painter.end()
			self.__icon = QtGui.QIcon(QtGui.QPixmap.fromImage(image))
		else:
			self.__icon = QtGui.QIcon(Registry().getIcon(s.type()))

		self.__isFolder = Registry().conformes(self.__uti, "org.peerdrive.folder")
		self.__replacable = not needMerge and not self.__isFolder
		self.__valid = True
		self.__updateColumns(s)

	def __updateColumns(self, stat = None):
		# This makes only sense if we're a valid entry
		if not self.__valid:
			return

		try:
			if stat is None:
				stat = Connector().stat(self.__rev)
			with Connector().peek(self.__store, self.__rev) as r:
				try:
					metaData = r.getData("/org.peerdrive.annotation")
				except:
					metaData = { }

			for i in xrange(len(self.__columnDefs)):
				column = self.__columnDefs[i]
				if column.derived():
					self.__columnValues[i] = column.extract(stat, metaData)

			self.__metaData = metaData

		except IOError:
			self.__metaData = None
			for i in xrange(len(self.__columnDefs)):
				column = self.__columnDefs[i]
				if column.derived():
					self.__columnValues[i] = column.default()

	# callback when watch was triggered
	def triggered(self, cause, store):
		if cause in [Watch.EVENT_MODIFIED, Watch.EVENT_REPLICATED, Watch.EVENT_DIMINISHED]:
			self.update()
			self.__model.entryChanged(self)
		elif cause == Watch.EVENT_APPEARED:
			self.update()
			self.__model.entryAppeared(self)
		elif cause == Watch.EVENT_DISAPPEARED:
			self.__valid = False
			self.__icon  = None
			self.__model.entryRemoved(self)


class FolderModel(QtCore.QAbstractTableModel):
	AUTOCLEAN = ["org.peerdrive.folder", "autoclean"]
	UTIs = ["org.peerdrive.folder", "org.peerdrive.store"]

	def __init__(self, parent = None):
		super(FolderModel, self).__init__(parent)
		self.__parent = parent

		self._listing = []
		self._columns = []
		self.__typeCodes = set()
		self.__changedContent = False
		self.__autoClean = False
		self.__mutable = False
		self.__store = None
		self.setColumns(["public.item:title"])

	def doLoad(self, handle, readWrite, autoClean):
		self.__mutable = readWrite
		self.__changedContent = False
		self.__autoClean = autoClean
		self.__typeCodes = set()
		self.__store = handle.getStore()
		self._listing = []
		data = handle.getData('/org.peerdrive.folder')
		listing = [ FolderEntry(item, self, self._columns) for item in data ]
		for entry in listing:
			if entry.isValid() or (not self.__autoClean):
				self.__typeCodes.add(entry.getTypeCode())
				self._listing.append(entry)
				Connector().watch(entry)
			else:
				self.__changedContent = True
		self.reset()

	def doSave(self, handle):
		data = [ item.getItem() for item in self._listing ]
		handle.setData('/org.peerdrive.folder', data)
		self.__changedContent = False

	def clear(self):
		for item in self._listing:
			Connector().unwatch(item)
		self._listing = []
		del self.__parent

	def hasChanged(self):
		return self.__changedContent

	def typeCodes(self):
		return self.__typeCodes

	def getItem(self, index):
		if index.isValid():
			return self._listing[index.row()]
		else:
			return None

	def isDoc(self):
		return self.__mutable

	def getStore(self):
		return self.__store

	def getItemLink(self, index):
		item = self.getItem(index)
		if item:
			return item.getLink()
		else:
			return None

	def setAutoClean(self, autoClean):
		self.__autoClean = autoClean
		if autoClean and self.__mutable:
			removed = [x for x in self._listing if not x.isValid()]
			self._listing = [x for x in self._listing if x.isValid()]
			if len(removed) > 0:
				self.__changedContent = True
				for item in removed:
					Connector().unwatch(item)
				self.reset()

	def getColumns(self):
		return [c.key() for c in self._columns]

	def setColumns(self, columns):
		self._columns = [ci for ci in [_columnFactory(c) for c in columns]
			if ci is not None]
		for i in self._listing:
			i.setColumns(self._columns)
		self.reset()

	def addColumn(self, columnKey):
		index = len(self._columns)
		self.insColumn(index, columnKey)

	def insColumn(self, index, columnKey):
		colInfo = _columnFactory(columnKey)
		if colInfo is not None:
			self.beginInsertColumns(QtCore.QModelIndex(), index, index)
			self._columns.insert(index, colInfo)
			for i in self._listing:
				i.insColumn(index, colInfo)
			self.endInsertColumns()

	def remColumn(self, columnKey):
		for index in xrange(len(self._columns)):
			if self._columns[index].key() == columnKey:
				self.beginRemoveColumns(QtCore.QModelIndex(), index, index)
				del self._columns[index]
				for i in self._listing:
					i.remColumn(index)
				self.endRemoveColumns()
				return

	# === view callbacks ===

	def hasChildren(self, parent):
		return False

	def rowCount(self, parent):
		if parent.isValid():
			return 0
		else:
			return len(self._listing)

	def columnCount(self, parent):
		return len(self._columns)

	def data(self, index, role):
		if not index.isValid():
			return QtCore.QVariant()
		if index.row() >= self.rowCount(QtCore.QModelIndex()):
			return QtCore.QVariant()
		if index.column() >= self.columnCount(None):
			return QtCore.QVariant()

		if (role == QtCore.Qt.DisplayRole) or (role == QtCore.Qt.EditRole):
			return QtCore.QVariant(self._listing[index.row()].getColumnData(index.column()))
		elif (role == QtCore.Qt.DecorationRole) and (index.column() == 0):
			return QtCore.QVariant(self._listing[index.row()].getIcon())
		#elif (role == QtCore.Qt.ForegroundRole):
		#	return QtCore.QVariant(QtGui.QColor(QtCore.Qt.red))
		else:
			return QtCore.QVariant()

	def setData(self, index, value, role):
		if index.isValid() and (role == QtCore.Qt.EditRole) and self.__mutable:
			col = index.column()
			if self._columns[col].editable():
				text = str(value.toString())
				# has the value changed at all?
				if self._listing[index.row()].getColumnData(col) == text:
					return False
				# valid edit?
				if not self.validateEdit(index, text):
					return False
				# valid... do it
				if self._listing[index.row()].setColumnData(col, text):
					self.__changedContent = True
					self.dataChanged.emit(index, index)
					return True
				else:
					return False
		return False

	def headerData(self, section, orientation, role):
		if role != QtCore.Qt.DisplayRole:
			return QtCore.QVariant()

		if orientation == QtCore.Qt.Horizontal:
			return QtCore.QVariant(self._columns[section].name())
		else:
			return QtCore.QVariant("Row %d" % (section))

	def removeRows(self, position, rows, parent):
		if not self.__mutable:
			return False
		self.__changedContent = True
		self.beginRemoveRows(QtCore.QModelIndex(), position, position+rows-1)
		for i in range(rows):
			Connector().unwatch(self._listing[position])
			del self._listing[position]
		self.endRemoveRows()
		return True

	def flags(self, index):
		if index.isValid():
			flags = QtCore.Qt.ItemIsEnabled | QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsDragEnabled
			if self._columns[index.column()].editable() and self._listing[index.row()].editable():
				flags |= QtCore.Qt.ItemIsEditable
			if self._listing[index.row()].overwritable():
				flags |= QtCore.Qt.ItemIsDropEnabled
		else:
			if self.__mutable:
				flags = QtCore.Qt.ItemIsDropEnabled
			else:
				flags = 0
		return flags

	# === DnD callbacks ===

	def supportedDropActions(self):
		if self.__mutable:
			return QtCore.Qt.CopyAction# | QtCore.Qt.MoveAction | QtCore.Qt.LinkAction
		else:
			return 0

	def mimeTypes(self):
		types = QtCore.QStringList()
		types << connector.LINK_MIME_TYPE
		types << "text/uri-list"
		types << "application/x-qt-windows-mime;value=\"FileContents\""
		types << "application/x-qt-windows-mime;value=\"FileGroupDescriptor\""
		types << "application/x-qt-windows-mime;value=\"FileGroupDescriptorW\""
		return types

	def mimeData(self, indexes):
		links = [self.getItemLink(index) for index in indexes
			if index.isValid() and (index.column() == 0)]
		if not links:
			return None

		mimeData = QtCore.QMimeData()
		connector.dumpMimeData(mimeData, links)
		fuseData = []
		for link in links:
			if isinstance(link, connector.DocLink):
				f = fuse.findFuseFile(link)
				if f:
					fuseData.append(f)
		if fuseData:
			mimeData.setUrls([QtCore.QUrl.fromLocalFile(f) for f in fuseData])
		return mimeData

	def dropMimeData(self, data, action, row, column, parent):
		if not self.__mutable:
			return True
		if action == QtCore.Qt.IgnoreAction:
			return True

		if data.hasFormat(connector.LINK_MIME_TYPE):
			return self.__dropLinks(connector.loadMimeData(data))
		if data.hasFormat('text/uri-list'):
			return self.__dropFile(data, parent)
		if data.hasFormat('FileContents'):
			return self.__dropContents(data)

		return False

	def __dropFile(self, data, onto):
		urlList = data.urls()
		if onto.isValid():
			if len(urlList) != 1:
				choice = QtGui.QMessageBox.question(self.__parent, "Overwrite",
					"Cannot overwrite: more than one dragged file. Import instead?",
					QtGui.QMessageBox.Yes | QtGui.QMessageBox.No,
					QtGui.QMessageBox.No)
				if choice != QtGui.QMessageBox.Yes:
					return False
			elif not os.path.isfile(str(urlList[0].toLocalFile().toUtf8())):
				choice = QtGui.QMessageBox.question(self.__parent, "Overwrite",
					"Cannot overwrite: dragged item is not a file. Import instead?",
					QtGui.QMessageBox.Yes | QtGui.QMessageBox.No,
					QtGui.QMessageBox.No)
				if choice != QtGui.QMessageBox.Yes:
					return False
			else:
				choice = QtGui.QMessageBox.question(self.__parent, "Overwrite",
					"Do you want to overwrite the selected item? If not, the file will be imported as new item.",
					QtGui.QMessageBox.Yes | QtGui.QMessageBox.No | QtGui.QMessageBox.Cancel,
					QtGui.QMessageBox.No)

				if choice == QtGui.QMessageBox.Cancel:
					return False
				elif choice == QtGui.QMessageBox.Yes:
					link = self.getItemLink(onto)
					path = str(urlList[0].toLocalFile().toUtf8())
					try:
						return importer.overwriteFile(link, path)
					except IOError:
						pass
					except OSError:
						pass
					return False

		# count the number of files
		progress = QtGui.QProgressDialog("Counting files...", "Abort", 0,
			len(urlList), self.__parent);
		progress.setWindowModality(QtCore.Qt.WindowModal)
		progress.setMinimumDuration(500)
		numFiles = 0
		i = 0
		try:
			for url in urlList:
				progress.setValue(i)
				if progress.wasCanceled():
					return False
				numFiles += countFilesRecursive(str(url.toLocalFile().toUtf8()))
				i += 1
		finally:
			progress.setValue(len(urlList))

		# import and add to folder
		progress = QtGui.QProgressDialog("Importing files...", "Abort", 0,
			numFiles, self.__parent);
		progress.setWindowModality(QtCore.Qt.WindowModal)
		progress.setMinimumDuration(500)

		try:
			helper = makeProgressHelper(progress)
			for url in urlList:
				path = str(url.toLocalFile().toUtf8())
				handle = importer.importFile(self.__store, path, progress=helper)
				if handle:
					try:
						self.insertLink(connector.DocLink(self.__store, handle.getDoc()))
						self.__parent.save()
					finally:
						handle.close()
		except AbortException:
			pass
		finally:
			progress.setValue(numFiles)

		return True

	def __dropLinks(self, links):
		if not links:
			return False

		dropMenu = QtGui.QMenu()
		repAct = dropMenu.addAction("Replicate here")
		copyAct = dropMenu.addAction("Copy here")
		dropMenu.addSeparator()
		dropMenu.addAction("Abort")

		copyAct.setEnabled(
			any([isinstance(l, connector.DocLink) for l in links]))
		action = dropMenu.exec_(QtGui.QCursor.pos())
		if action is repAct:
			for link in links:
				if self.validateDragEnter(link):
					handle = ReplicateHelper(self.__parent, self.__store, link)
					try:
						self.insertLink(link)
						self.__parent.save()
					finally:
						handle.close()
		elif action is copyAct:
			for link in links:
				if isinstance(link, connector.RevLink):
					if self.validateDragEnter(link):
						handle = ReplicateHelper(self.__parent, self.__store, link)
						try:
							self.insertLink(link)
							self.__parent.save()
						finally:
							handle.close()
				else:
					with struct.copyDoc(link, self.__store) as handle:
						self.insertLink(connector.DocLink(self.__store, handle.getDoc()))
						self.__parent.save()
		else:
			return False

		return True

	def __dropContents(self, mime):
		# unfortunately Qt will only return the first object and nothing
		# in case of Outlook messages
		content = str(mime.data('FileContents'))
		if len(content) == 0:
			return False

		name = getFileNamesFromMime(mime)[0]
		ext  = os.path.splitext(name)[1].lower()
		uti  = Registry().getUtiFromExtension(ext)
		data = { "org.peerdrive.annotation" : { "title" : name } },
		spec = [ ('_', content) ],
		handle = importer.importObject(self.__store, uti, data, spec, [])
		if handle:
			try:
				self.insertLink(connector.DocLink(self.__store, handle.getDoc()))
				self.__parent.save()
			finally:
				handle.close()

		return True

	def insertLink(self, link):
		entry = FolderEntry({'' : link}, self, self._columns)
		self.__typeCodes.add(entry.getTypeCode())

		# append new item
		self.__changedContent = True
		endRow = self.rowCount(QtCore.QModelIndex())
		self.beginInsertRows(QtCore.QModelIndex(), endRow, endRow)
		self._listing.append(entry)
		self.endInsertRows()
		Connector().watch(entry)

		return True

	def validateEdit(self, index, newValue):
		return True

	def validateDragEnter(self, link):
		for item in self._listing:
			if item.getLink() == link:
				return False
		return True

	# === Callbacks from a FolderEntry which has changed ===

	def entryChanged(self, entry):
		i = self._listing.index(entry)
		leftIdx  = self.index(i, 0)
		rightIdx = self.index(i, self.columnCount(None)-1)
		self.emit(QtCore.SIGNAL("dataChanged(const QModelIndex&,const QModelIndex&)"), leftIdx, rightIdx)

	def entryRemoved(self, entry):
		if self.__autoClean and self.__mutable:
			i = self._listing.index(entry)
			self.removeRows(i, 1, None)
		else:
			self.entryChanged(entry)

	def entryAppeared(self, entry):
		self.entryChanged(entry)


class FolderTreeView(QtGui.QTreeView):
	def __init__(self, parent):
		super(FolderTreeView, self).__init__()
		self.__parent = parent

	def dragEnterEvent(self, event):
		if event.source() is self:
			return
		else:
			data = event.mimeData()
			links = connector.loadMimeData(data)
			# drag to ourself or already contained?
			if any([l.doc() == self.__parent.doc() for l in links]):
				return
		QtGui.QTreeView.dragEnterEvent(self, event)

	def contextMenuEvent(self, event):
		menu = QtGui.QMenu(self)
		self.__parent.fillContextMenu(menu)
		menu.exec_(event.globalPos())


class FolderSortProxy(QtGui.QSortFilterProxyModel):
	def __init__(self, parent=None):
		super(FolderSortProxy, self).__init__(parent)

	def lessThan(self, left, right):
		src = self.sourceModel()
		leftItem = src.getItem(left)
		rightItem = src.getItem(right)
		if leftItem.isFolder() != rightItem.isFolder():
			return leftItem.isFolder() and not rightItem.isFolder()
		return (leftItem.getColumnData(left.column()) <
			rightItem.getColumnData(right.column()))


class FolderWidget(widgets.DocumentView):

	# itemOpen(link, executable, browseHint)
	itemOpen = QtCore.pyqtSignal(object, object, bool)

	selectionChanged = QtCore.pyqtSignal()

	def __init__(self, browseTypes=[]):
		super(FolderWidget, self).__init__("org.peerdrive.folderview")

		self.__settings = None
		self.__browseTypes = browseTypes
		self.__folderModel = None
		self.__filterModel = FolderSortProxy()
		self.__filterModel.setSortCaseSensitivity(QtCore.Qt.CaseInsensitive)
		self.__filterModel.setDynamicSortFilter(True)
		self.mutable.connect(self.__setMutable)

		self.itemDelAct = QtGui.QAction(QtGui.QIcon('icons/edittrash.png'), "&Delete", self)
		self.itemDelAct.setShortcut(QtGui.QKeySequence.Delete)
		self.itemDelAct.setStatusTip("Delete selected item(s)")
		self.itemDelAct.triggered.connect(self.__removeRows)
		self.itemPropertiesAct = QtGui.QAction("&Properties", self)
		self.itemPropertiesAct.triggered.connect(self.__showItemProperties)
		self.selfPropertiesAct = QtGui.QAction("&Properties", self)
		self.selfPropertiesAct.triggered.connect(self.__showSelfProperties)

		self.listView = FolderTreeView(self)
		self.listView.setSelectionMode(QtGui.QAbstractItemView.ExtendedSelection)
		self.listView.setDragEnabled(True)
		self.listView.setAutoScroll(True)
		self.listView.setExpandsOnDoubleClick(False)
		self.listView.setRootIsDecorated(False)
		self.listView.setEditTriggers(
			QtGui.QAbstractItemView.SelectedClicked |
			QtGui.QAbstractItemView.EditKeyPressed)
		self.listView.setSortingEnabled(True)
		self.listView.setDragDropOverwriteMode(True)
		self.listView.setDropIndicatorShown(True)
		self.listView.setModel(self.__filterModel)
		self.listView.addAction(self.itemDelAct)
		self.listView.activated.connect(self.__doubleClicked)
		self.setCentralWidget(self.listView)

	def docClose(self, save=True):
		super(FolderWidget, self).docClose(save)
		self.__setModel(None)

	def docRead(self, readWrite, handle):
		stat = Connector().stat(self.rev())
		uti = stat.type()
		if uti not in FolderModel.UTIs:
			raise TypeError('Unhandled type code: %s' % (uti))
		model = FolderModel(self)

		self.__setModel(model)
		if self.__settings is not None:
			self.__applySettings(self.__settings)

		model.rowsInserted.connect(self._emitSaveNeeded)
		model.rowsRemoved.connect(self._emitSaveNeeded)
		model.dataChanged.connect(self.__dataChanged)
		model.modelReset.connect(self.__dataChanged)
		model.modelReset.connect(lambda: self.selectionChanged.emit())
		self.listView.selectionModel().selectionChanged.connect(
			lambda: self.selectionChanged.emit())

		autoClean = self.metaDataGetField(FolderModel.AUTOCLEAN, False)
		model.doLoad(handle, readWrite, autoClean)
		if model.hasChanged():
			self._emitSaveNeeded()

	def docSave(self, handle):
		if self.model().hasChanged():
			self.model().doSave(handle)

	def docMergeCheck(self, heads, types, changedParts):
		(uti, handled) = super(FolderWidget, self).docMergeCheck(heads, types, changedParts)
		return (uti, handled | set(['PDSD']))

	def docMergePerform(self, writer, baseReader, mergeReaders, changedParts):
		conflicts = super(FolderWidget, self).docMergePerform(writer, baseReader, mergeReaders, changedParts)
		if 'PDSD' in changedParts:
			basePdsd = struct.loads(self.store(), baseReader.readAll('PDSD'))
			mergePdsd = []
			for r in mergeReaders:
				mergePdsd.append(struct.loads(self.store(), r.readAll('PDSD')))
			(newPdsd, newConflict) = struct.merge(basePdsd, mergePdsd)
			conflicts = conflicts or newConflict
			writer.writeAll('PDSD', struct.dumps(newPdsd))

		return conflicts

	def model(self):
		return self.__folderModel

	def modelMapIndex(self, index):
		return self.__filterModel.mapToSource(index)

	def __setModel(self, model):
		self.__filterModel.setSourceModel(model)
		if self.__folderModel:
			self.__folderModel.clear()
			self.__folderModel.deleteLater()
		self.__folderModel = model

	# reimplemented to catch changes to "autoClean"
	def metaDataSetField(self, field, value):
		super(FolderWidget, self).metaDataSetField(field, value)
		if field == FolderModel.AUTOCLEAN:
			model = self.model()
			if model:
				model.setAutoClean(value)

	def _saveSettings(self, settings):
		super(FolderWidget, self)._saveSettings(settings)
		if self.__folderModel:
			settings["columns"] = self.__folderModel.getColumns()
			settings["colwidths"] = [self.listView.columnWidth(i)
				for i in xrange(self.__folderModel.columnCount(None))]
			settings["sortcol"] = self.__filterModel.sortColumn()
			settings["sortorder"] = self.__filterModel.sortOrder()

	def _loadSettings(self, settings):
		super(FolderWidget, self)._loadSettings(settings)
		self.__settings = settings
		self.__applySettings(settings)

	def __applySettings(self, settings):
		if not self.__folderModel:
			return
		columns = settings.get("columns")
		if columns:
			self.__folderModel.setColumns(columns)
		widths = settings.get("colwidths", [])[:self.__folderModel.columnCount(None)]
		i = 0
		for w in widths[:-1]:
			self.listView.setColumnWidth(i, w)
			i += 1
		self.listView.resizeColumnToContents(i)
		sortColumn = settings.get("sortcol", 0)
		sortOrder  = settings.get("sortorder", QtCore.Qt.AscendingOrder)
		self.listView.sortByColumn(sortColumn, sortOrder)

	def __setMutable(self, enabled):
		self.isMutable = enabled
		self.listView.setAcceptDrops(enabled)
		self.listView.setDropIndicatorShown(enabled)

	def __dataChanged(self):
		# some fields in the model have changed. Doesn't mean we have to save...
		if self.model().hasChanged():
			self._emitSaveNeeded()

	def __doubleClicked(self, index):
		link = self.model().getItemLink(self.modelMapIndex(index))
		if link:
			try:
				uti = Connector().stat(link.rev()).type()
				executables = Registry().getExecutables(uti)
			except IOError:
				executables = []
			self.itemOpen.emit(link, None,
				"org.peerdrive.browser.py" in executables)

	def __showItemProperties(self):
		index = self.listView.selectionModel().currentIndex()
		link = self.model().getItemLink(self.modelMapIndex(index))
		if link:
			utils.showProperties(link)

	def __showSelfProperties(self):
		if self.doc():
			link = connector.DocLink(self.store(), self.doc(), False)
		else:
			link = connector.RevLink(self.store(), self.rev())
		utils.showProperties(link)

	def __removeRows(self):
		rows = [self.modelMapIndex(i) for i in self.listView.selectionModel().selectedRows()]
		rows = [(i.row(), i.row()) for i in rows if i.isValid()]
		rows.sort()
		rows = reduce(self.__concatRanges, rows, [])
		rows.reverse()
		model = self.model()
		for (start, end) in rows:
			model.removeRows(start, end-start+1, None)

	@staticmethod
	def __concatRanges(acc, right):
		if acc == []:
			return [right]
		else:
			(leftStart, leftStop) = left = acc.pop()
			(rightStart, rightStop) = right
			if leftStop+1 == rightStart:
				acc.append((leftStart, rightStop))
			else:
				acc.append(left)
				acc.append(right)
			return acc

	def getSelectedLinks(self):
		return [self.model().getItemLink(self.modelMapIndex(row)) for row in
			self.listView.selectionModel().selectedRows()]

	def fillContextMenu(self, menu):
		# get selected items
		isDoc = self.model().isDoc()
		links = [self.model().getItemLink(self.modelMapIndex(row)) for row in
			self.listView.selectionModel().selectedRows()]

		# selected an item?
		if len(links) > 0:
			if len(links) == 1:
				[link] = links
				if link:
					self.__addOpenActions(menu, link, isDoc)
					menu.addSeparator()
					self.__addReplicateActions(menu, link)
			# add default options
			if self.isMutable:
				menu.addSeparator()
				menu.addAction(self.itemDelAct)
			menu.addSeparator()
			menu.addAction(self.itemPropertiesAct)
		else:
			if self.isMutable:
				self.__addCreateActions(menu)
			menu.addSeparator()
			menu.addAction(self.selfPropertiesAct)

	def __addReplicateActions(self, menu, link):
		c = Connector()
		try:
			allVolumes = set(c.lookupRev(self.rev()))
			if isinstance(link, connector.DocLink):
				lookup = c.lookupDoc(link.doc())
				curVolumes = set(lookup.stores())
				try:
					for rev in lookup.revs():
						curVolumes = curVolumes & set(c.lookupRev(rev, curVolumes))
				except IOError:
					curVolumes = set()
			else:
				curVolumes = set(c.lookupRev(link.rev()))
		except IOError:
			return

		if not curVolumes:
			return

		srcVol = list(curVolumes)[0]
		repVolumes = allVolumes - curVolumes
		for store in repVolumes:
			name = struct.readTitle(connector.DocLink(store, store), "Unknown store")
			action = menu.addAction("Replicate item to '%s'" % name)
			action.triggered.connect(
				lambda x,l=link,s=store: self.__doReplicate(srcVol, l, s))

	def __doReplicate(self, srcStore, link, dstStore):
		if isinstance(link, connector.DocLink):
			Connector().replicateDoc(srcStore, link.doc(), dstStore, verbose=True)
		else:
			Connector().replicateRev(srcStore, link.rev(), dstStore, verbose=True)

	def __addCreateActions(self, menu):
		newMenu = menu.addMenu(QtGui.QIcon("icons/filenew.png"), "New document")
		action = newMenu.addAction(QtGui.QIcon("icons/uti/folder.png"), "Folder")
		action.triggered.connect(self.__doCreateFolder)
		newMenu.addSeparator()

		items = {}
		sysStore = Connector().enum().sysStore().sid
		sysDict = struct.Folder(connector.DocLink(sysStore, sysStore))
		templatesDoc = sysDict.get("templates")
		if templatesDoc:
			templatesDict = struct.Folder(templatesDoc.update(sysStore))
			items = templatesDict.items()
			items.sort(key=lambda item: item[0])
		if items:
			for (name, link) in items:
				rev = link.rev()
				icon = QtGui.QIcon(Registry().getIcon(Connector().stat(rev).type()))
				action = newMenu.addAction(icon, name)
				action.triggered.connect(lambda x,r=rev,n=name: self.__doCreateFromTemplate(sysStore, r, n))
		else:
			action = newMenu.addAction("No templates found")
			action.setEnabled(False)

	def __doCreateFolder(self):
		store = self.store()
		with Connector().create(store, "org.peerdrive.folder", "") as w:
			w.setData('', {
				"org.peerdrive.annotation" : {"title" : "Folder"},
				"org.peerdrive.folder" : []
			})
			w.setFlags([Stat.FLAG_STICKY])
			w.commit("Created")
			self.model().insertLink(connector.DocLink(store, w.getDoc()))
			self.save("Added new folder")

	def __doCreateFromTemplate(self, srcStore, srcRev, name):
		info = Connector().stat(srcRev, [srcStore])
		dstStore = self.store()
		with Connector().create(dstStore, info.type(), info.creator()) as w:
			with Connector().peek(srcStore, srcRev) as r:
				w.set_data('', r.get_data(''))
				for att in info.attachments():
					w.write(att, r.readAll(att))
				w.setFlags(r.stat().flags())
			w.commit("Created from template")
			destDoc = w.getDoc()
			# add link
			self.model().insertLink(connector.DocLink(dstStore, destDoc))
			# save immediately
			self.save("Added '"+name+"' from templates")

	def __addOpenActions(self, menu, link, isDoc):
		try:
			uti = Connector().stat(link.rev()).type()
			executables = Registry().getExecutables(uti)
		except IOError:
			executables = []

		prefix = "Open"
		browseHint = False
		browsePreferred = False
		if "org.peerdrive.browser.py" in executables:
			browsePreferred = True
		for e in executables:
			if e in self.__browseTypes:
				browseHint = True
				prefix = "Browse"
				break

		if browseHint:
			action = menu.addAction("&Browse")
			action.triggered.connect(lambda x,l=link: self.itemOpen.emit(l, None, True))
			if browsePreferred:
				menu.setDefaultAction(action)
			action = menu.addAction("&Open")
			action.triggered.connect(lambda x,l=link: self.itemOpen.emit(l, None, False))
			if not browsePreferred:
				menu.setDefaultAction(action)
		else:
			action = menu.addAction("&Open")
			action.triggered.connect(lambda x,l=link: self.itemOpen.emit(l, None, False))
			menu.setDefaultAction(action)
		if len(executables) > 1:
			openWith = menu.addMenu("Open with")
			for e in executables:
				action = openWith.addAction(e)
				action.triggered.connect(lambda x,l=link,e=e: self.itemOpen.emit(l, e, False))


class ReplicateHelper(object):
	def __init__(self, parent, dstStore, link):
		self.__src = link.store()
		self.__dst = dstStore
		self.__tag = None
		self.__result = None
		self.__progressBar = QtGui.QProgressDialog("Replicating...", "Abort",
			0, 255, parent)
		self.__progressBar.setWindowModality(QtCore.Qt.WindowModal)
		self.__progressBar.setMinimumDuration(250)
		self.__progressBar.canceled.connect(self.__cancel)
		Connector().regProgressHandler(start=self.__progressStart,
			progress=self.__progress)
		try:
			if isinstance(link, connector.DocLink):
				self.__item = link.doc()
				Connector().replicateDoc(link.store(),
					link.doc(), dstStore, verbose=True, async=self.__finished)
			else:
				self.__item = link.rev()
				Connector().replicateRev(link.store(),
					link.rev(), dstStore, verbose=True, async=self.__finished)

			while not self.__result:
				QtCore.QCoreApplication.processEvents(QtCore.QEventLoop.WaitForMoreEvents)

			if isinstance(self.__result, IOError):
				raise self.__result
		finally:
			self.__progressBar.setValue(255)
			self.__progressBar = None
			Connector().unregProgressHandler(start=self.__progressStart,
				progress=self.__progress)

	def close(self):
		if self.__result:
			self.__result.close()

	def __finished(self, result):
		self.__result = result

	def __cancel(self):
		if self.__tag is not None:
			Connector().progressStop(self.__tag)
			self.__tag = None

	def __progressStart(self, tag, typ, src, dst, item=None):
		if (src == self.__src) and (dst == self.__dst) and (item == self.__item):
			self.__tag = tag

	def __progress(self, tag, state, value, err_code=None, err_doc=None, err_rev=None):
		if self.__tag == tag:
			self.__progressBar.setValue(value)

