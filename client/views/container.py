#!/usr/bin/env python
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

import os.path
from PyQt4 import QtCore, QtGui
from datetime import datetime

from hotchpotch import Connector, Registry
from hotchpotch import struct, importer, fuse
from hotchpotch.connector import Watch
from hotchpotch.gui import widgets, utils


class NameColumnInfo(object):
	KEY = ":name"

	def __init__(self):
		pass

	def removable(self):
		return False

	def editable(self):
		return True

	def derived(self):
		return False

	def default(self):
		return None

	def name(self):
		return "Name"

	def key(self):
		return NameColumnInfo.KEY


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
		self.__default = ""

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
		item = metaData
		for step in self.__path:
			if step in item:
				item = item[step]
			else:
				return self.__default
		return self.__convert(item)

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
		size = 0
		for part in stat.parts():
			size += stat.size(part)
		for unit in ['Bytes', 'KiB', 'MiB', 'GiB']:
			if size < (1 << 10):
				break
			else:
				size = size >> 10
		return "%d %s" % (size, unit)


def _columnFactory(key):
	if key == NameColumnInfo.KEY:
		return NameColumnInfo()
	else:
		(uti, path) = key.split(':')
		if uti == "":
			return StatColumnInfo(key)
		else:
			for meta in Registry().search(uti, "meta", recursive=False, default=[]):
				if path == reduce(lambda x,y: x+"/"+y, meta["key"]):
					return MetaColumnInfo(key, meta)
	return None


class CollectionEntry(Watch):
	def __init__(self, link, model, columns):
		self.__model = model
		self.__link  = link
		self.__valid = False
		self.__icon  = None
		self.__uti   = None
		self.__replacable = False
		self.__columnValues = [ column.default() for column in columns ]
		self.__columnDefs = columns[:]

		if isinstance(link, struct.DocLink):
			super(CollectionEntry, self).__init__(Watch.TYPE_DOC, link.doc())
		else:
			super(CollectionEntry, self).__init__(Watch.TYPE_REV, link.rev())

		self.update()

	def isValid(self):
		return self.__valid

	def overwritable(self):
		return self.__valid and self.__replacable

	def getColumnData(self, index):
		return self.__columnValues[index]

	def setColumnData(self, index, data):
		self.__columnValues[index] = data

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
		return self.__link

	def getIcon(self):
		if (not self.__valid) and (not self.__icon):
			self.__icon = QtGui.QIcon("icons/uti/file_broken.png")
		return self.__icon

	def getTypeCode(self):
		return self.__uti

	def update(self):
		# reset everything
		self.__valid = False
		self.__icon = None
		for i in xrange(len(self.__columnDefs)):
			column = self.__columnDefs[i]
			if column.derived():
				self.__columnValues[i] = column.default()

		# determine revision
		needMerge = False
		if isinstance(self.__link, struct.DocLink):
			self.__link.update()
			revisions = self.__link.revs()
			if len(revisions) == 0:
				return
			elif len(revisions) > 1:
				needMerge = True
			# TODO: maybe sort by date
			rev = revisions[0]
		else:
			rev = self.__link.rev()
		self.__rev = rev

		# stat
		try:
			s = Connector().stat(rev)
		except IOError:
			return
		self.__uti = s.type()
		if needMerge:
			image = QtGui.QImage(Registry().getIcon(s.type()))
			painter = QtGui.QPainter()
			painter.begin(image)
			painter.drawImage(0, 0, QtGui.QImage("icons/uti/merge_overlay.png"))
			painter.end()
			self.__icon = QtGui.QIcon(QtGui.QPixmap.fromImage(image))
		else:
			self.__icon = QtGui.QIcon(Registry().getIcon(s.type()))

		# overwritable by external files?
		self.__replacable = (not needMerge) and (
			not Registry().conformes(self.__uti, "org.hotchpotch.container"))

		self.__valid = True
		self.__updateColumns()

	def __updateColumns(self):
		# This makes only sense if we're a valid entry
		if not self.__valid:
			return

		try:
			stat = Connector().stat(self.__rev)
			with Connector().peek(self.__rev) as r:
				try:
					metaData = struct.loads(r.readAll('META'))
				except:
					metaData = { }

			for i in xrange(len(self.__columnDefs)):
				column = self.__columnDefs[i]
				if column.derived():
					self.__columnValues[i] = column.extract(stat, metaData)

		except IOError:
			for i in xrange(len(self.__columnDefs)):
				column = self.__columnDefs[i]
				if column.derived():
					self.__columnValues[i] = column.default()

	# callback when watch was triggered
	def triggered(self, cause):
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


class CollectionModel(QtCore.QAbstractTableModel):
	AUTOCLEAN = ["org.hotchpotch.container", "autoclean"]

	def __init__(self, linkMap, parent = None):
		super(CollectionModel, self).__init__(parent)
		self.__linkMap = linkMap
		self.__parent = parent

		self._listing = []
		self._columns = []
		self.__typeCodes = set()
		self.__changedContent = False
		self.__autoClean = False
		self.__mutable = False
		self.setColumns(["public.item:org.hotchpotch.annotation/title"])

		self._dropMenu = QtGui.QMenu()
		self._docLinkAct = self._dropMenu.addAction("Link to Document")
		self._revLinkAct = self._dropMenu.addAction("Link to Version")
		self._dropMenu.addSeparator()
		self._abortAct = self._dropMenu.addAction("Abort")

	def doLoad(self, handle, readWrite, autoClean):
		self.__mutable = readWrite
		self.__changedContent = False
		self.__autoClean = autoClean
		self.__typeCodes = set()
		self._listing = []
		if self.__linkMap:
			data = struct.loads(handle.readAll('HPSD'),
				lookup=lambda doc: self.__linkMap.lookup(doc))
		else:
			data = struct.loads(handle.readAll('HPSD'))
		listing = self.decode(data)
		for entry in listing:
			if entry.isValid() or (not self.__autoClean):
				self.__typeCodes.add(entry.getTypeCode())
				self._listing.append(entry)
				Connector().watch(entry)
			else:
				self.__changedContent = True
		self.reset()

	def doSave(self, handle):
		data = self.encode()
		handle.writeAll('HPSD', struct.dumps(data))
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

	def getItemLinkReal(self, index):
		"""Returns the original link at the given position. Might point to nowhere."""
		item = self.getItem(index)
		if item:
			return item.getLink()
		else:
			return None

	def getItemLinkUser(self, index):
		"""Returns the link as the user would expect it. Might return None!"""
		link = self.getItemLinkReal(index)
		if link:
			if isinstance(link, struct.DocLink):
				if self.isDoc():
					return link
				else:
					if link.rev():
						return struct.RevLink(link.rev())
			elif isinstance(link, struct.RevLink):
				return link
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
				self._listing[index.row()].setColumnData(col, text)
				self.__changedContent = True
				self.emit(QtCore.SIGNAL("dataChanged(const QModelIndex&,const QModelIndex&)"), index, index)
				return True
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
			if self._columns[index.column()].editable() and self.__mutable:
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
		types << struct.DocLink.MIME_TYPE
		types << struct.RevLink.MIME_TYPE
		types << 'application/x-hotchpotch-linklist'
		types << "text/uri-list"
		return types

	def mimeData(self, indexes):
		nativeData = []
		fuseData = []
		for index in indexes:
			if index.isValid() and (index.column() == 0):
				link = self.getItemLinkUser(index)
				if isinstance(link, struct.DocLink):
					nativeData.append('doc:' + link.doc().encode('hex'))
					f = fuse.findFuseFile(link)
					if f:
						fuseData.append(f)
				elif isinstance(link, struct.RevLink):
					nativeData.append('rev:' + link.rev().encode('hex'))
		if nativeData == []:
			mimeData = None
		else:
			mimeData = QtCore.QMimeData()
			mimeData.setData('application/x-hotchpotch-linklist',
				reduce(lambda x,y: x+","+y, nativeData))
			if fuseData:
				fuseData = [QtCore.QUrl.fromLocalFile(f) for f in fuseData]
				mimeData.setUrls(fuseData)
		return mimeData

	def dropMimeData(self, data, action, row, column, parent):
		if not self.__mutable:
			return True
		if action == QtCore.Qt.IgnoreAction:
			return True

		if data.hasFormat('application/x-hotchpotch-linklist'):
			return self.__dropLinkList(data)
		if data.hasFormat('text/uri-list'):
			return self.__dropFile(data, parent)
		else:
			return self.__dropLink(data)

	def __dropFile(self, data, onto):
		# FIXME: find a better way than calling back to the parent
		stores = Connector().lookup_rev(self.__parent.rev())
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
					link = self.getItemLinkUser(onto)
					path = str(urlList[0].toLocalFile().toUtf8())
					try:
						return importer.overwriteFile(stores, link, path)
					except IOError:
						pass
					except OSError:
						pass
					return False

		# import and add to container
		for url in urlList:
			try:
				path = str(url.toLocalFile().toUtf8())
				handle = importer.importFile(stores, path)
				if handle:
					try:
						self.insertLink(struct.DocLink(handle.getDoc()))
						self.__parent.save()
					finally:
						handle.close()
			except IOError:
				pass
		return True

	def __dropLink(self, data):
		# parse link
		link = struct.loadMimeData(data)
		if not link:
			return False

		# what to do?
		if isinstance(link, struct.DocLink):
			self._docLinkAct.setEnabled(True)
			self._revLinkAct.setEnabled(len(link.revs()) == 1)
		else:
			self._docLinkAct.setEnabled(False)
		action = self._dropMenu.exec_(QtGui.QCursor.pos())
		if action is self._docLinkAct:
			pass
		elif action is self._revLinkAct:
			if isinstance(link, struct.DocLink):
				link = struct.RevLink(link.revs()[0])
		else:
			return False

		return self.insertLink(link)

	def __dropLinkList(self, mimeData):
		linkList = str(mimeData.data('application/x-hotchpotch-linklist'))
		for link in linkList.split(','):
			(cls, val) = link.split(':')
			if cls == 'doc':
				link = struct.DocLink(val.decode("hex"), False)
			elif cls == 'rev':
				link = struct.RevLink(val.decode("hex"))
			else:
				continue
			self.insertLink(link)
		return True

	def insertLink(self, link):
		entry = CollectionEntry(link, self, self._columns)
		if not self.validateInsert(entry):
			return False
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

	def validateInsert(self, entry):
		return True

	def validateDragEnter(self, link):
		return True

	# === Callbacks from a CollectionEntry which has changed ===

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


class DictModel(CollectionModel):
	UTIs = ["org.hotchpotch.dict", "org.hotchpotch.store"]

	def __init__(self, linkMap, parent = None):
		super(DictModel, self).__init__(linkMap, parent)

	def setColumns(self, columns):
		# make sure the static 'Name' column is included
		if NameColumnInfo.KEY not in columns:
			columns = [NameColumnInfo.KEY] + columns
		super(DictModel, self).setColumns(columns)

	def encode(self):
		data = { }
		for item in self._listing:
			data[item.getColumnData(0)] = item.getLink()
		return data

	def decode(self, data):
		listing = []
		for (name, link) in data.items():
			entry = CollectionEntry(link, self, self._columns)
			entry.setColumnData(0, name)
			listing.append(entry)
		return listing

	def validateEdit(self, index, newValue):
		if index.column() == 0:
			for item in self._listing:
				if item.getColumnData(0) == newValue:
					return False
			return True
		else:
			return False

	def validateInsert(self, entry):
		# create new item with unique name
		baseName = "Added document"
		name = baseName
		counter = 1
		while True:
			found = False
			for item in self._listing:
				if item.getColumnData(0) == name:
					found = True
			if found:
				name = (baseName + " (%d)") % (counter)
				counter = counter + 1
			else:
				break
		entry.setColumnData(0, name)
		return True


class SetModel(CollectionModel):
	UTIs = ["org.hotchpotch.set"]

	def __init__(self, linkMap, parent = None):
		super(SetModel, self).__init__(linkMap, parent)

	def setColumns(self, columns):
		# make sure the 'Name' column is not included
		columns = [col for col in columns if col != NameColumnInfo.KEY]
		super(SetModel, self).setColumns(columns)

	def encode(self):
		return [ item.getLink() for item in self._listing ]

	def decode(self, data):
		return [ CollectionEntry(link, self, self._columns) for link in data ]

	def validateDragEnter(self, link):
		for item in self._listing:
			if item.getLink() == link:
				return False
		return True


class CollectionTreeView(QtGui.QTreeView):
	def __init__(self, parent):
		super(CollectionTreeView, self).__init__()
		self.__parent = parent

	def dragEnterEvent(self, event):
		if event.source() is self:
			return
		else:
			data = event.mimeData()
			link = struct.loadMimeData(data)
			if link:
				# ourself?
				if isinstance(link, struct.DocLink):
					if link.doc() == self.__parent.doc():
						return
				# already contained?
				if not self.__parent.model().validateDragEnter(link):
					return
		QtGui.QTreeView.dragEnterEvent(self, event)

	def contextMenuEvent(self, event):
		menu = QtGui.QMenu(self)
		self.__parent.fillContextMenu(menu)
		menu.exec_(event.globalPos())


class CollectionWidget(widgets.DocumentView):

	# itemOpen(link, executable, browseHint)
	itemOpen = QtCore.pyqtSignal(object, object, bool)

	selectionChanged = QtCore.pyqtSignal()

	def __init__(self, browseTypes=[]):
		super(CollectionWidget, self).__init__("org.hotchpotch.containerview")

		self.__settings = None
		self.__browseTypes = browseTypes
		self.__containerModel = None
		self.__filterModel = QtGui.QSortFilterProxyModel()
		self.__filterModel.setSortCaseSensitivity(QtCore.Qt.CaseInsensitive)
		self.mutable.connect(self.__setMutable)

		self.itemDelAct = QtGui.QAction(QtGui.QIcon('icons/edittrash.png'), "&Delete", self)
		self.itemDelAct.setShortcut(QtGui.QKeySequence.Delete)
		self.itemDelAct.setStatusTip("Delete selected item(s)")
		self.itemDelAct.triggered.connect(self.__removeRows)
		self.itemPropertiesAct = QtGui.QAction("&Properties", self)
		self.itemPropertiesAct.triggered.connect(self.__showProperties)

		self.listView = CollectionTreeView(self)
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

	def docClose(self):
		super(CollectionWidget, self).docClose()
		self.__setModel(None)

	def docRead(self, readWrite, handle):
		stat = Connector().stat(self.rev())
		uti = stat.type()
		if readWrite:
			linkMap = None
		else:
			linkMap = stat.linkMap()
		if uti in DictModel.UTIs:
			model = DictModel(linkMap, self)
		elif uti in SetModel.UTIs:
			model = SetModel(linkMap, self)
		else:
			raise TypeError('Unhandled type code: %s' % (uti))

		self.__setModel(model)
		if self.__settings:
			self.__applySettings(self.__settings)

		model.rowsInserted.connect(self._emitSaveNeeded)
		model.rowsRemoved.connect(self._emitSaveNeeded)
		model.dataChanged.connect(self.__dataChanged)
		model.modelReset.connect(self.__dataChanged)
		model.modelReset.connect(lambda: self.selectionChanged.emit())
		self.listView.selectionModel().selectionChanged.connect(
			lambda: self.selectionChanged.emit())

		autoClean = self.metaDataGetField(CollectionModel.AUTOCLEAN, False)
		model.doLoad(handle, readWrite, autoClean)
		if model.hasChanged():
			self._emitSaveNeeded()

	def docSave(self, handle):
		if self.model().hasChanged():
			self.model().doSave(handle)

	def docMergeCheck(self, heads, types, changedParts):
		(uti, handled) = super(CollectionWidget, self).docMergeCheck(heads, types, changedParts)
		return (uti, handled | set(['HPSD']))

	def docMergePerform(self, writer, baseReader, mergeReaders, changedParts):
		conflicts = super(CollectionWidget, self).docMergePerform(writer, baseReader, mergeReaders, changedParts)
		if 'HPSD' in changedParts:
			baseHpsd = struct.loads(baseReader.readAll('HPSD'))
			mergeHpsd = []
			for r in mergeReaders:
				mergeHpsd.append(struct.loads(r.readAll('HPSD')))
			(newHpsd, newConflict) = struct.merge(baseHpsd, mergeHpsd)
			conflicts = conflicts or newConflict
			writer.writeAll('HPSD', struct.dumps(newHpsd))

		return conflicts

	def model(self):
		return self.__containerModel

	def modelMapIndex(self, index):
		return self.__filterModel.mapToSource(index)

	def __setModel(self, model):
		self.__filterModel.setSourceModel(model)
		if self.__containerModel:
			self.__containerModel.clear()
			self.__containerModel.deleteLater()
		self.__containerModel = model

	# reimplemented to catch changes to "autoClean"
	def metaDataSetField(self, field, value):
		super(CollectionWidget, self).metaDataSetField(field, value)
		if field == CollectionModel.AUTOCLEAN:
			model = self.model()
			if model:
				model.setAutoClean(value)

	def _saveSettings(self, settings):
		super(CollectionWidget, self)._saveSettings(settings)
		if self.__containerModel:
			settings["columns"] = self.__containerModel.getColumns()
			settings["colwidths"] = [self.listView.columnWidth(i)
				for i in xrange(self.__containerModel.columnCount(None))]
			settings["sortcol"] = self.__filterModel.sortColumn()
			settings["sortorder"] = self.__filterModel.sortOrder()

	def _loadSettings(self, settings):
		super(CollectionWidget, self)._loadSettings(settings)
		self.__settings = settings
		self.__applySettings(settings)

	def __applySettings(self, settings):
		if not self.__containerModel:
			return
		columns = settings.get("columns")
		if columns:
			self.__containerModel.setColumns(columns)
		widths = settings.get("colwidths", [])[:self.__containerModel.columnCount(None)]
		i = 0
		for w in widths[:-1]:
			self.listView.setColumnWidth(i, w)
			i += 1
		self.listView.resizeColumnToContents(i)
		sortColumn = settings.get("sortcol", -1)
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
		link = self.model().getItemLinkUser(self.modelMapIndex(index))
		if link:
			executables = []
			revs = link.revs()
			for rev in revs:
				try:
					uti = Connector().stat(rev).type()
					executables = Registry().getExecutables(uti)
					break
				except IOError:
					pass
			self.itemOpen.emit(link, None,
				"org.hotchpotch.containerbrowser.py" in executables)

	def __showProperties(self):
		index = self.listView.selectionModel().currentIndex()
		link = self.model().getItemLinkUser(self.modelMapIndex(index))
		if link:
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
		return [self.model().getItemLinkReal(self.modelMapIndex(row)) for row in
			self.listView.selectionModel().selectedRows()]

	def fillContextMenu(self, menu):
		# get selected items
		isDoc = self.model().isDoc()
		links = [self.model().getItemLinkReal(self.modelMapIndex(row)) for row in
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
		elif self.isMutable:
			self.__addCreateActions(menu)

	def __addReplicateActions(self, menu, link):
		c = Connector()
		try:
			allVolumes = set(c.lookup_rev(self.rev()))
			if isinstance(link, struct.DocLink):
				curVolumes = set(c.lookup_doc(link.doc()).stores())
			else:
				curVolumes = set(c.lookup_rev(link.rev()))
		except IOError:
			return
		repVolumes = allVolumes - curVolumes
		for store in repVolumes:
			try:
				rev = c.lookup_doc(store).rev(store)
				with c.peek(rev) as r:
					metaData = struct.loads(r.readAll('META'))
					try:
						name = metaData["org.hotchpotch.annotation"]["title"]
					except:
						name = "Unknown store"
					action = menu.addAction("Replicate item to '%s'" % name)
					action.triggered.connect(
						lambda x,l=link,s=store: self.__doReplicate(l, s))
			except:
				pass

	def __doReplicate(self, link, store):
		if isinstance(link, struct.DocLink):
			Connector().replicateDoc(link.doc(), dstStores=[store])
		else:
			Connector().replicateRev(link.rev(), dstStores=[store])

	def __addCreateActions(self, menu):
		newMenu = menu.addMenu(QtGui.QIcon("icons/filenew.png"), "New document")
		sysStore = struct.Container(struct.DocLink(Connector().enum().sysStore()))
		templatesDict = struct.Container(sysStore.get("templates:"))
		items = templatesDict.items()
		items.sort(key=lambda item: item[0])
		for (name, link) in items:
			rev = link.rev()
			icon = QtGui.QIcon(Registry().getIcon(Connector().stat(rev).type()))
			action = newMenu.addAction(icon, name)
			action.triggered.connect(lambda x,r=rev: self.__doCreate(r))

	def __doCreate(self, sourceRev):
		info = Connector().stat(sourceRev)
		destStores = Connector().lookup_rev(self.rev())
		with Connector().create(info.type(), info.creator(), destStores) as w:
			with Connector().peek(sourceRev) as r:
				for part in info.parts():
					w.write(part, r.readAll(part))
			w.commit()
			destDoc = w.getDoc()
			# add link
			self.model().insertLink(struct.DocLink(destDoc))
			# save immediately
			self.save()

	def __addOpenActions(self, menu, link, isDoc):
		executables = []
		revs = link.revs()
		for rev in revs:
			try:
				uti = Connector().stat(rev).type()
				executables = Registry().getExecutables(uti)
				break
			except IOError:
				pass

		prefix = "Open"
		browseHint = False
		browsePreferred = False
		if "org.hotchpotch.containerbrowser.py" in executables:
			browsePreferred = True
		for e in executables:
			if e in self.__browseTypes:
				browseHint = True
				prefix = "Browse"
				break

		if isDoc:
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
			if isinstance(link, struct.DocLink):
				links = [struct.RevLink(rev) for rev in link.revs()]
				if len(links) == 1:
					action = menu.addAction("Open revision (read only)")
					action.triggered.connect(lambda x,l=links[0]: self.itemOpen.emit(l, None, False))
				elif len(links) > 1:
					revMenu = menu.addMenu("Open revision (read only)")
					for link in links:
						date = str(Connector().stat(link.rev()).mtime())
						action = revMenu.addAction(date)
						action.triggered.connect(lambda x,l=link: self.itemOpen.emit(l, None, False))
		else:
			links = [struct.RevLink(rev) for rev in link.revs()]
			if len(links) == 1:
				action = menu.addAction(prefix)
				action.triggered.connect(lambda x,l=link,h=browseHint: self.itemOpen.emit(l, None, h))
			elif len(links) > 1:
				for link in links:
					date = prefix + " " + str(Connector().stat(link.rev()).mtime())
					action = menu.addAction(date)
					action.triggered.connect(lambda x,l=link,h=browseHint: self.itemOpen.emit(l, None, h))

