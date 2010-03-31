#!/usr/bin/env python
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

from datetime import datetime
from PyQt4 import QtCore, QtGui
import os

from hotchpotch import HpConnector, HpRegistry
from hotchpotch import hpgui, hpstruct, importer
from hotchpotch.hpconnector import HpWatch

class CollectionWindow(hpgui.HpMainWindow):
	def __init__(self, argv):
		super(CollectionWindow, self).__init__(argv, "org.hotchpotch.container", True)

		self.__knownColumns = set()

		# initialize main view
		self.listView = CollectionTreeView(self)
		self.setCentralWidget(self.listView)
		self.listView.setSelectionMode(QtGui.QAbstractItemView.ExtendedSelection)
		self.listView.setDragEnabled(True)
		self.listView.setAutoScroll(True)
		self.listView.setExpandsOnDoubleClick(False)
		self.listView.setRootIsDecorated(False)
		self.listView.setEditTriggers(
			QtGui.QAbstractItemView.SelectedClicked |
			QtGui.QAbstractItemView.EditKeyPressed)
		self.__createActions()
		self.__createMenus()
		self.__createToolBars()
		self.__createStatusBar()

		self.listView.addAction(self.delItemAct)
		QtCore.QObject.connect(
			self.listView,
			QtCore.SIGNAL("doubleClicked(const QModelIndex &)"),
			self.__doubleClicked)

		self.__setEditable(False)

	# HpGui callbacks

	def docRead(self, readWrite, r):
		uti = HpConnector().stat(self.rev()).uti()
		if uti in DictModel.UTIs:
			model = DictModel(self)
		elif uti in SetModel.UTIs:
			model = SetModel(self)
		else:
			raise TypeError('Unhandled UTI: %s' % (uti))
		self.listView.setModel(model)
		QtCore.QObject.connect(
			model,
			QtCore.SIGNAL("rowsInserted(const QModelIndex &, int, int)"),
			self.__checkMetaData)
		QtCore.QObject.connect(
			model,
			QtCore.SIGNAL("rowsInserted(const QModelIndex &, int, int)"),
			self.emitChanged)
		QtCore.QObject.connect(
			model,
			QtCore.SIGNAL("rowsRemoved(const QModelIndex &, int, int)"),
			self.emitChanged)
		QtCore.QObject.connect(
			model,
			QtCore.SIGNAL("dataChanged(const QModelIndex &, const QModelIndex &)"),
			self.__dataChanged)

		autoClean = self.metaDataGetField(CollectionModel.AUTOCLEAN, False)
		self.cleanAct.setChecked(autoClean)
		self.cleanAct.setEnabled(readWrite)
		model.doLoad(r, readWrite, autoClean)
		self.__setEditable(readWrite)
		self.__checkMetaData()
		if readWrite and HpConnector().stat(self.rev()).uti() == "org.hotchpotch.volume":
			self.fileToolBar.removeAction(self.delAct)
			self.fileMenu.removeAction(self.delAct)
			enum = HpConnector().enum()
			if enum.isRemovable(enum.store(self.uuid())):
				self.fileMenu.insertAction(self.cleanAct, self.unmountAct)
				actions = self.fileToolBar.actions()
				if len(actions) > 1:
					self.fileToolBar.insertAction(actions[1], self.unmountAct)
				else:
					self.fileToolBar.addAction(self.unmountAct)
			self.syncMenu = QtGui.QMenu("Synchronization")
			self.fileMenu.insertMenu(self.cleanAct, self.syncMenu)
			QtCore.QObject.connect(
				self.syncMenu,
				QtCore.SIGNAL("aboutToShow()"),
				self.__showStoreSyncMenu)

	def docMergeCheck(self, heads, utis, changedParts):
		(uti, handled) = super(CollectionWindow, self).docMergeCheck(heads, utis, changedParts)
		if not uti:
			return (None, handled)
		return (uti, changedParts & (handled | set(['HPSD'])))

	def docMergePerform(self, writer, baseReader, mergeReaders, changedParts):
		conflicts = super(CollectionWindow, self).docMergePerform(writer, baseReader, mergeReaders, changedParts)
		if 'HPSD' in changedParts:
			baseHpsd = hpstruct.loads(baseReader.readAll('HPSD'))
			mergeHpsd = []
			for r in mergeReaders:
				mergeHpsd.append(hpstruct.loads(r.readAll('HPSD')))
			(newHpsd, newConflict) = hpstruct.merge(baseHpsd, mergeHpsd)
			conflicts = conflicts or newConflict
			writer.writeAll('HPSD', hpstruct.dumps(newHpsd))

		return conflicts

	def docCheckpoint(self, w, force):
		if force or self.listView.model().hasChanged():
			self.listView.model().doSave(w)

	def needSave(self):
		default = super(CollectionWindow, self).needSave()
		return self.listView.model().hasChanged() or default

	# private methods

	def __checkMetaData(self):
		availColumns = self.listView.model().availColumns()
		columns = set(availColumns)
		todo = columns - self.__knownColumns
		self.__knownColumns = columns
		for col in todo:
			column = availColumns[col]
			action = ColumnAction(self, self.listView.model(), column)
			self.columnMenu.addAction(action)

	def __removeRow(self):
		index = self.listView.selectionModel().currentIndex()
		if index.isValid():
			model = self.listView.model()
			model.removeRow(index.row(), index.parent())

	def __toggleAutoClean(self, checked):
		self.metaDataSetField(CollectionModel.AUTOCLEAN, checked)
		self.listView.model().setAutoClean(checked)

	def __setEditable(self, enabled):
		self.mutable = enabled
		self.listView.setAcceptDrops(enabled)
		self.listView.setDropIndicatorShown(enabled)

	def __openItem(self):
		index = self.listView.selectionModel().currentIndex()
		if index.isValid():
			self.listView.model().openItem(index)

	def __doubleClicked(self, index):
		self.listView.model().openItem(index)

	def __showProperties(self):
		index = self.listView.selectionModel().currentIndex()
		item = self.listView.model().getItem(index)
		if item and item.isValid():
			hpgui.showProperties(item.getLink())

	def __createActions(self):
		self.delItemAct = QtGui.QAction(QtGui.QIcon('icons/edittrash.png'), "&Delete", self)
		self.delItemAct.setShortcut(QtGui.QKeySequence.Delete)
		self.delItemAct.setStatusTip("Delete selected items")
		QtCore.QObject.connect(self.delItemAct, QtCore.SIGNAL("triggered()"), self.__removeRow)

		self.cleanAct = QtGui.QAction("&Autoclean", self)
		self.cleanAct.setStatusTip("Toggle auto cleaning")
		self.cleanAct.setCheckable(True)
		self.cleanAct.setChecked(True)
		QtCore.QObject.connect(self.cleanAct, QtCore.SIGNAL("triggered(bool)"), self.__toggleAutoClean)

		self.openAct = QtGui.QAction("&Open", self)
		self.openAct.font().setBold(True)
		QtCore.QObject.connect(self.openAct, QtCore.SIGNAL("triggered(bool)"), self.__openItem)

		self.propertiesAct = QtGui.QAction("&Properties", self)
		QtCore.QObject.connect(self.propertiesAct, QtCore.SIGNAL("triggered(bool)"), self.__showProperties)

		self.unmountAct = QtGui.QAction(QtGui.QIcon('icons/unmount.png'), "&Unmount", self)
		self.unmountAct.setStatusTip("Unmount volume")
		QtCore.QObject.connect(self.unmountAct, QtCore.SIGNAL("triggered(bool)"), self.__unmountStore)

	def __createMenus(self):
		self.fileMenu.addSeparator();
		self.fileMenu.addAction(self.cleanAct)
		self.columnMenu = self.menuBar().addMenu("&Columns")

	def __createToolBars(self):
		pass

	def __createStatusBar(self):
		self.statusBar().showMessage("Ready")

	def __unmountStore(self):
		self.saveFile("Automatically saved before unmounting")
		enum = HpConnector().enum()
		HpConnector().unmount(enum.store(self.uuid()))

	def __dataChanged(self):
		# some fields in the model have changed. Doesn't mean we have to save...
		if self.listView.model().hasChanged():
			self.emitChanged()

	def __showStoreSyncMenu(self):
		self.syncMenu.clear()
		rules = SyncRules()
		this = self.uuid()
		enum = HpConnector().enum()
		for store in enum.allStores():
			if not enum.isMounted(store):
				continue
			guid = enum.guid(store)
			if guid == this:
				continue
			rev = HpConnector().lookup(guid).rev(guid)
			with HpConnector().read(rev) as r:
				try:
					metaData = hpstruct.loads(r.readAll('META'))
					storeName = metaData["org.hotchpotch.annotation"]["title"]
				except:
					storeName = "Unnamed store"
			menu = self.syncMenu.addMenu(storeName)
			menu.setSeparatorsCollapsible(False)
			menu.addSeparator().setText("Local changes")
			group = QtGui.QActionGroup(self)
			action = RuleAction(menu, this, guid, None)
			action.setChecked(not rules.mode(this, guid))
			group.addAction(action)
			menu.addAction(action)
			action = RuleAction(menu, this, guid, "ff")
			action.setChecked(rules.mode(this, guid) == "ff")
			group.addAction(action)
			menu.addAction(action)
			action = RuleAction(menu, this, guid, "savemerge")
			action.setChecked(rules.mode(this, guid) == "savemerge")
			group.addAction(action)
			menu.addAction(action)
			action = RuleAction(menu, this, guid, "automerge")
			action.setChecked(rules.mode(this, guid) == "automerge")
			group.addAction(action)
			menu.addAction(action)
			menu.addSeparator().setText("Peer changes")
			group = QtGui.QActionGroup(self)
			action = RuleAction(menu, guid, this, None)
			action.setChecked(not rules.mode(guid, this))
			group.addAction(action)
			menu.addAction(action)
			action = RuleAction(menu, guid, this, "ff")
			action.setChecked(rules.mode(guid, this) == "ff")
			group.addAction(action)
			menu.addAction(action)
			action = RuleAction(menu, guid, this, "savemerge")
			action.setChecked(rules.mode(guid, this) == "savemerge")
			group.addAction(action)
			menu.addAction(action)
			action = RuleAction(menu, guid, this, "automerge")
			action.setChecked(rules.mode(guid, this) == "automerge")
			group.addAction(action)
			menu.addAction(action)


class SyncRules(object):
	def __init__(self):
		sysUuid = HpConnector().enum().sysStoreGuid()
		sysRev = HpConnector().lookup(sysUuid).rev(sysUuid)
		with HpConnector().read(sysRev) as r:
			root = hpstruct.loads(r.readAll('HPSD'))
			self.syncUuid = root["syncrules"].uuid()
		self.syncRev = HpConnector().lookup(self.syncUuid).rev(sysUuid)
		with HpConnector().read(self.syncRev) as r:
			self.rules = hpstruct.loads(r.readAll('HPSD'))

	def save(self):
		with HpConnector().update(self.syncUuid, self.syncRev) as w:
			w.writeAll('HPSD', hpstruct.dumps(self.rules))
			self.rev = w.commit()

	def deleteRule(self, store, peer):
		self.rules = [r for r in self.rules if (r["from"].decode('hex') != store) or (r["to"].decode('hex') != peer)]

	def mode(self, store, peer):
		for rule in self.rules:
			if (rule["from"].decode('hex') == store) and (rule["to"].decode('hex') == peer):
				return rule["mode"]
		return None

	def setMode(self, store, peer, mode):
		for rule in self.rules:
			if (rule["from"].decode('hex') == store) and (rule["to"].decode('hex') == peer):
				rule["mode"] = mode
				return
		# add new rule
		rule = {}
		rule["from"] = store.encode('hex')
		rule["to"]   = peer.encode('hex')
		rule["mode"] = mode
		self.rules.append(rule)


class RuleAction(QtGui.QAction):
	def __init__(self, parent, store, peer, mode):
		super(RuleAction, self).__init__(parent)
		self.__store = store
		self.__peer = peer
		self.__mode = mode

		if mode == "ff":
			self.setText("Fast-forward")
		elif mode == "savemerge":
			self.setText("Merge (non-conflicting)")
		elif mode == "automerge":
			self.setText("Merge always")
		else:
			self.setText("No sync")
			
		self.setCheckable(True)
		self.setChecked(False)
		QtCore.QObject.connect(self, QtCore.SIGNAL("triggered(bool)"), self.__triggered)

	def __triggered(self, checked):
		rules = SyncRules()
		if self.__mode:
			rules.setMode(self.__store, self.__peer, self.__mode)
		else:
			rules.deleteRule(self.__store, self.__peer)
		rules.save()


class ColumnAction(QtGui.QAction):
	def __init__(self, parent, model, column):
		super(ColumnAction, self).__init__(parent)
		self.__model = model
		self.__column = column

		self.setText(column.name())
		self.setCheckable(True)
		self.setChecked(column in model.activeColumns())
		self.setEnabled(column.removable())
		QtCore.QObject.connect(self, QtCore.SIGNAL("triggered(bool)"), self.__triggered)

	def __triggered(self, checked):
		if checked:
			self.__model.addColumn(self.__column)
		else:
			self.__model.remColumn(self.__column)


class CollectionTreeView(QtGui.QTreeView):
	def __init__(self, parent):
		super(CollectionTreeView, self).__init__(parent)
		self.__parent = parent

	def dragEnterEvent(self, event):
		if event.source() is self:
			return
		else:
			data = event.mimeData()
			link = hpstruct.loadMimeData(data)
			if link:
				# ourself?
				if isinstance(link, hpstruct.DocLink):
					if link.uuid() == self.__parent.uuid():
						return
				# already contained?
				if not self.model().validateDragEnter(link):
					return
		QtGui.QTreeView.dragEnterEvent(self, event)

	def contextMenuEvent(self, event):
		menu = QtGui.QMenu(self)
		repActions = {}
		openRevActions = {}
		createActions = {}

		# selected an item?
		item = self.model().getItem(self.indexAt(event.pos()))
		if item:
			if item.isValid():
				link = item.getLink()
				openRevActions = self.__addOpenActions(menu, link)
				if self.__parent.mutable:
					menu.addSeparator()
					repActions = self.__addReplicateActions(menu, link)
			# add default options
			if self.__parent.mutable:
				menu.addSeparator()
				menu.addAction(self.__parent.delItemAct)
			menu.addSeparator()
			menu.addAction(self.__parent.propertiesAct)
		elif self.__parent.mutable:
			createActions = self.__addCreateActions(menu)
		else:
			return

		# execute
		choice = menu.exec_(event.globalPos())
		c = HpConnector()
		if choice in repActions:
			store = repActions[choice]
			if isinstance(link, hpstruct.DocLink):
				c.replicate_uuid(link.uuid(), [store])
			else:
				c.replicate_rev(link.rev(), [store])
		elif choice in createActions:
			sourceRev = createActions[choice].rev()
			info = c.stat(sourceRev)
			destStores = c.stat(self.__parent.rev()).volumes()
			# copy
			with c.fork(destStores[0], info.uti()) as w:
				with c.read(sourceRev) as r:
					for part in info.parts():
						w.write(part, r.readAll(part))
				w.commit()
				destUuid = w.getUUID()
			# add link
			self.model().insertLink(hpstruct.DocLink(destUuid))
		elif choice in openRevActions:
			rev = openRevActions[choice]
			hpgui.showDocument(hpstruct.RevLink(rev))

	def __addReplicateActions(self, menu, link):
		actions = { }
		c = HpConnector()
		menu.addSeparator()
		allVolumes = set(c.stat(self.__parent.rev()).volumes())
		if isinstance(link, hpstruct.DocLink):
			curVolumes = set(c.lookup(link.uuid()).stores())
			isUuid = True
		else:
			curVolumes = set(c.stat(link.rev()).volumes())
			isUuid = False
		repVolumes = allVolumes - curVolumes
		for store in repVolumes:
			try:
				rev = c.lookup(store).rev(store)
				with c.read(rev) as r:
					metaData = hpstruct.loads(r.readAll('META'))
					try:
						name = metaData["org.hotchpotch.annotation"]["title"]
					except:
						name = "Unknown store"
					action = menu.addAction("Replicate item to '%s'" % name)
					actions[action] = store
			except:
				pass
		return actions

	def __addCreateActions(self, menu):
		newMenu = menu.addMenu(QtGui.QIcon("icons/filenew.png"), "New document")
		actions = { }

		c = HpConnector()
		sysStore = hpstruct.HpContainer(hpstruct.DocLink(c.enum().sysStoreGuid()))
		templatesDict = hpstruct.HpContainer(sysStore.get("templates:"))
		for (name, link) in templatesDict.items():
			icon = QtGui.QIcon(HpRegistry().getIcon(c.stat(link.rev()).uti()))
			action = newMenu.addAction(icon, name)
			actions[action] = link

		return actions

	def __addOpenActions(self, menu, link):
		if self.__parent.mutable:
			menu.addAction(self.__parent.openAct)
			menu.setDefaultAction(self.__parent.openAct)
		actions = {}
		if isinstance(link, hpstruct.DocLink):
			c = HpConnector()
			revs = c.lookup(link.uuid()).revs()
			if len(revs) == 1:
				action = menu.addAction("Open revision (read only)")
				actions[action] = revs[0]
			elif len(revs) > 1:
				revMenu = menu.addMenu("Open revision (read only)")
				for rev in revs:
					date = str(c.stat(rev).mtime())
					action = revMenu.addAction(date)
					actions[action] = rev
		return actions


class ColumnInfo(object):
	def __init__(self):
		pass

	def removable(self):
		return True

	def editable(self):
		return False

	def derived(self):
		return False

	def default(self):
		return None


class NameColumnInfo(ColumnInfo):
	def __init__(self):
		pass

	def removable(self):
		return False

	def editable(self):
		return True

	def name(self):
		return "Name"


def convertNone(item):
	return unicode(item)

def convertList(item):
	if isinstance(item, list):
		if item == []:
			return ""
		else:
			return reduce(lambda x,y: unicode(x) + u', ' + unicode(y), item)
	else:
		return "#VALUE"

def convertDateTime(item):
	if isinstance(item, int) or isinstance(item, long):
		mtime = datetime.fromtimestamp(item)
		return str(mtime)
	else:
		return "#VALUE"

class MetaColumnInfo(ColumnInfo):
	def __init__(self, spec):
		self.__name = spec["display"]
		self.__key  = spec["key"]
		typ = spec["type"]
		if typ.startswith("list of"):
			self.__convert = convertList
		elif typ == "datetime":
			self.__convert = convertDateTime
		else:
			self.__convert = convertNone
		self.__default = ""

	def name(self):
		return self.__name

	def derived(self):
		return True

	def default(self):
		return self.__default

	def extract(self, metaData):
		item = metaData
		for step in self.__key:
			if step in item:
				item = item[step]
			else:
				return self.__default
		return self.__convert(item)


class CollectionModel(QtCore.QAbstractTableModel):
	AUTOCLEAN = ["org.hotchpotch.container", "autoclean"]

	def __init__(self, parent = None):
		super(CollectionModel, self).__init__(parent)
		self.__parent = parent
		self._connector = HpConnector()

		self._listing = []
		self.__changedContent = False
		self.__autoClean = True
		self.__mutable = True

		self.__seenUti = []
		self.__availColumns = {}
		self.__activeColumns = [ ]
		self.__metaDataHandling("public.item")
		self.__activeColumns.append(self.__availColumns["org.hotchpotch.annotation:title"])

		self._dropMenu = QtGui.QMenu()
		self._docLinkAct = self._dropMenu.addAction("Link to Document")
		self._revLinkAct = self._dropMenu.addAction("Link to Version")
		self._dropMenu.addSeparator()
		self._abortAct = self._dropMenu.addAction("Abort")

	def doSave(self, w):
		data = self.encode()
		w.writeAll('HPSD', hpstruct.dumps(data))
		self.__changedContent = False

	def doLoad(self, r, readWrite, autoClean):
		self.__mutable = readWrite
		self.__changedContent = False
		self.__autoClean = autoClean
		self._listing = []
		data = hpstruct.loads(r.readAll('HPSD'))
		listing = self.decode(data)
		for entry in listing:
			if entry.isValid() or (not self.__autoClean):
				self.__metaDataHandling(entry.getUti())
				self._listing.append(entry)
				self._connector.watch(entry)
			else:
				entry = None
				self.__changedContent = True
		self.reset()

	def readOnly(self):
		return not self.__mutable

	def hasChanged(self):
		return self.__changedContent

	def openItem(self, index):
		link = self._listing[index.row()].getLink()
		if isinstance(link, hpstruct.DocLink):
			if self.__mutable:
				hpgui.showDocument(link)
			else:
				if link.rev():
					hpgui.showDocument(hpstruct.RevLink(link.rev()))
		elif isinstance(link, hpstruct.RevLink):
			hpgui.showDocument(link)

	def setAutoClean(self, autoClean):
		self.__autoClean = autoClean
		if autoClean and self.__mutable:
			removed = [x for x in self._listing if not x.isValid()]
			self._listing = [x for x in self._listing if x.isValid()]
			if len(removed) > 0:
				self.__changedContent = True
				for item in removed:
					self._connector.unwatch(item)
				self.reset()

	def getItem(self, index):
		if index.isValid():
			return self._listing[index.row()]
		else:
			return None

	def availColumns(self):
		return self.__availColumns

	def activeColumns(self):
		return self.__activeColumns

	def addColumn(self, column):
		index = len(self.__activeColumns)
		self.insertColumn(index, column)

	def insertColumn(self, index, column):
		self.beginInsertColumns(QtCore.QModelIndex(), index, index)
		self.__activeColumns.insert(index, column)
		for i in self._listing:
			i.insertColumn(index, column)
		self.endInsertColumns()

	def remColumn(self, column):
		index = self.__activeColumns.index(column)
		self.beginRemoveColumns(QtCore.QModelIndex(), index, index)
		del self.__activeColumns[index]
		for i in self._listing:
			i.remColumn(index)
		self.endRemoveColumns()

	def __metaDataHandling(self, uti):
		if uti not in self.__seenUti:
			self.__seenUti.append(uti)
			metaFields = HpRegistry().getMeta(uti)
			for field in metaFields:
				key = reduce(lambda x,y: x+":"+y, field["key"])
				if key not in self.__availColumns:
					self._addColumnInfo(key, MetaColumnInfo(field))

	def _addColumnInfo(self, key, column):
		self.__availColumns[key] = column

	# === view callbacks ===

	def hasChildren(self, parent):
		return False

	def rowCount(self, parent):
		if parent.isValid():
			return 0
		else:
			return len(self._listing)

	def columnCount(self, parent):
		return len(self.__activeColumns)

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
			if self.__activeColumns[col].editable():
				text = str(value.toString())
				# has the value changed at all?
				if self._listing[index.row()].getColumnData(col) == text:
					return False
				# valid edit?
				if not self.validateEdit(index, text):
					return False
				# valid... do it
				self._listing[index.row()].setColumnData(col, text)
				self.emit(QtCore.SIGNAL("dataChanged(const QModelIndex&,const QModelIndex&)"), index, index)
				self.__changedContent = True
				return True
		return False

	def headerData(self, section, orientation, role):
		if role != QtCore.Qt.DisplayRole:
			return QtCore.QVariant()

		if orientation == QtCore.Qt.Horizontal:
			return QtCore.QVariant(self.__activeColumns[section].name())
		else:
			return QtCore.QVariant("Row %d" % (section))

	def removeRows(self, position, rows, parent):
		if not self.__mutable:
			return False
		self.__changedContent = True
		self.beginRemoveRows(QtCore.QModelIndex(), position, position+rows-1)
		for i in range(rows):
			self._connector.unwatch(self._listing[position])
			del self._listing[position]
		self.endRemoveRows()
		return True

	def flags(self, index):
		if index.isValid():
			flags = QtCore.Qt.ItemIsEnabled | QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsDragEnabled
			if self.__activeColumns[index.column()].editable() and self.__mutable:
				flags = flags | QtCore.Qt.ItemIsEditable
		else:
			if self.__mutable:
				flags = QtCore.Qt.ItemIsDropEnabled
			else:
				flags = 0
		return flags

	# === DnD ===

	def supportedDropActions(self):
		if self.__mutable:
			return QtCore.Qt.CopyAction | QtCore.Qt.MoveAction | QtCore.Qt.LinkAction
		else:
			return 0

	def mimeTypes(self):
		types = QtCore.QStringList()
		types << hpstruct.DocLink.MIME_TYPE
		types << hpstruct.RevLink.MIME_TYPE
		types << 'application/x-hotchpotch-linklist'
		types << "text/uri-list"
		return types

	def mimeData(self, indexes):
		data = ""
		for index in indexes:
			if index.isValid() and (index.column() == 0):
				link = self.getItem(index).getLink()
				if isinstance(link, hpstruct.DocLink):
					data += ',uuid:' + link.uuid().encode('hex')
				else:
					data += ',rev:' + link.rev().encode('hex')
		if data == "":
			mimeData = None
		else:
			mimeData = QtCore.QMimeData()
			mimeData.setData('application/x-hotchpotch-linklist', data[1:])
		return mimeData

	def dropMimeData(self, data, action, row, column, parent):
		if not self.__mutable:
			return True
		if action == QtCore.Qt.IgnoreAction:
			return True

		if data.hasFormat('application/x-hotchpotch-linklist'):
			return self.__dropLinkList(data)
		if data.hasFormat('text/uri-list'):
			return self.__dropFile(data)
		else:
			return self.__dropLink(data)

	def __dropFile(self, data):
		store = self._connector.stat(self.__parent.rev()).volumes()[0]
		urlList = data.urls()
		for url in urlList:
			try:
				path = str(url.toLocalFile().toUtf8())
				link = importer.importFile(store, path)
				if link:
					self.insertLink(link)
			except IOError:
				pass
		return True

	def __dropLink(self, data):
		# parse link
		link = hpstruct.loadMimeData(data)
		if not link:
			return False

		# what to do?
		if isinstance(link, hpstruct.DocLink):
			self._docLinkAct.setEnabled(True)
			self._revLinkAct.setEnabled(len(link.revs()) == 1)
		else:
			self._docLinkAct.setEnabled(False)
		action = self._dropMenu.exec_(QtGui.QCursor.pos())
		if action is self._docLinkAct:
			pass
		elif action is self._revLinkAct:
			if isinstance(link, hpstruct.DocLink):
				link = hpstruct.RevLink(link.revs()[0])
		else:
			return False

		return self.insertLink(link)

	def __dropLinkList(self, mimeData):
		linkList = str(mimeData.data('application/x-hotchpotch-linklist'))
		for link in linkList.split(','):
			(cls, val) = link.split(':')
			if cls == 'uuid':
				link = hpstruct.DocLink(val.decode("hex"), False)
			elif cls == 'rev':
				link = hpstruct.RevLink(val.decode("hex"))
			else:
				continue
			self.insertLink(link)
		return True

	def insertLink(self, link):
		entry = CEntry(link, self, self.__activeColumns)
		if not self.validateInsert(entry):
			return False
		self.__metaDataHandling(entry.getUti())

		# append new item
		endRow = self.rowCount(QtCore.QModelIndex())
		self.beginInsertRows(QtCore.QModelIndex(), endRow, endRow)
		self._listing.append(entry)
		self.endInsertRows()
		self._connector.watch(entry)
		self.__changedContent = True

		return True

	def validateEdit(self, index, newValue):
		return True

	def validateInsert(self, entry):
		return True

	def validateDragEnter(self, link):
		return True

	# === Callbacks from a CEntry which has changed ===

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


class CEntry(HpWatch):
	def __init__(self, link, model, columns):
		self.__model = model
		self.__link  = link
		self.__valid = False
		self.__icon  = None
		self.__uti   = None
		self.__columnValues = [ column.default() for column in columns ]
		self.__columnDefs = columns[:]

		if isinstance(link, hpstruct.DocLink):
			super(CEntry, self).__init__(HpWatch.TYPE_UUID, link.uuid())
		else:
			super(CEntry, self).__init__(HpWatch.TYPE_REV, link.rev())

		self.update()

	def isValid(self):
		return self.__valid

	def needsMerge(self):
		return isinstance(self.__link, hpstruct.DocLink) and (len(self.__link.revs()) > 1)

	def getColumnData(self, index):
		return self.__columnValues[index]

	def setColumnData(self, index, data):
		self.__columnValues[index] = data

	def insertColumn(self, index, column):
		self.__columnValues.insert(index, column.default())
		self.__columnDefs.insert(index, column)
		self.__updateColumns()

	def remColumn(self, index):
		del self.__columnValues[index]
		del self.__columnDefs[index]

	def getLink(self):
		return self.__link

	def getIcon(self):
		if (not self.__valid) and (not self.__icon):
			self.__icon = QtGui.QIcon("icons/uti/file_broken.png")
		return self.__icon

	def getUti(self):
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
		if isinstance(self.__link, hpstruct.DocLink):
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
			s = HpConnector().stat(rev)
		except IOError:
			return
		self.__uti = s.uti()
		if needMerge:
			image = QtGui.QImage(HpRegistry().getIcon(s.uti()))
			painter = QtGui.QPainter()
			painter.begin(image)
			painter.drawImage(0, 0, QtGui.QImage("icons/uti/merge_overlay.png"))
			painter.end()
			self.__icon = QtGui.QIcon(QtGui.QPixmap.fromImage(image))
		else:
			self.__icon = QtGui.QIcon(HpRegistry().getIcon(s.uti()))

		self.__updateColumns()
		self.__valid = True

	def __updateColumns(self):
		with HpConnector().read(self.__rev) as r:
			try:
				metaData = hpstruct.loads(r.readAll('META'))
			except:
				metaData = { }
		for i in xrange(len(self.__columnDefs)):
			column = self.__columnDefs[i]
			if column.derived():
				self.__columnValues[i] = column.extract(metaData)

	# callback when watch was triggered
	def triggered(self, cause):
		if cause in [HpWatch.CAUSE_MODIFIED, HpWatch.CAUSE_REPLICATED, HpWatch.CAUSE_DIMINISHED]:
			self.update()
			self.__model.entryChanged(self)
		elif cause == HpWatch.CAUSE_APPEARED:
			self.update()
			self.__model.entryAppeared(self)
		elif cause == HpWatch.CAUSE_DISAPPEARED:
			self.__valid = False
			self.__icon  = None
			self.__model.entryRemoved(self)


class DictModel(CollectionModel):
	UTIs = ["org.hotchpotch.dict", "org.hotchpotch.volume"]

	def __init__(self, parent = None):
		super(DictModel, self).__init__(parent)

		# add fixed 'Name' column
		nameColumn = NameColumnInfo()
		self._addColumnInfo("::name", nameColumn)
		self.insertColumn(0, nameColumn)

	def encode(self):
		data = { }
		for item in self._listing:
			data[item.getColumnData(0)] = item.getLink()
		return data

	def decode(self, data):
		listing = []
		activeColumns = self.activeColumns()
		for (name, link) in data.items():
			entry = CEntry(link, self, activeColumns)
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

	def __init__(self, parent = None):
		super(SetModel, self).__init__(parent)

	def encode(self):
		return [ item.getLink() for item in self._listing ]

	def decode(self, data):
		activeColumns = self.activeColumns()
		return [ CEntry(link, self, activeColumns) for link in data ]

	def validateDragEnter(self, link):
		for item in self._listing:
			if item.getLink() == link:
				return False
		return True


if __name__ == '__main__':

	import sys

	app = QtGui.QApplication(sys.argv)
	mainWin = CollectionWindow(sys.argv)
	mainWin.mainWindowInit()
	mainWin.show()
	sys.exit(app.exec_())

