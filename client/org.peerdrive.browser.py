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

from PyQt4 import QtCore, QtGui#, QtOpenGL
from datetime import datetime
import itertools, optparse, copy, pickle, os.path

from peerdrive import Connector, Registry, connector, settingsPath
from peerdrive.gui import utils
from peerdrive.gui.widgets import DocumentView, DocButton

from views.folder import FolderWidget
from views.text import TextEdit

class HistoryItem(object):

	def __init__(self, link, state):
		self.__link = link
		self.__text = "FIXME"
		self.__state = state

	def link(self):
		return self.__link

	def text(self):
		return self.__text

	def setText(self, text):
		self.__text = text

	def state(self):
		return self.__state


class History(QtCore.QObject):

	leaveItem = QtCore.pyqtSignal(HistoryItem)

	enterItem = QtCore.pyqtSignal(HistoryItem)

	def __init__(self):
		super(History, self).__init__()

		self.__items = []
		self.__current = -1

	def back(self):
		if self.__current > 0:
			self.leaveItem.emit(self.__items[self.__current])
			self.__current -= 1
			self.enterItem.emit(self.__items[self.__current])

	def forward(self):
		if self.__current < len(self.__items)-1:
			self.leaveItem.emit(self.__items[self.__current])
			self.__current += 1
			self.enterItem.emit(self.__items[self.__current])

	def backItems(self, maxItems):
		return self.__items[:self.__current]

	def forwardItems(self, maxItems):
		return self.__items[self.__current+1:]

	def canGoBack(self):
		return self.__current > 0

	def canGoForward(self):
		return self.__current < len(self.__items)-1

	def currentItem(self):
		return self.__items[self.__current]

	def goToItem(self, item):
		self.leaveItem.emit(self.__items[self.__current])
		self.__current = self.__items.index(item)
		self.enterItem.emit(self.__items[self.__current])

	def items(self):
		return self.__items[:]

	def push(self, link, state={}):
		if self.__current >= 0:
			self.leaveItem.emit(self.__items[self.__current])
		self.__current += 1
		self.__items[self.__current:] = [HistoryItem(link, state)]
		self.enterItem.emit(self.__items[self.__current])


class WarpProxy(QtGui.QGraphicsProxyWidget):

	def __init__(self, item, level):
		super(WarpProxy, self).__init__()
		self.__item = item
		self.__curLevel = level
		self.__newLevel = level
		self.__fadeTimeLine = QtCore.QTimeLine(250, self)
		self.__fadeTimeLine.valueChanged.connect(self.__fadeValueChanged)
		self.__fadeTimeLine.finished.connect(self.__fadeFinished)
		self.__levelTimeLine = QtCore.QTimeLine(250, self)
		self.__levelTimeLine.valueChanged.connect(self.__levelValueChanged)
		self.__levelTimeLine.finished.connect(self.__levelFinished)

		self.setOpacity(0.0)

		self.__setTransform(level)

	def setGeometry(self, geometry):
		super(WarpProxy, self).setGeometry(geometry)
		self.setTransformOriginPoint(QtCore.QPointF(geometry.width()/2,
			-geometry.height()/2))

	def fadeIn(self):
		if self.__fadeTimeLine.direction() != QtCore.QTimeLine.Forward:
			self.__fadeTimeLine.setDirection(QtCore.QTimeLine.Forward)
		if self.__fadeTimeLine.state() == QtCore.QTimeLine.NotRunning:
			self.__fadeTimeLine.start()

	def fadeOut(self):
		if self.__fadeTimeLine.direction() != QtCore.QTimeLine.Backward:
			self.__fadeTimeLine.setDirection(QtCore.QTimeLine.Backward)
		if self.__fadeTimeLine.state() == QtCore.QTimeLine.NotRunning:
			self.__fadeTimeLine.start()

	def fadeFull(self):
		if self.__fadeTimeLine.state() == QtCore.QTimeLine.Running:
			self.__fadeTimeLine.stop()
		self.__fadeTimeLine.setCurrentTime(250)
		self.__fadeValueChanged(1.0)

	def setLevel(self, level):
		if level != self.__curLevel:
			self.__newLevel = level
			if self.__levelTimeLine.state() == QtCore.QTimeLine.NotRunning:
				self.__levelTimeLine.start()

	def __fadeValueChanged(self, step):
		self.setOpacity(step)

	def __fadeFinished(self):
		if self.__fadeTimeLine.currentValue() == 0:
			self.__item._vanished()

	def __levelValueChanged(self, step):
		level = self.__curLevel + (self.__newLevel - self.__curLevel) * step
		self.__setTransform(level)

	def __setTransform(self, level):
		level = 1+level*0.2
		#tr = QtGui.QTransform()
		#tr.scale(1/level, 1/level)
		#self.setTransform(tr)
		self.setScale(1/level)
		self.setZValue(-level)

	def __levelFinished(self):
		self.__curLevel = self.__newLevel


class WarpItem(connector.Watch):

	def __init__(self, view, cls, size, scene, store, rev):
		connector.Watch.__init__(self, connector.Watch.TYPE_REV, rev)
		self.__view = view
		self.__class = cls
		self.__geometry = QtCore.QRectF(-size.width()/2.0, -size.height()/2.0,
			float(size.width()), float(size.height()))
		self.__scene = scene
		self.__store = store
		self.__rev = rev
		self.__widget = None
		self.__available = False
		self.__seen = False
		self.__watching = False
		self.__mtime = datetime.fromtimestamp(0)
		self.__state = {}
		self.__update()

	def setState(self, state):
		self.__state = state
		if self.__widget:
			self.__widget.widget()._loadSettings(state)

	def getState(self):
		state = {}
		if self.__widget:
			self.__widget.widget()._saveSettings(state)
		return state

	def delete(self):
		self.__unwatch()
		self._vanished()

	def getWidget(self):
		return self.__widget.widget()

	def available(self):
		return self.__available

	def rev(self):
		return self.__rev

	def mtime(self):
		return self.__mtime

	def setEnabled(self, enable):
		if self.__widget:
			self.__widget.setEnabled(enable)

	def resize(self, size):
		self.__geometry = QtCore.QRectF(-size.width()/2.0, -size.height()/2.0,
			float(size.width()), float(size.height()))
		if self.__widget:
			self.__widget.setGeometry(self.__geometry)

	def fadeIn(self, level):
		if self.__widget:
			self.__widget.setLevel(level)
		elif self.__available:
			self.__createWidget(level)
			self.__widget.fadeIn()

	def fadeFull(self, level):
		if self.__widget:
			self.__widget.setLevel(level)
		elif self.__available:
			self.__createWidget(level)
			self.__widget.fadeFull()

	def setLevel(self, level):
		if self.__widget:
			self.__widget.setLevel(level)

	def fadeOut(self):
		if self.__widget:
			self.__widget.fadeOut()

	def triggered(self, event, store):
		if event == connector.Watch.EVENT_DISAPPEARED:
			self.__available = False
			self.__watch()
			self.__view._update(0)
		elif event == connector.Watch.EVENT_APPEARED:
			self.__update()
			if self.__available:
				self.__view._update(0)

	def setCacheMode(self, mode):
		if self.__widget:
			self.__widget.setCacheMode(mode)

	def __createWidget(self, level):
		widget = self.__class()
		if self.__state:
			widget._loadSettings(self.__state)
		widget.docOpen(self.__store, self.__rev, False)
		#widget.itemOpen.connect(self.__itemOpen)
		proxy = WarpProxy(self, level)
		proxy.setWidget(widget)
		proxy.setGeometry(self.__geometry)
		proxy.setCacheMode(QtGui.QGraphicsItem.ItemCoordinateCache)
		self.__widget = proxy
		self.__scene.addItem(self.__widget)
		self.__watch()

	def _vanished(self):
		if self.__widget:
			self.__widget.widget().docClose()
			self.__scene.removeItem(self.__widget)
			self.__widget = None

	def __update(self):
		if self.__seen:
			self.__unwatch()
			self.__available = len(Connector().lookupRev(self.__rev)) > 0
		else:
			self.__available = False
			try:
				stat = Connector().stat(self.__rev)
				self.__mtime = stat.mtime()
				self.__seen = True
				self.__available = True
				self.__view._addParents(stat.parents())
				self.__unwatch()
			except IOError:
				self.__watch()
				return

	def __watch(self):
		if not self.__watching:
			Connector().watch(self)
			self.__watching = True

	def __unwatch(self):
		if self.__watching:
			Connector().unwatch(self)
			self.__watching = False

	def __itemOpen(self, link):
		self.__view._open(link)


class WarpView(QtGui.QGraphicsView):

	openItem = QtCore.pyqtSignal(connector.RevLink, object)

	def __init__(self, cls, store, rev, state):
		self.__class = cls
		self.__store = store
		self.__scene = QtGui.QGraphicsScene()
		super(WarpView, self).__init__(self.__scene)

		self.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
		self.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
		self.setRenderHints(self.renderHints()
			| QtGui.QPainter.Antialiasing
			| QtGui.QPainter.SmoothPixmapTransform)
		self.setBackgroundBrush(QtGui.QBrush(QtGui.QColor(0, 0, 0)))
#		self.setBackgroundBrush(QtGui.QBrush(QtGui.QPixmap("space_warp.jpg")))
#		self.setViewport(QtOpenGL.QGLWidget(QtOpenGL.QGLFormat(QtOpenGL.QGL.SampleBuffers)))

		self.__parentTimer = QtCore.QTimer(self)
		self.__parentTimer.timeout.connect(self.__inspectParents)
		self.__parentTimer.setSingleShot(True)
		self.__parentTimer.setInterval(100)
		self.__parentBacklog = []

		self.__goPast = QtGui.QPushButton("Past")
		self.__goPast.clicked.connect(self.__movePast)
		self.__goPresent = QtGui.QPushButton("Present")
		self.__goPresent.clicked.connect(self.__movePresent)
		self.__openBtn = QtGui.QPushButton("Open")
		self.__openBtn.clicked.connect(self.__open)
		self.__scene.addWidget(self.__goPast)
		self.__scene.addWidget(self.__goPresent)
		self.__scene.addWidget(self.__openBtn)

		item = WarpItem(self, self.__class, self.size(), self.__scene, store, rev)
		item.fadeFull(0)
		item.setState(state)
		self.__distance = 1
		self.__zoom = 1
		self.__state = state
		self.__allItems = { rev : item }
		self.__availItems = []
		self.__visibleItems = []
		self.__curItem = item
		self._update(0)

		self.__initTimeLine = QtCore.QTimeLine(250, self)
		self.__initTimeLine.valueChanged.connect(self.__initValueChanged)
		self.__initTimeLine.stateChanged.connect(self.__initStateChanged)
		self.__initTimeLine.start()

	def delete(self):
		for item in self.__allItems.values():
			item.delete()
		self.__visibleItems = []
		self.__allItems = {}
		self.__availItems = []
		self.__curItem = None

	def getCurrentView(self):
		return self.__curItem.getWidget()

	def resizeEvent(self, event):
		super(WarpView, self).resizeEvent(event)
		self.__resize()

	def _addParents(self, parents):
		self.__parentBacklog.extend(parents)
		if not self.__parentTimer.isActive():
			self.__parentTimer.start()

	def _update(self, motion):
		# sort items by time
		self.__availItems = [item for item in self.__allItems.values()
			if item.available()]
		self.__availItems.sort(key=lambda item: item.mtime(), reverse=True)
		if self.__curItem not in self.__availItems:
			self.__state = self.__curItem.getState()
			self.__curItem = self.__availItems[0] # FIXME

		old = self.__visibleItems[:]
		index = self.__availItems.index(self.__curItem)
		self.__visibleItems = self.__availItems[index:index+3]
		for (pos, item) in zip(itertools.count(0), old):
			if item not in self.__visibleItems:
				item.setEnabled(False)
				item.setLevel(pos+motion)
				item.fadeOut()
		for (pos, item) in zip(itertools.count(0), self.__visibleItems):
			if item in old:
				item.setLevel(pos)
			else:
				item.fadeIn(pos-motion)
				item.setLevel(pos)
			item.setEnabled(pos == 0)
			item.setState(self.__state)

		self.__goPast.setEnabled(index < len(self.__availItems)-1)
		self.__goPresent.setEnabled(index > 0)

	def __inspectParents(self):
		trigger = False
		todo = self.__parentBacklog[0:3]
		del self.__parentBacklog[0:3]

		for rev in todo:
			if rev not in self.__allItems:
				self.__allItems[rev] = WarpItem(self, self.__class,
					self.size()*self.__distance, self.__scene, self.__store, rev)
				trigger = True

		if trigger:
			self._update(0)
		if self.__parentBacklog and not self.__parentTimer.isActive():
			self.__parentTimer.start()

	def __resize(self):
		size = self.size()
		offset = size.height() * self.__zoom
		self.setSceneRect(
			-size.width()/2,
			-size.height()/2-offset,
			size.width()-2,
			size.height()-2)
		h = self.__goPast.height()
		self.__openBtn.move(size.width()/2-self.__openBtn.width()-2,
			self.height()/2-3*h-2-offset)
		self.__goPast.move(size.width()/2-self.__goPast.width()-2,
			self.height()/2-2*h-2-offset)
		self.__goPresent.move(size.width()/2-self.__goPresent.width()-2,
			self.height()/2-h-2-offset)

		size = self.size() * self.__distance
		for item in self.__allItems.values():
			item.resize(size)

	def __initValueChanged(self, step):
		self.__zoom = (0.25/4)*step
		self.__distance = 1 - 0.25*step
		self.__resize()

	def __initStateChanged(self, state):
		if state == QtCore.QTimeLine.Running:
			for item in self.__visibleItems:
				item.setCacheMode(QtGui.QGraphicsItem.ItemCoordinateCache)
		elif state == QtCore.QTimeLine.NotRunning:
			for item in self.__visibleItems:
				item.setCacheMode(QtGui.QGraphicsItem.DeviceCoordinateCache)

	def __movePast(self):
		self.__state = self.__curItem.getState()
		self.__curItem = self.__visibleItems[1]
		self._update(-1)

	def __movePresent(self):
		self.__state = self.__curItem.getState()
		index = self.__availItems.index(self.__curItem)
		self.__curItem = self.__availItems[index-1]
		self._update(1)

	def __open(self):
		self.__state = self.__curItem.getState()
		link = connector.RevLink(self.__store, self.__curItem.rev())
		state = self.__state
		self.__initTimeLine.setDirection(QtCore.QTimeLine.Backward)
		self.__initTimeLine.finished.connect(
			lambda l=link, s=state: self.openItem.emit(l, s))
		self.__initTimeLine.start()


class ViewHandler(object):

	def __init__(self, mainWindow, view):
		self._main = mainWindow
		self._view = view
		self._view.revChanged.connect(self._extractMetaData)

	def enter(self, link, state):
		self._view._loadSettings(state)
		if isinstance(link, connector.DocLink):
			self._view.docOpen(link.store(), link.doc(), True)
		else:
			self._view.docOpen(link.store(), link.rev(), False)

	def leave(self, state={}):
		self._view.checkpoint("<<Changed by browser>>")
		self._view._saveSettings(state)
		self._view.docClose(False)

	def getView(self):
		return self._view

	def rev(self):
		return self._view.rev()

	def delete(self):
		self._view.setParent(None)
		self._main.menuBar().clear()
		del self._main

	def _extractMetaData(self):
		caption = self._view.metaDataGetField(DocumentView.HPA_TITLE, "Unnamed")
		if not self._view.doc() and self._view.rev():
			try:
				mtime = Connector().stat(self._view.rev()).mtime()
				caption = caption + " @ " + str(mtime)
			except IOError:
				pass
		self._main.setCaption(caption)


class FolderViewHandler(ViewHandler):
	def __init__(self, mainWindow):
		ViewHandler.__init__(self, mainWindow,
			FolderWidget(BrowserWindow.TYPES.keys()))
		self._view.itemOpen.connect(self._main.itemOpen)
		self._view.checkpointNeeded.connect(self.__checkpoint)

		self.__fileMenu = self._main.menuBar().addMenu("File")
		self.__fileMenu.aboutToShow.connect(self.__showFileMenu)
		self.__colMenu = self._main.menuBar().addMenu("Columns")
		self.__colMenu.aboutToShow.connect(self.__columnsShow)

	def delete(self):
		super(FolderViewHandler, self).delete()

	def getClass(self):
		return FolderWidget

	def getName(self):
		return 'org.peerdrive.folder'

	def __checkpoint(self, cpNeeded):
		if cpNeeded:
			self._view.checkpoint("<<Changed by browser>>")

	def __showFileMenu(self):
		self.__fileMenu.clear()
		self._main.activeView().fillContextMenu(self.__fileMenu)
		self.__fileMenu.addSeparator()
		self.__fileMenu.addAction(self._main.exitAct)

	def __columnsShow(self):
		self.__colMenu.clear()
		reg = Registry()
		model = self._main.activeView().model()
		types = model.typeCodes().copy()
		columns = model.getColumns()
		for col in columns:
			(uti, path) = col.split(':')
			if uti != "":
				types.add(uti)
		# add stat columns
		self.__columnsShowAddEntry(self.__colMenu, columns, ":size", "Size")
		self.__columnsShowAddEntry(self.__colMenu, columns, ":mtime", "Modification time")
		self.__columnsShowAddEntry(self.__colMenu, columns, ":type", "Type code")
		self.__columnsShowAddEntry(self.__colMenu, columns, ":creator", "Creator code")
		self.__columnsShowAddEntry(self.__colMenu, columns, ":comment", "Comment")
		self.__colMenu.addSeparator()
		# add meta columns
		metaSpecs = {}
		for t in types:
			metaSpecs.update(reg.searchAll(t, "meta"))
		for (uti, specs) in metaSpecs.items():
			subMenu = self.__colMenu.addMenu(reg.getDisplayString(uti))
			for spec in specs:
				key = uti+":"+reduce(lambda x,y: x+"/"+y, spec["key"])
				self.__columnsShowAddEntry(subMenu, columns, key, spec["display"])

	def __columnsShowAddEntry(self, subMenu, columns, key, display):
		model = self._main.activeView().model()
		visible = key in columns
		action = subMenu.addAction(display)
		action.setCheckable(True)
		action.setChecked(visible)
		if visible:
			action.triggered.connect(lambda en, col=key: model.remColumn(col))
		else:
			action.triggered.connect(lambda en, col=key: model.addColumn(col))


class TextViewHandler(ViewHandler):
	def __init__(self, mainWindow):
		ViewHandler.__init__(self, mainWindow, TextEdit())
		self.__fileMenu = self._main.menuBar().addMenu("File")
		self.__fileMenu.addAction(self._main.exitAct)

	def getClass(self):
		return TextEdit

	def getName(self):
		return 'public.plain-text'


class BrowserWindow(QtGui.QMainWindow):

	TYPES = {
		"org.peerdrive.browser.py" : FolderViewHandler,
		"org.peerdrive.textedit.py" : TextViewHandler
	}

	def __init__(self):
		QtGui.QMainWindow.__init__(self)
		self.setAttribute(QtCore.Qt.WA_DeleteOnClose)
		self.setWindowIcon(QtGui.QIcon("icons/system-file-manager.png"))

		self.__viewHandler = None
		self.__store = None
		self.__storeButtons = { }

		self.__history = History()
		self.__history.leaveItem.connect(self.__leaveItem)
		self.__history.enterItem.connect(self.__enterItem)

		self.__backMenu = QtGui.QMenu()
		self.__backMenu.aboutToShow.connect(self.__showBackMenu)
		self.__backAct = QtGui.QAction(QtGui.QIcon('icons/back.png'), "Back", self)
		self.__backAct.setShortcuts(QtGui.QKeySequence.Back)
		self.__backAct.setMenu(self.__backMenu)
		self.__backAct.triggered.connect(self.__history.back)
		self.__forwardMenu = QtGui.QMenu()
		self.__forwardMenu.aboutToShow.connect(self.__showForwardMenu)
		self.__forwardAct = QtGui.QAction(QtGui.QIcon('icons/forward.png'), "Forward", self)
		self.__forwardAct.setShortcuts(QtGui.QKeySequence.Forward)
		self.__forwardAct.setMenu(self.__forwardMenu)
		self.__forwardAct.triggered.connect(self.__history.forward)
		self.exitAct = QtGui.QAction("Close", self)
		self.exitAct.setShortcut("Ctrl+Q")
		self.exitAct.setStatusTip("Close the document")
		self.exitAct.triggered.connect(self.close)

		self.__warpAct = QtGui.QAction("TimeWarp", self)
		self.__warpAct.setCheckable(True)
		self.__warpAct.toggled.connect(self.__warp)

		self.__browseToolBar = self.addToolBar("Browse")
		self.__browseToolBar.addAction(self.__backAct)
		self.__browseToolBar.addAction(self.__forwardAct)
		self.__browseToolBar.addAction(self.__warpAct)

	def closeEvent(self, event):
		super(BrowserWindow, self).closeEvent(event)
		if self.__viewHandler:
			self.__saveSettings()
			self.__viewHandler.leave()

	def open(self, argv):
		usage = ("usage: %prog [options] <Document>\n\n"
			"Document:\n"
			"    doc:<document>  ...open the latest version of the given document\n"
			"    rev:<revision>  ...display the given revision\n"
			"    <hp-path-spec>  ...open by path spec")
		parser = optparse.OptionParser(usage=usage)
		parser.add_option("--referrer", dest="referrer", metavar="REF",
			help="Document from which we're coming")
		(options, args) = parser.parse_args(args=argv[1:])
		if len(args) != 1:
			parser.error("incorrect number of arguments")

		# parse command line
		try:
			link = connector.Link(args[0])
			link.update()
		except IOError as e:
			parser.error(str(e))

		if not link.doc() and not link.rev():
			parser.error("document not found")

		# open the document
		self.__store = link.store()
		if not self.itemOpen(link):
			parser.error("cannot browse item")

		self.__loadSettings()

	def setCaption(self, caption):
		self.__history.currentItem().setText(caption)
		self.setWindowTitle(caption)

	def activeView(self):
		if self.__warpAct.isChecked():
			return self.centralWidget().getCurrentView()
		else:
			return self.centralWidget()

	def itemOpen(self, link, executable=None, browseHint=True, state={}):
		if browseHint:
			myLink = copy.deepcopy(link)
			myLink.update(self.__store)
			self.__history.push(myLink, state)
			return True
		else:
			utils.showDocument(link, executable=executable,
				referrer=self.__history.currentItem().link())
			return False

	def __leaveItem(self, item):
		if self.__warpAct.isChecked():
			self.__warpAct.setChecked(False)
		self.__viewHandler.leave(item.state())

	def __enterItem(self, item):
		link = item.link()
		self.__store = link.store()
		self.__setViewHandler(link)
		self.__viewHandler.enter(link, item.state())
		self.__backAct.setEnabled(self.__history.canGoBack())
		self.__forwardAct.setEnabled(self.__history.canGoForward())
		self.__updateStoreButtons()

	def __setViewHandler(self, link):
		link.update()
		try:
			type = Connector().stat(link.rev()).type()
			executables = Registry().getExecutables(type)
		except IOError:
			executables = []

		if not executables:
			# Probably a bad idea to leave the current view widget, but what
			# else can we do?
			return

		for executable in executables:
			if executable in BrowserWindow.TYPES:
				break
		handler = BrowserWindow.TYPES[executable]
		if self.__viewHandler:
			if isinstance(self.__viewHandler, handler):
				return
			self.__viewHandler.delete()
		self.__viewHandler = handler(self)
		self.setCentralWidget(self.__viewHandler.getView())
		self.__viewHandler.getView().distributionChanged.connect(
			self.__updateStoreButtons)

	def __showBackMenu(self):
		self.__backMenu.clear()
		items = self.__history.backItems(10)[:]
		items.reverse()
		for item in items:
			action = self.__backMenu.addAction(item.text())
			action.triggered.connect(lambda x,item=item: self.__history.goToItem(item))

	def __showForwardMenu(self):
		self.__forwardMenu.clear()
		for item in self.__history.forwardItems(10):
			action = self.__forwardMenu.addAction(item.text())
			action.triggered.connect(lambda x,item=item: self.__history.goToItem(item))

	def __warp(self, checked):
		if checked:
			self.centralWidget().setParent(None)
			state = {}
			self.__viewHandler.getView()._saveSettings(state)
			warp = WarpView(self.__viewHandler.getClass(), self.__store,
				self.__viewHandler.rev(), state)
			warp.openItem.connect(self.__warpOpen)
			self.setCentralWidget(warp)
		else:
			self.centralWidget().delete()
			self.setCentralWidget(self.__viewHandler.getView())

	def __warpOpen(self, link, state):
		if self.itemOpen(link, state=state):
			self.__warpAct.setChecked(False)

	def __updateStoreButtons(self):
		view = self.__viewHandler.getView()
		curStore = view.store()
		if view.doc():
			allStores = Connector().lookupDoc(view.doc()).stores()
		else:
			allStores = Connector().lookupRev(view.rev())

		# update store buttons in status bar
		for store in set(self.__storeButtons) ^ set(allStores):
			if store in allStores:
				button = DocButton(store, store, checkable=True)
				button.clicked.connect(lambda x,store=store: self.__switchStore(store))
				self.statusBar().addPermanentWidget(button)
				self.__storeButtons[store] = button
			else:
				self.statusBar().removeWidget(self.__storeButtons[store])
				del self.__storeButtons[store]

		for (store,button) in self.__storeButtons.items():
			button.setChecked(store == curStore)

	def __switchStore(self, store):
		self.__store = store
		self.__viewHandler.getView().switchStore(store)
		self.__updateStoreButtons()

	def __saveSettings(self):
		if not os.path.exists(settingsPath()):
			os.makedirs(settingsPath())
		path = os.path.join(settingsPath(), "browser-" + self.__viewHandler.getName())
		with open(path, 'w') as f:
			settings = { }
			settings["resx"] = self.size().width()
			settings["resy"] = self.size().height()
			self.__viewHandler.getView()._saveSettings(settings)
			pickle.dump(settings, f)

	def __loadSettings(self):
		settings = { }
		path = os.path.join(settingsPath(), "browser-" + self.__viewHandler.getName())
		try:
			if os.path.isfile(path):
				with open(path, 'r') as f:
					settings = pickle.load(f)
		except Exception as e:
			print "Failed to load settings!:", e

		self.__viewHandler.getView()._loadSettings(settings)
		if "resx" in settings and "resy" in settings:
			self.resize(settings["resx"], settings["resy"])


if __name__ == '__main__':

	import sys

	app = QtGui.QApplication(sys.argv)
	mainWin = BrowserWindow()
	mainWin.open(sys.argv)
	mainWin.show()
	sys.exit(app.exec_())

