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

from PyQt4 import QtCore, QtGui
import sys, os, subprocess, pickle, datetime, optparse

from ..connector import Watch, Connector, Stat
from ..registry import Registry
from .. import struct, fuse, settingsPath, connector
from .widgets import DocumentView, DocButton
from .utils import showProperties

class MainWindow(QtGui.QMainWindow, Watch):

	def __init__(self, viewWidget, isEditor):
		QtGui.QMainWindow.__init__(self)
		self.setAttribute(QtCore.Qt.WA_DeleteOnClose)

		self.__view     = viewWidget
		self.__isEditor = isEditor
		self.__mutable  = False
		self.__utiPixmap = None
		self.__storeButtons = { }
		self.__referrer = None
		self.setCentralWidget(self.__view)

		viewWidget.revChanged.connect(self.__extractMetaData)
		viewWidget.mutable.connect(self.__setMutable)
		viewWidget.distributionChanged.connect(self.__updateStoreButtons)

		# create standard actions
		if isEditor:
			self.__saveAct = QtGui.QAction(QtGui.QIcon('icons/save.png'), "Check&point", self)
			self.__saveAct.setEnabled(False)
			self.__saveAct.setShortcut(QtGui.QKeySequence.Save)
			self.__saveAct.setStatusTip("Create checkpoint of document")
			QtCore.QObject.connect(self.__saveAct, QtCore.SIGNAL("triggered()"), self.__checkpointFile)
			self.__view.checkpointNeeded.connect(lambda e: self.__saveAct.setEnabled(e))

			self.__revertAct = QtGui.QAction(QtGui.QIcon('icons/undo.png'), "Revert", self)
			self.__revertAct.setEnabled(False)
			self.__revertAct.setStatusTip("Revert to the last checkpoint")
			self.__revertAct.triggered.connect(self.__revertFile)
			self.__view.checkpointNeeded.connect(lambda e: self.__revertAct.setEnabled(e))

			self.__mergeMenu = QtGui.QMenu()
			self.__mergeMenu.aboutToShow.connect(self.__mergeShow)
			self.__mergeAct = QtGui.QAction(QtGui.QIcon('icons/merge.png'), "Merge", self)
			self.__mergeAct.setStatusTip("Merge other revisions into current document")
			self.__mergeAct.setMenu(self.__mergeMenu)
			self.__mergeAct.triggered.connect(lambda: self.__mergeMenu.exec_(QtGui.QCursor.pos()))
			self.__mergeAct.setVisible(False)
			self.__view.mergeNeeded.connect(lambda e: self.__mergeAct.setVisible(e))

			self.__stickyAct = QtGui.QAction("Sticky", self)
			self.__stickyAct.setStatusTip("Automatically replicate referenced documents")
			self.__stickyAct.setCheckable(True)
			self.__stickyAct.setEnabled(False)
			self.__stickyAct.triggered.connect(self.__toggleSticky)

		self.__nameEdit = QtGui.QLineEdit()
		self.__tagsEdit = QtGui.QLineEdit()
		self.__tagsEdit.setValidator(QtGui.QRegExpValidator(
			QtCore.QRegExp("(\\s*\\w+\\s*(,\\s*\\w+\\s*)*)?"),
			self))
		self.__descEdit = QtGui.QTextEdit()
		self.__descEdit.setAcceptRichText(False)
		QtCore.QObject.connect(self.__nameEdit, QtCore.SIGNAL("textEdited(const QString&)"), self.__nameChanged)
		QtCore.QObject.connect(self.__tagsEdit, QtCore.SIGNAL("textEdited(const QString&)"), self.__tagsChanged)
		QtCore.QObject.connect(self.__descEdit, QtCore.SIGNAL("textChanged()"), self.__descChanged)
		layout = QtGui.QVBoxLayout()

		gl = QtGui.QGridLayout()
		gl.addWidget(QtGui.QLabel("Title:"), 0, 0)
		gl.addWidget(self.__nameEdit, 0, 1)
		gl.addWidget(QtGui.QLabel("Tags:"), 1, 0)
		gl.addWidget(self.__tagsEdit, 1, 1)
		layout.addLayout(gl)

		layout.addWidget(QtGui.QLabel("Description:"))
		layout.addWidget(self.__descEdit)
		frame = QtGui.QWidget()
		frame.setLayout(layout)
		self._annotationDock = QtGui.QDockWidget("Annotation", self)
		self._annotationDock.setAllowedAreas(QtCore.Qt.LeftDockWidgetArea | QtCore.Qt.RightDockWidgetArea)
		self._annotationDock.setWidget(frame)
		self.addDockWidget(QtCore.Qt.RightDockWidgetArea, self._annotationDock)
		self._annotationDock.hide()

		self.__propertiesAct = QtGui.QAction("&Properties", self)
		QtCore.QObject.connect(self.__propertiesAct, QtCore.SIGNAL("triggered()"), self.__showProperties)

		self.__delMenu = QtGui.QMenu()
		self.__delMenu.aboutToShow.connect(self.__fillDelMenu)
		self.delAct = QtGui.QAction(QtGui.QIcon('icons/edittrash.png'), "&Delete", self)
		self.delAct.setStatusTip("Delete the document")
		self.delAct.setMenu(self.__delMenu)
		self.delAct.triggered.connect(self.__delete)

		self.__exitAct = QtGui.QAction("Close", self)
		self.__exitAct.setShortcut("Ctrl+Q")
		self.__exitAct.setStatusTip("Close the document")
		QtCore.QObject.connect(self.__exitAct, QtCore.SIGNAL("triggered()"), self.close)

		# create standard menu
		self.fileMenu = self.menuBar().addMenu("&Document")
		if isEditor:
			self.fileMenu.addAction(self.__saveAct)
			self.fileMenu.addAction(self.__revertAct)
			self.fileMenu.addAction(self.__mergeAct)
		self.fileMenu.addAction(self.delAct)
		if isEditor:
			self.repMenu = self.fileMenu.addMenu("Replication")
			self.repMenu.addAction(self.__stickyAct)
		self.fileMenu.addAction(self.__propertiesAct)
		self.fileMenu.addSeparator();
		self.fileMenu.addAction(self.__exitAct)

		# standard tool bars
		self.fileToolBar = self.addToolBar("Document")
		self.dragWidget = DragWidget(self.__view)
		self.dragWidget.setPixmap(QtGui.QPixmap("icons/uti/unknown.png"))
		self.fileToolBar.addWidget(self.dragWidget)
		self.fileToolBar.addSeparator()
		if isEditor:
			self.fileToolBar.addAction(self.__saveAct)
			self.fileToolBar.addAction(self.__mergeAct)
		self.fileToolBar.addAction(self.delAct)
		self.fileToolBar.addAction(self._annotationDock.toggleViewAction())

		# save comment popup
		self.__commentPopup = CommentPopup(self, lambda c: self.__view.checkpoint(c))

		# disable for now
		self.delAct.setEnabled(True)

	def viewWidget(self):
		return self.__view

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
		try:
			if options.referrer:
				self.__referrer = connector.Link(options.referrer)
		except IOError:
			parser.error("invalid referrer")

		# parse command line
		try:
			link = connector.Link(args[0])
		except IOError as e:
			parser.error(str(e))
		if isinstance(link, connector.DocLink):
			guid = link.doc()
			store = link.store()
			isDoc = True
		else:
			guid = link.rev()
			store = link.store()
			isDoc = False

		# open the document
		self.__loadSettings(guid)
		self.__view.docOpen(store, guid, isDoc)
		self.__updateStoreButtons()

	# === re-implemented inherited methods

	def closeEvent(self, event):
		event.accept()
		if self.__mutable:
			self.__view.checkpoint("<<Automatically saved>>")
		self.__saveSettings()
		self.__view.docClose()

	# === protected methos

	def _saveSettings(self, settings):
		settings["resx"] = self.size().width()
		settings["resy"] = self.size().height()
		settings["posx"] = self.pos().x()
		settings["posy"] = self.pos().y()

	def _loadSettings(self, settings):
		if "resx" in settings and "resy" in settings:
			self.resize(settings["resx"], settings["resy"])
		if "posx" in settings and "posy" in settings:
			self.move(settings["posx"], settings["posy"])

	def __saveSettings(self):
		if self.__view.doc():
			hash = self.__view.doc().encode('hex')
		else:
			hash = self.__view.rev().encode('hex')
		path = os.path.join(settingsPath(), 'views', hash[0:2])
		if not os.path.exists(path):
			os.makedirs(path)
		with open(os.path.join(path, hash[2:]), 'w') as f:
			settings = { }
			self._saveSettings(settings)
			self.__view._saveSettings(settings)
			pickle.dump(settings, f)

	def __loadSettings(self, guid):
		settings = { }

		guid = guid.encode('hex')
		path = os.path.join(settingsPath(), 'views', guid[0:2], guid[2:])
		try:
			if os.path.isfile(path):
				with open(path, 'r') as f:
					settings = pickle.load(f)
		except Exception as e:
			print "Failed to load settings!:", e

		self.__view._loadSettings(settings)
		self._loadSettings(settings)

	def __setMutable(self, mutable):
		self.__mutable = mutable
		self.__nameEdit.setReadOnly(not mutable)
		self.__tagsEdit.setReadOnly(not mutable)
		self.__descEdit.setReadOnly(not mutable)
		if self.__isEditor:
			self.__stickyAct.setEnabled(mutable)

	def __extractMetaData(self):
		# window icon
		self.setWindowIcon(QtGui.QIcon(self.__getUtiPixmap()))
		self.dragWidget.setPixmap(self.__getUtiPixmap())

		# meta data
		name = self.__view.metaDataGetField(DocumentView.HPA_TITLE, "Unnamed document")
		self.__nameEdit.setText(name)
		tagList = self.__view.metaDataGetField(DocumentView.HPA_TAGS, [])
		tagList.sort()
		if len(tagList) == 0:
			tagString = ""
		else:
			tagString = reduce(lambda x,y: x+', '+y, tagList)
		self.__tagsEdit.setText(tagString)
		self.__descEdit.setPlainText(self.__view.metaDataGetField(DocumentView.HPA_DESCRIPTION, ""))
		self.setWindowTitle(name)
		if self.__isEditor:
			self.__stickyAct.setEnabled(self.__mutable)
			self.__stickyAct.setChecked(self.__view.metaDataGetFlag(Stat.FLAG_STICKY))

	def __getUtiPixmap(self):
		if self.__utiPixmap is None:
			if self.__view.rev():
				uti = Connector().stat(self.__view.rev()).type()
			else:
				uti = "public.data"
			self.__utiPixmap = QtGui.QPixmap(Registry().getIcon(uti))
		return self.__utiPixmap

	def __updateStoreButtons(self):
		curStore = self.__view.store()
		if self.__view.doc():
			allStores = Connector().lookupDoc(self.__view.doc()).stores()
		else:
			allStores = Connector().lookupRev(self.__view.rev())

		if not allStores:
			self.close()
			return

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
		self.__view.switchStore(store)
		self.__updateStoreButtons()

	def __checkpointFile(self):
		self.__commentPopup.popup()

	def __revertFile(self):
		choice = QtGui.QMessageBox.question(self, 'Revert',
			'Throw away all changes since the last checkpoint?',
			QtGui.QMessageBox.Yes | QtGui.QMessageBox.No, QtGui.QMessageBox.No)
		if choice == QtGui.QMessageBox.Yes:
			self.__view.docRevert()

	def __nameChanged(self, name):
		self.__view.metaDataSetField(DocumentView.HPA_TITLE, str(self.__nameEdit.text()))
		self.setWindowTitle(name)

	def __tagsChanged(self, tagString):
		if self.__tagsEdit.hasAcceptableInput():
			tagSet = set([ tag.strip() for tag in str(tagString).split(',')])
			tagList = list(tagSet)
			self.__view.metaDataSetField(DocumentView.HPA_TAGS, tagList)

	def __descChanged(self):
		old = self.__view.metaDataGetField(DocumentView.HPA_DESCRIPTION, "")
		new = str(self.__descEdit.toPlainText())
		if old != new:
			self.__view.metaDataSetField(DocumentView.HPA_DESCRIPTION, new)

	def __toggleSticky(self, checked):
		self.__view.metaDataSetFlag(Stat.FLAG_STICKY, checked)

	def __mergeShow(self):
		lookup = Connector().lookupDoc(self.__view.doc())
		revs = set(lookup.revs())
		if self.__view.rev() in lookup.preRevs():
			revs -= set(Connector().stat(self.__view.rev()).parents())
		else:
			revs -= set([self.__view.rev()])

		self.__mergeMenu.clear()
		for rev in revs:
			for store in lookup.stores(rev):
				name = self.__getStoreName(store)
				action = self.__mergeMenu.addAction(name)
				action.triggered.connect(lambda x,store=store,rev=rev: self.__view.merge(store, rev))

	def __showProperties(self):
		if self.__view.doc():
			link = connector.DocLink(self.__view.store(), self.__view.doc(), False)
		else:
			link = connector.RevLink(self.__view.store(), self.__view.rev())
		showProperties(link)

	def __fillDelMenu(self):
		store = self.__view.store()
		doc = self.__view.doc()
		rev = self.__view.rev()
		self.__delMenu.clear()
		if isinstance(self.__referrer, connector.DocLink):
			if doc:
				link = connector.DocLink(store, doc, autoUpdate=False)
			else:
				link = connector.RevLink(store, rev)
			try:
				folder = struct.Folder(self.__referrer)
				title = folder.title()
				for (name, ref) in folder.items():
					if ref == link:
						action = self.__delMenu.addAction("Unlink '" + name +
							"' from '" + title + "'")
						action.triggered.connect(
							lambda x,n=name,l=ref: self.__unlink(n,l))
			except IOError:
				pass
			self.__delMenu.addSeparator()
		if doc:
			lookup = Connector().lookupDoc(doc)
			stores = lookup.stores()
			delFun = lambda store: Connector().deleteDoc(store, doc, lookup.rev(store))
		else:
			stores = Connector().lookupRev(rev)
			delFun = lambda store: Connector().deleteRev(store, rev)
		stores = [(self.__getStoreName(store), store) for store in stores]
		stores = filter(lambda(name,store):name, stores)
		for (name, store) in stores:
			action = self.__delMenu.addAction("Delete item from '%s'" % name)
			action.triggered.connect(lambda x,s=store: delFun(s))
			action.setEnabled(doc != store)

	def __unlink(self, name, link):
		try:
			folder = struct.Folder(self.__referrer)
			folder.remove(name, link)
			folder.save()
			self.close()
		except IOError:
			QtGui.QMessageBox.warning(self, 'Unlink failed',
				'Could not unlink from folder. Try again...')

	def __delete(self):
		try:
			if isinstance(self.__referrer, connector.DocLink):
				store = self.__view.store()
				doc = self.__view.doc()
				if doc:
					link = connector.DocLink(store, doc, autoUpdate=False)
				else:
					link = connector.RevLink(store, self.__view.rev())
				folder = struct.Folder(self.__referrer)
				candidates = [(name, ref) for (name, ref) in folder.items()
					if ref == link]
				if len(candidates) == 1:
					[(name, ref)] = candidates
					folder.remove(name, ref)
					folder.save()
					self.close()
					return
		except IOError:
			pass

		# fallback: show menu
		self.__delMenu.exec_(QtGui.QCursor.pos())

	def __getStoreName(self, store):
		try:
			rev = Connector().lookupDoc(store).rev(store)
			with Connector().peek(store, rev) as r:
				try:
					return r.getData("/org.peerdrive.annotation/title")
				except:
					return "Unnamed store"
		except:
			return None


class DragWidget(QtGui.QLabel):

	def __init__(self, view):
		super(DragWidget, self).__init__()
		self.__view = view
		self.setFrameShadow(QtGui.QFrame.Raised)
		self.setFrameShape(QtGui.QFrame.Box)

	def mousePressEvent(self, event):
		if event.button() == QtCore.Qt.LeftButton:
			self.dragStartPosition = event.pos()

	def mouseMoveEvent(self, event):
		if not (event.buttons() & QtCore.Qt.LeftButton):
			return
		#if (event.pos() - self.dragStartPosition).manhattanLength() < QtGui.QApplication.startDragDistance():
		#	return

		drag = QtGui.QDrag(self)
		mimeData = QtCore.QMimeData()
		store = self.__view.store()
		doc = self.__view.doc()
		if doc:
			link = connector.DocLink(store, doc)
		else:
			link = connector.RevLink(store, self.__view.rev())
		connector.dumpMimeData(mimeData, [link])
		f = fuse.findFuseFile(link)
		if f:
			mimeData.setUrls([QtCore.QUrl.fromLocalFile(f)])

		drag.setMimeData(mimeData)
		drag.setPixmap(self.pixmap())

		dropAction = drag.exec_(QtCore.Qt.CopyAction)


class CommentPopup(object):
	def __init__(self, parent, action):
		self.__parent = parent
		self.__action = action
		self.__commentEdit = QtGui.QLineEdit()
		self.__commentAct = QtGui.QWidgetAction(parent)
		self.__commentAct.setDefaultWidget(self.__commentEdit)
		self.__menu = QtGui.QMenu()
		self.__menu.addAction(self.__commentAct)
		self.__menu.addSeparator()
		self.__menu.addAction("Cancel")
		self.__menu.setActiveAction(self.__commentAct)
		QtCore.QObject.connect(
			self.__commentEdit,
			QtCore.SIGNAL("returnPressed()"),
			self.__returnPressed)

	def popup(self, oldComment = ""):
		self.__menu.popup(QtGui.QCursor.pos(), self.__commentAct)
		self.__commentEdit.setText(oldComment)
		self.__commentEdit.selectAll()
		self.__commentEdit.setFocus(QtCore.Qt.OtherFocusReason)

	def __returnPressed(self):
		self.__action(str(self.__commentEdit.text()))

