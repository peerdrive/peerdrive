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

from __future__ import absolute_import

from PyQt4 import QtCore, QtGui
import sys, os, subprocess, pickle, datetime

from ..connector import Watch, Connector
from ..registry import Registry
from .. import struct
from .widgets import DocumentView
from .utils import showProperties

class MainWindow(QtGui.QMainWindow, Watch):

	def __init__(self, viewWidget, isEditor):
		QtGui.QMainWindow.__init__(self)
		self.setAttribute(QtCore.Qt.WA_DeleteOnClose)

		self.__view     = viewWidget
		self.__isEditor = isEditor
		self.__mutable  = False
		self.__utiPixmap = None
		self.setCentralWidget(self.__view)

		viewWidget.checkpointNeeded.connect(lambda e: self.__saveAct.setEnabled(e))
		viewWidget.mergeNeeded.connect(lambda e: self.__mergeAct.setVisible(e))
		viewWidget.revChanged.connect(self.__extractMetaData)
		viewWidget.mutable.connect(self.__setMutable)

		# create standard actions
		if isEditor:
			self.__saveAct = QtGui.QAction(QtGui.QIcon('icons/save.png'), "Check&point", self)
			self.__saveAct.setEnabled(False)
			self.__saveAct.setShortcut(QtGui.QKeySequence.Save)
			self.__saveAct.setStatusTip("Create checkpoint of document")
			QtCore.QObject.connect(self.__saveAct, QtCore.SIGNAL("triggered()"), self.__checkpointFile)

			self.__mergeMenu = QtGui.QMenu()
			self.__mergeMenu.aboutToShow.connect(self.__mergeShow)
			self.__mergeAct = QtGui.QAction(QtGui.QIcon('icons/merge.png'), "Merge", self)
			self.__mergeAct.setStatusTip("Merge other revisions into current document")
			self.__mergeAct.setMenu(self.__mergeMenu)
			self.__mergeAct.triggered.connect(lambda: self.__mergeMenu.exec_(QtGui.QCursor.pos()))
			self.__mergeAct.setVisible(False)

			self.__stickyAct = QtGui.QAction("Sticky", self)
			self.__stickyAct.setStatusTip("Automatically replicate referenced documents")
			self.__stickyAct.setCheckable(True)
			self.__stickyAct.setEnabled(False)
			QtCore.QObject.connect(self.__stickyAct, QtCore.SIGNAL("triggered(bool)"), self.__toggleSticky)

			self.__historySpin = QtGui.QSpinBox(self)
			self.__historySpin.setMaximum(365)
			self.__historySpin.setSuffix(" days")
			self.__historySpin.setStatusTip("Maximum depth (in days) of the replicated documents histories")
			QtCore.QObject.connect(self.__historySpin, QtCore.SIGNAL("valueChanged(int)"), self.__toggleHistroy)

			self.__historyAct = QtGui.QWidgetAction(self)
			self.__historyAct.setDefaultWidget(self.__historySpin)
			self.__historyAct.setEnabled(False)

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
		self.__delMenu.aboutToShow.connect(self.__deleteFile)
		self.delAct = QtGui.QAction(QtGui.QIcon('icons/edittrash.png'), "&Delete", self)
		self.delAct.setStatusTip("Delete the document")
		self.delAct.setMenu(self.__delMenu)
		self.delAct.triggered.connect(lambda: self.__delMenu.exec_(QtGui.QCursor.pos()))

		self.__exitAct = QtGui.QAction("Close", self)
		self.__exitAct.setShortcut("Ctrl+Q")
		self.__exitAct.setStatusTip("Close the document")
		QtCore.QObject.connect(self.__exitAct, QtCore.SIGNAL("triggered()"), self.close)

		# create standard menu
		self.fileMenu = self.menuBar().addMenu("&Document")
		if isEditor:
			self.fileMenu.addAction(self.__saveAct)
			self.fileMenu.addAction(self.__mergeAct)
		self.fileMenu.addAction(self.delAct)
		if isEditor:
			self.repMenu = self.fileMenu.addMenu("Replication")
			self.repMenu.addAction(self.__stickyAct)
			self.repMenu.addAction(self.__historyAct)
		self.fileMenu.addAction(self.__propertiesAct)
		self.fileMenu.addSeparator();
		self.fileMenu.addAction(self.__exitAct)

		# standard tool bars
		self.fileToolBar = self.addToolBar("Document")
		self.dragWidget = DragWidget(self)
		self.fileToolBar.addWidget(self.dragWidget)
		self.fileToolBar.addSeparator()
		if isEditor:
			self.fileToolBar.addAction(self.__saveAct)
			self.fileToolBar.addAction(self.__mergeAct)
		self.fileToolBar.addAction(self.delAct)
		self.fileToolBar.addAction(self._annotationDock.toggleViewAction())

		# save comment popup
		self.__commentPopup = CommentPopup(self, lambda c,f: self.__view.checkpoint(c, f))

		# disable for now
		self.delAct.setEnabled(True)

	def viewWidget(self):
		return self.__view

	def open(self, argv):
		# parse command line
		if len(argv) == 2 and argv[1].startswith('doc:'):
			guid = argv[1][4:].decode("hex")
			isDoc = True
		elif len(argv) == 2 and argv[1].startswith('rev:'):
			guid = argv[1][4:].decode("hex")
			isDoc = False
		elif len(argv) == 2:
			link = struct.resolvePath(argv[1])
			if isinstance(link, struct.DocLink):
				guid = link.doc()
				isDoc = True
			else:
				guid = link.rev()
				isDoc = False
		else:
			print "Usage: %s <Document>" % (sys.argv[0])
			print
			print "Document:"
			print "    doc:<document>  ...open the latest version of the given document"
			print "    rev:<revision>  ...display the given revision"
			print "    <hp-path-spec>  ...open by path spec"
			sys.exit(1)

		# open the document
		self.__view.open(guid, isDoc)

	def __setMutable(self, mutable):
		self.__mutable = mutable
		self.__nameEdit.setReadOnly(not mutable)
		self.__tagsEdit.setReadOnly(not mutable)
		self.__descEdit.setReadOnly(not mutable)

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
			self.__stickyAct.setChecked(self.__view.metaDataGetField(DocumentView.SYNC_STICKY, False))
			self.__historyAct.setEnabled(self.__mutable and self.__stickyAct.isChecked())
			self.__historySpin.setValue(self.__view.metaDataGetField(DocumentView.SYNC_HISTROY, 0) / (24*60*60))

	def __getUtiPixmap(self):
		if self.__utiPixmap is None:
			if self.__view.rev():
				uti = Connector().stat(self.__view.rev()).type()
			else:
				uti = "public.data"
			self.__utiPixmap = QtGui.QPixmap(Registry().getIcon(uti))
		return self.__utiPixmap

	def __checkpointFile(self):
		self.__commentPopup.popup(self.__view.metaDataGetField(DocumentView.HPA_COMMENT, "Enter comment"))

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
		self.__view.metaDataSetField(DocumentView.SYNC_STICKY, checked)
		self.__historyAct.setEnabled(checked)

	def __toggleHistroy(self, value):
		self.__view.metaDataSetField(DocumentView.SYNC_HISTROY, value*24*60*60)

	def __mergeShow(self):
		pass

	def __showProperties(self):
		if self.__view.doc():
			link = struct.DocLink(self.__view.doc(), False)
		else:
			link = struct.RevLink(self.__view.rev())
		showProperties(link)

	def __deleteFile(self):
		self.__delMenu.clear()
		doc = self.__view.doc()
		rev = self.__view.rev()
		if doc:
			lookup = Connector().lookup_doc(doc)
			stores = lookup.stores()
			delFun = lambda store: Connector().deleteDoc(doc, lookup.rev(store), [store])
		else:
			stores = Connector().lookup_rev(rev)
			delFun = lambda store: Connector().deleteRev(rev, [store])
		stores = [(self.__getStoreName(store), store) for store in stores]
		stores = filter(lambda(name,store):name, stores)
		for (name, store) in stores:
			action = self.__delMenu.addAction("Delete item from '%s'" % name)
			action.triggered.connect(lambda: delFun(store))

		if len(stores) > 1:
			self.__delMenu.addSeparator()
			delAllAction = self.__delMenu.addAction("Delete from all stores")
			if doc:
				delAllAction.triggered.connect(lambda: Connector().deleteDoc(doc, rev))
			else:
				delAllAction.triggered.connect(lambda: Connector().deleteRev(rev))

	def __getStoreName(self, store):
		try:
			rev = Connector().lookup_doc(store).rev(store)
			with Connector().peek(rev) as r:
				try:
					metaData = struct.loads(r.readAll('META'))
					return metaData["org.hotchpotch.annotation"]["title"]
				except:
					return "Unnamed store"
		except:
			return None


class DragWidget(QtGui.QLabel):

	def __init__(self, parent):
		super(DragWidget, self).__init__()
		self.__parent = parent
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
		doc = self.__parent.doc()
		if doc:
			struct.DocLink(doc, False).mimeData(mimeData)
		else:
			rev = self.__parent.rev()
			struct.RevLink(rev).mimeData(mimeData)

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
		self.__action(str(self.__commentEdit.text()), True)

