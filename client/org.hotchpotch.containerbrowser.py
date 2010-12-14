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

from PyQt4 import QtCore, QtGui
from hotchpotch import Connector, struct
from hotchpotch.gui import utils
from hotchpotch.gui.widgets import DocumentView

from container import CollectionWidget, CollectionModel

class HistoryItem(object):

	def __init__(self, link):
		self.__link = link
		self.__text = "FIXME"
		self.__state = { }

	def link(self):
		return self.__link

	def text(self):
		return self.__text

	def setText(self, text):
		self.__text = text

	def state(self):
		return self.__state


class History(QtCore.QObject):

	navigated = QtCore.pyqtSignal()

	changed = QtCore.pyqtSignal()

	def __init__(self):
		super(History, self).__init__()

		self.__items = []
		self.__current = -1

	def back(self):
		if self.__current > 0:
			self.__current -= 1
			self.navigated.emit()

	def forward(self):
		if self.__current < len(self.__items)-1:
			self.__current += 1
			self.navigated.emit()

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
		self.__current = self.__items.index(item)
		self.navigated.emit()

	def items(self):
		return self.__items[:]

	def push(self, link):
		self.__current += 1
		self.__items[self.__current:] = [HistoryItem(link)]
		self.changed.emit()


class BrowserWindow(QtGui.QMainWindow):

	def __init__(self):
		QtGui.QMainWindow.__init__(self)
		self.setAttribute(QtCore.Qt.WA_DeleteOnClose)

		self.__view = CollectionWidget()
		self.__view.itemOpen.connect(self.__itemOpen)
		self.__view.revChanged.connect(self.__extractMetaData)
		self.setCentralWidget(self.__view)

		self.__history = History()
		self.__history.changed.connect(self.__updateBrowseActions)
		self.__history.navigated.connect(self.__navigate)

		self.__backMenu = QtGui.QMenu()
		self.__backMenu.aboutToShow.connect(self.__showBackMenu)
		self.__backAct = QtGui.QAction(QtGui.QIcon('icons/back.png'), "Back", self)
		self.__backAct.setMenu(self.__backMenu)
		self.__backAct.triggered.connect(self.__history.back)
		self.__forwardMenu = QtGui.QMenu()
		self.__forwardMenu.aboutToShow.connect(self.__showForwardMenu)
		self.__forwardAct = QtGui.QAction(QtGui.QIcon('icons/forward.png'), "Forward", self)
		self.__forwardAct.setMenu(self.__forwardMenu)
		self.__forwardAct.triggered.connect(self.__history.forward)

		self.__browseToolBar = self.addToolBar("Browse")
		self.__browseToolBar.addAction(self.__backAct)
		self.__browseToolBar.addAction(self.__forwardAct)

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
		if isDoc:
			self.__history.push(struct.DocLink(guid, False))
		else:
			self.__history.push(struct.RevLink(guid))
		self.__view.open(guid, isDoc)

	def __openLink(self, link):
		if isinstance(link, struct.DocLink):
			self.__view.open(link.doc(), True)
		else:
			self.__view.open(link.rev(), False)

	def __extractMetaData(self):
		caption = self.__view.metaDataGetField(DocumentView.HPA_TITLE, "Unnamed")
		self.__history.currentItem().setText(caption)
		self.setWindowTitle(caption)

	def __itemOpen(self, link):
		link.update()
		revs = link.revs()
		if len(revs) == 0:
			return
		try:
			type = Connector().stat(revs[0]).type()
			if type in ["org.hotchpotch.dict", "org.hotchpotch.store", "org.hotchpotch.set"]:
				self.__history.push(link)
				self.__openLink(link)
			else:
				utils.showDocument(link)
		except IOError:
			pass

	def __updateBrowseActions(self):
		self.__backAct.setEnabled(self.__history.canGoBack())
		self.__forwardAct.setEnabled(self.__history.canGoForward())

	def __navigate(self):
		self.__openLink(self.__history.currentItem().link())
		self.__updateBrowseActions()

	def __showBackMenu(self):
		self.__backMenu.clear()
		for item in self.__history.backItems(10):
			action = self.__backMenu.addAction(item.text())
			action.triggered.connect(lambda item=item: self.__history.goToItem(item))

	def __showForwardMenu(self):
		self.__forwardMenu.clear()
		for item in self.__history.forwardItems(10):
			action = self.__forwardMenu.addAction(item.text())
			action.triggered.connect(lambda item=item: self.__history.goToItem(item))


class BreadcrumBar(QtGui.QWidget):

	def __init__(self, parent=None):
		super(BreadcrumBar, self).__init__()
		self.__history = None

	def setHistory(self, history):
		self.__history = history
		self.__history.navigated.connect(self.__navigated)
		self.__history.changed.connect(self.__changed)

	def __navigated(self):
		pass

	def __changed(self):
		pass


if __name__ == '__main__':

	import sys

	app = QtGui.QApplication(sys.argv)
	mainWin = BrowserWindow()
	mainWin.open(sys.argv)
	mainWin.show()
	sys.exit(app.exec_())

