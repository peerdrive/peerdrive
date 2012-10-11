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

from PyQt4 import QtGui, QtCore

from peerdrive import struct, Registry, Connector
from peerdrive.gui import main, utils
from peerdrive.gui.widgets import DocButton, RevButton

from views.folder import FolderWidget, FolderModel

class FolderWindow(main.MainWindow):

	def __init__(self):
		widget = FolderWidget()
		super(FolderWindow, self).__init__(widget, True)

		self.__colMenu = self.menuBar().addMenu("Columns")
		self.__colMenu.aboutToShow.connect(self.__columnsShow)

		self.__cleanAct = QtGui.QAction("&Autoclean", self)
		self.__cleanAct.setStatusTip("Toggle auto cleaning")
		self.__cleanAct.setCheckable(True)
		self.__cleanAct.setChecked(False)
		self.__cleanAct.triggered.connect(self.__toggleAutoClean)

		before = self.fileMenu.actions()[-2]
		separator = QtGui.QAction(self.fileMenu)
		separator.setSeparator(True)
		self.fileMenu.insertAction(before, separator)
		self.fileMenu.insertAction(before, self.__cleanAct)

		widget.revChanged.connect(self.__revChanged)
		widget.mutable.connect(self.__setMutable)
		widget.itemOpen.connect(self.__itemOpen)

	def __columnsShow(self):
		self.__colMenu.clear()
		reg = Registry()
		model = self.viewWidget().model()
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
		model = self.viewWidget().model()
		visible = key in columns
		action = subMenu.addAction(display)
		action.setCheckable(True)
		action.setChecked(visible)
		if visible:
			action.triggered.connect(lambda en, col=key: model.remColumn(col))
		else:
			action.triggered.connect(lambda en, col=key: model.addColumn(col))

	def __toggleAutoClean(self, checked):
		self.viewWidget().metaDataSetField(FolderModel.AUTOCLEAN, checked)

	def __revChanged(self):
		self.__cleanAct.setChecked(self.viewWidget().metaDataGetField(
			FolderModel.AUTOCLEAN, False))

	def __setMutable(self, mutable):
		self.__cleanAct.setEnabled(mutable)

	def __itemOpen(self, link, executable):
		store = self.viewWidget().store()
		if self.viewWidget().doc():
			ref = struct.DocLink(store, self.viewWidget().doc(), autoUpdate=False)
		elif self.viewWidget().rev():
			ref = struct.RevLink(store, self.viewWidget().rev())
		else:
			ref = None
		utils.showDocument(link, executable=executable, referrer=ref)


if __name__ == '__main__':

	import sys

	app = QtGui.QApplication(sys.argv)
	mainWin = FolderWindow()
	mainWin.open(sys.argv)
	mainWin.show()
	sys.exit(app.exec_())

