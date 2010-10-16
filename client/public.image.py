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
from hotchpotch import hpgui


class ImageWindow(hpgui.HpMainWindow):

	def __init__(self, argv):
		super(ImageWindow, self).__init__(argv,
			"org.hotchpotch.xv",
			["public.image"],
			False)
		self.imageLabel = QtGui.QLabel()
		self.imageLabel.setBackgroundRole(QtGui.QPalette.Base)
		self.imageLabel.setSizePolicy(QtGui.QSizePolicy.Ignored, QtGui.QSizePolicy.Ignored)
		self.imageLabel.setScaledContents(True)

		self.scrollArea = QtGui.QScrollArea()
		self.scrollArea.setBackgroundRole(QtGui.QPalette.Dark)
		self.scrollArea.setWidget(self.imageLabel)
		self.setCentralWidget(self.scrollArea)

		self.createActions()
		self.createMenus()
		self.createToolBars()

	def createActions(self):
		self.zoomInAct = QtGui.QAction(QtGui.QIcon('icons/viewmag+.png'), "Zoom &In (25%)", self)
		self.zoomInAct.setShortcut("Ctrl++")
		QtCore.QObject.connect(self.zoomInAct, QtCore.SIGNAL("triggered()"), self.zoomIn)

		self.zoomOutAct = QtGui.QAction(QtGui.QIcon('icons/viewmag-.png'), "Zoom &Out (25%)", self)
		self.zoomOutAct.setShortcut("Ctrl+-")
		QtCore.QObject.connect(self.zoomOutAct, QtCore.SIGNAL("triggered()"), self.zoomOut)

		self.normalSizeAct = QtGui.QAction(QtGui.QIcon('icons/viewmag1.png'), "&Normal Size", self)
		self.normalSizeAct.setShortcut("Ctrl+S")
		QtCore.QObject.connect(self.normalSizeAct, QtCore.SIGNAL("triggered()"), self.normalSize)

		self.fitToWindowAct = QtGui.QAction(QtGui.QIcon('icons/viewmagfit.png'), "&Fit to Window", self)
		self.fitToWindowAct.setCheckable(True)
		self.fitToWindowAct.setShortcut("Ctrl+F")
		QtCore.QObject.connect(self.fitToWindowAct, QtCore.SIGNAL("triggered(bool)"), self.fitToWindow)

	def createMenus(self):
		self.viewMenu = QtGui.QMenu("&View", self)
		self.viewMenu.addAction(self.zoomInAct)
		self.viewMenu.addAction(self.zoomOutAct)
		self.viewMenu.addAction(self.normalSizeAct)
		self.viewMenu.addSeparator()
		self.viewMenu.addAction(self.fitToWindowAct)
		self.menuBar().addMenu(self.viewMenu)

	def createToolBars(self):
		self.viewToolBar = self.addToolBar("Mail")
		self.viewToolBar.addAction(self.zoomInAct)
		self.viewToolBar.addAction(self.zoomOutAct)
		self.viewToolBar.addAction(self.normalSizeAct)
		self.viewToolBar.addAction(self.fitToWindowAct)

	def docRead(self, readWrite, r):
		data = r.readAll('FILE')
		image = QtGui.QImage()
		image.loadFromData(QtCore.QByteArray(data))
		if image.isNull():
			return
		self.imageLabel.setPixmap(QtGui.QPixmap.fromImage(image))
		self.scaleFactor = 1.0
		self.oldFactor = 1.0
		self.imageLabel.adjustSize()
		self.updateActions()

	def zoomIn(self):
		self.scaleImage(1.25)

	def zoomOut(self):
		self.scaleImage(0.8)

	def normalSize(self):
		self.imageLabel.adjustSize()
		self.scaleFactor = 1.0
		self.oldFactor = 1.0

	def fitToWindow(self):
		fitToWindow = self.fitToWindowAct.isChecked()
		self.scrollArea.setWidgetResizable(fitToWindow)
		if not fitToWindow:
			self.scaleFactor = self.oldFactor
			self.scaleImage(1.0)
		self.updateActions()

	def updateActions(self):
		self.zoomInAct.setEnabled(not self.fitToWindowAct.isChecked())
		self.zoomOutAct.setEnabled(not self.fitToWindowAct.isChecked())
		self.normalSizeAct.setEnabled(not self.fitToWindowAct.isChecked())

	def scaleImage(self, factor):
		self.scaleFactor *= factor
		self.oldFactor = self.scaleFactor
		self.imageLabel.resize(self.scaleFactor * self.imageLabel.pixmap().size())

		self.adjustScrollBar(self.scrollArea.horizontalScrollBar(), factor)
		self.adjustScrollBar(self.scrollArea.verticalScrollBar(), factor)

		self.zoomInAct.setEnabled(self.scaleFactor < 3.0)
		self.zoomOutAct.setEnabled(self.scaleFactor > 0.333)

	def adjustScrollBar(self, scrollBar, factor):
		scrollBar.setValue(int(factor * scrollBar.value()
			+ ((factor - 1) * scrollBar.pageStep()/2)))


if __name__ == '__main__':

	import sys

	app = QtGui.QApplication(sys.argv)
	mainWin = ImageWindow(sys.argv)
	mainWin.show()
	sys.exit(app.exec_())

