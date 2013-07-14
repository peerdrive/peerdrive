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

from PyQt4 import QtCore, QtGui
from peerdrive.gui import main, widgets


class ImageWidget(widgets.DocumentView):

	def __init__(self):
		super(ImageWidget, self).__init__("org.peerdrive.imageview")

		self.__scale = None
		self.__manualScale = 1.0

		self.imageLabel = QtGui.QLabel()
		self.imageLabel.setBackgroundRole(QtGui.QPalette.Base)
		self.imageLabel.setSizePolicy(QtGui.QSizePolicy.Ignored, QtGui.QSizePolicy.Ignored)
		self.imageLabel.setScaledContents(True)
		self.scrollArea = QtGui.QScrollArea()
		self.scrollArea.setBackgroundRole(QtGui.QPalette.Dark)
		self.scrollArea.setWidget(self.imageLabel)
		self.setCentralWidget(self.scrollArea)

	def docRead(self, readWrite, r):
		data = r.readAll('_')
		image = QtGui.QImage()
		image.loadFromData(QtCore.QByteArray(data))
		if image.isNull():
			return
		self.imageLabel.setPixmap(QtGui.QPixmap.fromImage(image))
		self.__adjustSize()

	def zoomIn(self):
		self.__scaleImage(1.25)

	def zoomOut(self):
		self.__scaleImage(0.8)

	def zoomNormal(self):
		self.__scale = 1.0
		self.__manualScale = 1.0

	def fitToWindow(self, enable):
		if enable:
			self.__scale = None
		else:
			self.__scale = self.__manualScale
		self.__adjustSize()

	def currentScale(self):
		return self.__scale

	def __scaleImage(self, factor):
		if self.__scale is None:
			return
		self.__scale = self.__scale * factor
		self.__manualScale = self.__manualScale
		self.__adjustSize()
		self.__adjustScrollBar(self.scrollArea.horizontalScrollBar(), factor)
		self.__adjustScrollBar(self.scrollArea.verticalScrollBar(), factor)

	def __adjustScrollBar(self, scrollBar, factor):
		scrollBar.setValue(int(factor * scrollBar.value()
			+ ((factor - 1) * scrollBar.pageStep()/2)))

	def __adjustSize(self):
		if self.__scale is None:
			hScale = float(self.size().width()-3) / self.imageLabel.pixmap().size().width()
			vScale = float(self.size().height()-3) / self.imageLabel.pixmap().size().height()
			if hScale < vScale:
				factor = hScale
			else:
				factor = vScale
		else:
			factor = self.__scale
		self.imageLabel.resize(factor * self.imageLabel.pixmap().size())

	def resizeEvent(self, event):
		if (self.__scale is None) and (self.imageLabel.pixmap() is not None):
			self.__adjustSize()
		super(ImageWidget, self).resizeEvent(event)


class ImageWindow(main.MainWindow):

	def __init__(self):
		super(ImageWindow, self).__init__(ImageWidget(), False)

		self.createActions()
		self.createMenus()
		self.createToolBars()
		self.updateActions()

	def createActions(self):
		self.zoomInAct = QtGui.QAction(QtGui.QIcon('icons/viewmag+.png'), "Zoom &In (25%)", self)
		self.zoomInAct.setShortcut("Ctrl++")
		self.zoomInAct.triggered.connect(self.zoomIn)

		self.zoomOutAct = QtGui.QAction(QtGui.QIcon('icons/viewmag-.png'), "Zoom &Out (25%)", self)
		self.zoomOutAct.setShortcut("Ctrl+-")
		self.zoomOutAct.triggered.connect(self.zoomOut)

		self.normalSizeAct = QtGui.QAction(QtGui.QIcon('icons/viewmag1.png'), "&Normal Size", self)
		self.normalSizeAct.setShortcut("Ctrl+S")
		self.normalSizeAct.triggered.connect(self.normalSize)

		self.fitToWindowAct = QtGui.QAction(QtGui.QIcon('icons/viewmagfit.png'), "&Fit to Window", self)
		self.fitToWindowAct.setCheckable(True)
		self.fitToWindowAct.setShortcut("Ctrl+F")
		self.fitToWindowAct.triggered.connect(self.fitToWindow)

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

	def updateActions(self):
		scale = self.viewWidget().currentScale()
		fit = scale is None
		if fit:
			self.zoomInAct.setEnabled(False)
			self.zoomOutAct.setEnabled(False)
		else:
			self.zoomInAct.setEnabled(scale < 16.0)
			self.zoomOutAct.setEnabled(scale > 0.03)
		self.normalSizeAct.setEnabled(not fit)
		if fit != self.fitToWindowAct.isChecked():
			self.fitToWindowAct.setChecked(fit)

	def zoomIn(self):
		self.viewWidget().zoomIn()
		self.updateActions()

	def zoomOut(self):
		self.viewWidget().zoomOut()
		self.updateActions()

	def normalSize(self):
		self.viewWidget().zoomNormal()
		self.updateActions()

	def fitToWindow(self, enabled):
		self.viewWidget().fitToWindow(enabled)
		self.updateActions()


if __name__ == '__main__':

	import sys

	app = QtGui.QApplication(sys.argv)
	mainWin = ImageWindow()
	mainWin.open(sys.argv)
	mainWin.show()
	sys.exit(app.exec_())

