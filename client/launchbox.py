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

import sys
from PyQt4 import QtCore, QtGui
from hotchpotch import Connector
from hotchpotch.gui import DocButton, RevButton
from hotchpotch.connector import Watch

PROGRESS_SYNC = 0
PROGRESS_REP_DOC = 1
PROGRESS_REP_REV = 2

class Launchbox(QtGui.QDialog):
	def __init__(self, parent=None):
		super(Launchbox, self).__init__(parent)

		self.setSizePolicy(
			QtGui.QSizePolicy.Preferred,
			QtGui.QSizePolicy.Minimum )
		self.progressWidgets = {}
		self.progressContainer = QtGui.QWidget()

		self.progressLayout = QtGui.QVBoxLayout()
		self.progressLayout.setMargin(0)
		self.progressContainer.setLayout(self.progressLayout)

		self.mainLayout = QtGui.QVBoxLayout()
		self.mainLayout.setSizeConstraint(QtGui.QLayout.SetMinimumSize)
		enum = Connector().enum()
		for store in enum.allStores():
			self.mainLayout.addWidget(StoreWidget(store))
		hLine = QtGui.QFrame()
		hLine.setFrameStyle(QtGui.QFrame.HLine | QtGui.QFrame.Raised)
		self.mainLayout.addWidget(hLine)
		self.mainLayout.addWidget(self.progressContainer)
		self.mainLayout.addStretch()

		self.setLayout(self.mainLayout)
		self.setWindowTitle("Hotchpotch launch box")
		self.setWindowIcon(QtGui.QIcon("icons/launch.png"))
		#self.setWindowFlags(QtCore.Qt.Dialog | QtCore.Qt.WindowCloseButtonHint)

		Connector().regProgressHandler(self.progress)

	def progress(self, typ, value, tag):
		if value == 0:
			if typ == PROGRESS_SYNC:
				widget = SyncWidget(tag)
			else:
				widget = ReplicationWidget(typ, tag)
			self.progressWidgets[tag] = widget
			self.progressLayout.addWidget(widget)
		elif value == 0xffff:
			widget = self.progressWidgets[tag]
			del self.progressWidgets[tag]
			widget.remove()
			#self.adjustSize()

class StoreWidget(QtGui.QWidget):
	class StoreWatch(Watch):
		def __init__(self, doc, callback):
			self.__callback = callback
			super(StoreWidget.StoreWatch, self).__init__(Watch.TYPE_DOC, doc)
		
		def triggered(self, cause):
			if cause == Watch.EVENT_DISAPPEARED:
				self.__callback()

	def __init__(self, mountId, parent=None):
		super(StoreWidget, self).__init__(parent)
		self.mountId = mountId
		self.watch = None

		self.mountBtn = QtGui.QPushButton("")
		self.storeBtn = DocButton(None, True)
		QtCore.QObject.connect(self.mountBtn, QtCore.SIGNAL("clicked()"), self.mountUnmount)

		layout = QtGui.QHBoxLayout()
		layout.setMargin(0)
		layout.addWidget(self.storeBtn.getWidget())
		layout.addStretch()
		layout.addWidget(self.mountBtn)
		self.setLayout(layout)

		self.update()

	def update(self):
		if self.watch:
			Connector().unwatch(self.watch)
			self.watch = None

		enum = Connector().enum()
		self.mountBtn.setEnabled(enum.isRemovable(self.mountId))
		if enum.isMounted(self.mountId):
			doc = enum.doc(self.mountId)
			self.mountBtn.setText("Unmount")
			self.storeBtn.setDocument(doc)
			self.watch = StoreWidget.StoreWatch(doc, self.update)
			Connector().watch(self.watch)
			self.mounted = True
		else:
			self.mountBtn.setText("Mount")
			self.storeBtn.getWidget().setText(enum.name(self.mountId))
			self.mounted = False

	def mountUnmount(self):
		if self.mounted:
			Connector().unmount(self.mountId)
		else:
			Connector().mount(self.mountId)
			self.update()


class SyncWidget(QtGui.QFrame):
	def __init__(self, tag, parent=None):
		super(SyncWidget, self).__init__(parent)
		self.tag = tag
		fromUuid = tag[0:16]
		toUuid = tag[16:32]

		self.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Sunken)

		self.fromBtn = DocButton(fromUuid, True)
		self.toBtn = DocButton(toUuid, True)
		self.progressBar = QtGui.QProgressBar()
		self.progressBar.setMaximum(256)

		layout = QtGui.QHBoxLayout()
		layout.setMargin(0)
		layout.addWidget(self.fromBtn.getWidget())
		layout.addWidget(self.progressBar)
		layout.addWidget(self.toBtn.getWidget())
		self.setLayout(layout)

		Connector().regProgressHandler(self.progress)

	def remove(self):
		Connector().unregProgressHandler(self.progress)
		self.fromBtn.cleanup()
		self.toBtn.cleanup()
		self.deleteLater()

	def progress(self, typ, value, tag):
		if self.tag == tag:
			self.progressBar.setValue(value)


class ReplicationWidget(QtGui.QFrame):
	def __init__(self, typ, tag, parent=None):
		super(ReplicationWidget, self).__init__(parent)
		self.tag = tag
		uuid = tag[0:16]
		stores = tag[16:]
		self.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Sunken)

		if typ == PROGRESS_REP_DOC:
			self.docBtn = DocButton(uuid, True)
		else:
			self.docBtn = RevButton(uuid, True)
		self.progressBar = QtGui.QProgressBar()
		self.progressBar.setMaximum(256)

		self.storeButtons = []
		layout = QtGui.QHBoxLayout()
		layout.setMargin(0)
		layout.addWidget(self.docBtn.getWidget())
		layout.addWidget(self.progressBar)
		while stores:
			store = stores[0:16]
			button = DocButton(store)
			self.storeButtons.append(button)
			layout.addWidget(button.getWidget())
			stores = stores[16:]
		self.setLayout(layout)

		Connector().regProgressHandler(self.progress)

	def remove(self):
		Connector().unregProgressHandler(self.progress)
		self.docBtn.cleanup()
		for button in self.storeButtons:
			button.cleanup()
		self.deleteLater()

	def progress(self, typ, value, tag):
		if self.tag == tag:
			self.progressBar.setValue(value)


app = QtGui.QApplication(sys.argv)
dialog = Launchbox()
sys.exit(dialog.exec_())

