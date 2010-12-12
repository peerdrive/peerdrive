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

import sys, itertools
from PyQt4 import QtCore, QtGui

from hotchpotch import struct
from hotchpotch.connector import Connector, Watch
from hotchpotch.gui.widgets import DocButton, RevButton

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
			if not enum.isSystem(store):
				self.mainLayout.addWidget(StoreWidget(store))

		syncButton = QtGui.QPushButton("Synchronization")
		syncButton.clicked.connect(lambda: SyncEditor().exec_())
		setupLayout = QtGui.QHBoxLayout()
		setupLayout.addWidget(syncButton)
		setupLayout.addStretch()
		hLine = QtGui.QFrame()
		hLine.setFrameStyle(QtGui.QFrame.HLine | QtGui.QFrame.Raised)
		self.mainLayout.addWidget(hLine)
		self.mainLayout.addLayout(setupLayout)

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


class SyncEditor(QtGui.QDialog):
	MODES = [None, "ff", "latest", "merge"]
	MAP = {
		None     : "",
		"ff"     : "Fast-forward",
		"latest" : "Take latest revision",
		"merge"  : "3-way merge"
	}

	def __init__(self, parent=None):
		super(SyncEditor, self).__init__(parent)

		self.__changed = False
		self.__rules = SyncRules()
		enum = Connector().enum()
		stores = zip(itertools.count(1), [enum.doc(s) for s in enum.allStores()
			if not enum.isSystem(s) and enum.isMounted(s)])

		mainLayout = QtGui.QVBoxLayout()

		layout = QtGui.QGridLayout()
		for (pos, store) in stores:
			layout.addWidget(DocButton(store, True), 0, pos)
			layout.addWidget(DocButton(store, True), pos, 0)
		for (row, store) in stores:
			for (col, peer) in stores:
				if store==peer:
					continue
				box = QtGui.QComboBox()
				box.addItems([SyncEditor.MAP[m] for m in SyncEditor.MODES])
				box.setCurrentIndex(SyncEditor.MODES.index(self.__rules.mode(store, peer)))
				box.currentIndexChanged.connect(
					lambda i, store=store, peer=peer: self.__setRule(store, peer, i))
				layout.addWidget(box, row, col)

		mainLayout.addLayout(layout)

		buttonBox = QtGui.QDialogButtonBox(QtGui.QDialogButtonBox.Ok
			| QtGui.QDialogButtonBox.Cancel);
		buttonBox.accepted.connect(self.accept)
		buttonBox.rejected.connect(self.reject)
		mainLayout.addWidget(buttonBox)

		self.setLayout(mainLayout)
		self.setWindowTitle("Synchronization rules")

	def accept(self):
		if self.__changed:
			self.__rules.save()
		super(SyncEditor, self).accept()

	def __setRule(self, store, peer, index):
		mode = SyncEditor.MODES[index]
		self.__rules.setMode(store, peer, mode)
		self.__changed = True


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
		self.mountBtn.clicked.connect(self.mountUnmount)

		layout = QtGui.QHBoxLayout()
		layout.setMargin(0)
		layout.addWidget(self.storeBtn)
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
			self.storeBtn.setText(enum.name(self.mountId))
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
		layout.addWidget(self.fromBtn)
		layout.addWidget(self.progressBar)
		layout.addWidget(self.toBtn)
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
		layout.addWidget(self.docBtn)
		layout.addWidget(self.progressBar)
		while stores:
			store = stores[0:16]
			button = DocButton(store)
			self.storeButtons.append(button)
			layout.addWidget(button)
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


class SyncRules(object):
	def __init__(self):
		sysDoc = Connector().enum().sysStore()
		sysRev = Connector().lookup_doc(sysDoc).rev(sysDoc)
		with Connector().peek(sysRev) as r:
			root = struct.loads(r.readAll('HPSD'))
			self.syncDoc = root["syncrules"].doc()
		self.syncRev = Connector().lookup_doc(self.syncDoc).rev(sysDoc)
		with Connector().peek(self.syncRev) as r:
			self.rules = struct.loads(r.readAll('HPSD'))

	def save(self):
		with Connector().update(self.syncDoc, self.syncRev) as w:
			w.writeAll('HPSD', struct.dumps(self.rules))
			w.commit()
			self.rev = w.getRev()

	def mode(self, store, peer):
		for rule in self.rules:
			if (rule["from"].decode('hex') == store) and (rule["to"].decode('hex') == peer):
				return rule["mode"]
		return None

	def setMode(self, store, peer, mode):
		if mode:
			# try to update rule
			for rule in self.rules:
				if ((rule["from"].decode('hex') == store) and
					(rule["to"].decode('hex') == peer)):
					rule["mode"] = mode
					return

			# add new rule
			rule = {}
			rule["from"] = store.encode('hex')
			rule["to"]   = peer.encode('hex')
			rule["mode"] = mode
			self.rules.append(rule)
		else:
			# delete rule
			self.rules = [r for r in self.rules if (r["from"].decode('hex') != store)
				or (r["to"].decode('hex') != peer)]



app = QtGui.QApplication(sys.argv)
dialog = Launchbox()
sys.exit(dialog.exec_())

