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

import sys, itertools
from PyQt4 import QtCore, QtGui

from peerdrive import struct
from peerdrive.connector import Connector, Watch
from peerdrive.gui.widgets import DocButton, RevButton

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
		self.setWindowTitle("PeerDrive launch box")
		self.setWindowIcon(QtGui.QIcon("icons/launch.png"))
		self.setWindowFlags(QtCore.Qt.Window
			| QtCore.Qt.WindowCloseButtonHint
			| QtCore.Qt.WindowMinimizeButtonHint)

		Connector().regProgressHandler(start=self.progressStart,
			stop=self.progressStop)

	def progressStart(self, tag, typ, src, dst, item=None):
		widget = ProgressWidget(tag, typ, src, dst, item)
		self.progressWidgets[tag] = widget
		self.progressLayout.addWidget(widget)

	def progressStop(self, tag):
		if tag in self.progressWidgets:
			widget = self.progressWidgets[tag]
			del self.progressWidgets[tag]
			widget.remove()


class SyncEditor(QtGui.QDialog):
	def __init__(self, parent=None):
		super(SyncEditor, self).__init__(parent)
		self.setAttribute(QtCore.Qt.WA_DeleteOnClose)

		self.__changed = False
		self.__lines = []
		self.__rules = SyncRules()

		self.__addChooser = QtGui.QComboBox()
		self.__addChooser.setSizePolicy(
			QtGui.QSizePolicy.MinimumExpanding,
			QtGui.QSizePolicy.Minimum)
		self.__addBtn = QtGui.QPushButton('Add')
		self.__addBtn.clicked.connect(self.__add)
		frame = QtGui.QFrame()
		frame.setFrameShape(QtGui.QFrame.HLine)

		addLayout = QtGui.QHBoxLayout()
		addLayout.addWidget(self.__addChooser)
		addLayout.addWidget(self.__addBtn)

		# Add rules
		self.__ruleLayout = QtGui.QVBoxLayout()
		rules = self.__rules.rules()
		# filter out 2nd rules in other direction
		rules = [(s,p) for (s,p) in rules if not (((p,s) in rules) and (p<=s))]
		for (store, peer) in rules:
			line = SyncRuleWidget(store, peer, self.__rules, self)
			line.removed.connect(self.__removed)
			self.__lines.append(line)
			self.__ruleLayout.addWidget(line)

		buttonBox = QtGui.QDialogButtonBox(QtGui.QDialogButtonBox.Ok
			| QtGui.QDialogButtonBox.Cancel);
		buttonBox.accepted.connect(self.accept)
		buttonBox.rejected.connect(self.reject)

		mainLayout = QtGui.QVBoxLayout()
		mainLayout.addLayout(addLayout)
		mainLayout.addWidget(frame)
		mainLayout.addLayout(self.__ruleLayout)
		mainLayout.addStretch()
		mainLayout.addWidget(buttonBox)

		self.setLayout(mainLayout)
		self.setWindowTitle("Synchronization rules")
		self.__fillAddChooser()

	def accept(self):
		self.__rules.save()
		super(SyncEditor, self).accept()
		self.__cleanup()

	def reject(self):
		super(SyncEditor, self).reject()
		self.__cleanup()

	def __add(self):
		(store, peer) = self.__addItems[self.__addChooser.currentIndex()]
		self.__rules.setMode(store, peer, 'ff')
		self.__rules.setDescr(store, peer, "Sync '" +
			struct.readTitle(struct.DocLink(store, store, False), '?') +	"' and '" +
			struct.readTitle(struct.DocLink(peer, peer, False), '?') + "'")
		line = SyncRuleWidget(store, peer, self.__rules, self)
		line.removed.connect(self.__removed)
		self.__lines.append(line)
		self.__ruleLayout.addWidget(line)
		self.__fillAddChooser()

	def __fillAddChooser(self):
		self.__addChooser.clear()
		self.__addItems = []

		enum = Connector().enum()
		rules = self.__rules.rules()
		stores = [enum.doc(s) for s in enum.allStores()
			if not enum.isSystem(s) and enum.isMounted(s)]
		self.__addItems = [(s, p) for s in stores for p in stores
			if s < p and (s,p) not in rules and (p,s) not in rules ]

		self.__addBtn.setEnabled(bool(self.__addItems))
		for (store, peer) in self.__addItems:
			title = (struct.readTitle(struct.DocLink(store, store, False), '?') +
				' - ' + struct.readTitle(struct.DocLink(peer, peer, False), '?'))
			self.__addChooser.addItem(title)

	def __cleanup(self):
		for line in self.__lines:
			line.cleanup()
		self.__lines = []

	def __removed(self, line):
		self.__lines.remove(line)
		self.__fillAddChooser()

class SyncRuleWidget(QtGui.QWidget):
	MODES = [("ff", None), ("merge", None), ("merge", "merge")]
	MAP = {
		("ff", None)       : "Forward",
		("merge", None)    : "One way sync",
		("merge", "merge") : "Full sync"
	}

	removed = QtCore.pyqtSignal(object)

	def __init__(self, store, peer, rules, parent):
		super(SyncRuleWidget, self).__init__(parent)

		self.__store = store
		self.__peer = peer
		self.__rules = rules

		self.__descrEdit = QtGui.QLineEdit()
		self.__descrEdit.textEdited.connect(self.__setRule)
		self.__storeBtn = DocButton(store, store, True)
		self.__peerBtn = DocButton(peer, peer, True)
		self.__modeSel = QtGui.QComboBox()
		self.__modeSel.addItems([SyncRuleWidget.MAP[m] for m in SyncRuleWidget.MODES])
		self.__modeSel.currentIndexChanged.connect(self.__setRule)
		self.__reverseBtn = QtGui.QPushButton('Reverse')
		self.__reverseBtn.clicked.connect(self.__reverse)
		self.__removeBtn = QtGui.QPushButton('Remove')
		self.__removeBtn.clicked.connect(self.__remove)

		layout = QtGui.QHBoxLayout()
		layout.addWidget(self.__descrEdit)
		layout.addWidget(self.__storeBtn)
		layout.addWidget(self.__modeSel)
		layout.addWidget(self.__peerBtn)
		layout.addWidget(self.__reverseBtn)
		layout.addWidget(self.__removeBtn)
		self.setLayout(layout)

		mode = (rules.mode(store, peer), rules.mode(peer, store))
		self.__modeSel.setCurrentIndex(SyncRuleWidget.MODES.index(mode))
		self.__descrEdit.setText(rules.descr(store, peer))

	def cleanup(self):
		self.__storeBtn.cleanup()
		self.__peerBtn.cleanup()

	def __setRule(self):
		descr = unicode(self.__descrEdit.text())
		(ltrMode, rtlMode) = SyncRuleWidget.MODES[self.__modeSel.currentIndex()]
		self.__rules.setMode(self.__store, self.__peer, ltrMode)
		self.__rules.setDescr(self.__store, self.__peer, descr)
		self.__rules.setMode(self.__peer, self.__store, rtlMode)
		if rtlMode:
			self.__rules.setDescr(self.__peer, self.__store, descr)

	def __reverse(self):
		store = self.__store
		self.__store = self.__peer
		self.__peer = store
		self.__storeBtn.setDocument(self.__store, self.__store)
		self.__peerBtn.setDocument(self.__peer, self.__peer)
		self.__setRule()

	def __remove(self):
		self.__rules.setMode(self.__store, self.__peer, None)
		self.__rules.setMode(self.__peer, self.__store, None)
		self.removed.emit(self)
		self.cleanup()
		self.deleteLater()

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
		self.storeBtn = DocButton(withText=True)
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
			self.storeBtn.setDocument(doc, doc)
			self.watch = StoreWidget.StoreWatch(doc, self.update)
			Connector().watch(self.watch)
			self.mounted = True
		else:
			self.mountBtn.setText("Mount")
			self.storeBtn.setText(enum.name(self.mountId))
			self.mounted = False

	def mountUnmount(self):
		try:
			if self.mounted:
				Connector().unmount(self.mountId)
			else:
				Connector().mount(self.mountId)
				self.update()
		except IOError as e:
			QtGui.QMessageBox.warning(self, 'Error', 'Mount opertaion failed: ' +
				str(e))


class ProgressWidget(QtGui.QFrame):
	def __init__(self, tag, typ, fromStore, toStore, item):
		super(ProgressWidget, self).__init__()
		self.tag = tag
		self.__type = typ
		self.__state = Connector().PROGRESS_RUNNING
		self.__store = fromStore

		self.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Sunken)

		self.__progressInd = QtGui.QLabel()
		self.__progressInd.setMargin(4)
		if typ == Connector().PROGRESS_SYNC:
			self.fromBtn = DocButton(fromStore, fromStore, True)
			self.__progressInd.setPixmap(QtGui.QPixmap("icons/progress-sync.png"))
		elif typ == Connector().PROGRESS_REP_DOC:
			self.fromBtn = DocButton(fromStore, item, True)
			self.__progressInd.setPixmap(QtGui.QPixmap("icons/progress-replicate.png"))
		elif typ == Connector().PROGRESS_REP_REV:
			self.fromBtn = RevButton(fromStore, item, True)
			self.__progressInd.setPixmap(QtGui.QPixmap("icons/progress-replicate.png"))
		self.toBtn = DocButton(toStore, toStore, True)
		self.progressBar = QtGui.QProgressBar()
		self.progressBar.setMaximum(255)
		self.__pauseBtn = QtGui.QToolButton()
		self.__pauseBtn.setToolButtonStyle(QtCore.Qt.ToolButtonIconOnly)
		self.__pauseBtn.setIcon(QtGui.QIcon("icons/progress-pause.png"))
		self.__pauseBtn.clicked.connect(self.__pause)
		self.__stopBtn = QtGui.QToolButton()
		self.__stopBtn.setToolButtonStyle(QtCore.Qt.ToolButtonIconOnly)
		self.__stopBtn.setIcon(QtGui.QIcon("icons/progress-stop.png"))
		self.__stopBtn.clicked.connect(self.__stop)
		self.__errorMsg = QtGui.QLabel()
		self.__errorMsg.setWordWrap(True)
		self.__errorMsg.hide()

		layout = QtGui.QHBoxLayout()
		layout.setMargin(0)
		layout.addWidget(self.__progressInd)
		layout.addWidget(self.fromBtn)
		layout.addWidget(self.progressBar)
		layout.addWidget(self.__errorMsg)
		layout.addWidget(self.toBtn)
		layout.addWidget(self.__pauseBtn)
		layout.addWidget(self.__stopBtn)
		self.setLayout(layout)

		Connector().regProgressHandler(progress=self.progress)

	def remove(self):
		Connector().unregProgressHandler(progress=self.progress)
		self.fromBtn.cleanup()
		self.toBtn.cleanup()
		self.deleteLater()

	def progress(self, tag, state, value, err_code=None, err_doc=None, err_rev=None):
		if self.tag != tag:
			return

		self.progressBar.setValue(value)
		if self.__state == state:
			return

		self.__state = state
		self.progressBar.setVisible(state != Connector().PROGRESS_ERROR)
		self.__errorMsg.setVisible(state == Connector().PROGRESS_ERROR)
		if state == Connector().PROGRESS_RUNNING:
			self.__pauseBtn.setIcon(QtGui.QIcon("icons/progress-pause.png"))
			if self.__type == Connector().PROGRESS_SYNC:
				self.__progressInd.setPixmap(QtGui.QPixmap("icons/progress-sync.png"))
			else:
				self.__progressInd.setPixmap(QtGui.QPixmap("icons/progress-replicate.png"))
		elif state == Connector().PROGRESS_PAUSED:
			self.__pauseBtn.setIcon(QtGui.QIcon("icons/progress-start.png"))
			self.__progressInd.setPixmap(QtGui.QPixmap("icons/progress-pause.png"))
		elif state == Connector().PROGRESS_ERROR:
			self.__pauseBtn.setIcon(QtGui.QIcon("icons/progress-retry.png"))
			self.__progressInd.setPixmap(QtGui.QPixmap("icons/progress-error.png"))
			self.__errorMsg.setText("Error '" + err_code[1] + "' while processing '" +
				struct.readTitle(struct.RevLink(self.__store, err_rev), 'unknown document') + "'!")

	def __pause(self):
		if self.__state == Connector().PROGRESS_RUNNING:
			Connector().progressPause(self.tag)
		else:
			Connector().progressResume(self.tag)

	def __stop(self):
		Connector().progressStop(self.tag)


class SyncRules(object):
	def __init__(self):
		self.sysStore = Connector().enum().sysStore()
		self.syncDoc = struct.Folder(struct.DocLink(self.sysStore, self.sysStore))["syncrules"].doc()
		self.syncRev = Connector().lookupDoc(self.syncDoc).rev(self.sysStore)
		with Connector().peek(self.sysStore, self.syncRev) as r:
			rules = struct.loads(self.sysStore, r.readAll('PDSD'))

		self.__changed = False
		self.__rules = {}
		for rule in rules:
			self.__rules[(rule['from'].decode('hex'), rule['to'].decode('hex'))] = rule

	def save(self):
		if not self.__changed:
			return

		rules = [ rule for rule in self.__rules.values() ]
		with Connector().update(self.sysStore, self.syncDoc, self.syncRev) as w:
			w.writeAll('PDSD', struct.dumps(rules))
			w.commit()
			self.rev = w.getRev()

	def rules(self):
		return self.__rules.keys()

	def mode(self, store, peer):
		key = (store, peer)
		if key in self.__rules:
			return self.__rules[key]['mode']
		return None

	def setMode(self, store, peer, mode):
		key = (store, peer)
		if key in self.__rules:
			if mode:
				self.__rules[key]['mode'] = mode
			else:
				del self.__rules[(store, peer)]
		elif mode:
			rule = {}
			rule["from"] = store.encode('hex')
			rule["to"]   = peer.encode('hex')
			rule["mode"] = mode
			self.__rules[key] = rule
		self.__changed = True

	def descr(self, store, peer):
		return self.__rules[(store, peer)].get('descr', '')

	def setDescr(self, store, peer, descr):
		self.__rules[(store, peer)]['descr'] = descr
		self.__changed = True


app = QtGui.QApplication(sys.argv)
dialog = Launchbox()
sys.exit(dialog.exec_())

