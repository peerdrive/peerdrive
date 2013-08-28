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

import sys, itertools, os, os.path
from PyQt4 import QtCore, QtGui

from peerdrive import struct, Registry
from peerdrive.connector import Connector, Watch, DocLink, RevLink
from peerdrive.gui.widgets import DocButton, RevButton
from peerdrive.gui.utils import showDocument, showProperties

class Launchbox(QtGui.QDialog):

	class RootWatch(Watch):
		def __init__(self, trayIcon):
			self.__trayIcon = trayIcon
			enum = Connector().enum()
			self.__mounted = set([s.label for s in enum.regularStores()])
			Watch.__init__(self, Watch.TYPE_DOC, Watch.ROOT_DOC)

		def triggered(self, cause, store):
			enum = Connector().enum()
			mounted = set([s.label for s in enum.regularStores()])
			for s in (self.__mounted - mounted):
				self.__trayIcon.showMessage("Unmount", "Store '"+s+"' has been unmounted")
			for s in (mounted - self.__mounted):
				self.__trayIcon.showMessage("Mount", "Store '"+s+"' has been mounted")
			self.__mounted = mounted

	def __init__(self, parent=None):
		super(Launchbox, self).__init__(parent)

		self.setSizePolicy(QtGui.QSizePolicy.Preferred, QtGui.QSizePolicy.Minimum)
		self.progressWidgets = {}
		self.__idleMessage = QtGui.QLabel("PeerDrive is idle...")
		self.__idleMessage.setAlignment(QtCore.Qt.AlignCenter)

		self.progressLayout = QtGui.QVBoxLayout()
		self.progressLayout.setMargin(0)
		self.progressLayout.setSizeConstraint(QtGui.QLayout.SetMinimumSize)
		self.progressLayout.addWidget(self.__idleMessage)
		self.setLayout(self.progressLayout)
		self.setWindowTitle("PeerDrive status")
		self.setWindowIcon(QtGui.QIcon("icons/peerdrive.png"))
		self.setWindowFlags(QtCore.Qt.Window
			| QtCore.Qt.WindowCloseButtonHint
			| QtCore.Qt.WindowMinimizeButtonHint)

		self.__syncRulesAction = QtGui.QAction("Manage synchronization...", self)
		self.__syncRulesAction.triggered.connect(lambda: SyncEditor().exec_())
		self.__quitAction = QtGui.QAction("Quit", self)
		self.__quitAction.triggered.connect(QtCore.QCoreApplication.instance().quit)
		self.__restoreAction = QtGui.QAction("Show status", self)
		self.__restoreAction.triggered.connect(lambda: self.setVisible(True))

		self.__trayIconMenu = QtGui.QMenu(self)
		self.__trayIconMenu.aboutToShow.connect(self.__trayMenuShow)

		self.__trayIcon = QtGui.QSystemTrayIcon(self)
		self.__trayIcon.setContextMenu(self.__trayIconMenu)
		self.__trayIcon.setIcon(QtGui.QIcon("icons/peerdrive.png"))
		self.__trayIcon.setToolTip("PeerDrive")
		self.__trayIcon.activated.connect(self.__trayActivated)
		self.__trayIcon.show()

		self.__fstab = struct.FSTab()

		self.__watch = Launchbox.RootWatch(self.__trayIcon)
		Connector().watch(self.__watch)

		Connector().regProgressHandler(start=self.__progressStart,
			stop=self.__progressStop)

	def __trayMenuShow(self):
		self.__trayIconMenu.clear()
		try:
			stores = Connector().enum().regularStores()
			for store in stores:
				try:
					self.__addStoreMenu(store, True)
				except IOError:
					pass

			unmounted = (set(self.__fstab.knownLabels()) -
				set([ s.label for s in stores ]))
			for label in unmounted:
				action = self.__trayIconMenu.addAction("Mount "+label)
				action.triggered.connect(lambda x,l=label: self.__mount(l))
		except IOError:
			pass
		self.__trayIconMenu.addSeparator()
		self.__trayIconMenu.addAction(self.__syncRulesAction)
		self.__trayIconMenu.addAction(self.__restoreAction)
		self.__trayIconMenu.addSeparator()
		self.__trayIconMenu.addAction(self.__quitAction)

	def __trayActivated(self, reason):
		if reason == QtGui.QSystemTrayIcon.Trigger:
			self.setVisible(not self.isVisible())

	def __addStoreMenu(self, store, removable):
		l = DocLink(store.sid, store.sid)
		type = Connector().stat(l.rev(), [store.sid]).type()
		executables = Registry().getExecutables(type)
		title = struct.readTitle(l)
		if len(title) > 20:
			title = title[:20] + '...'
		title += ' ['+store.label+']'

		menu = self.__trayIconMenu.addMenu(QtGui.QIcon("icons/uti/store.png"), title)
		if removable:
			menu.aboutToShow.connect(lambda m=menu, l=l, s=store: self.__fillMenu(m, l, s))
		else:
			menu.aboutToShow.connect(lambda m=menu, l=l: self.__fillMenu(m, l))

	def __fillMenu(self, menu, menuLink, store=None):
		menu.clear()
		c = struct.Folder(menuLink)
		listing = []
		for (title, link) in c.items():
			link.update()
			try:
				type = Connector().stat(link.rev()).type()
			except IOError:
				type = None

			if not type:
				continue

			if len(title) > 40:
				title = title[:40] + '...'

			listing.append((title, link, Registry().conformes(type,
				"org.peerdrive.folder"), QtGui.QIcon(Registry().getIcon(type))))

		listing = sorted(listing, cmp=Launchbox.__cmp)

		for (title, link, folder, icon) in listing:
			if folder:
				m = menu.addMenu(icon, title)
				m.aboutToShow.connect(lambda m=m, l=link: self.__fillMenu(m, l))
			else:
				a = menu.addAction(icon, title)
				a.triggered.connect(lambda x,l=link,r=menuLink: showDocument(l, referrer=r))

		menu.addSeparator()
		action = menu.addAction("Open")
		action.triggered.connect(lambda x,l=menuLink: showDocument(l))
		try:
			type = Connector().stat(menuLink.rev(), [menuLink.store()]).type()
			executables = Registry().getExecutables(type)
		except IOError:
			executables = []
		if len(executables) > 1:
			openMenu = menu.addMenu("Open with")
			for e in executables:
				action = openMenu.addAction(e)
				action.triggered.connect(lambda x,l=menuLink,e=e: showDocument(l, executable=e))
		menu.addSeparator()
		if store:
			action = menu.addAction(QtGui.QIcon("icons/unmount.png"), "Unmount")
			action.triggered.connect(lambda x,s=store: self.__unmount(s))
		action = menu.addAction("Properties")
		action.triggered.connect(lambda x,l=menuLink: showProperties(l))

	@staticmethod
	def __cmp((t1,l1,f1,i1), (t2,l2,f2,i2)):
		ret = f2 - f1
		if ret == 0:
			ret = cmp(t1.lower(), t2.lower())
		return ret

	def __mount(self, label):
		try:
			self.__fstab.mount(label)
		except IOError as e:
			QtGui.QMessageBox.warning(self, label, 'Mount opertaion failed: ' +
				str(e))

	def __unmount(self, store):
		try:
			Connector().unmount(store.sid)
		except IOError as e:
			QtGui.QMessageBox.warning(self, store.label, 'Unmount opertaion failed: ' +
				str(e))

	def __progressStart(self, tag, typ, src, dst, item=None):
		self.progressLayout.removeWidget(self.__idleMessage)
		self.__idleMessage.hide()
		widget = ProgressWidget(tag, typ, src, dst, item, self.__trayIcon)
		self.progressWidgets[tag] = widget
		self.progressLayout.addWidget(widget)

	def __progressStop(self, tag):
		if tag in self.progressWidgets:
			widget = self.progressWidgets[tag]
			del self.progressWidgets[tag]
			widget.remove()
			if self.progressWidgets == {}:
				self.progressLayout.addWidget(self.__idleMessage)
				self.__idleMessage.show()

	# reimplemented
	def setVisible(self, visible):
		self.__restoreAction.setEnabled(not visible)
		super(Launchbox, self).setVisible(visible)

	# reimplemented
	def closeEvent(self, event):
		self.hide()
		event.ignore()


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
			struct.readTitle(DocLink(store, store, False), '?') + "' and '" +
			struct.readTitle(DocLink(peer, peer, False), '?') + "'")
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
		stores = [s.sid for s in enum.regularStores()]
		self.__addItems = [(s, p) for s in stores for p in stores
			if s < p and (s,p) not in rules and (p,s) not in rules ]

		self.__addBtn.setEnabled(bool(self.__addItems))
		for (store, peer) in self.__addItems:
			fromStore = '[' + enum.fromSId(store).label + '] '
			fromStore += struct.readTitle(DocLink(store, store, False), '?')
			peerStore = '[' + enum.fromSId(peer).label + '] '
			peerStore += struct.readTitle(DocLink(peer, peer, False), '?')
			self.__addChooser.addItem(fromStore + ' - ' + peerStore)

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
		self.__storeBtn = DocButton(store, store, True)
		self.__peerBtn = DocButton(peer, peer, True)
		self.__modeSel = QtGui.QComboBox()
		self.__modeSel.addItems([SyncRuleWidget.MAP[m] for m in SyncRuleWidget.MODES])
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

		# connect signals as last step, otherwise loading the rule already triggers
		self.__descrEdit.textEdited.connect(self.__setRule)
		self.__modeSel.currentIndexChanged.connect(self.__setRule)

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

class ProgressWidget(QtGui.QFrame):
	def __init__(self, tag, typ, fromStore, toStore, item, trayIcon):
		super(ProgressWidget, self).__init__()
		self.tag = tag
		self.__type = typ
		self.__state = Connector().PROGRESS_RUNNING
		self.__fromStore = fromStore
		self.__toStore = toStore
		self.__trayIcon = trayIcon

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
		self.progressBar.setMinimum(0)
		self.progressBar.setMaximum(0)
		self.__pauseBtn = QtGui.QToolButton()
		self.__pauseBtn.setToolButtonStyle(QtCore.Qt.ToolButtonIconOnly)
		self.__pauseBtn.setIcon(QtGui.QIcon("icons/progress-pause.png"))
		self.__pauseBtn.setToolTip("Pause")
		self.__pauseBtn.clicked.connect(self.__pause)
		self.__stopBtn = QtGui.QToolButton()
		self.__stopBtn.setToolButtonStyle(QtCore.Qt.ToolButtonIconOnly)
		self.__stopBtn.setIcon(QtGui.QIcon("icons/progress-stop.png"))
		self.__stopBtn.setToolTip("Abort")
		self.__stopBtn.clicked.connect(self.__stop)
		self.__skipBtn = QtGui.QToolButton()
		self.__skipBtn.setToolButtonStyle(QtCore.Qt.ToolButtonIconOnly)
		self.__skipBtn.setIcon(QtGui.QIcon("icons/progress-skip.png"))
		self.__skipBtn.setToolTip("Skip")
		self.__skipBtn.clicked.connect(self.__skip)
		self.__skipBtn.hide()
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
		layout.addWidget(self.__skipBtn)
		if typ != Connector().PROGRESS_SYNC:
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

		if value > 0:
			self.progressBar.setMaximum(255)
		self.progressBar.setValue(value)
		if self.__state == state:
			return

		self.__state = state
		self.progressBar.setVisible(state != Connector().PROGRESS_ERROR)
		self.__errorMsg.setVisible(state == Connector().PROGRESS_ERROR)
		self.__skipBtn.setVisible(state == Connector().PROGRESS_ERROR)
		if state == Connector().PROGRESS_RUNNING:
			self.__pauseBtn.setIcon(QtGui.QIcon("icons/progress-pause.png"))
			self.__pauseBtn.setToolTip("Pause")
			if self.__type == Connector().PROGRESS_SYNC:
				self.__progressInd.setPixmap(QtGui.QPixmap("icons/progress-sync.png"))
			else:
				self.__progressInd.setPixmap(QtGui.QPixmap("icons/progress-replicate.png"))
		elif state == Connector().PROGRESS_PAUSED:
			self.__pauseBtn.setIcon(QtGui.QIcon("icons/progress-start.png"))
			self.__pauseBtn.setToolTip("Resume")
			self.__progressInd.setPixmap(QtGui.QPixmap("icons/progress-pause.png"))
		elif state == Connector().PROGRESS_ERROR:
			self.__pauseBtn.setIcon(QtGui.QIcon("icons/progress-retry.png"))
			self.__pauseBtn.setToolTip("Retry")
			self.__progressInd.setPixmap(QtGui.QPixmap("icons/progress-error.png"))
			doc = struct.readTitle(RevLink(self.__fromStore, err_rev),
				'unknown document')
			self.__errorMsg.setText("Error '" + err_code[1] + "' while processing '"
				+ doc + "'!")
			if self.__type == Connector().PROGRESS_SYNC:
				title = "Synchronization error"
				message = "synchronizing"
			else:
				title = "Replication error"
				message = "replicating"
			message = "Error '" + err_code[1] + "' while " + message + " '" + doc + \
				"' \nfrom '" + struct.readTitle(DocLink(self.__fromStore, self.__fromStore), "unknown store") + \
				"' to '" + struct.readTitle(DocLink(self.__toStore, self.__toStore), "unknown store") + \
				"'!"
			self.__trayIcon.showMessage(title, message, QtGui.QSystemTrayIcon.Warning)

	def __pause(self):
		if self.__state == Connector().PROGRESS_RUNNING:
			Connector().progressPause(self.tag)
		else:
			Connector().progressResume(self.tag)

	def __stop(self):
		Connector().progressStop(self.tag)

	def __skip(self):
		Connector().progressResume(self.tag, True)


class SyncRules(object):
	def __init__(self):
		self.sysStore = Connector().enum().sysStore().sid
		self.syncDoc = struct.Folder(DocLink(self.sysStore, self.sysStore))["syncrules"].doc()
		self.syncRev = Connector().lookupDoc(self.syncDoc).rev(self.sysStore)
		with Connector().peek(self.sysStore, self.syncRev) as r:
			rules = r.getData('/org.peerdrive.syncrules')

		self.__changed = False
		self.__rules = {}
		for rule in rules:
			self.__rules[(rule['from'].decode('hex'), rule['to'].decode('hex'))] = rule

	def save(self):
		if not self.__changed:
			return

		rules = [ rule for rule in self.__rules.values() ]
		with Connector().update(self.sysStore, self.syncDoc, self.syncRev) as w:
			w.setData('/org.peerdrive.syncrules', rules)
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

# change to our base directory
basePath = os.path.dirname(sys.argv[0])
if basePath:
	os.chdir(basePath)

app = QtGui.QApplication(sys.argv)
app.setQuitOnLastWindowClosed(False)
dialog = Launchbox()
sys.exit(app.exec_())

