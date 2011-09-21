#!/usr/bin/env python
# vim: set fileencoding=utf-8 :
#
# Hotchpotch
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

import sys, itertools, os, os.path, hashlib, stat
from PyQt4 import QtCore, QtGui, QtNetwork

from hotchpotch import struct, Registry, fuse
from hotchpotch.connector import Connector, Watch
from hotchpotch.gui.widgets import DocButton, RevButton

PROGRESS_SYNC = 0
PROGRESS_REP_DOC = 1
PROGRESS_REP_REV = 2

PIPE_NAME = "org.hotchpotch.oslauncher"

class Syncer(object):

	def __init__(self, basePath):
		self._basePath = basePath
		self._rev = None
		self._path = None

	def _updateFileName(self, guid):
		try:
			with Connector().peek(self._rev) as r:
				meta = struct.loads(r.readAll('META'))
		except IOError:
			meta = {}

		name = ''
		ext = ''
		if "org.hotchpotch.annotation" in meta:
			annotation = meta["org.hotchpotch.annotation"]
			if "title" in annotation:
				(name, ext) = os.path.splitext(annotation["title"])
		if name == '':
			if "origin" in annotation:
				name = os.path.splitext(annotation["origin"])[0]
		if name == '':
			name = guid.encode('hex')
		if ext == '':
			uti = Connector().stat(self._rev).type()
			extensions = Registry().search(uti, "extensions")
			if extensions:
				ext = extensions[0]
		if ext == '':
			if "origin" in annotation:
				ext  = os.path.splitext(annotation["origin"])[1]
		if ext == '':
			ext = '.bin'

		basePath = os.path.join(self._basePath, guid.encode('hex'))
		if not os.path.isdir(basePath):
			os.makedirs(basePath)
		newPath = os.path.join(basePath, name+ext)
		oldPath = self._path
		if oldPath != newPath:
			# try to rename the current file
			try:
				if oldPath and os.path.isfile(oldPath):
					os.rename(oldPath, newPath)
				self._path = newPath
			except OSError:
				# FIXME: inform user
				pass

	def _syncToFilesystem(self):
		try:
			if not os.access(self._path, os.W_OK):
				os.chmod(self._path, stat.S_IREAD | stat.S_IWRITE)
		except OSError:
			pass

		with open(self._path, "wb") as f:
			with Connector().peek(self._rev) as reader:
				data = reader.read('FILE', 0x10000)
				while data:
					f.write(data)
					data = reader.read('FILE', 0x10000)

	def _hashFile(self):
		try:
			sha = hashlib.sha1()
			with open(self._path, "rb") as f:
				data = f.read(0x10000)
				while data:
					sha.update(data)
					data = f.read(0x10000)
			return sha.digest()[:16]
		except IOError:
			return None

	def getPath(self):
		return self._path


class DocSyncer(QtCore.QObject, Syncer, Watch):

	fileNameChanged = QtCore.pyqtSignal(object)

	def __init__(self, doc, basePath, parent=None):
		QtCore.QObject.__init__(self, parent)
		Syncer.__init__(self, basePath)
		Watch.__init__(self, Watch.TYPE_DOC, doc)

		self.__doc = doc
		self.__metaHash = None
		self.__fileHash = None

		self.__timer = QtCore.QTimer(self)
		self.__timer.setInterval(3000)
		self.__timer.setSingleShot(True)
		self.__timer.timeout.connect(self.__syncToHotchpotch)

	def startSync(self):
		Connector().watch(self)
		try:
			self.__update()
		except:
			Connector().unwatch(self)
			raise

	def stopSync(self):
		Connector().unwatch(self)
		self.__timer.stop()

	def getDoc(self):
		return self.__doc

	def triggered(self, event):
		if event == Watch.EVENT_MODIFIED and not self.__timer.isActive():
			self.__update()

	def modified(self):
		self.__timer.start()

	def __update(self):
		revs = Connector().lookupDoc(self.__doc).revs()
		if self._rev in revs:
			return
		# FIXME: prompt if more than one version
		self._rev = revs[0]

		s = Connector().stat(self._rev)
		# FIXME: META and/or FILE part may be missing
		metaHash = s.hash('META')
		fileHash = s.hash('FILE')

		if metaHash != self.__metaHash:
			self.__metaHash = metaHash
			self._updateFileName()

		if fileHash != self.__fileHash:
			if self.__fileHash is None:
				if self._hashFile() != fileHash:
					self._syncToFilesystem()
			else:
				self._syncToFilesystem()
			self.__fileHash = fileHash

	def _updateFileName(self):
		oldPath = self._path
		Syncer._updateFileName(self, self.__doc)
		newPath = self._path
		if (newPath != oldPath) and (oldPath is not None):
			self.fileNameChanged.emit(oldPath)

	def __syncToHotchpotch(self):
		# will also be triggered by _syncToFilesystem
		# apply hash to check if really changed from outside
		newFileHash = self._hashFile()
		if newFileHash != self.__fileHash:
			with open(self._path, "rb") as f:
				with Connector().update(self.__doc, self._rev) as w:
					meta = struct.loads(w.readAll('META'))
					if not "org.hotchpotch.annotation" in meta:
						meta["org.hotchpotch.annotation"] = {}
					meta["org.hotchpotch.annotation"]["comment"] = "<<Changed by external app>>"
					w.writeAll('META', struct.dumps(meta))
					w.writeAll('FILE', f.read())
					w.commit()
					self._rev = w.getRev()
					self.__fileHash = newFileHash

	def _syncToFilesystem(self):
		self.__timer.stop()
		Syncer._syncToFilesystem(self)


class RevSyncer(Syncer):

	def __init__(self, rev, basePath):
		super(RevSyncer, self).__init__(basePath)
		self._rev = rev

	def sync(self):
		self._updateFileName(self._rev)
		s = Connector().stat(self._rev)
		fileHash = s.hash('FILE')
		if fileHash != self._hashFile():
			self._syncToFilesystem()
			os.chmod(self._path, stat.S_IREAD)


class SyncManager(QtCore.QObject):
	def __init__(self, basePath, parent=None):
		super(SyncManager, self).__init__(parent)
		self.__basePath = basePath
		self.__pathToSyncer = {}
		self.__docToSyncer = {}
		self.__revToPath = {}
		self.__watcher = QtCore.QFileSystemWatcher()
		self.__watcher.fileChanged.connect(self.__fileChanged)

	def getDocFile(self, doc):
		if doc in self.__docToSyncer:
			return self.__docToSyncer[doc].getPath()
		else:
			path = fuse.findFuseFile(struct.DocLink(doc, autoUpdate=False))
			if not path:
				syncer = DocSyncer(doc, self.__basePath)
				syncer.startSync()
				path = syncer.getPath()
				self.__docToSyncer[doc] = syncer
				self.__pathToSyncer[path] = syncer
				self.__watcher.addPath(path)
				syncer.fileNameChanged.connect(self.__pathChanged)
			return path

	def getRevFile(self, rev):
		if rev in self.__revToPath:
			return self.__revToPath[rev]
		else:
			path = fuse.findFuseFile(struct.RevLink(rev))
			if not path:
				syncer = RevSyncer(rev, self.__basePath)
				syncer.sync()
				path = syncer.getPath()
				self.__revToPath[rev] = path
			return path

	def quit(self):
		for syncer in self.__docToSyncer.values():
			syncer.stopSync()
			self.__watcher.removePath(syncer.getPath())
		self.__pathToSyncer = {}
		self.__docToSyncer = {}
		# Need to explicitly destroy QFileSystemWatcher before QCoreApplication,
		# otherwise we may deadlock on exit.
		del self.__watcher

	def __pathChanged(self, oldPath):
		syncer = self.__pathToSyncer[oldPath]
		newPath = syncer.getPath()
		del self.__pathToSyncer[oldPath]
		self.__watcher.removePath(oldPath)
		self.__pathToSyncer[newPath] = syncer
		self.__watcher.addPath(newPath)

	def __fileChanged(self, path):
		path = str(path) # is a QString
		# might have been renamed
		if path in self.__pathToSyncer:
			syncer = self.__pathToSyncer[path]
			if os.path.isfile(path):
				self.__watcher.removePath(path)
				syncer.modified()
				self.__watcher.addPath(path)
			else:
				syncer.stopSync()
				self.__watcher.removePath(path)
				del self.__pathToSyncer[path]
				del self.__docToSyncer[syncer.getDoc()]


class LauchServer(QtCore.QObject):

	def __init__(self, parent=None):
		super(LauchServer, self).__init__(parent)
		self.__clients = []
		self.__syncManager = SyncManager('.checkout')
		self.__server = QtNetwork.QLocalServer(self)
		self.__server.newConnection.connect(self.__serverConnect)
		if not self.__server.listen(PIPE_NAME):
			print "Unable to listen: ", self.__server.errorString()

	def quit(self):
		self.__syncManager.quit()
		self.__server.close()
		for client in self.__clients:
			client.close()

	def __serverConnect(self):
		socket = self.__server.nextPendingConnection()
		if socket:
			self.__clients.append(socket)
			socket.disconnected.connect(lambda s=socket: self.__serverDisconnect(s))
			socket.readyRead.connect(lambda s=socket: self.__serverReady(s))
			self.__serverReady(socket)

	def __serverDisconnect(self, socket):
		self.__clients.remove(socket)
		socket.setParent(None)
		socket.deleteLater()

	def __serverReady(self, socket):
		if socket.canReadLine():
			self.__process(str(socket.readLine())[:-1])

	def __process(self, request):
		if request.startswith('doc:'):
			doc = request[4:].decode("hex")
			path = self.__syncManager.getDocFile(doc)
		elif request.startswith('rev:'):
			rev = request[4:].decode("hex")
			path = self.__syncManager.getRevFile(rev)
		elif request == "kill":
			self.__finished()
			return
		else:
			return

		# start external program
		path = os.path.abspath(path)
		QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(path))


class Launchbox(QtGui.QDialog):
	def __init__(self, parent=None):
		super(Launchbox, self).__init__(parent)

		self.__server = LauchServer()

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
		self.setWindowFlags(QtCore.Qt.Window
			| QtCore.Qt.WindowCloseButtonHint
			| QtCore.Qt.WindowMinimizeButtonHint)

		Connector().regProgressHandler(start=self.progressStart,
			stop=self.progressStop)

	def progressStart(self, tag, typ, info):
		if typ == PROGRESS_SYNC:
			widget = SyncWidget(tag, info)
		else:
			widget = ReplicationWidget(tag, typ, info)
		self.progressWidgets[tag] = widget
		self.progressLayout.addWidget(widget)

	def progressStop(self, tag):
		widget = self.progressWidgets[tag]
		del self.progressWidgets[tag]
		widget.remove()

	def reject(self):
		super(Launchbox, self).reject()
		self.__server.quit()


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
		self.setAttribute(QtCore.Qt.WA_DeleteOnClose)

		self.__changed = False
		self.__buttons = []
		self.__rules = SyncRules()
		enum = Connector().enum()
		stores = zip(itertools.count(1), [enum.doc(s) for s in enum.allStores()
			if not enum.isSystem(s) and enum.isMounted(s)])

		mainLayout = QtGui.QVBoxLayout()

		layout = QtGui.QGridLayout()
		layout.addWidget(QtGui.QLabel("From \\ To"), 0, 0)
		for (pos, store) in stores:
			button = DocButton(store, store, True)
			self.__buttons.append(button)
			layout.addWidget(button, 0, pos)
			button = DocButton(store, store, True)
			self.__buttons.append(button)
			layout.addWidget(button, pos, 0)
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
		self.__cleanup()

	def reject(self):
		super(SyncEditor, self).reject()
		self.__cleanup()

	def __cleanup(self):
		for btn in self.__buttons:
			btn.cleanup()
		self.__buttons = []

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
		if self.mounted:
			Connector().unmount(self.mountId)
		else:
			Connector().mount(self.mountId)
			self.update()


class SyncWidget(QtGui.QFrame):
	def __init__(self, tag, info, parent=None):
		super(SyncWidget, self).__init__(parent)
		self.tag = tag
		fromStore = info[0:16]
		toStore = info[16:32]

		self.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Sunken)

		self.fromBtn = DocButton(fromStore, fromStore, True)
		self.toBtn = DocButton(toStore, toStore, True)
		self.progressBar = QtGui.QProgressBar()
		self.progressBar.setMaximum(255)

		layout = QtGui.QHBoxLayout()
		layout.setMargin(0)
		layout.addWidget(self.fromBtn)
		layout.addWidget(self.progressBar)
		layout.addWidget(self.toBtn)
		self.setLayout(layout)

		Connector().regProgressHandler(progress=self.progress)

	def remove(self):
		Connector().unregProgressHandler(progress=self.progress)
		self.fromBtn.cleanup()
		self.toBtn.cleanup()
		self.deleteLater()

	def progress(self, tag, value):
		if self.tag == tag:
			self.progressBar.setValue(value)


class ReplicationWidget(QtGui.QFrame):
	def __init__(self, tag, typ, info, parent=None):
		super(ReplicationWidget, self).__init__(parent)
		self.tag = tag
		uuid = info[0:16]
		store = info[16:32]
		self.setFrameStyle(QtGui.QFrame.StyledPanel | QtGui.QFrame.Sunken)

		if typ == PROGRESS_REP_DOC:
			self.docBtn = DocButton(store, uuid, True)
		else:
			self.docBtn = RevButton(store, uuid, True)
		self.progressBar = QtGui.QProgressBar()
		self.progressBar.setMaximum(255)

		self.storeButtons = []
		layout = QtGui.QHBoxLayout()
		layout.setMargin(0)
		layout.addWidget(self.docBtn)
		layout.addWidget(self.progressBar)
		button = DocButton(store, store)
		self.storeButtons.append(button)
		layout.addWidget(button)
		self.setLayout(layout)

		Connector().regProgressHandler(progress=self.progress)

	def remove(self):
		Connector().unregProgressHandler(progress=self.progress)
		self.docBtn.cleanup()
		for button in self.storeButtons:
			button.cleanup()
		self.deleteLater()

	def progress(self, tag, value):
		if self.tag == tag:
			self.progressBar.setValue(value)


class SyncRules(object):
	def __init__(self):
		self.sysStore = Connector().enum().sysStore()
		sysRev = Connector().lookupDoc(self.sysStore).rev(self.sysStore)
		with Connector().peek(self.sysStore, sysRev) as r:
			root = struct.loads(self.sysStore, r.readAll('HPSD'))
			self.syncDoc = root["syncrules"].doc()
		self.syncRev = Connector().lookupDoc(self.syncDoc).rev(self.sysStore)
		with Connector().peek(self.sysStore, self.syncRev) as r:
			self.rules = struct.loads(self.sysStore, r.readAll('HPSD'))

	def save(self):
		with Connector().update(self.sysStore, self.syncDoc, self.syncRev) as w:
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

