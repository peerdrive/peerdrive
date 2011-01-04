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

from PyQt4 import QtCore, QtNetwork, QtGui
import sys, tempfile, os, os.path, subprocess, hashlib, stat

from hotchpotch import struct, Registry
from hotchpotch.connector import Connector, Watch

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
				f.write(reader.readAll('FILE'))

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
		revs = Connector().lookup_doc(self.__doc).revs()
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


class RequestManager(QtCore.QObject):

	finished = QtCore.pyqtSignal()

	def __init__(self, parent=None):
		super(RequestManager, self).__init__(parent)
		self.__server = None
		self.__clients = []
		self.__quitRequested = False

	def request(self, request):
		self.__request = request
		self.__socket = QtNetwork.QLocalSocket(self)
		self.__socket.connected.connect(self.__clientConnect)
		self.__socket.error.connect(self.__clientError)
		self.__socket.connectToServer(PIPE_NAME)
		return not self.__quitRequested

	def serve(self):
		self.__syncManager = SyncManager('.checkout')
		self.__server = QtNetwork.QLocalServer(self)
		self.__server.newConnection.connect(self.__serverConnect)
		if not self.__server.listen(PIPE_NAME):
			print "Unable to listen: ", self.__server.errorString()
			self.__finished()
		return not self.__quitRequested

	def __finished(self):
		self.__quitRequested = True
		if self.__server:
			self.__syncManager.quit()
			self.__server.close()
		for client in self.__clients:
			client.close()
		self.finished.emit()

	def __clientConnect(self):
		self.__socket.write(self.__request + '\n')
		self.__socket.disconnected.connect(self.__finished)
		self.__socket.disconnectFromServer()

	def __clientError(self, error):
		if self.__request == "kill":
			print "Could not kill server:", error
			self.__finished()
		elif error == QtNetwork.QLocalSocket.ServerNotFoundError:
			# dispose client socket
			self.__socket.setParent(None)
			del self.__socket

			# start server socket
			self.serve()

			# process local request in any case
			self.__process(self.__request)
		else:
			print "Unexpected QLocalSocket error:", error
			self.__finished()

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


def usage():
	print "Usage: public.data.py [Request]"
	print
	print "Request:"
	print "    doc:<document>  ...open the latest version of the given document"
	print "    rev:<revision>  ...display the given revision"
	print "    kill            ...kill server"
	print
	print "If no request is given then a server is started."
	sys.exit(1)


if len(sys.argv) > 2:
	usage()
elif (len(sys.argv) == 2 and not
		(sys.argv[1].startswith('doc:') or
		sys.argv[1].startswith('rev:') or
		sys.argv[1] == "kill")):
	usage()

app = QtCore.QCoreApplication(sys.argv)
mgr = RequestManager(app)
mgr.finished.connect(app.quit)
if len(sys.argv) > 1:
	start = mgr.request(sys.argv[1])
else:
	start = mgr.serve()

if start:
	sys.exit(app.exec_())
else:
	sys.exit(1)

