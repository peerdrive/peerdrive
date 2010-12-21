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

from PyQt4 import QtCore, QtNetwork
import sys, tempfile, os.path, subprocess

from hotchpotch import struct, Connector, Registry

PIPE_NAME = "org.hotchpotch.oslauncher"


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
		self.__server = QtNetwork.QLocalServer(self)
		self.__server.newConnection.connect(self.__serverConnect)
		if not self.__server.listen(PIPE_NAME):
			print "Unable to listen: ", self.__server.errorString()
			self.__finished()
		return not self.__quitRequested

	def __finished(self):
		self.__quitRequested = True
		if self.__server:
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
			rev = Connector().lookup_doc(doc).revs()[0]
		elif request.startswith('rev:'):
			rev = request[4:].decode("hex")
		elif request == "kill":
			self.__finished()
			return
		else:
			return

		stat = Connector().stat(rev)
		uti  = stat.type()
		hash = stat.hash('FILE')

		# determine extension
		extensions = Registry().search(uti, "extensions")
		if extensions:
			ext = extensions[0]
		else:
			ext = ""
			with Connector().peek(rev) as r:
				meta = struct.loads(r.readAll('META'))

			if "org.hotchpotch.annotation" in meta:
				annotation = meta["org.hotchpotch.annotation"]
				# try origin
				if "origin" in annotation:
					ext  = os.path.splitext(annotation["origin"])[1]
				# try title
				if not ext:
					if "title" in annotation:
						ext = os.path.splitext(annotation["title"])[1]

		# copy out file (if necessary)
		path = os.path.join(tempfile.gettempdir(), hash.encode('hex')+ext)
		if not os.path.isfile(path):
			with open(path, "wb") as file:
				with Connector().peek(rev) as reader:
					file.write(reader.readAll('FILE'))

		# start external program
		if sys.platform == "win32":
			subprocess.call(["start", path], shell=True)
		else:
			subprocess.call(["xdg-open", path])


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

