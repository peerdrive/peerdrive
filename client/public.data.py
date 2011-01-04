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

class RequestManager(QtCore.QObject):

	finished = QtCore.pyqtSignal()

	def __init__(self, parent=None):
		super(RequestManager, self).__init__(parent)
		self.__quitRequested = False

	def request(self, request):
		self.__request = request
		self.__socket = QtNetwork.QLocalSocket(self)
		self.__socket.connected.connect(self.__clientConnect)
		self.__socket.error.connect(self.__clientError)
		self.__socket.connectToServer(PIPE_NAME)
		return not self.__quitRequested

	def __finished(self):
		self.__quitRequested = True
		self.finished.emit()

	def __clientConnect(self):
		self.__socket.write(self.__request + '\n')
		self.__socket.disconnected.connect(self.__finished)
		self.__socket.disconnectFromServer()

	def __clientError(self, error):
		#if error == QtNetwork.QLocalSocket.ServerNotFoundError:
		print "Cannot launch:", self.__socket.errorString()
		self.__finished()


def usage():
	print "Usage: public.data.py [Request]"
	print
	print "Request:"
	print "    doc:<document>  ...open the latest version of the given document"
	print "    rev:<revision>  ...display the given revision"
	print
	print "If no request is given then a server is started."
	sys.exit(1)


if len(sys.argv) != 2:
	usage()
elif (not sys.argv[1].startswith('doc:') and
		not sys.argv[1].startswith('rev:')):
	usage()

app = QtCore.QCoreApplication(sys.argv)
mgr = RequestManager(app)
mgr.finished.connect(app.quit)
if mgr.request(sys.argv[1]):
	sys.exit(app.exec_())
else:
	sys.exit(1)

