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

from PyQt4 import QtCore, QtNetwork
import sys, optparse

from hotchpotch import struct

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


usage = ("usage: %prog [options] <Document>\n\n"
	"Document:\n"
	"    doc:<document>  ...open the latest version of the given document\n"
	"    rev:<revision>  ...display the given revision\n"
	"    <hp-path-spec>  ...open by path spec")
parser = optparse.OptionParser(usage=usage)
parser.add_option("--referrer", dest="referrer", metavar="REF",
	help="Document from which we're coming")
(options, args) = parser.parse_args()
if len(args) != 1:
	parser.error("incorrect number of arguments")
try:
	link = struct.Link(args[0])
except IOError as e:
	parser.error(str(e))

app = QtCore.QCoreApplication(sys.argv)
mgr = RequestManager(app)
mgr.finished.connect(app.quit)
if mgr.request(str(link)):
	sys.exit(app.exec_())
else:
	sys.exit(1)

