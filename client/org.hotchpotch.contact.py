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

from __future__ import with_statement

from PyQt4 import QtCore, QtGui
from hotchpotch import Connector, Registry, struct, gui

class ContactWindow(gui.MainWindow):

	def __init__(self, argv):
		super(ContactWindow, self).__init__(argv,
			"org.hotchpotch.contact"
			["org.hotchpotch.contact"],
			True)

		self.textEdit = QtGui.QTextEdit()
		self.setCentralWidget(self.textEdit)

		self.createActions()
		self.createMenus()
		self.createToolBars()
		self.createStatusBar()

	def createActions(self):
		pass

	def createMenus(self):
		pass

	def createToolBars(self):
		pass

	def createStatusBar(self):
		pass

	def docRead(self, readWrite, r):
		#QtCore.QObject.disconnect(self.textEdit, QtCore.SIGNAL("textChanged()"), self.emitChanged)
		#self.textEdit.setPlainText(r.readAll('FILE'))
		#self.textEdit.document().setModified(False)
		#self.textEdit.setReadOnly(not readWrite)
		#QtCore.QObject.connect(self.textEdit, QtCore.SIGNAL("textChanged()"), self.emitChanged)
		pass

	def docCheckpoint(self, w, force):
		if force or self.textEdit.document().isModified():
			pass
			#w.writeAll('FILE', self.textEdit.toPlainText().toUtf8())
			#self.textEdit.document().setModified(False)

	def docMergeCheck(self, heads, utis, changedParts):
		(uti, handled) = super(TextEditWindow, self).docMergeCheck(heads, utis, changedParts)
		return (uti, handled | set(['HPSD']))

	def docMergePerform(self, writer, baseReader, mergeReaders, changedParts):
		conflicts = super(TextEditWindow, self).docMergePerform(writer, baseReader, mergeReaders, changedParts)
		if 'HPSD' in changedParts:
			baseHpsd = struct.loads(baseReader.readAll('HPSD'))
			mergeHpsd = []
			for r in mergeReaders:
				mergeHpsd.append(struct.loads(r.readAll('HPSD')))
			(newHpsd, newConflict) = struct.merge(baseHpsd, mergeHpsd)
			conflicts = conflicts or newConflict
			writer.writeAll('HPSD', struct.dumps(newHpsd))
		return conflicts

	def needSave(self):
		default = super(TextEditWindow, self).needSave()
		return default or self.textEdit.document().isModified()



if __name__ == '__main__':

	import sys

	app = QtGui.QApplication(sys.argv)
	mainWin = ContactWindow(sys.argv)
	mainWin.show()
	sys.exit(app.exec_())

