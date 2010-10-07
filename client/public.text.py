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

from PyQt4 import QtCore, QtGui
from hotchpotch import hpgui2
import diff3

class TextEditWindow(hpgui2.HpMainWindow):

	def __init__(self, argv):
		super(TextEditWindow, self).__init__(
			argv,
			"org.hotchpotch.textedit",
			["public.plain-text"],
			True)

		self.textEdit = QtGui.QTextEdit()
		self.setCentralWidget(self.textEdit)

		self.createActions()
		self.createMenus()
		self.createToolBars()
		self.createStatusBar()

	def createActions(self):
		self.cutAct = QtGui.QAction(QtGui.QIcon('icons/cut.png'), "Cu&t", self)
		self.cutAct.setShortcut(QtGui.QKeySequence.Cut)
		self.cutAct.setStatusTip("Cut the current selection's contents to the clipboard")
		QtCore.QObject.connect(self.cutAct, QtCore.SIGNAL("triggered()"),
			self.textEdit, QtCore.SLOT("cut()"))

		self.copyAct = QtGui.QAction(QtGui.QIcon('icons/copy.png'), "&Copy", self)
		self.copyAct.setShortcut(QtGui.QKeySequence.Copy)
		self.copyAct.setStatusTip("Copy the current selection's contents to the clipboard")
		QtCore.QObject.connect(self.copyAct, QtCore.SIGNAL("triggered()"),
			self.textEdit, QtCore.SLOT("copy()"))

		self.pasteAct = QtGui.QAction(QtGui.QIcon('icons/paste.png'), "&Paste", self)
		self.pasteAct.setShortcut(QtGui.QKeySequence.Paste)
		self.pasteAct.setStatusTip("Paste the clipboard's contents into the current selection")
		QtCore.QObject.connect(self.pasteAct, QtCore.SIGNAL("triggered()"),
				self.textEdit, QtCore.SLOT("paste()"))

		self.cutAct.setEnabled(False)
		self.copyAct.setEnabled(False)
		QtCore.QObject.connect(self.textEdit, QtCore.SIGNAL("copyAvailable(bool)"),
				self.cutAct, QtCore.SLOT("setEnabled(bool)"))
		QtCore.QObject.connect(self.textEdit, QtCore.SIGNAL("copyAvailable(bool)"),
				self.copyAct, QtCore.SLOT("setEnabled(bool)"))
		QtCore.QObject.connect(self.textEdit, QtCore.SIGNAL("textChanged()"),
				self.emitChanged)

	def createMenus(self):
		self.editMenu = self.menuBar().addMenu("&Edit")
		self.editMenu.addAction(self.cutAct)
		self.editMenu.addAction(self.copyAct)
		self.editMenu.addAction(self.pasteAct)

	def createToolBars(self):
		self.editToolBar = self.addToolBar("Edit")
		self.editToolBar.addAction(self.cutAct)
		self.editToolBar.addAction(self.copyAct)
		self.editToolBar.addAction(self.pasteAct)

	def createStatusBar(self):
		self.statusBar().showMessage("Ready")

	def docRead(self, readWrite, r):
		QtCore.QObject.disconnect(self.textEdit, QtCore.SIGNAL("textChanged()"), self.emitChanged)
		self.textEdit.setPlainText(r.readAll('FILE'))
		self.textEdit.document().setModified(False)
		self.textEdit.setReadOnly(not readWrite)
		QtCore.QObject.connect(self.textEdit, QtCore.SIGNAL("textChanged()"), self.emitChanged)

	def docSave(self, w):
		if self.textEdit.document().isModified():
			w.writeAll('FILE', self.textEdit.toPlainText().toUtf8())
			self.textEdit.document().setModified(False)

	#def docMergeCheck(self, heads, utis, changedParts):
	#	(uti, handled) = super(TextEditWindow, self).docMergeCheck(heads, utis, changedParts)
	#	if not uti:
	#		return (None, handled)
	#	if heads != 2:
	#		return (None, handled)
	#	return (uti, changedParts & (handled | set(['FILE'])))

	#def docMergePerform(self, writer, baseReader, mergeReaders, changedParts):
	#	conflicts = super(TextEditWindow, self).docMergePerform(writer, baseReader, mergeReaders, changedParts)
	#	if 'FILE' in changedParts:
	#		baseFile = baseReader.readAll('FILE')
	#		rev1File = mergeReaders[0].readAll('FILE')
	#		rev2File = mergeReaders[1].readAll('FILE')
	#		newFile = diff3.text_merge(baseFile, rev1File, rev2File)
	#		writer.writeAll('FILE', newFile)

	#	return conflicts

	def needSave(self):
		default = super(TextEditWindow, self).needSave()
		return default or self.textEdit.document().isModified()


if __name__ == '__main__':

	import sys

	app = QtGui.QApplication(sys.argv)
	mainWin = TextEditWindow(sys.argv)
	mainWin.show()
	sys.exit(app.exec_())

