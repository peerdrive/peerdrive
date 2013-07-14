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

from __future__ import absolute_import

from PyQt4 import QtGui
from peerdrive.gui import widgets
from . import diff3

class TextEdit(widgets.DocumentView):

	TE_FONT_SIZE  = ["org.peerdrive.textedit", "format", "pointsize"]
	TE_FONT_FIXED = ["org.peerdrive.textedit", "format", "fixedpitch"]
	TE_WORD_WRAP  = ["org.peerdrive.textedit", "format", "wordwrap"]

	def __init__(self):
		super(TextEdit, self).__init__("org.peerdrive.textedit")
		self.textEdit = QtGui.QTextEdit()
		self.textEdit.setAcceptRichText(False)
		self.textEdit.setReadOnly(True)
		self.textEdit.setTabStopWidth(40)
		self.textEdit.textChanged.connect(self._emitSaveNeeded)
		self.setCentralWidget(self.textEdit)
		self.mutable.connect(self.__setMutable)

		self.textWrap = QtGui.QAction("Wrap words", self)
		self.textWrap.setCheckable(True)
		self.textWrap.setChecked(True)
		self.textWrap.triggered.connect(self.__setWordWrap)
		self.textFixedPitch = QtGui.QAction("Fixed pitch", self)
		self.textFixedPitch.setCheckable(True)
		self.textFixedPitch.triggered.connect(self.__setFixedPitch)
		self.textStoreSettings = QtGui.QAction("Store format settings", self)
		self.textStoreSettings.setEnabled(False)
		self.textStoreSettings.triggered.connect(self.__storeSettings)

	def docRead(self, readWrite, r):
		self.textEdit.textChanged.disconnect(self._emitSaveNeeded)
		self.textEdit.setPlainText(r.readAll('_'))
		self.textEdit.document().setModified(False)
		self.textEdit.textChanged.connect(self._emitSaveNeeded)

		fontPoints = self.metaDataGetField(TextEdit.TE_FONT_SIZE, 10)
		self.setFontSize(fontPoints)
		fontFixed = self.metaDataGetField(TextEdit.TE_FONT_FIXED, False)
		self.__setFixedPitch(fontFixed)
		self.textFixedPitch.setChecked(fontFixed)
		wordWrap = self.metaDataGetField(TextEdit.TE_WORD_WRAP, True)
		self.__setWordWrap(wordWrap)
		self.textWrap.setChecked(wordWrap)

	def docSave(self, w):
		if self.textEdit.document().isModified():
			w.writeAll('_', str(self.textEdit.toPlainText().toUtf8()))

	def docMergeCheck(self, heads, types, changedParts):
		(uti, handled) = super(TextEdit, self).docMergeCheck(heads, types, changedParts)
		if heads == 2:
			return (uti, handled | set(['FILE']))
		else:
			return (uti, handled)

	def docMergePerform(self, writer, baseReader, mergeReaders, changedParts):
		conflicts = super(TextEdit, self).docMergePerform(writer, baseReader, mergeReaders, changedParts)
		if 'FILE' in changedParts:
			baseFile = baseReader.readAll('FILE')
			rev1File = mergeReaders[0].readAll('FILE')
			rev2File = mergeReaders[1].readAll('FILE')
			if rev1File == baseFile:
				newFile = rev2File
			elif rev2File == baseFile:
				newFile = rev1File
			else:
				newFile = diff3.text_merge(baseFile, rev1File, rev2File)
				# the diff3 module is broken, basically there's never a clean
				# merge
				conflicts = True
			writer.writeAll('FILE', newFile)

		return conflicts

	def setFontSize(self, size):
		font = QtGui.QFont(self.textEdit.document().defaultFont())
		font.setPointSize(size)
		self.textEdit.document().setDefaultFont(font)

	def __setMutable(self, mutable):
		self.textEdit.setReadOnly(not mutable)
		self.textStoreSettings.setEnabled(mutable)

	def __setWordWrap(self, enabled):
		if enabled:
			self.textEdit.setWordWrapMode(QtGui.QTextOption.WordWrap)
		else:
			self.textEdit.setWordWrapMode(QtGui.QTextOption.NoWrap)

	def __setFixedPitch(self, enabled):
		font = QtGui.QFont(self.textEdit.document().defaultFont())
		if enabled:
			font.setFamily("Courier")
			font.setFixedPitch(True)
		else:
			font.setFamily("Sans")
			font.setFixedPitch(False)
		self.textEdit.document().setDefaultFont(font)

	def __storeSettings(self):
		font = QtGui.QFont(self.textEdit.document().defaultFont())
		self.metaDataSetField(TextEdit.TE_FONT_SIZE, font.pointSize())
		self.metaDataSetField(TextEdit.TE_FONT_FIXED, font.fixedPitch())
		wordWrap = self.textEdit.wordWrapMode() == QtGui.QTextOption.WordWrap
		self.metaDataSetField(TextEdit.TE_WORD_WRAP, wordWrap)
