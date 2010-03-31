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
from hotchpotch import HpConnector, HpRegistry, hpstruct, hpgui

def extractMetaData(metaData, path, default):
	item = metaData
	for step in path:
		if step in item:
			item = item[step]
		else:
			return default
	return item


buttons = []

def genStoreButton(store):
	b = hpgui.DocButton(store, True)
	buttons.append(b)
	button = b.getWidget()
	return button

def genRevButton(rev):
	b = hpgui.RevButton(rev, True)
	buttons.append(b)
	button = b.getWidget()
	return button

class PropertiesDialog(QtGui.QDialog):
	def __init__(self, guid, isUuid, parent=None):
		super(PropertiesDialog, self).__init__(parent)

		mainLayout = QtGui.QVBoxLayout()
		mainLayout.setSizeConstraint(QtGui.QLayout.SetFixedSize)

		if isUuid:
			info = HpConnector().lookup(guid)
			mainLayout.addWidget(DocumentTab(info.stores(), "document"))
			revs = info.revs()
		else:
			info = HpConnector().stat(guid)
			mainLayout.addWidget(DocumentTab(info.volumes(), "revision"))
			revs = [guid]

		if len(revs) == 0:
			QtGui.QMessageBox.warning(self, 'Missing document', 'The requested document was not found on any store.')
			sys.exit(1)

		tabWidget = QtGui.QTabWidget()
		annoTab = AnnotationTab(revs)
		tabWidget.addTab(annoTab, "Annotation")
		tabWidget.addTab(HistoryTab(revs), "History")
		if isUuid:
			tabWidget.addTab(RevisionTab(revs), "Revisions")
		else:
			tabWidget.addTab(RevisionTab(revs), "Revision")
		mainLayout.addWidget(tabWidget)

		buttonBox = QtGui.QDialogButtonBox(QtGui.QDialogButtonBox.Ok)
		QtCore.QObject.connect(buttonBox, QtCore.SIGNAL("accepted()"), self.accept)
		mainLayout.addWidget(buttonBox)
		self.setLayout(mainLayout)
		self.setWindowTitle("Properties of %s" % (annoTab.getTitle()))


class DocumentTab(QtGui.QWidget):
	def __init__(self, stores, descr, parent=None):
		super(DocumentTab, self).__init__(parent)

		mainLayout = QtGui.QVBoxLayout()
		mainLayout.addWidget(QtGui.QLabel("The "+descr+" exists on the following stores:"))
		subLayout = QtGui.QHBoxLayout()
		for store in stores:
			subLayout.addWidget(genStoreButton(store))
		subLayout.addStretch()
		mainLayout.addLayout(subLayout)
		self.setLayout(mainLayout)


class RevisionTab(QtGui.QWidget):
	def __init__(self, revs, parent=None):
		super(RevisionTab, self).__init__(parent)

		layout = QtGui.QGridLayout()
		layout.addWidget(QtGui.QLabel("Type:"), 1, 0)
		layout.addWidget(QtGui.QLabel("Modification time:"), 2, 0)
		layout.addWidget(QtGui.QLabel("Size:"), 3, 0)
		layout.addWidget(QtGui.QLabel("Stores:"), 4, 0)

		col = 1
		for rev in revs:
			stat = HpConnector().stat(rev)

			layout.addWidget(genRevButton(rev), 0, col)
			layout.addWidget(QtGui.QLabel(HpRegistry().getDisplayString(stat.uti())), 1, col)
			layout.addWidget(QtGui.QLabel(str(stat.mtime())), 2, col)
			size = 0
			for part in stat.parts():
				size += stat.size(part)
			for unit in ['Bytes', 'KiB', 'MiB', 'GiB']:
				if size < (1 << 10):
					break
				else:
					size = size >> 10
			sizeText = "%d %s (%d parts)" % (size, unit, len(stat.parts()))
			layout.addWidget(QtGui.QLabel(sizeText), 3, col)

			storeLayout = QtGui.QVBoxLayout()
			for store in stat.volumes():
				storeLayout.addWidget(genStoreButton(store))
			layout.addLayout(storeLayout, 4, col)

			col += 1

		self.setLayout(layout)


class HistoryTab(QtGui.QWidget):
	def __init__(self, revs, parent=None):
		super(HistoryTab, self).__init__(parent)

		# TODO: implement something nice gitk like...
		self.__historyList = []
		self.__historyRevs = []
		heads = revs[:]
		while len(heads) > 0:
			newHeads = []
			for rev in heads:
				try:
					if rev not in self.__historyRevs:
						stat = HpConnector().stat(rev)
						mtime = str(stat.mtime())
						comment = ""
						if 'META' in stat.parts():
							try:
								with HpConnector().read(rev) as r:
									metaData = hpstruct.loads(r.readAll('META'))
									comment = extractMetaData(
										metaData,
										["org.hotchpotch.annotation", "comment"],
										"")
							except IOError:
								pass
						self.__historyList.append(mtime + " - " + comment)
						self.__historyRevs.append(rev)
						newHeads.extend(stat.parents())
				except IOError:
					pass
			heads = newHeads

		self.__historyListBox = QtGui.QListWidget()
		self.__historyListBox.setSizePolicy(
			QtGui.QSizePolicy.Ignored,
			QtGui.QSizePolicy.Ignored )
		self.__historyListBox.insertItems(0, self.__historyList)
		QtCore.QObject.connect(
			self.__historyListBox,
			QtCore.SIGNAL("itemDoubleClicked(QListWidgetItem *)"),
			self.__open)

		layout = QtGui.QVBoxLayout()
		layout.addWidget(self.__historyListBox)
		self.setLayout(layout)

	def __open(self, item):
		row = self.__historyListBox.row(item)
		rev = self.__historyRevs[row]
		hpgui.showDocument(hpstruct.RevLink(rev))


class AnnotationTab(QtGui.QWidget):
	def __init__(self, revs, parent=None):
		super(AnnotationTab, self).__init__(parent)

		titles = set()
		self.__title = ""
		descriptions = set()
		description = ""
		comments = set()
		comment = ""
		tagSets = set()
		tags = []
		for rev in revs:
			try:
				with HpConnector().read(rev) as r:
					metaData = hpstruct.loads(r.readAll('META'))
					self.__title = extractMetaData(
						metaData,
						["org.hotchpotch.annotation", "title"],
						"")
					titles.add(self.__title)
					description = extractMetaData(
						metaData,
						["org.hotchpotch.annotation", "description"],
						"")
					descriptions.add(description)
					comment = extractMetaData(
						metaData,
						["org.hotchpotch.annotation", "comment"],
						"")
					comments.add(comment)
					tags = extractMetaData(
						metaData,
						["org.hotchpotch.annotation", "tags"],
						[])
					tagSets.add(frozenset(tags))
			except IOError:
				pass

		layout = QtGui.QGridLayout()

		layout.addWidget(QtGui.QLabel("Title:"), 0, 0)
		if len(titles) > 1:
			layout.addWidget(QtGui.QLabel("<ambiguous>"), 0, 1)
		else:
			layout.addWidget(QtGui.QLabel(self.__title), 0, 1)

		layout.addWidget(QtGui.QLabel("Description:"), 1, 0)
		if len(descriptions) > 1:
			layout.addWidget(QtGui.QLabel("<ambiguous>"), 1, 1)
		else:
			descLabel = QtGui.QLabel(description)
			descLabel.setWordWrap(True)
			descLabel.setScaledContents(True)
			layout.addWidget(descLabel, 1, 1)

		layout.addWidget(QtGui.QLabel("Comment:"), 2, 0)
		if len(comments) > 1:
			layout.addWidget(QtGui.QLabel("<ambiguous>"), 2, 1)
		else:
			commentLabel = QtGui.QLabel(comment)
			commentLabel.setWordWrap(True)
			commentLabel.setScaledContents(True)
			layout.addWidget(commentLabel, 2, 1)

		layout.addWidget(QtGui.QLabel("Tags:"), 3, 0)
		if len(tagSets) > 1:
			layout.addWidget(QtGui.QLabel("<ambiguous>"), 3, 1)
		else:
			tags.sort()
			tagList = ""
			for tag in tags:
				tagList += ", " + tag
			layout.addWidget(QtGui.QLabel(tagList[2:]), 3, 1)

		self.setLayout(layout)

	def getTitle(self):
		return self.__title


if __name__ == '__main__':

	import sys

	app = QtGui.QApplication(sys.argv)

	if (len(sys.argv) == 3) and (sys.argv[1] == 'uuid'):
		guid = sys.argv[2].decode('hex')
		dialog = PropertiesDialog(guid, True)
	elif (len(sys.argv) == 3) and (sys.argv[1] == 'rev'):
		guid = sys.argv[2].decode('hex')
		dialog = PropertiesDialog(guid, False)
	else:
		print "Usage: properties.py [uuid|rev] GUID"
		sys.exit(1)

	sys.exit(dialog.exec_())

