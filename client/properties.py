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

from PyQt4 import QtCore, QtGui
from peerdrive import Connector, Registry, struct, connector
from peerdrive.gui.widgets import DocButton, RevButton
from peerdrive.gui.utils import showDocument

def extractMetaData(metaData, path, default):
	item = metaData
	for step in path:
		if step in item:
			item = item[step]
		else:
			return default
	return item


def setMetaData(metaData, field, value):
	item = metaData
	path = field[:]
	while len(path) > 1:
		if path[0] not in item:
			item[path[0]] = { }
		item = item[path[0]]
		path = path[1:]
	item[path[0]] = value


flagToText = {
	0 : "Sticky",
	1 : "Ephemeral"
}


class PropertiesDialog(QtGui.QDialog):
	def __init__(self, link, parent=None):
		super(PropertiesDialog, self).__init__(parent)

		mainLayout = QtGui.QVBoxLayout()
		#mainLayout.setSizeConstraint(QtGui.QLayout.SetFixedSize)

		self.__doc = link.doc()
		self.__rev = link.rev()
		if self.__doc:
			isDoc = True
			banner = "document"
			l = Connector().lookupDoc(self.__doc)
			stores = [s for s in l.stores() if Connector().lookupRev(l.rev(s), [s])
				== [s] ]
		else:
			isDoc = False
			banner = "revision"
			stores = Connector().lookupRev(self.__rev)

		if len(stores) == 0:
			QtGui.QMessageBox.warning(self, 'Missing document', 'The requested document was not found on any store.')
			sys.exit(1)

		self.__docTab = DocumentTab(link.store(), stores, banner)
		self.__docTab.switchStore.connect(self.__switchStore)
		self.__revTab = RevisionTab()
		self.__annoTab = AnnotationTab(isDoc)
		self.__annoTab.changed.connect(self.__changed)
		self.__hisTab = HistoryTab()

		tabWidget = QtGui.QTabWidget()
		tabWidget.addTab(self.__annoTab, "Annotation")
		tabWidget.addTab(self.__hisTab, "History")

		if isDoc:
			self.__buttonBox = QtGui.QDialogButtonBox(
				QtGui.QDialogButtonBox.Save |
				QtGui.QDialogButtonBox.Close)
			self.__buttonBox.button(QtGui.QDialogButtonBox.Save).setEnabled(False)
			self.__buttonBox.accepted.connect(self.__save)
			self.__buttonBox.rejected.connect(self.reject)
		else:
			self.__buttonBox = QtGui.QDialogButtonBox(QtGui.QDialogButtonBox.Ok)
			self.__buttonBox.accepted.connect(self.accept)

		mainLayout.addWidget(self.__docTab)
		mainLayout.addWidget(self.__revTab)
		mainLayout.addWidget(tabWidget)
		mainLayout.addWidget(self.__buttonBox)
		self.setLayout(mainLayout)

		self.__switchStore(self.__docTab.activeStore())

		self.setWindowTitle("Properties of %s" % (self.__annoTab.getTitle()))

	def __changed(self):
		self.__buttonBox.button(QtGui.QDialogButtonBox.Save).setEnabled(True)

	def __switchStore(self, store):
		if self.__doc:
			self.__rev = Connector().lookupDoc(self.__doc, [store]).rev(store)

		self.__store = store
		self.__revTab.load(store, self.__rev)
		self.__annoTab.load(store, self.__rev)
		self.__hisTab.load(store, self.__rev)

	def __save(self):
		self.__buttonBox.button(QtGui.QDialogButtonBox.Save).setEnabled(False)

		with Connector().update(self.__store, self.__doc, self.__rev) as writer:
			writer.setData("/org.peerdrive.annotation/title",
				self.__annoTab.getTitle())
			writer.setData("/org.peerdrive.annotation/description",
				self.__annoTab.getDescription())
			tagString = self.__annoTab.getTags()
			if tagString is not None:
				tagList = [ tag.strip() for tag in tagString.split(',')]
				tagList = [ tag for tag in tagList if tag != '' ]
				tagList = list(set(tagList))
				writer.setData("/org.peerdrive.annotation/tags", tagList)

			writer.commit()
			self.__rev = writer.getRev()

		self.__switchStore(self.__store)


class DocumentTab(QtGui.QWidget):

	switchStore = QtCore.pyqtSignal(object)

	def __init__(self, store, stores, descr, parent=None):
		super(DocumentTab, self).__init__(parent)

		if store in stores:
			self.__store = store
		else:
			self.__store = stores[0]
		self.__buttons = { }

		mainLayout = QtGui.QVBoxLayout()
		mainLayout.addWidget(QtGui.QLabel("The "+descr+" exists on the following stores:"))
		subLayout = QtGui.QHBoxLayout()
		for store in stores:
			button = DocButton(store, store, True, True)
			button.setCheckable(True)
			button.setChecked(store == self.__store)
			button.clicked.connect(lambda x,store=store: self.__switchStore(store))
			subLayout.addWidget(button)
			self.__buttons[store] = button
		subLayout.addStretch()
		mainLayout.addLayout(subLayout)
		self.setLayout(mainLayout)

	def activeStore(self):
		return self.__store

	def __switchStore(self, store):
		if store != self.__store:
			self.__buttons[self.__store].setChecked(False)
			self.__store = store
			self.__buttons[self.__store].setChecked(True)
			self.switchStore.emit(store)


class RevisionTab(QtGui.QWidget):
	def __init__(self, parent=None):
		super(RevisionTab, self).__init__(parent)

		self.__typeLabel = QtGui.QLabel()
		self.__crtimeLabel = QtGui.QLabel()
		self.__mtimeLabel = QtGui.QLabel()
		self.__sizeLabel = QtGui.QLabel()
		self.__flagsLabel = QtGui.QLabel()

		layout = QtGui.QGridLayout()
		layout.addWidget(QtGui.QLabel("Type:"), 0, 0)
		layout.addWidget(QtGui.QLabel("Creation time:"), 1, 0)
		layout.addWidget(QtGui.QLabel("Modification time:"), 2, 0)
		layout.addWidget(QtGui.QLabel("Size:"), 3, 0)
		layout.addWidget(QtGui.QLabel("Flags:"), 4, 0)
		layout.addWidget(self.__typeLabel, 0, 1)
		layout.addWidget(self.__crtimeLabel, 1, 1)
		layout.addWidget(self.__mtimeLabel, 2, 1)
		layout.addWidget(self.__sizeLabel, 3, 1)
		layout.addWidget(self.__flagsLabel, 4, 1)

		self.setLayout(layout)

	def load(self, store, rev):
		try:
			stat = Connector().stat(rev, [store])
			self.__typeLabel.setText(Registry().getDisplayString(stat.type()))
			self.__crtimeLabel.setText(str(stat.crtime()))
			self.__mtimeLabel.setText(str(stat.mtime()))
			size = stat.dataSize()
			for a in stat.attachments():
				size += stat.size(a)
			for unit in ['Bytes', 'KiB', 'MiB', 'GiB']:
				if size < (1 << 10):
					break
				else:
					size = size >> 10
			sizeText = "%d %s (%d attachments)" % (size, unit, len(stat.attachments()))
			self.__sizeLabel.setText(sizeText)
			if stat.flags():
				flagsText = reduce(lambda x,y: x+", "+y,
					[ flagToText.get(f, "<"+str(f)+">") for f in stat.flags() ] )
			else:
				flagsText = "-"
			self.__flagsLabel.setText(flagsText)
		except IOError:
			self.__typeLabel.setText("n/a")
			self.__mtimeLabel.setText("n/a")
			self.__sizeLabel.setText("n/a")


class HistoryTab(QtGui.QWidget):

	# TODO: implement something nice gitk like...
	class HistoryList(QtGui.QListWidget):
		def __init__(self, parent=None):
			super(HistoryTab.HistoryList, self).__init__(parent)
			self.__revs = []
			self.itemDoubleClicked.connect(self.__open)
			self.setDragEnabled(True)

		def setRevs(self, store, revs):
			self.__store = store
			self.__revs = revs[:]
			self.clear()
			self.insertItems(0, [self.__loadComment(rev) for rev in revs])

		def supportedDropActions(self):
			return QtCore.Qt.IgnoreAction

		def mimeTypes(self):
			types = QtCore.QStringList()
			types << connector.LINK_MIME_TYPE
			return types

		def mimeData(self, items):
			links = [ connector.RevLink(self.__store, self.__revs[self.row(item)])
				for item in items ]
			if not links:
				return None

			mimeData = QtCore.QMimeData()
			connector.dumpMimeData(mimeData, links)
			return mimeData

		def __loadComment(self, rev):
			try:
				stat = Connector().stat(rev, [self.__store])
				mtime = str(stat.mtime())
				comment = stat.comment()
				if comment:
					return mtime + " - " + comment
				else:
					return mtime
			except IOError:
				return "???"

		def __open(self, item):
			row = self.row(item)
			rev = self.__revs[row]
			showDocument(connector.RevLink(self.__store, rev))


	def __init__(self, parent=None):
		super(HistoryTab, self).__init__(parent)

		self.__historyListBox = HistoryTab.HistoryList(self)
		self.__historyListBox.setSizePolicy(
			QtGui.QSizePolicy.Ignored,
			QtGui.QSizePolicy.Ignored )

		layout = QtGui.QVBoxLayout()
		layout.addWidget(self.__historyListBox)
		self.setLayout(layout)

	def load(self, store, rev):
		revs = []
		heads = [rev]
		while len(heads) > 0:
			newHeads = []
			for rev in heads:
				try:
					if rev not in revs:
						revs.append(rev)
						newHeads.extend(Connector().stat(rev, [store]).parents())
				except IOError:
					pass
			heads = newHeads

		self.__historyListBox.setRevs(store, revs)


class AnnotationTab(QtGui.QWidget):

	changed = QtCore.pyqtSignal()

	def __init__(self, edit, parent=None):
		super(AnnotationTab, self).__init__(parent)

		if edit:
			self.__titleEdit = QtGui.QLineEdit()
			self.__titleEdit.textEdited.connect(self.__changed)
			self.__descEdit = QtGui.QLineEdit()
			self.__descEdit.textEdited.connect(self.__changed)
			self.__tagsEdit = QtGui.QLineEdit()
			self.__tagsEdit.setValidator(QtGui.QRegExpValidator(
				QtCore.QRegExp("(\\s*\\w+\\s*(,\\s*\\w+\\s*)*)?"),
				self))
			self.__tagsEdit.textEdited.connect(self.__changed)
		else:
			self.__titleEdit = QtGui.QLabel()
			self.__descEdit = QtGui.QLabel()
			self.__descEdit.setWordWrap(True)
			self.__descEdit.setScaledContents(True)
			self.__tagsEdit = QtGui.QLabel()

		layout = QtGui.QGridLayout()
		layout.addWidget(QtGui.QLabel("Title:"), 0, 0)
		layout.addWidget(self.__titleEdit, 0, 1)
		layout.addWidget(QtGui.QLabel("Description:"), 1, 0)
		layout.addWidget(self.__descEdit, 1, 1)
		layout.addWidget(QtGui.QLabel("Tags:"), 2, 0)
		layout.addWidget(self.__tagsEdit, 2, 1)
		self.setLayout(layout)

	def load(self, store, rev):
		title = ""
		description = ""
		tags = []

		try:
			with Connector().peek(store, rev) as r:
				metaData = r.getData("/org.peerdrive.annotation")
				title = extractMetaData(metaData, ["title"], "")
				description = extractMetaData(metaData, ["description"], "")
				tags = extractMetaData(metaData, ["tags"], [])
		except IOError:
			pass

		tags.sort()
		self.__titleEdit.setText(title)
		self.__descEdit.setText(description)
		if tags:
			self.__tagsEdit.setText(reduce(lambda x, y: x+", "+y, tags))
		else:
			self.__tagsEdit.setText("")

	def getTitle(self):
		return unicode(self.__titleEdit.text())

	def getDescription(self):
		return unicode(self.__descEdit.text())

	def getTags(self):
		if self.__tagsEdit.hasAcceptableInput():
			return str(self.__tagsEdit.text())
		else:
			return None

	def __changed(self):
		self.changed.emit()


if __name__ == '__main__':

	import sys

	app = QtGui.QApplication(sys.argv)

	link = None
	if len(sys.argv) == 2:
		link = connector.Link(sys.argv[1])

	if not link:
		print "Usage: properties.py [doc:|rev:]UUID"
		sys.exit(1)

	dialog = PropertiesDialog(link)
	sys.exit(dialog.exec_())

