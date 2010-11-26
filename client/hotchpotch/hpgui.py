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
import sys, os, subprocess, pickle
import datetime

from hpconnector import HpWatch, HpConnector
from hpregistry import HpRegistry
import hpstruct


def showDocument(link):
	if isinstance(link, hpstruct.DocLink):
		args = ['doc:'+link.doc().encode('hex')]
		rev = HpConnector().lookup_doc(link.doc()).revs()[0]
	else:
		args = ['rev:'+link.rev().encode('hex')]
		rev = link.rev()
	uti = HpConnector().stat(rev).type()
	executable = HpRegistry().getExecutable(uti)
	if executable:
		if sys.platform == "win32":
			subprocess.Popen([executable] + args, shell=True)
		else:
			executable = './' + executable
			os.spawnv(os.P_NOWAIT, executable, [executable] + args)


def showProperties(link):
	if isinstance(link, hpstruct.DocLink):
		args = ['doc:'+link.doc().encode('hex')]
	else:
		args = ['rev:'+link.rev().encode('hex')]
	if sys.platform == "win32":
		subprocess.Popen(['properties.py'] + args, shell=True)
	else:
		os.spawnv(os.P_NOWAIT, './properties.py', ['./properties.py'] + args)


class DocButton(object):

	# convenient class to watch store
	class DocumentWatch(HpWatch):
		def __init__(self, doc, callback):
			self.__callback = callback
			super(DocButton.DocumentWatch, self).__init__(HpWatch.TYPE_DOC, doc)

		def triggered(self, cause):
			self.__callback(cause)

	def __init__(self, doc=None, withText=False):
		self.__watch = None
		self.__withText = withText
		self.__button = QtGui.QToolButton()
		self.__button.setAutoRaise(True)
		QtCore.QObject.connect(self.__button, QtCore.SIGNAL("clicked()"), self.__clicked)
		if withText:
			self.__button.setToolButtonStyle(QtCore.Qt.ToolButtonTextBesideIcon)
		self.setDocument(doc)

	def cleanup(self):
		self.setDocument(None)

	def setDocument(self, doc):
		self.__doc = doc
		if self.__watch:
			HpConnector().unwatch(self.__watch)
			self.__watch = None
		if doc:
			self.__watch = DocButton.DocumentWatch(doc, self.__update)
			HpConnector().watch(self.__watch)
		self.__update(0)

	def __update(self, cause):
		if cause == HpWatch.CAUSE_DISAPPEARED:
			self.setDocument(None)

		if self.__doc:
			self.__button.setEnabled(True)
			try:
				rev = HpConnector().lookup_doc(self.__doc).revs()[0]
				docIcon = None
				with HpConnector().peek(rev) as r:
					try:
						metaData = hpstruct.loads(r.readAll('META'))
						docName = metaData["org.hotchpotch.annotation"]["title"]
						# TODO: individual store icon...
					except:
						docName = "Unnamed"
				if not docIcon:
					uti = HpConnector().stat(rev).type()
					docIcon = QtGui.QIcon(HpRegistry().getIcon(uti))
			except IOError:
				docName = ''
				docIcon = QtGui.QIcon("icons/uti/file_broken.png")
				self.__button.setEnabled(False)
		else:
			docName = ''
			docIcon = QtGui.QIcon("icons/uti/store.png")
			self.__button.setEnabled(False)

		if len(docName) > 20:
			docName = docName[:20] + '...'
		self.__button.setIcon(docIcon)
		if self.__withText:
			self.__button.setText(docName)
		else:
			self.__button.setToolTip(docName)

	def __clicked(self):
		if self.__doc:
			showDocument(hpstruct.DocLink(self.__doc, False))

	def getWidget(self):
		return self.__button


class RevButton(object):
	def __init__(self, rev, withText=False):
		self.__rev = rev
		self.__button = QtGui.QToolButton()
		self.__button.font().setItalic(True)

		comment = None
		tags = None
		mtime = None
		try:
			title = "Unnamed"
			with HpConnector().peek(rev) as r:
				try:
					metaData = hpstruct.loads(r.readAll('META'))
					if "org.hotchpotch.annotation" in metaData:
						metaData = metaData["org.hotchpotch.annotation"]
						if "title" in metaData:
							title = metaData["title"]
						if "comment" in metaData:
							comment = metaData["comment"]
						if "tags" in metaData:
							tagList = metaData["tags"]
							tagList.sort()
							if len(tagList) == 0:
								tags = ""
							else:
								tags = reduce(lambda x,y: x+', '+y, tagList)
				except:
					pass
			stat = HpConnector().stat(rev)
			uti = stat.type()
			mtime = stat.mtime()
			revIcon = QtGui.QIcon(HpRegistry().getIcon(uti))
		except IOError:
			title = ''
			revIcon = QtGui.QIcon("icons/uti/file_broken.png")
			self.__button.setEnabled(False)

		if len(title) > 20:
			title = title[:20] + '...'

		self.__button.setIcon(revIcon)
		self.__button.setAutoRaise(True)
		if withText:
			self.__button.setToolButtonStyle(QtCore.Qt.ToolButtonTextBesideIcon)
			self.__button.setText(title)
			toolTip = "Open revision (read only)"
		else:
			toolTip = title + " (open read only)"
		if mtime:
			toolTip += "\n\nMtime: " + str(mtime)
		if comment:
			toolTip += "\nComment: " + comment
		if tags:
			toolTip += "\nTags: " + tags
		self.__button.setToolTip(toolTip)
		QtCore.QObject.connect(self.__button, QtCore.SIGNAL("clicked()"), self.__clicked)

	def cleanup(self):
		pass

	def __clicked(self):
		showDocument(hpstruct.RevLink(self.__rev))

	def getWidget(self):
		return self.__button


class HpMainWindow(QtGui.QMainWindow, HpWatch):
	HPA_TITLE        = ["org.hotchpotch.annotation", "title"]
	HPA_TAGS         = ["org.hotchpotch.annotation", "tags"]
	HPA_COMMENT      = ["org.hotchpotch.annotation", "comment"]
	HPA_DESCRIPTION  = ["org.hotchpotch.annotation", "description"]
	SYNC_STICKY      = ["org.hotchpotch.sync", "sticky"]
	SYNC_HISTROY     = ["org.hotchpotch.sync", "history"]

	class UserEvent(QtCore.QEvent):
		def __init__(self, eventType, action):
			self.__action = action
			QtCore.QEvent.__init__(self, eventType)

		def execute(self):
			self.__action()

	def __init__(self, argv, creator, types, isEditor):
		QtGui.QMainWindow.__init__(self)
		self.setAttribute(QtCore.Qt.WA_DeleteOnClose)

		self.__creator    = creator
		self.__types      = types
		self.__isEditor   = isEditor
		self.__utiPixmap  = None
		self.__connection = HpConnector()
		self.__storeButtons = { }
		self.__userEventType = QtCore.QEvent.registerEventType()

		# parse command line
		self.__doc = None
		self.__rev = None
		self.__preliminary = False
		self.__mutable = False
		if len(argv) == 2 and argv[1].startswith('doc:'):
			self.__doc = argv[1][4:].decode("hex")
		elif len(argv) == 2 and argv[1].startswith('rev:'):
			self.__rev = argv[1][4:].decode("hex")
		elif len(argv) == 2:
			link = hpstruct.resolvePath(argv[1])
			if isinstance(link, hpstruct.DocLink):
				self.__doc = link.doc()
			else:
				self.__rev = link.rev()
		else:
			print "Usage: %s <Document>" % (self.__creator)
			print
			print "Document:"
			print "    doc:<document>  ...open the latest version of the given document"
			print "    rev:<revision>  ...display the given revision"
			print "    <hp-path-spec>  ...open by path spec"
			sys.exit(1)

		# create standard actions
		if isEditor:
			self.__saveAct = QtGui.QAction(QtGui.QIcon('icons/save.png'), "Check&point", self)
			self.__saveAct.setEnabled(False)
			self.__saveAct.setShortcut(QtGui.QKeySequence.Save)
			self.__saveAct.setStatusTip("Create checkpoint of document")
			QtCore.QObject.connect(self.__saveAct, QtCore.SIGNAL("triggered()"), self.__checkpointFile)

			self.__mergeMenu = QtGui.QMenu()
			self.__mergeMenu.aboutToShow.connect(self.__mergeShow)
			self.__mergeAct = QtGui.QAction(QtGui.QIcon('icons/merge.png'), "Merge", self)
			self.__mergeAct.setStatusTip("Merge other revisions into current document")
			self.__mergeAct.setMenu(self.__mergeMenu)
			self.__mergeAct.triggered.connect(lambda: self.__mergeMenu.exec_(QtGui.QCursor.pos()))
			self.__mergeAct.setVisible(False)

			self.__stickyAct = QtGui.QAction("Sticky", self)
			self.__stickyAct.setStatusTip("Automatically replicate referenced documents")
			self.__stickyAct.setCheckable(True)
			QtCore.QObject.connect(self.__stickyAct, QtCore.SIGNAL("triggered(bool)"), self.__toggleSticky)

			self.__historySpin = QtGui.QSpinBox(self)
			self.__historySpin.setMaximum(365)
			self.__historySpin.setSuffix(" days")
			self.__historySpin.setStatusTip("Maximum depth (in days) of the replicated documents histories")
			QtCore.QObject.connect(self.__historySpin, QtCore.SIGNAL("valueChanged(int)"), self.__toggleHistroy)

			self.__historyAct = QtGui.QWidgetAction(self)
			self.__historyAct.setDefaultWidget(self.__historySpin)
			self.__historyAct.setEnabled(False)

		self.__nameEdit = QtGui.QLineEdit()
		self.__tagsEdit = QtGui.QLineEdit()
		self.__tagsEdit.setValidator(QtGui.QRegExpValidator(
			QtCore.QRegExp("(\\s*\\w+\\s*(,\\s*\\w+\\s*)*)?"),
			self))
		self.__descEdit = QtGui.QTextEdit()
		self.__descEdit.setAcceptRichText(False)
		QtCore.QObject.connect(self.__nameEdit, QtCore.SIGNAL("textEdited(const QString&)"), self.__nameChanged)
		QtCore.QObject.connect(self.__tagsEdit, QtCore.SIGNAL("textEdited(const QString&)"), self.__tagsChanged)
		QtCore.QObject.connect(self.__descEdit, QtCore.SIGNAL("textChanged()"), self.__descChanged)
		layout = QtGui.QVBoxLayout()

		gl = QtGui.QGridLayout()
		gl.addWidget(QtGui.QLabel("Title:"), 0, 0)
		gl.addWidget(self.__nameEdit, 0, 1)
		gl.addWidget(QtGui.QLabel("Tags:"), 1, 0)
		gl.addWidget(self.__tagsEdit, 1, 1)
		layout.addLayout(gl)

		layout.addWidget(QtGui.QLabel("Description:"))
		layout.addWidget(self.__descEdit)
		frame = QtGui.QWidget()
		frame.setLayout(layout)
		self._annotationDock = QtGui.QDockWidget("Annotation", self)
		self._annotationDock.setAllowedAreas(QtCore.Qt.LeftDockWidgetArea | QtCore.Qt.RightDockWidgetArea)
		self._annotationDock.setWidget(frame)
		self.addDockWidget(QtCore.Qt.RightDockWidgetArea, self._annotationDock)
		self._annotationDock.hide()

		self.__propertiesAct = QtGui.QAction("&Properties", self)
		QtCore.QObject.connect(self.__propertiesAct, QtCore.SIGNAL("triggered()"), self.__showProperties)

		self.__delMenu = QtGui.QMenu()
		self.__delMenu.aboutToShow.connect(self.__deleteFile)
		self.delAct = QtGui.QAction(QtGui.QIcon('icons/edittrash.png'), "&Delete", self)
		self.delAct.setStatusTip("Delete the document")
		self.delAct.setMenu(self.__delMenu)
		self.delAct.triggered.connect(lambda: self.__delMenu.exec_(QtGui.QCursor.pos()))

		self.__exitAct = QtGui.QAction("Close", self)
		self.__exitAct.setShortcut("Ctrl+Q")
		self.__exitAct.setStatusTip("Close the document")
		QtCore.QObject.connect(self.__exitAct, QtCore.SIGNAL("triggered()"), self.close)

		# create standard menu
		self.fileMenu = self.menuBar().addMenu("&Document")
		if isEditor:
			self.fileMenu.addAction(self.__saveAct)
			self.fileMenu.addAction(self.__mergeAct)
		self.fileMenu.addAction(self.delAct)
		if isEditor:
			self.repMenu = self.fileMenu.addMenu("Replication")
			self.repMenu.addAction(self.__stickyAct)
			self.repMenu.addAction(self.__historyAct)
		self.fileMenu.addAction(self.__propertiesAct)

		# standard tool bars
		self.fileToolBar = self.addToolBar("Document")
		self.dragWidget = DragWidget(self)
		self.fileToolBar.addWidget(self.dragWidget)
		self.fileToolBar.addSeparator()
		if isEditor:
			self.fileToolBar.addAction(self.__saveAct)
			self.fileToolBar.addAction(self.__mergeAct)
		self.fileToolBar.addAction(self.delAct)

		# save comment popup
		self.__commentPopup = CommentPopup(self)
		self.__saveTimer = QtCore.QTimer(self)
		self.__saveTimer.timeout.connect(lambda: self.__saveFile("<<Periodic checkpoint>>"))
		self.__saveTimer.setSingleShot(True)
		self.__saveTimer.setInterval(10000)

		# disable for now
		self.delAct.setEnabled(True)

		# load settings
		self.__loadSettings()

		# post init event for deferred part
		self.__postEvent(lambda: self.__startup())

	def doc(self):
		return self.__doc

	def rev(self):
		return self.__rev

	def needSave(self):
		return self.__metaDataChanged

	def metaDataSetField(self, field, value):
		item = self.__metaData
		path = field[:]
		while len(path) > 1:
			if path[0] not in item:
				item[path[0]] = { }
			item = item[path[0]]
			path = path[1:]
		if path[0] in item:
			if item[path[0]] == value:
				# no real change...
				return
		item[path[0]] = value
		self.emitChanged()
		self.__metaDataChanged = True

	def metaDataGetField(self, field, default):
		item = self.__metaData
		for step in field:
			if step in item:
				item = item[step]
			else:
				return default
		return item

	def emitChanged(self):
		if self.__isEditor:
			self.__saveAct.setEnabled(True)
			self.__saveTimer.start()

	def saveSettings(self, settings):
		settings["resx"] = self.size().width()
		settings["resy"] = self.size().height()
		settings["posx"] = self.pos().x()
		settings["posy"] = self.pos().y()

	def loadSettings(self, settings):
		self.resize(settings["resx"], settings["resy"])
		self.move(settings["posx"], settings["posy"])

	def save(self):
		self.__saveFile("<<Periodic checkpoint>>")

	def checkpoint(self, comment, forceComment=False):
		# explicitly set comment, the user expects it's comment to be applied
		if forceComment:
			self.metaDataSetField(HpMainWindow.HPA_COMMENT, comment)
		self.__saveFile(comment)
		if self.__preliminary:
			with self.__connection.resume(self.__doc, self.__rev) as writer:
				writer.commit()
			self.__rev = writer.getRev()
			self.__preliminary = False
			print "rev:%s" % self.__rev.encode("hex")
			self.__sync()
		if self.__isEditor:
			self.__saveAct.setEnabled(False)

	def docSaved(self):
		self.__metaDataChanged = False

	# returns (type, handled) where:
	#   type:    the resulting type code (if we would handle it)
	#   handled: set of parts which this instance can merge automatically
	def docMergeCheck(self, heads, types, changedParts):
		# don't care about the number of heads
		if len(types) != 1:
			return (None, set(['META'])) # cannot merge different types
		return (types.copy().pop(), set(['META']))

	# return conflict True/False
	def docMergePerform(self, writer, baseReader, mergeReaders, changedParts):
		baseMeta = hpstruct.loads(baseReader.readAll('META'))
		if 'META' in changedParts:
			mergeMeta = []
			for mr in mergeReaders:
				mergeMeta.append(hpstruct.loads(mr.readAll('META')))
			(newMeta, conflict) = hpstruct.merge(baseMeta, mergeMeta)
		else:
			newMeta = baseMeta

		# FIXME: ugly, should be common code
		comment = "<<Automatic merge>>"
		if "org.hotchpotch.annotation" in newMeta:
			newMeta["org.hotchpotch.annotation"]["comment"] = comment
		else:
			newMeta["org.hotchpotch.annotation"] = { "comment" : comment }
		writer.writeAll('META', hpstruct.dumps(newMeta))
		return False

	# === re-implemented inherited methods

	def closeEvent(self, event):
		event.accept()
		if self.__isMutable():
			self.checkpoint("<<Automatically saved>>")
		self.__connection.unwatch(self)
		self.__saveSettings()

	def triggered(self, cause):
		#print >>sys.stderr, "watch %d for %s" % (cause, self.getHash().encode('hex'))
		if cause == HpWatch.CAUSE_DISAPPEARED:
			# FIXME: maybe we don't want to loose all changes...
			self.__mutable = False # prevent useless saving
			self.close()
		else:
			self.__postEvent(lambda: self.__update())

	def event(self, event):
		if event.type() == self.__userEventType:
			event.execute()
			return True
		else:
			return QtGui.QMainWindow.event(self, event)

	# === private methods

	def __postEvent(self, action):
		event = HpMainWindow.UserEvent(self.__userEventType, action)
		QtGui.QApplication.postEvent(self, event)

	def __startup(self):
		self.fileMenu.addSeparator();
		self.fileMenu.addAction(self.__exitAct)
		self.fileToolBar.addAction(self._annotationDock.toggleViewAction())

		# see what we have to do
		if self.__doc:
			l = self.__connection.lookup_doc(self.__doc)
			revs = l.revs()
			preRevs = filter(self.__filterPreRev, l.preRevs())
			if len(revs) == 0:
				print "Could not find the document"
				sys.exit(2)
			elif (len(revs) == 1) and (preRevs == []):
				self.__rev = revs[0]
				self.statusBar().showMessage("Document loaded", 3000)
			else:
				pixmap = QtGui.QPixmap(HpRegistry().getIcon(self.__types[0]))
				self.setWindowIcon(QtGui.QIcon(pixmap))
				self.dragWidget.setPixmap(pixmap)
				if not self.__chooseStartRev(l, revs, preRevs):
					print "No document choosen"
					sys.exit(2)

			self.__setMutable(True)
			self.__loadFile()
			HpWatch.__init__(self, HpWatch.TYPE_DOC, self.__doc)
		else:
			self.__setMutable(False)
			self.__loadFile()
			self.statusBar().showMessage("Revision loaded", 3000)
			HpWatch.__init__(self, HpWatch.TYPE_REV, self.__rev)
		self.__connection.watch(self)

		# window icon
		self.setWindowIcon(QtGui.QIcon(self.__getUtiPixmap()))
		self.dragWidget.setPixmap(self.__getUtiPixmap())
		self.__update()

	def __update(self):
		if self.__isMutable():
			# make a preliminary commit if necessary
			self.__saveFile('<<Internal checkpoint>>')

			# ok, whats up?
			info = self.__connection.lookup_doc(self.__doc)
			allStores = info.stores()
			currentRevs = set(info.revs())
			if self.__preliminary:
				validRevs = set(self.__connection.stat(self.__rev).parents())
			else:
				validRevs = set([self.__rev])

			# check if we're still up-to-date
			if currentRevs.isdisjoint(validRevs):
				if self.__preliminary:
					self.__updateRebase()
				else:
					self.__updateFastForward()
			elif self.__isEditor:
				self.__mergeAct.setVisible(bool(currentRevs - validRevs))

		else:
			allStores = self.__connection.lookup_rev(self.__rev)

		# update store buttons in status bar
		for store in allStores:
			if store not in self.__storeButtons:
				button = DocButton(store)
				self.statusBar().addPermanentWidget(button.getWidget())
				self.__storeButtons[store] = button
		remStores = set(self.__storeButtons) - set(allStores)
		for store in remStores:
			self.statusBar().removeWidget(self.__storeButtons[store].getWidget())
			del self.__storeButtons[store]

	def __updateRebase(self):
		# get current revs on all stores where the preliminary versions exists
		heads = set()
		lookup = self.__connection.lookup_doc(self.__doc)
		for store in lookup.stores(self.__rev):
			heads.add(lookup.rev(store))

		# ask user which rev he wants to overwrite or discard current prerev
		dialog = OverwriteDialog(lookup, heads, self)
		if dialog.exec_():
			(rev, overwrite) = dialog.getResult()
			if overwrite:
				with self.__connection.resume(self.__doc, self.__rev) as writer:
					writer.setParents([rev]) # FIXME: will wreck a merge prerev
					writer.suspend()
				self.__rev = writer.getRev()
				print "rev:%s" % self.__rev.encode("hex")
			else:
				self.__connection.forget(self.__doc, self.__rev)
				self.__rev = rev
				self.__preliminary = False
				self.__loadFile()
				self.__saveAct.setEnabled(False)
		else:
			self.__rev = None
			self.__mutable = False
			self.close()

	def __updateFastForward(self):
		# find all heads which lead to current rev
		found = []
		target = self.__rev
		try:
			lookup = self.__connection.lookup_doc(self.__doc)
			depth = self.__connection.stat(target).mtime() - datetime.timedelta(days=1)
		except IOError:
			# seems we're gone
			self.close()
			return
		heads = [(rev, set([rev])) for rev in lookup.revs()]
		while heads:
			oldHeads = heads
			heads = []
			for (rev, tips) in oldHeads:
				newTips = set()
				for tip in tips:
					try:
						stat = self.__connection.stat(tip)
						parents = stat.parents()
						if target in parents:
							found.append(rev)
							newTips = None
							break
						elif depth < stat.mtime():
							newTips |= set(parents)
					except IOError:
						pass

				if newTips:
					heads.append((rev, newTips))

		if len(found) == 1:
			# if exactly one head then just load file
			self.__rev = found[0]
			self.__loadFile()
		elif found == []:
			# if no head found quit
			# FIXME: let user choose to store as new document!
			self.close()
		else:
			# if more than one head ask user
			if self.__chooseStartRev(self, lookup, found, []):
				self.__loadFile()
			else:
				self.close()

	def __sync(self):
		lookup = self.__connection.lookup_doc(self.__doc)
		stat = self.__connection.stat(self.__rev)

		# get all revs which are heads of __doc and are parents of __rev
		parents = set(stat.parents())
		heads = set(lookup.revs())
		revs = (parents & heads) | set([self.__rev])

		# get all the stores of these revs
		stores = set()
		for rev in revs:
			stores |= set(lookup.stores(rev))

		# sync if more than one store is involved
		if len(stores) > 1:
			self.__connection.sync(self.__doc, stores=stores)

	def __chooseStartRev(self, lookup, revs, preRevs):
		dialog = ChooseWindow(lookup, self.__doc, revs, preRevs, self)
		if dialog.exec_():
			(self.__rev, self.__preliminary) = dialog.getResult()
			self.__saveAct.setEnabled(self.__preliminary)
			return True
		else:
			return False

	def __loadFile(self):
		QtGui.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
		try:
			with self.__connection.peek(self.__rev) as r:
				try:
					self.__metaData = hpstruct.loads(r.readAll('META'))
				except IOError:
					self.__metaData = { }
				self.__metaDataChanged = False
				self.docRead(self.__isMutable(), r)
			self.__extractMetaData()
		finally:
			QtGui.QApplication.restoreOverrideCursor()

	def __saveFile(self, comment = ""):
		self.__saveTimer.stop()
		if self.__isMutable() and self.needSave():
			if self.__preliminary:
				with self.__connection.resume(self.__doc, self.__rev) as writer:
					self.__saveFileInternal(comment, writer)
					writer.suspend()
			else:
				with self.__connection.update(self.__doc, self.__rev, self.__creator) as writer:
					self.__saveFileInternal(comment, writer)
					writer.suspend()
			newRev = writer.getRev()
			print "rev:%s" % newRev.encode("hex")
			sys.stdout.flush()
			self.__rev = newRev
			self.__preliminary = True
			self.docSaved()

	def __saveFileInternal(self, comment, writer):
		QtGui.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
		try:
			self.metaDataSetField(HpMainWindow.HPA_COMMENT, comment)
			if self.__metaDataChanged:
				writer.writeAll('META', hpstruct.dumps(self.__metaData))
			if self.__isEditor:
				self.docSave(writer)
		finally:
			QtGui.QApplication.restoreOverrideCursor()

	def __filterPreRev(self, rev):
		try:
			return self.__connection.stat(rev).creator() == self.__creator
		except IOError:
			return False

	def __extractMetaData(self):
		name = self.metaDataGetField(HpMainWindow.HPA_TITLE, "Unnamed document")
		self.__nameEdit.setText(name)
		tagList = self.metaDataGetField(HpMainWindow.HPA_TAGS, [])
		tagList.sort()
		if len(tagList) == 0:
			tagString = ""
		else:
			tagString = reduce(lambda x,y: x+', '+y, tagList)
		self.__tagsEdit.setText(tagString)
		self.__descEdit.setPlainText(self.metaDataGetField(HpMainWindow.HPA_DESCRIPTION, ""))
		self.setWindowTitle(name)
		if self.__isEditor:
			self.__stickyAct.setEnabled(self.__isMutable())
			self.__stickyAct.setChecked(self.metaDataGetField(HpMainWindow.SYNC_STICKY, False))
			self.__historyAct.setEnabled(self.__isMutable() and self.__stickyAct.isChecked())
			self.__historySpin.setValue(self.metaDataGetField(HpMainWindow.SYNC_HISTROY, 0) / (24*60*60))

	def __showProperties(self):
		if self.__isMutable():
			link = hpstruct.DocLink(self.__doc, False)
		else:
			link = hpstruct.RevLink(self.__rev)
		showProperties(link)

	def __saveSettings(self):
		if self.__doc:
			hash = self.__doc.encode('hex')
		else:
			hash = self.__rev.encode('hex')
		path = ".settings/" + hash[0:2]
		if not os.path.exists(path):
			os.makedirs(path)
		with open(path + "/" + hash[2:], 'w') as f:
			settings = { }
			self.saveSettings(settings)
			pickle.dump(settings, f)

	def __loadSettings(self):
		if self.__doc:
			hash = self.__doc.encode('hex')
		else:
			hash = self.__rev.encode('hex')
		path = ".settings/" + hash[0:2] + "/" + hash[2:]
		try:
			if os.path.isfile(path):
				with open(path, 'r') as f:
					settings = pickle.load(f)
				self.loadSettings(settings)
		except:
			print "Failed to load settings!"

	def __setMutable(self, value):
		self.__mutable = value
		self.__nameEdit.setReadOnly(not value)
		self.__tagsEdit.setReadOnly(not value)
		self.__descEdit.setReadOnly(not value)

	def __isMutable(self):
		return self.__mutable

	def __getUtiPixmap(self):
		if self.__utiPixmap is None:
			if self.__rev:
				uti = self.__connection.stat(self.__rev).type()
			else:
				uti = self.__types[0]
			self.__utiPixmap = QtGui.QPixmap(HpRegistry().getIcon(uti))
		return self.__utiPixmap

	def __nameChanged(self, name):
		self.metaDataSetField(HpMainWindow.HPA_TITLE, str(self.__nameEdit.text()))
		self.setWindowTitle(name)

	def __tagsChanged(self, tagString):
		if self.__tagsEdit.hasAcceptableInput():
			tagSet = set([ tag.strip() for tag in str(tagString).split(',')])
			tagList = list(tagSet)
			self.metaDataSetField(HpMainWindow.HPA_TAGS, tagList)

	def __descChanged(self):
		old = self.metaDataGetField(HpMainWindow.HPA_DESCRIPTION, "")
		new = str(self.__descEdit.toPlainText())
		if old != new:
			self.metaDataSetField(HpMainWindow.HPA_DESCRIPTION, new)

	def __toggleSticky(self, checked):
		self.metaDataSetField(HpMainWindow.SYNC_STICKY, checked)
		self.__historyAct.setEnabled(checked)

	def __toggleHistroy(self, value):
		self.metaDataSetField(HpMainWindow.SYNC_HISTROY, value*24*60*60)

	def __checkpointFile(self):
		self.__commentPopup.popup(self.metaDataGetField(HpMainWindow.HPA_COMMENT, "Enter comment"))

	def __deleteFile(self):
		self.__delMenu.clear()
		if self.__isMutable():
			lookup = self.__connection.lookup_doc(self.__doc)
			stores = lookup.stores()
			delFun = lambda store: self.__connection.deleteDoc(self.__doc, lookup.rev(store), [store])
		else:
			stores = self.__connection.lookup_rev(self.__rev)
			delFun = lambda store: self.__connection.deleteRev(self.__rev, [store])
		stores = [(self.__getStoreName(store), store) for store in stores]
		stores = filter(lambda(name,store):name, stores)
		for (name, store) in stores:
			action = self.__delMenu.addAction("Delete item from '%s'" % name)
			action.triggered.connect(lambda: delFun(store))

		if len(stores) > 1:
			self.__delMenu.addSeparator()
			delAllAction = self.__delMenu.addAction("Delete from all stores")
			if self.__isMutable():
				delAllAction.triggered.connect(lambda: self.__connection.deleteDoc(self.__doc, self.__rev))
			else:
				delAllAction.triggered.connect(lambda: self.__connection.deleteRev(self.__rev))

	def __getStoreName(self, store):
		try:
			rev = self.__connection.lookup_doc(store).rev(store)
			with self.__connection.peek(rev) as r:
				try:
					metaData = hpstruct.loads(r.readAll('META'))
					return metaData["org.hotchpotch.annotation"]["title"]
				except:
					return "Unnamed store"
		except:
			return None

	def __mergeShow(self):
		lookup = self.__connection.lookup_doc(self.__doc)
		revs = set(lookup.revs())
		if self.__preliminary:
			revs -= set(self.__connection.stat(self.__rev).parents())
		else:
			revs -= set([self.__rev])

		self.__mergeMenu.clear()
		for rev in revs:
			stores = [self.__getStoreName(store) for store in lookup.stores(rev)]
			names = reduce(lambda x,y: x+", "+y, stores)
			action = self.__mergeMenu.addAction(names)
			action.triggered.connect(lambda: self.__merge(rev))

	def __merge(self, rev):
		self.checkpoint("<<Save before merge>>")

		lookup = self.__connection.lookup_doc(self.__doc)
		stores = set(lookup.stores(self.__rev))
		stores |= set(lookup.stores(rev))
		revs = [self.__rev, rev]

		# determine common ancestor
		(fastForward, base) = self.__calculateMergeBase(revs, stores)
		#print "ff:%s, base:%s" % (fastForward, base.encode("hex"))
		if base:
			# fast-forward merge?
			if fastForward:
				self.__rev = self.__connection.sync(self.__doc, stores=stores)
				self.__loadFile()
				self.statusBar().showMessage("Document merged by fast-forward", 10000)
				return

			# can the application help?
			if self.__mergeAuto(base, revs, stores):
				self.statusBar().showMessage("Document merged automatically", 10000)
				return

		if self.__mergeOurs(revs, stores):
			self.statusBar().showMessage("Document merged by user choice", 10000)
			return

	def __mergeOurs(self, revs, stores):
		# last resort: "ours"-merge
		options = []
		for rev in revs:
			revDate = self.__connection.stat(rev, stores).mtime()
			revStores = self.__connection.lookup_rev(rev, stores)
			stores = reduce(
				lambda x,y: x+", "+y,
				[self.__getStoreName(s) for s in revStores])
			options.append(str(revDate) + ": " + stores)

		(choice, ok) = QtGui.QInputDialog.getItem(
			self,
			"Select version",
			"The document could not be merged automatically.\nPlease select" \
			" a version which should become the current version...",
			options,
			0,
			False)
		if ok:
			base = revs[options.index(choice)]
			with self.__connection.update(self.__doc, base, self.__creator, stores) as w:
				w.setParents(revs)
				w.suspend()
			self.__rev = w.getRev()
			self.__preliminary = True
			print "rev:%s" % self.__rev.encode("hex")
			self.__loadFile()
			self.emitChanged()
			return True
		else:
			return False

	def __mergeAuto(self, baseRev, mergeRevs, stores):
		# see what has changed...
		s = self.__connection.stat(baseRev, stores)
		types = set([s.type()])
		origParts = set(s.parts())
		origHashes = {}
		changedParts = set()
		for part in origParts:
			origHashes[part] = s.hash(part)
		for rev in mergeRevs:
			s = self.__connection.stat(rev, stores)
			types.add(s.type())
			mergeParts = set(s.parts())
			# account for added/removed parts
			changedParts |= mergeParts ^ origParts
			# check for changed parts
			for part in (mergeParts & origParts):
				if s.hash(part) != origHashes[part]:
					changedParts.add(part)

		# vote
		(uti, handledParts) = self.docMergeCheck(len(mergeRevs), types, changedParts)
		if not uti:
			return False # couldn't agree on resulting uti
		if not changedParts.issubset(handledParts):
			return False # not all changed parts are handled

		# don't use that for large documents... ;-)
		mergeReaders = []
		conflicts = False
		try:
			# open all contributing revisions
			for rev in mergeRevs:
				mergeReaders.append(self.__connection.peek(rev, stores))

			with self.__connection.peek(baseRev, stores) as baseReader:
				with self.__connection.update(self.__doc, self.__rev, self.__creator, stores) as writer:
					writer.setType(uti)
					writer.setParents(mergeRevs)
					conflicts = self.docMergePerform(writer, baseReader, mergeReaders, changedParts)
					writer.suspend()
				self.__rev = writer.getRev()
				self.__preliminary = True
				print "rev:%s" % self.__rev.encode("hex")
		finally:
			for r in mergeReaders:
				r.close()

		self.__loadFile()
		self.emitChanged()
		if conflicts:
			QtGui.QMessageBox.warning(self, 'Merge conflict', 'There were merge conflicts. Please check the new version...')
		return True


	# FIXME: This whole method is severely broken. It traverses the _whole_
	# history and selects the merge base based on the mtime. It has also no
	# provisions for criss-cross merges...
	def __calculateMergeBase(self, baseVersions, stores):
		#print "Start: ", [rev.encode('hex') for rev in baseVersions]
		heads = [[rev] for rev in baseVersions] # list of heads for each rev
		paths = [set([rev]) for rev in baseVersions] # visited revs for each rev
		times = { }

		# traverse all paths from all baseVersions
		addedSth = True
		while addedSth:
			addedSth = False
			for i in xrange(len(heads)):
				oldHeads = heads[i]
				newHeads = []
				for head in oldHeads:
					try:
						stat = self.__connection.stat(head, stores)
						parents = stat.parents()
						times[head] = stat.mtime()
						for parent in parents:
							newHeads.append(parent)
							paths[i].add(parent)
							addedSth = True
					except:
						pass
				heads[i] = newHeads

		#print "End:"
		#for path in paths:
		#	print "  Path: ", [rev.encode('hex') for rev in path]

		# determine youngest common version
		commonBase = reduce(lambda x, y: x&y, paths)
		if len(commonBase) == 0:
			return (False, None)
		else:
			# fast-forward merge?
			# criteria: all base versions are in the same path
			for i in xrange(len(baseVersions)):
				path = paths[i]
				all = True
				for rev in baseVersions:
					all = all and (rev in path)
				if all:
					return (True, baseVersions[i])

			# normal merge
			mergeBase = max(commonBase, key=lambda x: times[x])
			return (False, mergeBase)

class DragWidget(QtGui.QLabel):

	def __init__(self, parent):
		super(DragWidget, self).__init__()
		self.__parent = parent
		self.setFrameShadow(QtGui.QFrame.Raised)
		self.setFrameShape(QtGui.QFrame.Box)

	def mousePressEvent(self, event):
		if event.button() == QtCore.Qt.LeftButton:
			self.dragStartPosition = event.pos()

	def mouseMoveEvent(self, event):
		if not (event.buttons() & QtCore.Qt.LeftButton):
			return
		#if (event.pos() - self.dragStartPosition).manhattanLength() < QtGui.QApplication.startDragDistance():
		#	return

		drag = QtGui.QDrag(self)
		mimeData = QtCore.QMimeData()
		doc = self.__parent.doc()
		if doc:
			hpstruct.DocLink(doc, False).mimeData(mimeData)
		else:
			rev = self.__parent.rev()
			hpstruct.RevLink(rev).mimeData(mimeData)

		drag.setMimeData(mimeData)
		drag.setPixmap(self.pixmap())

		dropAction = drag.exec_(QtCore.Qt.CopyAction)


class CommentPopup(object):
	def __init__(self, parent):
		self.__parent = parent
		self.__commentEdit = QtGui.QLineEdit()
		self.__commentAct = QtGui.QWidgetAction(parent)
		self.__commentAct.setDefaultWidget(self.__commentEdit)
		self.__menu = QtGui.QMenu()
		self.__menu.addAction(self.__commentAct)
		self.__menu.addSeparator()
		self.__menu.addAction("Cancel")
		self.__menu.setActiveAction(self.__commentAct)
		QtCore.QObject.connect(
			self.__commentEdit,
			QtCore.SIGNAL("returnPressed()"),
			self.__returnPressed)

	def popup(self, oldComment = ""):
		self.__menu.popup(QtGui.QCursor.pos(), self.__commentAct)
		self.__commentEdit.setText(oldComment)
		self.__commentEdit.selectAll()
		self.__commentEdit.setFocus(QtCore.Qt.OtherFocusReason)

	def __returnPressed(self):
		self.__parent.checkpoint(str(self.__commentEdit.text()), True)


class ChooseWindow(QtGui.QDialog):

	def __init__(self, lookup, doc, revs, preRevs, parent=None):
		super(ChooseWindow, self).__init__(parent)

		self.__doc = doc
		self.__result = (None, False)
		self.__buttons = []

		mainLayout = QtGui.QGridLayout()
		mainLayout.addWidget(QtGui.QLabel("Current revisions:"), 0, 0, 1, 3)
		row = 1
		for rev in revs:
			self.createStoreLine(mainLayout, row, lookup, rev, False)
			row += 1

		if preRevs:
			mainLayout.addWidget(QtGui.QLabel("Pending preliminary revisions:"),
				row, 0, 1, 3)
			row += 1
			for rev in preRevs:
				self.createStoreLine(mainLayout, row, lookup, rev, True)
				row += 1

		okButton = QtGui.QPushButton("Quit")
		okButton.clicked.connect(self.reject)
		buttonsLayout = QtGui.QHBoxLayout()
		buttonsLayout.addStretch()
		buttonsLayout.addWidget(okButton)

		mainLayout.addLayout(buttonsLayout, row, 0, 1, 3)
		self.setLayout(mainLayout)
		self.__layout = mainLayout

		self.setWindowTitle("Choose starting revision")

	def buttonClicked(self, result):
		self.__result = result
		self.accept()

	def getResult(self):
		return self.__result

	def createStoreLine(self, layout, row, lookup, rev, preliminary):
		widgets = []

		button = RevButton(rev, True)
		self.__buttons.append(button)
		widgets.append(button.getWidget())
		layout.addWidget(button.getWidget(), row, 0)

		storesLayout = QtGui.QHBoxLayout()
		stores = lookup.stores(rev)
		for doc in stores:
			button = DocButton(doc, True)
			self.__buttons.append(button)
			widgets.append(button.getWidget())
			storesLayout.addWidget(button.getWidget())
		storesLayout.addStretch()
		layout.addLayout(storesLayout, row, 1)

		openLayout = QtGui.QHBoxLayout()
		if preliminary:
			button1 = QtGui.QPushButton("Purge")
			widgets.append(button1)
			openLayout.addWidget(button1)
			button2 = QtGui.QPushButton("Resume")
			widgets.append(button2)
			openLayout.addWidget(button2)
			button1.clicked.connect(lambda: self.purgeRev(rev, stores, row, widgets))
			button2.clicked.connect(lambda: self.buttonClicked((rev, True)))
		else:
			openLayout.addStretch()
			button = QtGui.QPushButton("Open")
			button.clicked.connect(lambda: self.buttonClicked((rev, False)))
			openLayout.addWidget(button)
		layout.addLayout(openLayout, row, 2)

	def purgeRev(self, rev, stores, row, widgets):
		HpConnector().forget(self.__doc, rev, stores)
		for i in xrange(3):
			item = self.__layout.itemAtPosition(row, i)
			self.__layout.removeItem(item)
		for w in widgets:
			w.deleteLater()


class OverwriteDialog(QtGui.QDialog):

	def __init__(self, lookup, revs, parent=None):
		super(OverwriteDialog, self).__init__(parent)

		self.__result = (None, False)
		self.__buttons = []

		mainLayout = QtGui.QGridLayout()
		mainLayout.addWidget(
			QtGui.QLabel("Choose revision to overwrite or load:"),
			0, 0, 1, 4)
		row = 1
		for rev in revs:
			button = RevButton(rev, True)
			self.__buttons.append(button)
			mainLayout.addWidget(button.getWidget(), row, 0)

			storesLayout = QtGui.QHBoxLayout()
			for doc in lookup.stores(rev):
				button = DocButton(doc, True)
				self.__buttons.append(button)
				storesLayout.addWidget(button.getWidget())
			storesLayout.addStretch()
			mainLayout.addLayout(storesLayout, row, 1)

			button = QtGui.QPushButton("Open")
			button.clicked.connect(lambda: self.buttonClicked((rev, False)))
			mainLayout.addWidget(button, row, 2)
			button = QtGui.QPushButton("Overwrite")
			button.clicked.connect(lambda: self.buttonClicked((rev, True)))
			mainLayout.addWidget(button, row, 3)
			row += 1

		okButton = QtGui.QPushButton("Quit")
		okButton.clicked.connect(self.reject)
		buttonsLayout = QtGui.QHBoxLayout()
		buttonsLayout.addStretch()
		buttonsLayout.addWidget(okButton)

		mainLayout.addLayout(buttonsLayout, row, 0, 1, 4)
		self.setLayout(mainLayout)

		self.setWindowTitle("Choose new revision")

	def buttonClicked(self, result):
		self.__result = result
		self.accept()

	def getResult(self):
		return self.__result

