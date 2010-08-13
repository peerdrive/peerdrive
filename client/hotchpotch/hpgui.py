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

from hpconnector import HpWatch, HpConnector
from hpregistry import HpRegistry
import hpstruct


def showDocument(link):
	if isinstance(link, hpstruct.DocLink):
		args = ['doc:'+link.uuid().encode('hex')]
		rev = HpConnector().lookup(link.uuid()).revs()[0]
	else:
		args = ['rev:'+link.rev().encode('hex')]
		rev = link.rev()
	uti = HpConnector().stat(rev).uti()
	executable = HpRegistry().getExecutable(uti)
	if executable:
		if sys.platform == "win32":
			subprocess.Popen([executable] + args, shell=True)
		else:
			executable = './' + executable
			os.spawnv(os.P_NOWAIT, executable, [executable] + args)


def showProperties(link):
	if isinstance(link, hpstruct.DocLink):
		args = ['doc:'+link.uuid().encode('hex')]
	else:
		args = ['rev:'+link.rev().encode('hex')]
	if sys.platform == "win32":
		subprocess.Popen(['properties.py'] + args, shell=True)
	else:
		os.spawnv(os.P_NOWAIT, './properties.py', ['./properties.py'] + args)


class HpMainWindow(QtGui.QMainWindow, HpWatch):
	HPA_TITLE        = ["org.hotchpotch.annotation", "title"]
	HPA_TAGS         = ["org.hotchpotch.annotation", "tags"]
	HPA_COMMENT      = ["org.hotchpotch.annotation", "comment"]
	HPA_DESCRIPTION  = ["org.hotchpotch.annotation", "description"]
	SYNC_STICKY      = ["org.hotchpotch.sync", "sticky"]
	SYNC_HISTROY     = ["org.hotchpotch.sync", "history"]

	def __init__(self, argv, uti, isEditor):
		QtGui.QMainWindow.__init__(self)
		self.setAttribute(QtCore.Qt.WA_DeleteOnClose)
		self.__uti        = uti
		self.__isEditor   = isEditor
		self.__utiPixmap  = None
		self.__connection = HpConnector()
		self.__storeButtons = { }

		# parse command line
		if len(argv) == 2 and argv[1].startswith('doc:'):
			self.__uuid = argv[1][4:].decode("hex")
			self.__rev = None
		elif len(argv) == 2 and argv[1].startswith('rev:'):
			self.__uuid = None
			self.__rev = argv[1][4:].decode("hex")
		elif len(argv) == 2:
			link = hpstruct.resolvePath(argv[1])
			if isinstance(link, hpstruct.DocLink):
				self.__uuid = link.uuid()
				self.__rev = None
			else:
				self.__uuid = None
				self.__rev = link.rev()
		else:
			print "Usage: %s <Document>" % (self.__uti)
			print
			print "Document:"
			print "    doc:<UUID>      ...open the latest version of the given document"
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

			self.__stickyAct = QtGui.QAction("Sticky", self)
			self.__stickyAct.setStatusTip("Automatically replicate referenced documents")
			self.__stickyAct.setCheckable(True)
			QtCore.QObject.connect(self.__stickyAct, QtCore.SIGNAL("triggered(bool)"), self.__toggleSticky)

			self.__histroyAct = QtGui.QAction("Replicate histroy", self)
			self.__histroyAct.setStatusTip("Replicate whole history")
			self.__histroyAct.setCheckable(True)
			self.__histroyAct.setEnabled(False)
			QtCore.QObject.connect(self.__histroyAct, QtCore.SIGNAL("triggered(bool)"), self.__toggleHistroy)

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

		self.__propertiesAct = QtGui.QAction("&Properties", self)
		QtCore.QObject.connect(self.__propertiesAct, QtCore.SIGNAL("triggered()"), self.__showProperties)

		self.delAct = QtGui.QAction(QtGui.QIcon('icons/edittrash.png'), "&Delete", self)
		self.delAct.setStatusTip("Delete the document")
		QtCore.QObject.connect(self.delAct, QtCore.SIGNAL("triggered()"), self.__deleteFile)

		self.__exitAct = QtGui.QAction("Close", self)
		self.__exitAct.setShortcut("Ctrl+Q")
		self.__exitAct.setStatusTip("Close the document")
		QtCore.QObject.connect(self.__exitAct, QtCore.SIGNAL("triggered()"), self.close)

		# create standard menu
		self.fileMenu = self.menuBar().addMenu("&Document")
		if isEditor:
			self.fileMenu.addAction(self.__saveAct)
		self.fileMenu.addAction(self.delAct)
		if isEditor:
			self.repMenu = self.fileMenu.addMenu("Replication")
			self.repMenu.addAction(self.__stickyAct)
			self.repMenu.addAction(self.__histroyAct)
		self.fileMenu.addAction(self.__propertiesAct)

		# standard tool bars
		self.fileToolBar = self.addToolBar("Document")
		self.dragWidget = DragWidget(self)
		self.fileToolBar.addWidget(self.dragWidget)
		self.fileToolBar.addSeparator()
		if isEditor:
			self.fileToolBar.addAction(self.__saveAct)
		self.fileToolBar.addAction(self.delAct)

		# save comment popup
		self.__commentPopup = CommentPopup(self)

		# disable for now
		self.delAct.setEnabled(True)

		# load settings
		self.__loadSettings()

	# === public methods

	def mainWindowInit(self):
		self.fileMenu.addSeparator();
		self.fileMenu.addAction(self.__exitAct)
		self.fileToolBar.addAction(self._annotationDock.toggleViewAction())
		self._annotationDock.hide()

		# see what we have to do
		if self.__uuid:
			revs = self.__connection.lookup(self.__uuid).revs()
			if len(revs) == 0:
				print "Could not find the document"
				sys.exit(2)
			elif len(revs) == 1:
				self.__rev = revs[0]
				self.statusBar().showMessage("Document loaded", 2000)
			else:
				self.__rev = self.__merge(revs)
				print "rev:%s" % self.__rev.encode("hex")
			self.__setMutable(True)
			self.__loadFile(True, self.__rev)
			HpWatch.__init__(self, HpWatch.TYPE_UUID, self.__uuid)
		else:
			self.__setMutable(False)
			self.__loadFile(False, self.__rev)
			self.statusBar().showMessage("Revision loaded", 2000)
			HpWatch.__init__(self, HpWatch.TYPE_REV, self.__rev)
		self.__connection.watch(self)

		# window icon
		self.setWindowIcon(QtGui.QIcon(self.__getUtiPixmap()))
		self.dragWidget.setPixmap(self.__getUtiPixmap())
		self.__update()

	def uuid(self):
		return self.__uuid

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

	# returns (uti, handled) where:
	#   uti:     the resulting UTI (if we would handle it)
	#   handled: set of parts which this instance can merge automatically
	def docMergeCheck(self, heads, utis, changedParts):
		# don't care about the number of heads
		if len(utis) != 1:
			return (None, set()) # cannot merge different types
		return (utis.copy().pop(), changedParts & set(['META']))

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

	def emitChanged(self):
		if self.__isEditor:
			self.__saveAct.setEnabled(True)

	def saveSettings(self, settings):
		settings["resx"] = self.size().width()
		settings["resy"] = self.size().height()
		settings["posx"] = self.pos().x()
		settings["posy"] = self.pos().y()

	def loadSettings(self, settings):
		self.resize(settings["resx"], settings["resy"])
		self.move(settings["posx"], settings["posy"])

	# === re-implemented inherited methods

	def closeEvent(self, event):
		event.accept()
		self.saveFile("<<Automatically saved>>")
		self.__connection.unwatch(self)
		self.__saveSettings()

	def triggered(self, cause):
		#print >>sys.stderr, "watch %d for %s" % (cause, self.getHash().encode('hex'))
		if cause == HpWatch.CAUSE_DISAPPEARED:
			# FIXME: maybe we don't want to loose all changes...
			self.__mutable = False # prevent useless saving
			self.close()
		else:
			self.__update()

	# === private methods

	def __merge(self, revs):
		# determine common ancestor
		(fastForward, base) = self.__calculateMergeBase(revs)
		#print "ff:%s, base:%s" % (fastForward, base.encode("hex"))
		if base:
		#try:
			# fast-forward merge?
			if fastForward:
				self.statusBar().showMessage("Document merged by fast-forward", 10000)
				return self.__connection.mergeTrivial(self.__uuid, base, [])

			# can the application help?
			newRev = self.__mergeAuto(base, revs)
			if newRev:
				self.statusBar().showMessage("Document merged automatically", 10000)
				return newRev
		#except IOError:
		#	pass # ignore...

		newRev = self.__mergeOurs(revs)
		if newRev:
			self.statusBar().showMessage("Document merged by user choice", 10000)
			return newRev
		else:
			print "Needs merge..."
			sys.exit(3)

	def __mergeOurs(self, revs):
		# last resort: "ours"-merge
		options = []
		for rev in revs:
			revInfo = self.__connection.stat(rev)
			date = str(revInfo.mtime())
			volumes = None
			for vol in revInfo.volumes():
				if volumes:
					volumes += ", "
				else:
					volumes = ""
				volumes += self.__getStoreName(vol)
			options.append(date + ": " + volumes)

		(choice, ok) = QtGui.QInputDialog.getItem(
			self,
			"Select version",
			"The document currently exists in different versions.\nPlease select" \
			" a version which should become the current version...",
			options,
			0,
			False)
		if ok:
			i = options.index(choice)
			base = revs[i]
			otherRevs = revs[:]
			del otherRevs[i]
			#print "merge uuid:%s, base:%s, otherRevs:%s" % (
			#	self.__uuid.encode("hex"),
			#	base.encode("hex"),
			#	map(lambda x: x.encode("hex"), otherRevs))
			return self.__connection.mergeTrivial(self.__uuid, base, otherRevs)
		else:
			return None

	def __mergeAuto(self, baseRev, mergeRevs):
		# see what has changed...
		s = self.__connection.stat(baseRev)
		utis = set([s.uti()])
		origParts = set(s.parts())
		origHashes = {}
		changedParts = set()
		for part in origParts:
			origHashes[part] = s.hash(part)
		for rev in mergeRevs:
			s = self.__connection.stat(rev)
			utis.add(s.uti())
			mergeParts = set(s.parts())
			# account for added/removed parts
			changedParts |= mergeParts ^ origParts
			# check for changed parts
			for part in (mergeParts & origParts):
				if s.hash(part) != origHashes[part]:
					changedParts.add(part)

		# vote
		(uti, handledParts) = self.docMergeCheck(len(mergeRevs), utis, changedParts)
		if not uti:
			return None # couldn't agree on resulting uti
		if not changedParts.issubset(handledParts):
			return None # not all changed parts are handled

		# don't use that for large documents... ;-)
		commonParts = origParts - changedParts
		mergeReaders = []
		newRev = None
		conflicts = False
		try:
			# open all contributing revisions
			for rev in mergeRevs:
				mergeReaders.append(self.__connection.peek(rev))

			with self.__connection.peek(baseRev) as baseReader:
				with self.__connection.merge(self.__uuid, mergeRevs, uti) as writer:
					for part in commonParts:
						writer.writeAll(part, baseReader.readAll(part))
					conflicts = self.docMergePerform(writer, baseReader, mergeReaders, changedParts)
					newRev = writer.commit()
		finally:
			for r in mergeReaders:
				r.done()

		if conflicts:
			QtGui.QMessageBox.warning(self, 'Merge conflict', 'There were merge conflicts. Please check the new version...')
		return newRev


	# FIXME: This whole method is severely broken. It traverses the _whole_
	# history and selects the merge base based on the mtime. It has also no
	# provisions for criss-cross merges...
	def __calculateMergeBase(self, baseVersions):
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
						stat = self.__connection.stat(head)
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

	def __loadFile(self, mutable, rev):
		QtGui.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
		try:
			with self.__connection.peek(rev) as r:
				try:
					self.__metaData = hpstruct.loads(r.readAll('META'))
				except IOError:
					self.__metaData = { }
				self.__metaDataChanged = False
				self.docRead(mutable, r)
			self.__extractMetaData()
			if self.__isEditor:
				self.__saveAct.setEnabled(False)
		finally:
			QtGui.QApplication.restoreOverrideCursor()

	def saveFile(self, comment = ""):
		if self.__isMutable() and self.needSave():
			with self.__connection.update(self.__uuid, self.__rev) as writer:
				self.__saveFileInternal(comment, False, writer)
				newRev = writer.commit()
			print "rev:%s" % newRev.encode("hex")
			sys.stdout.flush()
			self.__rev = newRev
			if self.__isEditor:
				self.__saveAct.setEnabled(False)

	def __saveFileInternal(self, comment, force, writer):
		QtGui.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
		try:
			self.metaDataSetField(HpMainWindow.HPA_COMMENT, comment)
			if force or self.__metaDataChanged:
				writer.writeAll('META', hpstruct.dumps(self.__metaData))
				self.__metaDataChanged = False
			if self.__isEditor:
				self.docCheckpoint(writer, force)
		finally:
			QtGui.QApplication.restoreOverrideCursor()

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
				uti = self.__connection.stat(self.__rev).uti()
			else:
				uti = self.__uti
			self.__utiPixmap = QtGui.QPixmap(HpRegistry().getIcon(uti))
		return self.__utiPixmap

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
			self.__stickyAct.setEnabled(self.__mutable)
			self.__stickyAct.setChecked(self.metaDataGetField(HpMainWindow.SYNC_STICKY, False))
			self.__histroyAct.setEnabled(self.__mutable and self.__stickyAct.isChecked())
			self.__histroyAct.setChecked(self.metaDataGetField(HpMainWindow.SYNC_HISTROY, False))

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
		self.__histroyAct.setEnabled(checked)

	def __toggleHistroy(self, checked):
		self.metaDataSetField(HpMainWindow.SYNC_HISTROY, checked)

	def __checkpointFile(self):
		self.__commentPopup.popup(self.metaDataGetField(HpMainWindow.HPA_COMMENT, "Enter comment"))

	def __deleteFile(self):
		menu = QtGui.QMenu(self)
		options = {}
		if self.__isMutable():
			volumes = self.__connection.lookup(self.__uuid).stores()
		else:
			volumes = self.__connection.stat(self.__rev).volumes()
		for store in volumes:
			name = self.__getStoreName(store)
			if name:
				action = menu.addAction("Delete item from '%s'" % name)
				options[action] = store

		if len(options) > 1:
			menu.addSeparator()
			delAllAction = menu.addAction("Delete from all stores")
		else:
			delAllAction = None
		menu.addSeparator()
		menu.addAction("Cancel")
		choice = menu.exec_(QtGui.QCursor.pos())

		if choice is None:
			volumes = []
		elif choice in options:
			volumes = [options[choice]]
		elif choice is delAllAction:
			volumes = options.values()
		else:
			volumes = []

		for volume in volumes:
			if self.__isMutable():
				self.__connection.delete_uuid(volume, self.__uuid)
			else:
				self.__connection.delete_rev(volume, self.__rev)
			# the watch will trigger and close the window

	def __getStoreName(self, store):
		try:
			rev = self.__connection.lookup(store).rev(store)
			with self.__connection.peek(rev) as r:
				try:
					metaData = hpstruct.loads(r.readAll('META'))
					return metaData["org.hotchpotch.annotation"]["title"]
				except:
					return "Unnamed store"
		except:
			return None

	def __update(self):
		if self.__isMutable():
			info = self.__connection.lookup(self.__uuid)
			allStores = info.stores()

			# check for all the nasty conditions
			revs = info.revs()
			if len(revs) > 1:
				self.__updateWithMerge(revs)
				self.__loadFile(True, self.__rev)
			elif revs[0] != self.__rev:
				# changed from somewhere else
				if self.needSave():
					self.__updateWithMerge(revs)
				else:
					self.__rev = revs[0]
				self.__loadFile(True, self.__rev)
		else:
			allStores = self.__connection.stat(self.__rev).volumes()

		for store in allStores:
			if store not in self.__storeButtons:
				button = DocButton(store)
				self.statusBar().addPermanentWidget(button.getWidget())
				self.__storeButtons[store] = button
		remStores = set(self.__storeButtons) - set(allStores)
		for store in remStores:
			self.statusBar().removeWidget(self.__storeButtons[store].getWidget())
			del self.__storeButtons[store]

	def __updateWithMerge(self, revs):
		tmpUuid = None
		tmpStore = None
		mergeRevs = revs[:]

		self.__connection.unwatch(self)
		try:
			if self.needSave():
				s = self.__connection.stat(self.__rev)
				tmpStore = s.volumes()[0]
				with self.__connection.fork(tmpStore, s.uti(), self.__rev) as w:
					self.__saveFileInternal("<<Temporary automatic checkpoint>>", True, w)
					mergeRevs.append(w.commit())
					tmpUuid = w.getUUID()

			self.__rev = self.__merge(mergeRevs)

		finally:
			if tmpUuid:
				self.__connection.delete_uuid(tmpStore, tmpUuid)
		self.__connection.watch(self)

	def __showProperties(self):
		if self.__isMutable():
			link = hpstruct.DocLink(self.__uuid, False)
		else:
			link = hpstruct.RevLink(self.__rev)
		showProperties(link)

	def __saveSettings(self):
		if self.__uuid:
			hash = self.__uuid.encode('hex')
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
		if self.__uuid:
			hash = self.__uuid.encode('hex')
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
		self.__parent.saveFile(str(self.__commentEdit.text()))


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
		uuid = self.__parent.uuid()
		if uuid:
			hpstruct.DocLink(uuid, False).mimeData(mimeData)
		else:
			rev = self.__parent.rev()
			hpstruct.RevLink(rev).mimeData(mimeData)
			

		drag.setMimeData(mimeData)
		drag.setPixmap(self.pixmap())

		dropAction = drag.exec_(QtCore.Qt.CopyAction)


class DocButton(object):

	# convenient class to watch store
	class DocumentWatch(HpWatch):
		def __init__(self, uuid, callback):
			self.__callback = callback
			super(DocButton.DocumentWatch, self).__init__(HpWatch.TYPE_UUID, uuid)
		
		def triggered(self, cause):
			self.__callback(cause)

	def __init__(self, uuid=None, withText=False):
		self.__watch = None
		self.__withText = withText
		self.__button = QtGui.QToolButton()
		self.__button.setAutoRaise(True)
		QtCore.QObject.connect(self.__button, QtCore.SIGNAL("clicked()"), self.__clicked)
		if withText:
			self.__button.setToolButtonStyle(QtCore.Qt.ToolButtonTextBesideIcon)
		self.setDocument(uuid)

	def cleanup(self):
		self.setDocument(None)

	def setDocument(self, uuid):
		self.__uuid = uuid
		if self.__watch:
			HpConnector().unwatch(self.__watch)
			self.__watch = None
		if uuid:
			self.__watch = DocButton.DocumentWatch(uuid, self.__update)
			HpConnector().watch(self.__watch)
		self.__update(0)

	def __update(self, cause):
		if cause == HpWatch.CAUSE_DISAPPEARED:
			self.setDocument(None)

		if self.__uuid:
			self.__button.setEnabled(True)
			try:
				rev = HpConnector().lookup(self.__uuid).revs()[0]
				docIcon = None
				with HpConnector().peek(rev) as r:
					try:
						metaData = hpstruct.loads(r.readAll('META'))
						docName = metaData["org.hotchpotch.annotation"]["title"]
						# TODO: individual store icon...
					except:
						docName = "Unnamed"
				if not docIcon:
					uti = HpConnector().stat(rev).uti()
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
		if self.__uuid:
			showDocument(hpstruct.DocLink(self.__uuid, False))

	def getWidget(self):
		return self.__button


class RevButton(object):
	def __init__(self, rev, withText=False):
		self.__rev = rev
		self.__button = QtGui.QToolButton()
		self.__button.font().setItalic(True)

		try:
			with HpConnector().peek(rev) as r:
				try:
					metaData = hpstruct.loads(r.readAll('META'))
					title = metaData["org.hotchpotch.annotation"]["title"]
				except:
					title = "Unnamed"
			uti = HpConnector().stat(rev).uti()
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
			self.__button.setToolTip("Open revision (read only)")
		else:
			self.__button.setToolTip(title)
		QtCore.QObject.connect(self.__button, QtCore.SIGNAL("clicked()"), self.__clicked)

	def cleanup(self):
		pass

	def __clicked(self):
		showDocument(hpstruct.RevLink(self.__rev))

	def getWidget(self):
		return self.__button


