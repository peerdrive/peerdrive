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

from __future__ import absolute_import

from PyQt4 import QtCore, QtGui
import sys, os, subprocess, pickle, datetime

from ..connector import Watch, Connector
from ..registry import Registry
from .. import struct
from .utils import showDocument


class DocButton(QtGui.QToolButton):

	# convenient class to watch store
	class DocumentWatch(Watch):
		def __init__(self, doc, callback):
			self.__callback = callback
			super(DocButton.DocumentWatch, self).__init__(Watch.TYPE_DOC, doc)

		def triggered(self, cause):
			self.__callback(cause)

	def __init__(self, doc=None, withText=False):
		super(DocButton, self).__init__()
		self.__watch = None
		self.__withText = withText
		self.setAutoRaise(True)
		self.clicked.connect(self.__clicked)
		if withText:
			self.setToolButtonStyle(QtCore.Qt.ToolButtonTextBesideIcon)
		self.setDocument(doc)

	def cleanup(self):
		self.setDocument(None)

	def setDocument(self, doc):
		self.__doc = doc
		if self.__watch:
			Connector().unwatch(self.__watch)
			self.__watch = None
		if doc:
			self.__watch = DocButton.DocumentWatch(doc, self.__update)
			Connector().watch(self.__watch)
		self.__update(0)

	def __update(self, cause):
		if cause == Watch.EVENT_DISAPPEARED:
			self.setDocument(None)

		if self.__doc:
			self.setEnabled(True)
			try:
				rev = Connector().lookup_doc(self.__doc).revs()[0]
				docIcon = None
				with Connector().peek(rev) as r:
					try:
						metaData = struct.loads(r.readAll('META'))
						docName = metaData["org.hotchpotch.annotation"]["title"]
						# TODO: individual store icon...
					except:
						docName = "Unnamed"
				if not docIcon:
					uti = Connector().stat(rev).type()
					docIcon = QtGui.QIcon(Registry().getIcon(uti))
			except IOError:
				docName = ''
				docIcon = QtGui.QIcon("icons/uti/file_broken.png")
				self.setEnabled(False)
		else:
			docName = ''
			docIcon = QtGui.QIcon("icons/uti/store.png")
			self.setEnabled(False)

		if len(docName) > 20:
			docName = docName[:20] + '...'
		self.setIcon(docIcon)
		if self.__withText:
			self.setText(docName)
		else:
			self.setToolTip(docName)

	def __clicked(self):
		if self.__doc:
			showDocument(struct.DocLink(self.__doc, False))


class RevButton(QtGui.QToolButton):
	def __init__(self, rev, withText=False):
		super(RevButton, self).__init__()
		self.__rev = rev
		self.font().setItalic(True)

		comment = None
		tags = None
		mtime = None
		try:
			title = "Unnamed"
			with Connector().peek(rev) as r:
				try:
					metaData = struct.loads(r.readAll('META'))
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
			stat = Connector().stat(rev)
			uti = stat.type()
			mtime = stat.mtime()
			revIcon = QtGui.QIcon(Registry().getIcon(uti))
		except IOError:
			title = ''
			revIcon = QtGui.QIcon("icons/uti/file_broken.png")
			self.setEnabled(False)

		if len(title) > 20:
			title = title[:20] + '...'

		self.setIcon(revIcon)
		self.setAutoRaise(True)
		if withText:
			self.setToolButtonStyle(QtCore.Qt.ToolButtonTextBesideIcon)
			self.setText(title)
			toolTip = "Open revision (read only)"
		else:
			toolTip = title + " (open read only)"
		if mtime:
			toolTip += "\n\nMtime: " + str(mtime)
		if comment:
			toolTip += "\nComment: " + comment
		if tags:
			toolTip += "\nTags: " + tags
		self.setToolTip(toolTip)
		self.clicked.connect(self.__clicked)

	def cleanup(self):
		pass

	def __clicked(self):
		showDocument(struct.RevLink(self.__rev))


class DocumentView(QtGui.QStackedWidget, Watch):
	HPA_TITLE        = ["org.hotchpotch.annotation", "title"]
	HPA_TAGS         = ["org.hotchpotch.annotation", "tags"]
	HPA_COMMENT      = ["org.hotchpotch.annotation", "comment"]
	HPA_DESCRIPTION  = ["org.hotchpotch.annotation", "description"]
	SYNC_STICKY      = ["org.hotchpotch.sync", "sticky"]
	SYNC_HISTROY     = ["org.hotchpotch.sync", "history"]

	STATE_NO_DOC = 1
	STATE_EDITING = 2
	STATE_CHOOSE_START = 3
	STATE_CHOOSE_FF = 4
	STATE_CHOOSE_REBASE = 5
	STATE_CHOOSE_ALTERNATE = 6

	# checkpointNeeded: Will get True when a new checkpoint can be created,
	# otherwise it will stay False.
	checkpointNeeded = QtCore.pyqtSignal(bool)

	# mergeNeeded: Goes to True if more than one revision exists while we are
	# editing the document.
	mergeNeeded = QtCore.pyqtSignal(bool)

	mutable = QtCore.pyqtSignal(bool)

	# revChanged: Fired whenever the backing document changes. You should update
	# any meta data you have about the document.
	revChanged = QtCore.pyqtSignal()

	class UserEvent(QtCore.QEvent):
		def __init__(self, eventType, action):
			self.__action = action
			QtCore.QEvent.__init__(self, eventType)

		def execute(self):
			self.__action()

	def __init__(self, parent, widget, creator):
		self.__userEventType = QtCore.QEvent.registerEventType()
		QtGui.QStackedWidget.__init__(self, parent)

		self.__creator = creator
		self.__doc = None
		self.__rev = None
		self.__mutable = False
		self.__state = DocumentView.STATE_NO_DOC
		self.__saveNeeded = False
		self.__preliminary = False
		self.__mergeNeeded = False

		self.__saveTimer = QtCore.QTimer(self)
		self.__saveTimer.timeout.connect(lambda: self.__saveFile("<<Periodic checkpoint>>"))
		self.__saveTimer.setSingleShot(True)
		self.__saveTimer.setInterval(10000)

		self.__noDocWidget = QtGui.QLabel("No doc")
		self.addWidget(self.__noDocWidget)
		self.__editWidget = widget
		self.addWidget(self.__editWidget)
		self.__chooseOverwriteWidget = QtGui.QLabel("TODO: Choose overwrite target")
		self.addWidget(self.__chooseOverwriteWidget)
		self.__chooseSaveAsWidget = QtGui.QLabel("TODO: Choose new location")
		self.addWidget(self.__chooseSaveAsWidget)

		self.setCurrentWidget(self.__noDocWidget)

	def open(self, guid, isDoc):
		self.__mutable = isDoc
		if isDoc:
			self.__doc = guid
			Watch.__init__(self, Watch.TYPE_DOC, guid)
			self.__chooseRevWidget = ChooseWidget(self, guid, self.__chooseRev)
			self.addWidget(self.__chooseRevWidget)
		else:
			self.__rev = guid
			Watch.__init__(self, Watch.TYPE_REV, guid)
		Connector().watch(self)
		self.__update()

	def doc(self):
		return self.__doc

	def rev(self):
		return self.__rev

	def save(self):
		self.__saveFile("<<Periodic checkpoint>>")

	def checkpoint(self, comment, forceComment=False):
		# explicitly set comment, the user expects it's comment to be applied
		if forceComment:
			self.metaDataSetField(DocumentView.HPA_COMMENT, comment)
		self.__saveFile(comment)
		if self.__preliminary:
			with Connector().resume(self.__doc, self.__rev) as writer:
				writer.commit()
			self.__rev = writer.getRev()
			self.__setPreliminary(False)
			self.__emitNewRev()
			self.__sync()

	def merge(self, rev):
		self.checkpoint("<<Save before merge>>")

		lookup = Connector().lookup_doc(self.__doc)
		stores = set(lookup.stores(self.__rev))
		stores |= set(lookup.stores(rev))
		revs = [self.__rev, rev]

		# determine common ancestor
		(fastForward, base) = self.__calculateMergeBase(revs, stores)
		#print "ff:%s, base:%s" % (fastForward, base.encode("hex"))
		if base:
			# fast-forward merge?
			if fastForward:
				self.__rev = Connector().sync(self.__doc, stores=stores)
				self.__loadFile()
				return

			# can the application help?
			if self.__mergeAuto(base, revs, stores):
				return

		if self.__mergeOurs(revs, stores):
			return

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
		self.__metaDataChanged = True
		self._emitSaveNeeded()

	def metaDataGetField(self, field, default):
		item = self.__metaData
		for step in field:
			if step in item:
				item = item[step]
			else:
				return default
		return item

	# === re-implemented inherited methods

	def triggered(self, event):
		#print >>sys.stderr, "watch %d for %s" % (event, self.getHash().encode('hex'))
		self.__postEvent(lambda: self.__update())

	def event(self, event):
		if event.type() == self.__userEventType:
			event.execute()
			return True
		else:
			return QtGui.QStackedWidget.event(self, event)

	# === protected methods

	def _emitSaveNeeded(self):
		self.__setSaveNeeded(True)

	# === private methods

	def __setSaveNeeded(self, state):
		if self.__saveNeeded == state:
			return
		self.__saveNeeded = state
		if state:
			self.__saveTimer.start()
		else:
			self.__saveTimer.stop()
		self.checkpointNeeded.emit(state or self.__preliminary)

	def __setPreliminary(self, state):
		if self.__preliminary == state:
			return
		self.__preliminary = state
		self.checkpointNeeded.emit(state or self.__saveNeeded)

	def __setMergeNeeded(self, state):
		if self.__mergeNeeded == state:
			return
		self.__mergeNeeded = state
		self.mergeNeeded.emit(state)

	def __emitNewRev(self):
		print "rev:%s" % self.__rev.encode("hex")
		sys.stdout.flush()
		self.revChanged.emit()

	def __postEvent(self, action):
		event = DocumentView.UserEvent(self.__userEventType, action)
		QtGui.QApplication.postEvent(self, event)

	def __update(self):
		if self.__mutable:
			self.__updateDoc()
		else:
			self.__updateRev()

	def __updateRev(self):
		avail = len(Connector().lookup_rev(self.__rev)) > 0
		if self.__state == DocumentView.STATE_NO_DOC:
			self.__setState(DocumentView.STATE_EDITING)
		else:
			self.__setState(DocumentView.STATE_NO_DOC)

	def __updateDoc(self):
		if self.__state == DocumentView.STATE_NO_DOC:
			l = Connector().lookup_doc(self.__doc)
			revs = l.revs()
			preRevs = filter(self.__filterPreRev, l.preRevs())
			if (len(revs) == 1) and (preRevs == []):
				self.__rev = revs[0]
				self.__setPreliminary(False)
				self.__setState(DocumentView.STATE_EDITING)
				self.__emitNewRev()
			elif (len(revs) > 1) or (len(preRevs) > 0):
				self.__setState(DocumentView.STATE_CHOOSE_START)
		elif self.__state == DocumentView.STATE_EDITING:
			# are we gone?
			info = Connector().lookup_doc(self.__doc)
			if ((not self.__preliminary and (self.__rev in info.revs())) or
			(self.__preliminary and (self.__rev in info.preRevs()))):
				# still there, make a preliminary commit if necessary
				self.__saveFile('<<Internal checkpoint>>')

				# ok, whats up?
				currentRevs = set(info.revs())
				if self.__preliminary:
					validRevs = set(Connector().stat(self.__rev).parents())
				else:
					validRevs = set([self.__rev])

				# check if we're still up-to-date
				if currentRevs.isdisjoint(validRevs):
					if self.__preliminary:
						self.__setState(DocumentView.STATE_CHOOSE_REBASE)
					else:
						self.__setState(DocumentView.STATE_CHOOSE_FF)
				else:
					self.__setMergeNeeded(bool(currentRevs - validRevs))
			else:
				# we've disappeared
				self.__setState(DocumentView.STATE_CHOOSE_ALTERNATE)
		elif self.__state == DocumentView.STATE_CHOOSE_START:
			self.__updateDocStartRev()
		elif self.__state == DocumentView.STATE_CHOOSE_FF:
			self.__updateDocFastForward()
		elif self.__state == DocumentView.STATE_CHOOSE_REBASE:
			self.__updateDocRebase()
		elif self.__state == DocumentView.STATE_CHOOSE_ALTERNATE:
			self.__updateDocSaveAs()

	def __setState(self, state):
		self.__state = state
		if self.__state == DocumentView.STATE_EDITING:
			self.checkpointNeeded.emit(self.__preliminary or self.__saveNeeded)
			self.mergeNeeded.emit(self.__mergeNeeded)
			self.mutable.emit(self.__mutable)
			self.__loadFile()
			self.setCurrentWidget(self.__editWidget)
		else:
			self.checkpointNeeded.emit(False)
			self.mergeNeeded.emit(False)
			self.mutable.emit(False)
			if self.__state == DocumentView.STATE_NO_DOC:
				self.setCurrentWidget(self.__noDocWidget)
			elif self.__state == DocumentView.STATE_CHOOSE_START:
				self.__updateDocStartRev()
				self.setCurrentWidget(self.__chooseRevWidget)
			elif self.__state == DocumentView.STATE_CHOOSE_FF:
				self.__updateDocFastForward()
				self.setCurrentWidget(self.__chooseRevWidget)
			elif self.__state == DocumentView.STATE_CHOOSE_REBASE:
				self.__updateDocRebase()
				self.setCurrentWidget(self.__chooseOverwriteWidget)
			elif self.__state == DocumentView.STATE_CHOOSE_ALTERNATE:
				self.__updateDocSaveAs()
				self.setCurrentWidget(self.__chooseSaveAsWidget)

	def __updateDocStartRev(self):
		l = Connector().lookup_doc(self.__doc)
		revs = l.revs()
		preRevs = filter(self.__filterPreRev, l.preRevs())
		if (len(revs) == 1) and (preRevs == []):
			self.__rev = revs[0]
			self.__setPreliminary(False)
			self.__setState(DocumentView.STATE_EDITING)
			self.__emitNewRev()
		else:
			self.__chooseRevWidget.updateChoices(l, revs, preRevs)

	def __updateDocFastForward(self):
		# find all heads which lead to current rev
		found = []
		target = self.__rev
		try:
			lookup = Connector().lookup_doc(self.__doc)
			depth = Connector().stat(target).mtime() - datetime.timedelta(days=1)
		except IOError:
			# seems we're gone
			self.__setState(DocumentView.STATE_CHOOSE_ALTERNATE)
			return
		heads = [(rev, set([rev])) for rev in lookup.revs()]
		while heads:
			oldHeads = heads
			heads = []
			for (rev, tips) in oldHeads:
				newTips = set()
				for tip in tips:
					try:
						stat = Connector().stat(tip)
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
			self.__setPreliminary(False)
			self.__setState(DocumentView.STATE_EDITING)
			self.__emitNewRev()
		elif found == []:
			# if no head found quit
			self.__setState(DocumentView.STATE_CHOOSE_ALTERNATE)
		else:
			self.__chooseRevWidget.updateChoices(lookup, found, [])

	def __updateDocRebase(self):
		# get current revs on all stores where the preliminary versions exists
		heads = set()
		lookup = Connector().lookup_doc(self.__doc)
		for store in lookup.stores(self.__rev):
			heads.add(lookup.rev(store))

		# ask user which rev he wants to overwrite or discard current prerev
		self.__chooseOverwriteWidget.updateChoices(lookup, heads)

	def __updateDocSaveAs(self):
		info = Connector().lookup_doc(self.__doc)
		if ((not self.__preliminary and (self.__rev in info.revs())) or
			(self.__preliminary and (self.__rev in info.preRevs()))):
			# document appeared again
			self.__setState(DocumentView.STATE_EDITING)

	def __chooseRev(self, rev, preliminary):
		self.__rev = rev
		self.__setPreliminary(preliminary)
		self.__setState(DocumentView.STATE_EDITING)
		self.__emitNewRev()

	def __loadFile(self):
		QtGui.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
		try:
			with Connector().peek(self.__rev) as r:
				try:
					self.__metaData = struct.loads(r.readAll('META'))
				except IOError:
					self.__metaData = { }
				self.__metaDataChanged = False
				self.docRead(self.__mutable, r)
		finally:
			QtGui.QApplication.restoreOverrideCursor()

	def __saveFile(self, comment = ""):
		if self.__mutable and self.__saveNeeded:
			if self.__preliminary:
				with Connector().resume(self.__doc, self.__rev) as writer:
					self.__saveFileInternal(comment, writer)
					writer.suspend()
			else:
				with Connector().update(self.__doc, self.__rev, self.__creator) as writer:
					self.__saveFileInternal(comment, writer)
					writer.suspend()
			self.__metaDataChanged = False
			self.__rev = writer.getRev()
			self.__setPreliminary(True)
			self.__emitNewRev()
			self.__setSaveNeeded(False)

	def __saveFileInternal(self, comment, writer):
		QtGui.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
		try:
			self.metaDataSetField(DocumentView.HPA_COMMENT, comment)
			if self.__metaDataChanged:
				writer.writeAll('META', struct.dumps(self.__metaData))
			self.docSave(writer)
		finally:
			QtGui.QApplication.restoreOverrideCursor()

	def __sync(self):
		lookup = Connector().lookup_doc(self.__doc)
		stat = Connector().stat(self.__rev)

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
			Connector().sync(self.__doc, stores=stores)

	def __filterPreRev(self, rev):
		try:
			return Connector().stat(rev).creator() == self.__creator
		except IOError:
			return False

	def __mergeOurs(self, revs, stores):
		# last resort: "ours"-merge
		options = []
		for rev in revs:
			revDate = Connector().stat(rev, stores).mtime()
			revStores = Connector().lookup_rev(rev, stores)
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
			with Connector().update(self.__doc, base, self.__creator, stores) as w:
				w.setParents(revs)
				w.suspend()
			self.__rev = w.getRev()
			self.__setPreliminary(True)
			self.__loadFile()
			self.__emitNewRev()
			return True
		else:
			return False

	def __mergeAuto(self, baseRev, mergeRevs, stores):
		# see what has changed...
		s = Connector().stat(baseRev, stores)
		types = set([s.type()])
		origParts = set(s.parts())
		origHashes = {}
		changedParts = set()
		for part in origParts:
			origHashes[part] = s.hash(part)
		for rev in mergeRevs:
			s = Connector().stat(rev, stores)
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
				mergeReaders.append(Connector().peek(rev, stores))

			with Connector().peek(baseRev, stores) as baseReader:
				with Connector().update(self.__doc, self.__rev, self.__creator, stores) as writer:
					writer.setType(uti)
					writer.setParents(mergeRevs)
					conflicts = self.docMergePerform(writer, baseReader, mergeReaders, changedParts)
					writer.suspend()
				self.__rev = writer.getRev()
				self.__setPreliminary(True)
				print "rev:%s" % self.__rev.encode("hex")
		finally:
			for r in mergeReaders:
				r.close()

		self.__loadFile()
		self.__emitNewRev()
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
						stat = Connector().stat(head, stores)
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


class ChooseWidget(QtGui.QWidget):

	def __init__(self, parent, doc, action):
		super(ChooseWidget, self).__init__(parent)
		self.__doc = doc
		self.__action = action
		self.__revs = {}
		self.__preRevs = {}

		self.__revsLayout = QtGui.QVBoxLayout()
		self.__preRevsLayout = QtGui.QVBoxLayout()

		mainLayout = QtGui.QVBoxLayout()
		mainLayout.addWidget(QtGui.QLabel("Current revisions:"))
		mainLayout.addLayout(self.__revsLayout)
		mainLayout.addWidget(QtGui.QLabel("Pending preliminary revisions:"))
		mainLayout.addLayout(self.__preRevsLayout)
		self.setLayout(mainLayout)

	def updateChoices(self, lookup, revs, preRevs):
		for rev in set(self.__revs.keys()) ^ set(revs):
			if rev in revs:
				line = self.createStoreLine(lookup, rev, False)
				self.__revs[rev] = line
				self.__revsLayout.addWidget(line)
			else:
				self.__revs[rev].setParent(None)
				del self.__revs[rev]

		for rev in set(self.__preRevs.keys()) ^ set(preRevs):
			if rev in preRevs:
				line = self.createStoreLine(lookup, rev, True)
				self.__preRevs[rev] = line
				self.__preRevsLayout.addWidget(line)
			else:
				self.__preRevs[rev].setParent(None)
				del self.__preRevs[rev]

	def createStoreLine(self, lookup, rev, preliminary):
		widget = QtGui.QWidget()
		layout = QtGui.QHBoxLayout()

		layout.addWidget(RevButton(rev, True))

		stores = lookup.stores(rev)
		for doc in stores:
			layout.addWidget(DocButton(doc, True))

		layout.addStretch()
		if preliminary:
			button1 = QtGui.QPushButton("Purge")
			button1.clicked.connect(lambda: Connector().forget(self.__doc, rev, stores))
			layout.addWidget(button1)
			button2 = QtGui.QPushButton("Resume")
			button2.clicked.connect(lambda: self.__action(rev, True))
			layout.addWidget(button2)
		else:
			button = QtGui.QPushButton("Open")
			button.clicked.connect(lambda: self.__action(rev, False))
			layout.addWidget(button)

		widget.setLayout(layout)
		return widget


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

