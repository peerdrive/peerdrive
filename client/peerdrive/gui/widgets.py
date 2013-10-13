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

from PyQt4 import QtCore, QtGui
import sys, os, subprocess, pickle, datetime

from ..connector import Watch, Connector
from ..registry import Registry
from .. import struct
from .utils import showDocument, showProperties


class AbortException(Exception):
	def __init__(self):
		pass

class DocButton(QtGui.QToolButton, Watch):

	def __init__(self, store=None, doc=None, withText=False, checkable=False):
		QtGui.QToolButton.__init__(self)
		self.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
		self.__watching = None
		self.__withText = withText
		self.__checkable = checkable
		self.__docName = ""
		self.setAutoRaise(True)
		if checkable:
			self.setCheckable(True)
		else:
			self.clicked.connect(self.__clicked)
		self.customContextMenuRequested.connect(self.__showContextMenu)
		self.setDocument(store, doc)

	def cleanup(self):
		self.setDocument(None, None)

	def setDocument(self, store, doc):
		self.__store = store
		self.__doc = doc
		if self.__watching:
			Connector().unwatch(self)
			self.__watching = False
		if doc:
			Watch.__init__(self, Watch.TYPE_DOC, doc)
			Connector().watch(self)
			self.__watching = True
		self.triggered(Watch.EVENT_APPEARED, store)

	def triggered(self, cause, store):
		try:
			if cause == Watch.EVENT_REPLICATED:
				return
			elif cause == Watch.EVENT_DIMINISHED:
				return
			elif cause == Watch.EVENT_DISAPPEARED:
				raise IOError()
			elif self.__doc is None:
				raise IOError()

			self.setEnabled(True)
			l = Connector().lookupDoc(self.__doc, [self.__store])
			if self.__store not in l.stores():
				raise IOError()
			rev = l.rev(self.__store)
			docIcon = None
			with Connector().peek(self.__store, rev) as r:
				try:
					docName = r.getData("/org.peerdrive.annotation/title")
				except:
					docName = "Unnamed"
			if not docIcon:
				uti = Connector().stat(rev, [self.__store]).type()
				docIcon = QtGui.QIcon(Registry().getIcon(uti))

			if self.__doc == self.__store:
				label = Connector().enum().fromSId(self.__store).label
				docName = '[' + label + '] ' + docName
		except IOError:
			docName = ''
			docIcon = QtGui.QIcon("icons/uti/file_broken.png")
			self.setEnabled(False)

		if len(docName) > 20:
			docName = docName[:20] + '...'
		self.setIcon(docIcon)
		self.__docName = docName
		self.__updateText()

	def setChecked(self, checked):
		QtGui.QToolButton.setChecked(self, checked)
		self.__updateText()

	def __updateText(self):
		self.setText(self.__docName)
		if self.__withText or (self.__checkable and self.isChecked()):
			self.setToolButtonStyle(QtCore.Qt.ToolButtonTextBesideIcon)
			self.setToolTip('')
		else:
			self.setToolButtonStyle(QtCore.Qt.ToolButtonIconOnly)
			self.setToolTip(self.__docName)

	def __clicked(self):
		if self.__doc:
			showDocument(connector.DocLink(self.__store, self.__doc, False))

	def __showContextMenu(self, pos):
		type = None
		executables = []
		rev = Connector().lookupDoc(self.__doc, [self.__store]).rev(self.__store)
		try:
			type = Connector().stat(rev, [self.__store]).type()
			executables = Registry().getExecutables(type)
		except IOError:
			pass

		menu = QtGui.QMenu()

		if Registry().conformes(type, "org.peerdrive.folder"):
			m = menu.addMenu(self.__docName)
			l = connector.DocLink(self.__store, self.__doc, False)
			m.aboutToShow.connect(lambda m=m, l=l: self.__fillMenu(m, l))
			menu.addSeparator()

		action = menu.addAction("Open")
		action.triggered.connect(self.__clicked)
		menu.setDefaultAction(action)
		if len(executables) > 1:
			openMenu = menu.addMenu("Open with")
			for e in executables:
				action = openMenu.addAction(e)
				action.triggered.connect(
					lambda x,s=self.__store,d=self.__doc,e=e: showDocument(connector.DocLink(s, d, False), executable=e))
		menu.addSeparator()
		action = menu.addAction("Properties")
		action.triggered.connect(lambda x,s=self.__store,d=self.__doc:
			showProperties(connector.DocLink(s, d, False)))

		menu.exec_(self.mapToGlobal(pos))

	def __fillMenu(self, menu, menuLink):
		menu.clear()
		c = struct.Folder(menuLink)
		listing = []
		for (title, link) in c.items():
			link.update(self.__store)
			try:
				type = Connector().stat(link.rev()).type()
			except IOError:
				type = None

			if not type:
				continue

			if len(title) > 40:
				title = title[:40] + '...'

			listing.append((title, link, Registry().conformes(type,
				"org.peerdrive.folder"), QtGui.QIcon(Registry().getIcon(type))))

		listing = sorted(listing, cmp=DocButton.__cmp)

		for (title, link, folder, icon) in listing:
			if folder:
				m = menu.addMenu(icon, title)
				m.aboutToShow.connect(lambda m=m, l=link: self.__fillMenu(m, l))
			else:
				a = menu.addAction(icon, title)
				a.triggered.connect(lambda x,l=link,r=menuLink: showDocument(l, referrer=r))

	@staticmethod
	def __cmp((t1,l1,f1,i1), (t2,l2,f2,i2)):
		ret = f2 - f1
		if ret == 0:
			ret = cmp(t1.lower(), t2.lower())
		return ret


class RevButton(QtGui.QToolButton):
	def __init__(self, store, rev, withText=False):
		super(RevButton, self).__init__()
		self.__store = store
		self.__rev = rev
		self.font().setItalic(True)

		comment = None
		tags = None
		mtime = None
		try:
			title = "Unnamed"
			with Connector().peek(store, rev) as r:
				try:
					metaData = r.getData("/org.peerdrive.annotation")
					if "title" in metaData:
						title = metaData["title"]
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
			comment = stat.comment()
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
		showDocument(connector.RevLink(self.__store, self.__rev))


class DocumentView(QtGui.QStackedWidget, Watch):
	HPA_TITLE        = ["title"]
	HPA_TAGS         = ["tags"]
	HPA_DESCRIPTION  = ["description"]

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

	# mutable: Reflects if the document is currently mutable or read-only
	mutable = QtCore.pyqtSignal(bool)

	# revChanged: Fired whenever the backing document changes. You should update
	# any meta data you have about the document.
	revChanged = QtCore.pyqtSignal()

	# fired whenever the documents distribution changed
	distributionChanged = QtCore.pyqtSignal()

	class UserEvent(QtCore.QEvent):
		def __init__(self, eventType, action):
			self.__action = action
			QtCore.QEvent.__init__(self, eventType)

		def execute(self):
			self.__action()

	def __init__(self, creator, parent=None):
		self.__userEventType = QtCore.QEvent.registerEventType()
		QtGui.QStackedWidget.__init__(self, parent)

		self.__creator = creator
		self.__open = False
		self.__doc = None
		self.__rev = None
		self.__store = None
		self.__mutable = False
		self.__state = DocumentView.STATE_NO_DOC
		self.__saveNeeded = False
		self.__preliminary = False
		self.__mergeNeeded = False

		self.__saveTimer = QtCore.QTimer(self)
		self.__saveTimer.timeout.connect(lambda: self.__saveFile())
		self.__saveTimer.setSingleShot(True)
		self.__saveTimer.setInterval(10000)

		self.__noDocWidget = QtGui.QLabel("The document is no longer accessible")
		self.__noDocWidget.setAlignment(QtCore.Qt.AlignCenter)
		self.addWidget(self.__noDocWidget)
		self.__chooseSaveAsWidget = QtGui.QLabel("TODO: Choose new location")
		self.addWidget(self.__chooseSaveAsWidget)
		self.__chooseRevWidget = None
		self.__chooseOverwriteWidget = None

		self.setCurrentWidget(self.__noDocWidget)

	def setCentralWidget(self, widget):
		self.__editWidget = widget
		self.addWidget(self.__editWidget)

	def docOpen(self, store, guid, isDoc):
		self.docClose()
		self.__store = store
		self.__mutable = isDoc
		if isDoc:
			#print "doc:%s" % guid.encode("hex")
			self.__doc = guid
			Watch.__init__(self, Watch.TYPE_DOC, guid)
			self.__chooseRevWidget = _ChooseWidget(self, guid, self.__chooseRev)
			self.addWidget(self.__chooseRevWidget)
			self.__chooseOverwriteWidget = _OverwriteWidget(self, self.__rebase)
			self.addWidget(self.__chooseOverwriteWidget)
		else:
			self.__rev = guid
			Watch.__init__(self, Watch.TYPE_REV, guid)
		Connector().watch(self)
		self.__open = True
		self.__update()

	def docClose(self, save=True):
		if self.__open:
			self.__open = False
			if self.__state != DocumentView.STATE_NO_DOC:
				if (self.__state == DocumentView.STATE_EDITING) and save:
					self.__saveFile('<<Document closed>>')
				self.__setState(DocumentView.STATE_NO_DOC)
			if self.__chooseRevWidget:
				self.removeWidget(self.__chooseRevWidget)
				self.__chooseRevWidget = None
			if self.__chooseOverwriteWidget:
				self.removeWidget(self.__chooseOverwriteWidget)
				self.__chooseOverwriteWidget = None
			Connector().unwatch(self)
			self.__doc = None
			self.__rev = None
			self.__mutable = False

	def docRevert(self):
		if not (self.__open and self.__mutable):
			return
		store = self.__store
		doc = self.__doc
		rev = self.__rev
		pre = self.__preliminary
		self.docClose(False)
		if pre:
			Connector().forget(store, doc, rev)
		self.docOpen(store, doc, True)

	def switchStore(self, store):
		if self.__doc:
			doc = self.__doc
			self.docClose()
			self.docOpen(store, doc, True)
		else:
			rev = self.__rev
			self.docClose()
			self.docOpen(store, rev, False)

	def doc(self):
		return self.__doc

	def rev(self):
		return self.__rev

	def store(self):
		return self.__store

	def save(self, comment=None):
		self.__saveFile(comment)

	def checkpoint(self, comment=None):
		# explicitly set comment, the user expects it's comment to be applied
		self.__saveFile(comment)
		if self.__preliminary:
			with Connector().resume(self.__store, self.__doc, self.__rev) as writer:
				writer.commit()
			self.__rev = writer.getRev()
			self.__setPreliminary(False)
			self.__emitNewRev()

	def merge(self, store, rev):
		self.checkpoint("<<Save before merge>>")

		# determine common ancestor
		(fastForward, base) = self.__calculateMergeBase(store, rev)
		#print "ff:%s, base:%s" % (fastForward, base.encode("hex"))
		if base:
			# fast-forward merge?
			if fastForward:
				if base == self.__rev:
					QtGui.QMessageBox.warning(self, 'Merge revision outdated',
						'The selected revision is outdated')
				else:
					Connector().forwardDoc(self.__store, self.__doc, self.__rev,
						rev, store)
					self.__rev = rev
					self.__loadFile()
				return

			# can the application help?
			if self.__mergeAuto(store, rev, base):
				return

		if self.__mergeOurs(store, rev):
			return

	def docSave(self, writer):
		pass

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
		if 'META' in changedParts:
			baseMeta = struct.loads(self.__store, baseReader.readAll('META'))
			mergeMeta = []
			for mr in mergeReaders:
				mergeMeta.append(struct.loads(self.__store, mr.readAll('META')))
			(newMeta, conflict) = struct.merge(baseMeta, mergeMeta)
			writer.writeAll('META', struct.dumps(newMeta))
		return False

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

	def metaDataSetFlag(self, flag, value):
		oldFlags = self.__flags
		if value:
			self.__flags.add(flag)
		else:
			self.__flags.discard(flag)

		if self.__flags != oldFlags:
			self._emitSaveNeeded()

	def metaDataGetFlag(self, flag):
		return flag in self.__flags

	# === re-implemented inherited methods

	def triggered(self, event, store):
		#print >>sys.stderr, "watch %d for %s" % (event, self.getHash().encode('hex'))
		if event != Watch.EVENT_MODIFIED:
			self.distributionChanged.emit()
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

	def _loadSettings(self, settings):
		# will be called by folder, may be reimplemented...
		pass

	def _saveSettings(self, settings):
		# will be called by folder, may be reimplemented...
		pass

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
		#print "rev:%s" % self.__rev.encode("hex")
		#sys.stdout.flush()
		self.revChanged.emit()

	def __postEvent(self, action):
		event = DocumentView.UserEvent(self.__userEventType, action)
		QtGui.QApplication.postEvent(self, event)

	def __update(self):
		if self.__rev is None and self.__doc is None:
			return
		elif self.__mutable:
			self.__updateDoc()
		else:
			self.__updateRev()

	def __updateRev(self):
		avail = len(Connector().lookupRev(self.__rev, [self.__store])) > 0
		if (self.__state == DocumentView.STATE_NO_DOC) and avail:
			self.__setState(DocumentView.STATE_EDITING)
			self.__emitNewRev()
		elif (self.__state == DocumentView.STATE_EDITING) and not avail:
			self.__setState(DocumentView.STATE_NO_DOC)

	def __updateDoc(self):
		if self.__state == DocumentView.STATE_NO_DOC:
			l = Connector().lookupDoc(self.__doc, [self.__store])
			revs = l.revs()
			preRevs = filter(self.__filterPreRev, l.preRevs())
			if (len(revs) == 1) and (preRevs == []):
				self.__rev = revs[0]
				self.__setPreliminary(False)
				self.__setState(DocumentView.STATE_EDITING)
				self.__emitNewRev()
			elif len(revs) == 1:
				self.__setState(DocumentView.STATE_CHOOSE_START)
		elif self.__state == DocumentView.STATE_EDITING:
			# are we gone?
			stores = Connector().lookupRev(self.__rev)
			info = Connector().lookupDoc(self.__doc)
			if self.__store in (set(stores) & set(info.stores())):
				# still there, make a preliminary commit if necessary
				self.__saveFile()

				# ok, whats up?
				if self.__preliminary:
					validRevs = set(Connector().stat(self.__rev).parents())
				else:
					validRevs = set([self.__rev])

				# check if we're still up-to-date
				if info.rev(self.__store) in validRevs:
					currentRevs = set(info.revs())
					self.__setMergeNeeded(bool(currentRevs - validRevs))
				else:
					if self.__preliminary:
						if not self.__tryRebase():
							self.__setState(DocumentView.STATE_CHOOSE_REBASE)
					else:
						self.__setState(DocumentView.STATE_CHOOSE_FF)
			else:
				# we've disappeared
				if self.__mutable and self.__saveNeeded:
					self.__setState(DocumentView.STATE_CHOOSE_ALTERNATE)
				else:
					self.__setState(DocumentView.STATE_NO_DOC)
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
				self.setCurrentWidget(self.__chooseRevWidget)
			elif self.__state == DocumentView.STATE_CHOOSE_FF:
				self.setCurrentWidget(self.__chooseRevWidget)
			elif self.__state == DocumentView.STATE_CHOOSE_REBASE:
				self.setCurrentWidget(self.__chooseOverwriteWidget)
			elif self.__state == DocumentView.STATE_CHOOSE_ALTERNATE:
				self.setCurrentWidget(self.__chooseSaveAsWidget)

		# try to sync with the document state
		if self.__open:
			self.__update()

	def __updateDocStartRev(self):
		l = Connector().lookupDoc(self.__doc, [self.__store])
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
			lookup = Connector().lookupDoc(self.__doc, [self.__store])
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
		lookup = Connector().lookupDoc(self.__doc, [self.__store])
		for store in lookup.stores(self.__rev):
			heads.add(lookup.rev(store))

		# ask user which rev he wants to overwrite or discard current prerev
		self.__chooseOverwriteWidget.updateChoices(lookup, heads)

	def __updateDocSaveAs(self):
		info = Connector().lookupDoc(self.__doc, [self.__store])
		if ((not self.__preliminary and (self.__rev in info.revs())) or
			(self.__preliminary and (self.__rev in info.preRevs()))):
			# document appeared again
			self.__setState(DocumentView.STATE_EDITING)

	def __chooseRev(self, rev, preliminary):
		self.__rev = rev
		self.__setPreliminary(preliminary)
		self.__setState(DocumentView.STATE_EDITING)
		self.__emitNewRev()

	def __rebase(self, rev, overwrite):
		if overwrite:
			with Connector().resume(self.__store, self.__doc, self.__rev) as writer:
				writer.rebase(rev)
				writer.suspend()
			self.__rev = writer.getRev()
		else:
			Connector().forget(self.__store, self.__doc, self.__rev)
			self.__rev = rev
			self.__setPreliminary(False)
		self.__setState(DocumentView.STATE_EDITING)
		self.__emitNewRev()

	# Tries to handle the common case where the meta data was changed externally.
	# We'll look if our preRev and the current main rev have the same parent
	# and then try a automatic merge...
	def __tryRebase(self):
		mainRev = Connector().lookupDoc(self.__doc, [self.__store]).rev(self.__store)
		mainStat = Connector().stat(mainRev, [self.__store])
		preStat = Connector().stat(self.__rev, [self.__store])

		# common single parent?
		parents = set(mainStat.parents()) & set(preStat.parents())
		if len(parents) != 1:
			return False
		[baseRev] = parents

		# ler's try
		return self.__mergeAuto(self.__store, mainRev, baseRev, True)

	def __loadFile(self):
		QtGui.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
		try:
			with Connector().peek(self.__store, self.__rev) as r:
				try:
					self.__metaData = r.getData("/org.peerdrive.annotation")
					self.__flags = r.stat().flags()
				except IOError:
					self.__metaData = { }
					self.__flags = set()
				self.__metaDataChanged = False
				self.docRead(self.__mutable, r)
				self.__setSaveNeeded(False)
		finally:
			QtGui.QApplication.restoreOverrideCursor()

	def __saveFile(self, comment = None):
		if self.__mutable and self.__saveNeeded:
			if self.__preliminary:
				with Connector().resume(self.__store, self.__doc, self.__rev) as writer:
					self.__saveFileInternal(writer)
					writer.suspend(comment)
			else:
				with Connector().update(self.__store, self.__doc, self.__rev, self.__creator) as writer:
					self.__saveFileInternal(writer)
					writer.suspend(comment)
			self.__metaDataChanged = False
			self.__rev = writer.getRev()
			self.__setPreliminary(True)
			self.__emitNewRev()
			self.__setSaveNeeded(False)

	def __saveFileInternal(self, writer):
		QtGui.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
		try:
			if self.__metaDataChanged:
				writer.setData("/org.peerdrive.annotation", self.__metaData)
				writer.setFlags(self.__flags)
			self.docSave(writer)
		finally:
			QtGui.QApplication.restoreOverrideCursor()

	def __filterPreRev(self, rev):
		try:
			return Connector().stat(rev).creator() == self.__creator
		except IOError:
			return False

	def __getStoreName(self, store):
		try:
			rev = Connector().lookupDoc(store).rev(store)
			with Connector().peek(store, rev) as r:
				try:
					return r.getData("/org.peerdrive.annotation/title")
				except:
					return "Unnamed store"
		except:
			return None

	def __mergeOurs(self, mergeStore, mergeRev):
		# last resort: "ours"-merge
		options = []
		for rev in revs:
			revDate = Connector().stat(rev, stores).mtime()
			revStores = Connector().lookupRev(rev, stores)
			storeNames = reduce(
				lambda x,y: x+", "+y,
				[self.__getStoreName(s) for s in revStores])
			options.append(str(revDate) + ": " + storeNames)

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
			with Connector().update(self.__store, self.__doc, base, self.__creator) as w:
				for rev in revs:
					w.merge(revs)
				w.suspend()
			self.__rev = w.getRev()
			self.__setPreliminary(True)
			self.__loadFile()
			self.__emitNewRev()
			return True
		else:
			return False

	def __mergeAuto(self, mergeStore, mergeRev, baseRev, rebase=False):
		stores = [self.__store, mergeStore]

		# see what has changed...
		s = Connector().stat(baseRev, stores)
		types = set([s.type()])
		origParts = set(s.parts())
		origHashes = {}
		changedParts = set()
		for part in origParts:
			origHashes[part] = s.hash(part)
		for rev in [self.__rev, mergeRev]:
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
		(uti, handledParts) = self.docMergeCheck(2, types, changedParts)
		if not uti:
			return False # couldn't agree on resulting uti
		if not changedParts.issubset(handledParts):
			return False # not all changed parts are handled

		# don't use that for large documents... ;-)
		mergeReaders = []
		conflicts = False
		try:
			# open all contributing revisions
			mergeReaders.append(Connector().peek(self.__store, self.__rev))
			mergeReaders.append(Connector().peek(mergeStore, mergeRev))

			if self.__preliminary:
				updateFun = Connector().resume
			else:
				updateFun = Connector().update

			with Connector().peek(Connector().lookupRev(baseRev)[0], baseRev) as baseReader:
				with updateFun(self.__store, self.__doc, self.__rev, self.__creator) as writer:
					writer.setType(uti)
					conflicts = self.docMergePerform(writer, baseReader, mergeReaders, changedParts)
					if rebase:
						if conflicts:
							raise AbortException
						writer.rebase(mergeRev)
					else:
						writer.merge(mergeStore, mergeRev)
					writer.suspend("<<Automatic merge>>")
				self.__rev = writer.getRev()
				self.__setPreliminary(True)
		except AbortException:
			return False
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
	def __calculateMergeBase(self, store, mergeRev):
		stores = [self.__store, store]
		baseVersions = [self.__rev, mergeRev]
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


class _ChooseWidget(QtGui.QWidget):

	def __init__(self, parent, doc, action):
		super(_ChooseWidget, self).__init__(parent)
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
				line = self.__createStoreLine(lookup, rev, False)
				self.__revs[rev] = line
				self.__revsLayout.addWidget(line)
			else:
				self.__revs[rev].setParent(None)
				del self.__revs[rev]

		for rev in set(self.__preRevs.keys()) ^ set(preRevs):
			if rev in preRevs:
				line = self.__createStoreLine(lookup, rev, True)
				self.__preRevs[rev] = line
				self.__preRevsLayout.addWidget(line)
			else:
				self.__preRevs[rev].setParent(None)
				del self.__preRevs[rev]

	def __createStoreLine(self, lookup, rev, preliminary):
		widget = QtGui.QWidget()
		layout = QtGui.QHBoxLayout()

		stores = lookup.stores(rev)
		layout.addWidget(RevButton(stores[0], rev, True))
		for doc in stores:
			layout.addWidget(DocButton(doc, doc, True))

		layout.addStretch()
		if preliminary:
			button1 = QtGui.QPushButton("Purge")
			button1.clicked.connect(lambda: Connector().forget(self.__doc, rev, stores))
			layout.addWidget(button1)
			button2 = QtGui.QPushButton("Resume")
			button2.clicked.connect(lambda: self.__trigger(rev, True))
			layout.addWidget(button2)
		else:
			button = QtGui.QPushButton("Open")
			button.clicked.connect(lambda: self.__trigger(rev, False))
			layout.addWidget(button)

		widget.setLayout(layout)
		return widget

	def __trigger(self, rev, preliminary):
		self.updateChoices(None, [], [])
		self.__action(rev, preliminary)


class _OverwriteWidget(QtGui.QWidget):

	def __init__(self, parent, action):
		super(_OverwriteWidget, self).__init__(parent)

		self.__action = action
		self.__revs = {}
		self.__layout = QtGui.QVBoxLayout()
		label = QtGui.QLabel(
			"Your document contains currently unsaved changes which are " +
			"based on an outdated revision. Choose a revision below to " +
			"overwrite or load (and discarding your current changes):")
		label.setWordWrap(True)
		self.__layout.addWidget(label)
		self.setLayout(self.__layout)

	def updateChoices(self, lookup, revs):
		for rev in set(self.__revs.keys()) ^ set(revs):
			if rev in revs:
				line = self.__createStoreLine(lookup, rev)
				self.__revs[rev] = line
				self.__layout.addWidget(line)
			else:
				self.__revs[rev].setParent(None)
				del self.__revs[rev]

	def __createStoreLine(self, lookup, rev):
		widget = QtGui.QWidget()
		layout = QtGui.QHBoxLayout()

		stores = lookup.stores(rev)
		layout.addWidget(RevButton(stores[0], rev, True))
		for doc in stores:
			layout.addWidget(DocButton(doc, doc, True))

		layout.addStretch()
		button = QtGui.QPushButton("Open")
		button.clicked.connect(lambda: self.__trigger(rev, False))
		layout.addWidget(button)
		button = QtGui.QPushButton("Overwrite")
		button.clicked.connect(lambda: self.__trigger(rev, True))
		layout.addWidget(button)

		widget.setLayout(layout)
		return widget

	def __trigger(self, rev, overwrite):
		self.updateChoices(None, [])
		self.__action(rev, overwrite)

