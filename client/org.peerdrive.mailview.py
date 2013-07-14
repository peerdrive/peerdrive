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

from PyQt4 import QtCore, QtGui, QtWebKit
from peerdrive.gui import widgets, main

import sys
import email
import email.utils
import email.header


def __formatDecode(data, coding):
	if coding:
		return data.decode(coding)
	else:
		return data

def format(addr):
	(name, dest) = addr
	unicodeName = reduce(
		lambda x,y: x + u' ' + y,
		[ __formatDecode(data, coding) for (data, coding) in email.header.decode_header(name) ])
	return email.utils.formataddr((unicodeName, dest))


class MailWidget(widgets.DocumentView):

	def __init__(self):
		super(MailWidget, self).__init__("org.peerdrive.mailview")

		settings = QtWebKit.QWebSettings.globalSettings()
		settings.setAttribute(QtWebKit.QWebSettings.AutoLoadImages, False)
		settings.setAttribute(QtWebKit.QWebSettings.JavascriptEnabled, False)
		settings.setAttribute(QtWebKit.QWebSettings.JavaEnabled, False)
		settings.setAttribute(QtWebKit.QWebSettings.PluginsEnabled, False)
		settings.setAttribute(QtWebKit.QWebSettings.PrivateBrowsingEnabled, True)

		self.htmlView = QtWebKit.QWebView(self)
		self.textView = QtGui.QTextEdit(self)
		self.textView.setReadOnly(True)
		self.textView.setFontFamily("Courier")

		self.view = QtGui.QStackedWidget(self)
		self.view.addWidget(self.htmlView)
		self.view.addWidget(self.textView)
		self.view.setCurrentWidget(self.textView)

		self.header = QtGui.QWidget()
		self.headerLayout = QtGui.QGridLayout()
		#self.headerLayout.setSpacing(0)
		self.headerLayout.setColumnStretch(1, 1)
		self.dateLabel = QtGui.QLabel()
		self.headerLayout.addWidget(QtGui.QLabel("Date:"), 0, 0)
		self.headerLayout.addWidget(self.dateLabel, 0, 1)
		self.fromLabel = QtGui.QLabel()
		self.headerLayout.addWidget(QtGui.QLabel("From:"), 1, 0)
		self.headerLayout.addWidget(self.fromLabel, 1, 1)
		self.toLabel = QtGui.QLabel()
		self.toLabel.setWordWrap(True)
		self.toLabel.setScaledContents(True)
		self.headerLayout.addWidget(QtGui.QLabel("To:"), 2, 0)
		self.headerLayout.addWidget(self.toLabel, 2, 1)
		self.ccLabel = QtGui.QLabel()
		self.ccLabel.setWordWrap(True)
		self.ccLabel.setScaledContents(True)
		self.bccLabel = QtGui.QLabel()
		self.bccLabel.setWordWrap(True)
		self.bccLabel.setScaledContents(True)
		self.header.setLayout(self.headerLayout)

		central = QtGui.QWidget(self)
		layout = QtGui.QVBoxLayout()
		layout.setSpacing(0)
		layout.addWidget(self.header)
		layout.addWidget(self.view)
		central.setLayout(layout)
		self.setCentralWidget(central)

	def docRead(self, readWrite, r):
		self.__mail = email.message_from_string(r.readAll('_'))
		content = self.__findPart(self.__mail)
		if not content:
			self.view.setCurrentWidget(self.textView)
			self.view.setPlainText("Well, I was too stupid to find the mail text...")
		else:
			if "text/html" in content:
				self.view.setCurrentWidget(self.htmlView)
				self.htmlView.setContent(content["text/html"], "text/html")
			elif "text/plain" in content:
				self.view.setCurrentWidget(self.textView)
				self.textView.setPlainText(content["text/plain"])
			else:
				self.view.setCurrentWidget(self.textView)
				self.textView.setPlainText("No suitable content encoding found...")

		self.dateLabel.setText(self.__mail['date'])
		self.fromLabel.setText(self.__getAddresses('from'))
		self.toLabel.setText(self.__getAddresses('to'))
		cc = self.__getAddresses('cc')
		if cc:
			self.ccLabel.setText(cc)
			self.headerLayout.addWidget(QtGui.QLabel("CC:"), 3, 0)
			self.headerLayout.addWidget(self.ccLabel, 3, 1)

	def __findPart(self, part):
		content = {}
		if part.is_multipart():
			subParts = part.get_payload()
			for sub in subParts:
				if sub.get_content_maintype() == 'multipart':
					content.update( self.__findPart(sub) )
				elif sub.get_content_type() == "text/plain":
					data = sub.get_payload(decode=True)
					charset = sub.get_content_charset(None)
					if charset:
						data = data.decode(charset)
					content["text/plain"] = data
				elif sub.get_content_type() == "text/html":
					content["text/html"] = sub.get_payload(decode=True)
		else:
			content[part.get_content_type()] = part.get_payload(decode=True)
		return content

	def __getAddresses(self, field):
		raw = email.utils.getaddresses(self.__mail.get_all(field, []))
		pretty = [ format(addr) for addr in raw ]
		if pretty:
			return reduce(lambda x,y: x+', '+y, pretty)
		else:
			return ""


class MailWindow(main.MainWindow):

	def __init__(self):
		super(MailWindow, self).__init__(MailWidget(), False)
		self.createActions()
		self.createMenus()
		self.createToolBars()

		self.viewWidget().revChanged.connect(self.__readMetaData)

	def createActions(self):
		self.unreadAct = QtGui.QAction(QtGui.QIcon('icons/mail_new.png'), "Mark unread", self)
		self.unreadAct.setStatusTip("Mark the mail as unread")
		self.unreadAct.setCheckable(True)
		self.unreadAct.triggered.connect(self.__unreadChanged)

	def createMenus(self):
		pass

	def createToolBars(self):
		self.mailToolBar = self.addToolBar("Mail")
		self.mailToolBar.addAction(self.unreadAct)

	def __readMetaData(self):
		tags = self.viewWidget().metaDataGetField(widgets.DocumentView.HPA_TAGS, [])
		self.unreadAct.setChecked("unread" in tags)

	def __unreadChanged(self, checked):
		tags = self.viewWidget().metaDataGetField(widgets.DocumentView.HPA_TAGS, [])
		if checked:
			if "unread" not in tags:
				tags.append("unread")
		else:
			tags = [tag for tag in tags if tag != "unread"]
		self.viewWidget().metaDataSetField(widgets.DocumentView.HPA_TAGS, tags)

if __name__ == '__main__':

	import sys

	app = QtGui.QApplication(sys.argv)
	mainWin = MailWindow()
	mainWin.open(sys.argv)
	mainWin.show()
	sys.exit(app.exec_())


