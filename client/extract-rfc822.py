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

import sys, email, email.utils, email.header, json
from datetime import datetime

with open(sys.argv[1]) as fp:
	msg = msg = email.message_from_file(fp)

tos = msg.get_all('to', [])
ccs = msg.get_all('cc', [])
resent_tos = msg.get_all('resent-to', [])
resent_ccs = msg.get_all('resent-cc', [])
allRecipients = email.utils.getaddresses(tos + ccs + resent_tos + resent_ccs)

def __decode(data, coding):
	if coding:
		return data.decode(coding).replace('\n', '')
	else:
		return data.replace('\n', '')

def decodeHeader(header):
	return reduce(
		lambda x,y: x + u' ' + y,
		[ __decode(data, coding) for (data, coding) in email.header.decode_header(header) ])

def format(addr):
	(name, dest) = addr
	unicodeName = decodeHeader(name)
	return email.utils.formataddr((unicodeName, dest))



# basic data
data = {
	"org.peerdrive.annotation" : {
		"title" : decodeHeader(msg['subject']),
		"tags" : ["unread"]
	},
	"public.message" : {
		"from" : format(email.utils.parseaddr(msg['from'])),
		"to"   : [ format(addr) for addr in allRecipients ],
		"date" : long(email.utils.mktime_tz(email.utils.parsedate_tz(msg['date'])))
	}
}

if msg['Message-Id']:
	data["public.message"]["rfc822"] = {}
	data["public.message"]["rfc822"]["id"] = msg['Message-Id']

# attachments
attachments = []
for part in msg.walk():
	# multipart/* are just containers
	if part.get_content_maintype() == 'multipart':
		continue
	name = part.get_filename()
	if name:
		attachments.append(name)

if attachments != []:
	if "rfc822" not in data["public.message"]:
		data["public.message"]["rfc822"] = {}
	data["public.message"]["rfc822"]["attachments"] = attachments

print json.dumps(data)

