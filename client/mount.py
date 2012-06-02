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

import sys, optparse
from peerdrive import Connector, struct

parser = optparse.OptionParser(usage="usage: %prog [options] <src> <label>")
parser.add_option("-o", "--options",
	help="Comma separated string of mount options")
parser.add_option("-t", "--type", default="file",
	help="Store type (default: %default)")
parser.add_option("-c", "--credentials", default="",
	help="Credentials to mount store")

(options, args) = parser.parse_args()
if len(args) != 2:
	parser.error("incorrect number of arguments")

try:
	sid = Connector().mount(args[0], args[1], type=options.type,
		options=options.options, credentials=options.credentials)
except IOError as error:
	print >>sys.stderr, "Mount failed: " + str(error)
	sys.exit(2)

try:
	rev = Connector().lookupDoc(sid, [sid]).rev(sid)
	with Connector().peek(sid, rev) as r:
		metaData = struct.loads(sid, r.readAll('META'))
		name = metaData["org.peerdrive.annotation"]["title"]
except IOError:
	name = "Unnamed store"
print "Mounted '%s'" % name

