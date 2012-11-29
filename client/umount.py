#!/usr/bin/env python
# vim: set fileencoding=utf-8 :
#
# PeerDrive
# Copyright (C) 2012  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
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

parser = optparse.OptionParser(usage="usage: %prog [options] <label>")
parser.add_option("-r", "--remove", action="store_true",
	help="Remove mount label from fstab")

(options, args) = parser.parse_args()
if len(args) != 1:
	parser.error("incorrect number of arguments")

label = args[0]
try:
	store = Connector().enum().fromLabel(label)
	if store is None:
		print >>sys.stderr, "Label '%s' not mounted" % label
		if not options.remove:
			sys.exit(2)
	else:
		Connector().unmount(store.sid)
except IOError as error:
	print "Unmount failed: " + str(error)
	sys.exit(2)

if options.remove:
	fstab = struct.FSTab()
	if label in fstab.knownLabels():
		try:
			fstab.remove(label)
			fstab.save()
		except IOError as error:
			print "Remove failed: " + str(error)
			sys.exit(2)
	else:
		print >>sys.stderr, "Remove failed: label '%s' not found in fstab" % label

