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

def printStore(store, verbose):
	line = "'%s' as '%s' type '%s'" % (store.src, store.label, store.type)
	if store.options:
		line += " (" + store.options + ")"
	if verbose:
		line += " [" + store.sid.encode("hex") + "]"
	print line

parser = optparse.OptionParser()
parser.add_option("-v", "--verbose", action="store_true", help="Verbose output")

(options, args) = parser.parse_args()

# enumerate all stores
enum = Connector().enum()
if options.verbose:
	printStore(enum.sysStore(), True)
for store in enum.regularStores():
	printStore(store, options.verbose)
