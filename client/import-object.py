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

import sys, os

from peerdrive import connector
from peerdrive.importer import importObjectByPath


def usage():
	print """Usage: hp-import.py <hp-path-spec> UTI <json> <attachments>

       <hp-path-spec>   = PeerDrive path
       <json>           = JSON data
       <attachments>    = [<name>:<file>]+
"""
	sys.exit(1)


# === main

if len(sys.argv) < 4:
	usage()

# parse command line
objPath = sys.argv[1]
objUti  = sys.argv[2]
objData = connector.loadJSON(sys.argv[3])
objSpec = []
for spec in sys.argv[4:]:
	if spec[4] != ':':
		usage()
	fourCC = spec[:4]
	content = spec[5:]
	with open(content, "rb") as file:
		part = file.read()
	objSpec.append((fourCC, part))

# let's do it
if not importObjectByPath(objPath, objUti, objData, objSpec):
	print "Import failed"

