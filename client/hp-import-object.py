#!/usr/bin/env python
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

import sys, os

from hotchpotch import struct
from hotchpotch.importer import importObjectByPath


def usage():
	print """Usage: hp-import.py <hp-path-spec> UTI <hp-obj-spec>

       <hp-path-spec>   = tag | 7bf187e7... (unique UUID prefix)
       <hp-obj-spec>    = [FOUR:<content>]+
       <content> = local file | json data
"""
	sys.exit(1)


# === main

if len(sys.argv) < 4:
	usage()

# parse command line
objPath = sys.argv[1]
objUti  = sys.argv[2]
objSpec = []
for spec in sys.argv[3:]:
	if spec[4] != ':':
		usage()
	fourCC = spec[:4]
	content = spec[5:]
	if os.path.isfile(content):
		with open(content, "rb") as file:
			part = file.read()
	else:
		part = struct.dumps(struct.loadJSON(content))
	objSpec.append((fourCC, part))

# let's do it
if not importObjectByPath(objPath, objUti, objSpec):
	print "Import failed"

