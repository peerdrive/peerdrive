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

from __future__ import with_statement
import sys, os

from hotchpotch import hpstruct
from hotchpotch.importer import importFileByPath


def usage():
	print """Usage: hp-import-file.py <hp-path-spec> file

       <hp-path-spec>   = tag | 7bf187e7... (unique UUID prefix)
"""
	sys.exit(1)


# === main

if len(sys.argv) != 3:
	usage()

# parse command line
importPath = sys.argv[1]
importFile = sys.argv[2]

# let's do it
importFileByPath(importPath, importFile)

