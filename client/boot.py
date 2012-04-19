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

import peerdrive.struct
from peerdrive.importer import importObjectByPath
import json


with open("registry.json") as file:
	registry = json.load(file)

importObjectByPath(
	"sys/registry",
	"org.peerdrive.registry",
	[
		("PDSD", peerdrive.struct.dumps(registry)),
		("META", peerdrive.struct.dumps({
			"org.peerdrive.annotation" : {
				"title"   : "registry"
			}
		}))
	],
	True)

# set default sync rules
importObjectByPath(
	"sys/syncrules",
	"org.peerdrive.syncrules",
	[
		("PDSD", peerdrive.struct.dumps([])),
		("META", peerdrive.struct.dumps({
			"org.peerdrive.annotation" : {
				"title" : "syncrules",
				"description" : "Static synchronization rules between stores"
			}
		}))
	])

# import templates to store
importObjectByPath(
	"sys/templates/Text document",
	"public.plain-text",
	[
		("FILE", "Empty document"),
		("META", peerdrive.struct.dumps({
			"org.peerdrive.annotation" : {
				"title" : "Text document"
			}
		}))
	])
importObjectByPath(
	"sys/templates/Folder",
	"org.peerdrive.folder",
	[
		("PDSD", peerdrive.struct.dumps( [] )),
		("META", peerdrive.struct.dumps({
			"org.peerdrive.annotation" : {
				"title" : "Folder"
			}
		}))
	],
	flags=[peerdrive.connector.Stat.FLAG_STICKY])

