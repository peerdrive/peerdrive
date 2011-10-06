#!/usr/bin/env python
# vim: set fileencoding=utf-8 :
#
# Hotchpotch
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

import hotchpotch.struct
from hotchpotch.importer import importObjectByPath
import json


with open("registry.json") as file:
	registry = json.load(file)

importObjectByPath(
	"sys/registry",
	"org.hotchpotch.registry",
	[
		("HPSD", hotchpotch.struct.dumps(registry)),
		("META", hotchpotch.struct.dumps({
			"org.hotchpotch.annotation" : {
				"title"   : "registry",
				"comment" : "Import by boot.py"
			}
		}))
	],
	True)

# set default sync rules
importObjectByPath(
	"sys/syncrules",
	"org.hotchpotch.syncrules",
	[
		("HPSD", hotchpotch.struct.dumps([])),
		("META", hotchpotch.struct.dumps({
			"org.hotchpotch.annotation" : {
				"title" : "syncrules",
				"description" : "Static synchronization rules between stores"
			}
		}))
	])

# import templates to store
importObjectByPath(
	"sys/templates/Plain text:",
	"public.plain-text",
	[
		("FILE", "Empty document"),
		("META", hotchpotch.struct.dumps({
			"org.hotchpotch.annotation" : {
				"title" : "New, empty text document",
				"comment" : "Created from template"
			}
		}))
	])
importObjectByPath(
	"sys/templates/Folder:",
	"org.hotchpotch.set",
	[
		("HPSD", hotchpotch.struct.dumps( [] )),
		("META", hotchpotch.struct.dumps({
			"org.hotchpotch.annotation" : {
				"title" : "New folder",
				"description" : "A unsorted collection of documents",
				"comment" : "Created from template"
			}
		}))
	],
	flags=[hotchpotch.connector.Stat.FLAG_STICKY])
#importObjectByPath(
#	"sys/templates/Directory:",
#	"org.hotchpotch.dict",
#	[
#		("HPSD", hotchpotch.struct.dumps( {} )),
#		("META", hotchpotch.struct.dumps({
#			"org.hotchpotch.annotation" : {
#				"title" : "New directory",
#				"description" : "A list of documents indexed by distinct names",
#				"comment" : "Created from template"
#			}
#		}))
#	],
#	flags=[hotchpotch.connector.Stat.FLAG_STICKY])

