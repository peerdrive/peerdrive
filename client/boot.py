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
				"title"   : "UTI registry",
				"comment" : "Import by boot.py"
			}
		}))
	],
	True)

# set default sync rules
importObjectByPath(
	"sys/syncrules",
	"public.data",
	[
		("HPSD", hotchpotch.struct.dumps([])),
		("META", hotchpotch.struct.dumps({
			"org.hotchpotch.annotation" : {
				"title" : "Synchronization rules"
			}
		}))
	])

# import templates to store
importObjectByPath(
	"sys/templates:Document templates/Plain text:",
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
	"sys/templates:Document templates/Dictionary:",
	"org.hotchpotch.dict",
	[
		("HPSD", hotchpotch.struct.dumps( {} )),
		("META", hotchpotch.struct.dumps({
			"org.hotchpotch.annotation" : {
				"title" : "New, empty dictionary",
				"comment" : "Created from template"
			},
			"org.hotchpotch.sync" : {
				"sticky"  : True,
				"history" : 31*24*60*60 # 1 month
			}
		}))
	])
importObjectByPath(
	"sys/templates:Document templates/Collection:",
	"org.hotchpotch.set",
	[
		("HPSD", hotchpotch.struct.dumps( [] )),
		("META", hotchpotch.struct.dumps({
			"org.hotchpotch.annotation" : {
				"title" : "New, empty collection",
				"comment" : "Created from template"
			},
			"org.hotchpotch.sync" : {
				"sticky"  : True,
				"history" : 31*24*60*60 # 1 month
			}
		}))
	])

