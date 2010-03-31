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

from hotchpotch import hpstruct
from hotchpotch.importer import importObjectByPath
from simplejson import loads as loadJSON


with open("registry.json") as file:
	registry = loadJSON(file.read())

importObjectByPath(
	"sys/registry",
	"org.hotchpotch.registry",
	[
		("HPSD", hpstruct.dumps(registry)),
		("META", hpstruct.dumps({
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
		("HPSD", hpstruct.dumps([])),
		("META", hpstruct.dumps({
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
		("META", hpstruct.dumps({
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
		("HPSD", hpstruct.dumps( {} )),
		("META", hpstruct.dumps({
			"org.hotchpotch.annotation" : {
				"title" : "New, empty dictionary",
				"comment" : "Created from template"
			},
			"org.hotchpotch.sync" : {
				"sticky" : True
			}
		}))
	])
importObjectByPath(
	"sys/templates:Document templates/Collection:",
	"org.hotchpotch.set",
	[
		("HPSD", hpstruct.dumps( [] )),
		("META", hpstruct.dumps({
			"org.hotchpotch.annotation" : {
				"title" : "New, empty collection",
				"comment" : "Created from template"
			},
			"org.hotchpotch.sync" : {
				"sticky" : True
			}
		}))
	])

