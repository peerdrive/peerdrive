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
from hotchpotch import HpConnector, hpstruct

# enumerate all stores
enum = HpConnector().enum()
for store in enum.allStores():
	mountName = enum.name(store)

	state = ""
	if enum.isMounted(store):
		state += 'M'
	else:
		state += '-'
	if enum.isSystem(store):
		state += 'S'
	else:
		state += '-'
	if enum.isRemovable(store):
		state += 'R'
	else:
		state += '-'

	if enum.isMounted(store):
		doc = enum.doc(store)
		try:
			rev = HpConnector().lookup(doc).rev(doc)
			with HpConnector().peek(rev) as r:
				metaData = hpstruct.loads(r.readAll('META'))
				realName = metaData["org.hotchpotch.annotation"]["title"]
		except:
			realName = "unknwown"
		print "%s  %s %s  %s [%s]" % (state, store.ljust(8), doc.encode("hex"), realName, mountName)
	else:
		print "%s  %s [%s]" % (state, store.ljust(8), mountName)
