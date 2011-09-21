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

import sys
from hotchpotch import Connector, struct

if len(sys.argv) == 2:
	if Connector().mount(sys.argv[1]):
		sid = Connector().enum().doc(sys.argv[1])
		try:
			rev = Connector().lookupDoc(sid, [sid]).rev(sid)
			with Connector().peek(sid, rev) as r:
				metaData = struct.loads(sid, r.readAll('META'))
				name = metaData["org.hotchpotch.annotation"]["title"]
		except:
			name = "Unnamed store"
		print "Mounted '%s'" % name
	else:
		print "Mount failed"
		sys.exit(2)
else:
	print "Usage: hp-mount.py <id>"
	sys.exit(1)

