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

from __future__ import absolute_import

import os
from . import struct
from .connector import Connector

def findFuseFile(link):
	global _root

	if '_root' not in globals():
		try:
			_root = Connector().sysInfo("vfs.mountpath")
		except IOError:
			_root = None

	if _root is None:
		return None

	fn = struct.readTitle(link)
	if not fn:
		return None
	if isinstance(link, struct.DocLink):
		uuid = link.doc()
		stores = Connector().lookup_doc(uuid).stores()
		dir = ".docs"
	else:
		uuid = link.rev()
		stores = Connector().lookup_rev(uuid)
		dir = ".revs"
	enum = Connector().enum()
	for store in stores:
		path = os.path.join(_root, enum.store(store), dir, uuid.encode("hex"), fn)
		if os.path.exists(path):
			return path
	return None

