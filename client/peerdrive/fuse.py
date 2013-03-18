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

from __future__ import absolute_import

import os
from . import connector
from .connector import Connector

def findFuseFile(link):
	try:
		if isinstance(link, connector.DocLink):
			path = Connector().getDocPath(link.store(), link.doc())
		else:
			path = Connector().getRevPath(link.store(), link.rev())
		if os.path.exists(path):
			return path
		return None
	except IOError:
		return None

