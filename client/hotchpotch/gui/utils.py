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

from __future__ import absolute_import

import sys, os, subprocess

from ..connector import Connector
from ..registry import Registry
from .. import struct


def showDocument(link, executable=None):
	if isinstance(link, struct.DocLink):
		args = ['doc:'+link.doc().encode('hex')]
		rev = Connector().lookup_doc(link.doc()).revs()[0]
	else:
		args = ['rev:'+link.rev().encode('hex')]
		rev = link.rev()
	uti = Connector().stat(rev).type()
	if not executable:
		executable = Registry().getExecutables(uti)[0]
	if executable:
		if sys.platform == "win32":
			subprocess.Popen([executable] + args, shell=True)
		else:
			executable = './' + executable
			os.spawnv(os.P_NOWAIT, executable, [executable] + args)


def showProperties(link):
	if isinstance(link, struct.DocLink):
		args = ['doc:'+link.doc().encode('hex')]
	else:
		args = ['rev:'+link.rev().encode('hex')]
	if sys.platform == "win32":
		subprocess.Popen(['properties.py'] + args, shell=True)
	else:
		os.spawnv(os.P_NOWAIT, './properties.py', ['./properties.py'] + args)


