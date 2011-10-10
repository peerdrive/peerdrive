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

from PyQt4 import QtCore
import sys, subprocess

from ..connector import Connector
from ..registry import Registry


def showDocument(link, executable=None, referrer=None):
	args = [str(link)]
	if referrer:
		args.append('--referrer')
		args.append(str(referrer))
	if not executable:
		link.update()
		rev = link.rev()
		uti = Connector().stat(rev).type()
		executable = Registry().getExecutables(uti)[0]
	if executable:
		if sys.platform == "win32":
			subprocess.Popen([executable] + args, shell=True)
		else:
			executable = './' + executable
			QtCore.QProcess.startDetached(executable, args, '.')


def showProperties(link):
	args = [str(link)]
	if sys.platform == "win32":
		subprocess.Popen(['properties.py'] + args, shell=True)
	else:
		QtCore.QProcess.startDetached('./properties.py', args, '.')

