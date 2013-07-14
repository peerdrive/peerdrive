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

import sys, optparse, subprocess, os.path, stat, tempfile
from peerdrive import Connector, Registry, fuse
from peerdrive.connector import Link

usage = ("usage: %prog [options] <Document>\n\n"
	"Document:\n"
	"    doc:<document>  ...open the latest version of the given document\n"
	"    rev:<revision>  ...display the given revision\n"
	"    <hp-path-spec>  ...open by path spec")
parser = optparse.OptionParser(usage=usage)
parser.add_option("--referrer", dest="referrer", metavar="REF",
	help="Document from which we're coming")
(options, args) = parser.parse_args()
if len(args) != 1:
	parser.error("incorrect number of arguments")
try:
	link = Link(args[0])
except IOError as e:
	parser.error(str(e))

# mounted through user space file system?
path = fuse.findFuseFile(link)
if not path:
	s = Connector().stat(link.rev())
	hash = s.hash('_')

	name = hash.encode('hex')
	ext = ""
	with Connector().peek(link.store(), link.rev()) as r:
		annotation = r.getData("/org.peerdrive.annotation")

	# read title
	if "title" in annotation:
		(name, ext) = os.path.splitext(annotation["title"])

	# try to get extension from Registry if title has none
	if not ext:
		extensions = Registry().search(s.type(), "extensions")
		if extensions:
			ext = extensions[0]

	# try to get extension from origin if we don't have one already
	if not ext and ("origin" in annotation):
		ext = os.path.splitext(annotation["origin"])[1]

	# copy out file (if necessary)
	path = os.path.join(tempfile.gettempdir(), hash.encode('hex'), name+ext)
	if not os.path.isdir(os.path.dirname(path)):
		os.makedirs(os.path.dirname(path))
	if not os.path.isfile(path):
		with open(path, "wb") as file:
			with Connector().peek(link.store(), link.rev()) as reader:
				file.write(reader.readAll('_'))
		os.chmod(path, stat.S_IREAD)


# start external program
if sys.platform == "win32":
	subprocess.Popen(["start", path, path], shell=True)
else:
	subprocess.Popen(["xdg-open", path])
