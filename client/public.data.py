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
import sys, tempfile, os.path, subprocess

from hotchpotch import hpstruct, HpConnector, HpRegistry


# parse command line
if len(sys.argv) == 2 and sys.argv[1].startswith('doc:'):
	doc = sys.argv[1][4:].decode("hex")
	rev = HpConnector().lookup_doc(doc).revs()[0]
elif len(sys.argv) == 2 and sys.argv[1].startswith('rev:'):
	rev = sys.argv[1][4:].decode("hex")
else:
	print "Usage: public.data.py <Document> ..."
	print
	print "Document:"
	print "    doc:<document>  ...open the latest version of the given document"
	print "    rev:<revision>  ...display the given revision"
	sys.exit(1)


stat = HpConnector().stat(rev)
uti  = stat.type()
hash = stat.hash('FILE')

# determine extension
extensions = HpRegistry().search(uti, "extensions")
if extensions:
	ext = extensions[0]
else:
	ext = ""
	with HpConnector().peek(rev) as r:
		meta = hpstruct.loads(r.readAll('META'))

	if "org.hotchpotch.annotation" in meta:
		annotation = meta["org.hotchpotch.annotation"]
		# try origin
		if "origin" in annotation:
			ext  = os.path.splitext(annotation["origin"])[1]
		# try title
		if not ext:
			if "title" in annotation:
				ext = os.path.splitext(annotation["title"])[1]

# copy out file (if necessary)
path = os.path.join(tempfile.gettempdir(), hash.encode('hex')+ext)
if not os.path.isfile(path):
	with open(path, "wb") as file:
		with HpConnector().peek(rev) as reader:
			file.write(reader.readAll('FILE'))

# start external program
if sys.platform == "win32":
	subprocess.Popen(["start", path], shell=True)
else:
	subprocess.Popen(["xdg-open", path])

