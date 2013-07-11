#!/usr/bin/env python
# vim: set fileencoding=utf-8 :
#
# PeerDrive
# Copyright (C) 2012  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
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

import sys, optparse
from peerdrive import Connector, struct

def printStore(store, verbose, auto=False):
	line = "'%s' as '%s' type '%s'" % (store.src, store.label, store.type)
	if store.options:
		line += " (" + store.options + ")"
	if verbose:
		line += " ["
		if auto:
			line += "auto, "
		line += store.sid.encode("hex") + "]"
	print line


parser = optparse.OptionParser(usage="usage: %prog [options] [[<src>] <label>]")
parser.add_option("-o", "--options",
	help="Comma separated string of mount options")
parser.add_option("-t", "--type", help="Store type (default: 'file')")
parser.add_option("-c", "--credentials", default="",
	help="Credentials to mount store")
parser.add_option("-p", "--persist", action="store_true",
	help="Save mount label permanently in fstab")
parser.add_option("-a", "--auto", action="store_true",
	help="Mount store automatically on startup (depends on '-p')")
parser.add_option("-v", "--verbose", action="store_true", help="Verbose output")

(options, args) = parser.parse_args()
if len(args) > 2:
	parser.error("incorrect number of arguments")
if options.auto and not options.persist:
	parser.error("'--auto' depends on '--persist'")
if options.persist and (len(args) < 2):
	parser.error("incorrect number of arguments")

fstab = struct.FSTab()

if len(args) == 0:
	enum = Connector().enum()
	if options.verbose:
		printStore(enum.sysStore(), True)
	for store in enum.regularStores():
		auto = False
		if store.label in fstab.knownLabels():
			auto = fstab.get(store.label).get('auto', False)
		printStore(store, options.verbose, auto)
	if options.verbose:
		print "\nUnmounted stores:"
		unmounted = set(fstab.knownLabels()) - set([s.label for s in enum.regularStores()])
		for label in unmounted:
			store = fstab.get(label)
			type = store.get("type", "file")
			options = store.get("options", "")
			credentials = store.get("credentials", "")
			auto = store.get("auto", False)
			line = "'%s' as '%s' type '%s'" % (store['src'], label, type)
			if options:
				line += " (" + options + ")"
			if auto:
				line += " [auto]"
			print line
else:
	type = "file"
	opts = ""
	creds = ""
	if len(args) == 1:
		label = args[0]
		if label not in fstab.knownLabels():
			print >>sys.stderr, "Label '%s' not found in fstab" % label
			sys.exit(1)
		entry = fstab.get(label)
		src = entry['src']
		if 'type' in entry: type = entry['type']
		if 'options' in entry: opts = entry['options']
		if 'credentials' in entry: creds = entry['credentials']
	else:
		src = args[0]
		label = args[1]

	# command line overrides defaults
	if options.type: type = options.type
	if options.options: opts = options.options
	if options.credentials: creds = options.credentials

	line = "'%s' as '%s' type '%s'" % (src, label, type)
	if opts:
		line += " (" + opts + ")"

	# Add to fstab?
	if options.persist:
		if label in fstab.knownLabels():
			print >>sys.stderr, "Label '%s' already defined in fstab" % label
			sys.exit(1)
		if options.verbose:
			if options.auto:
				auto = "[auto]"
			else:
				auto = ""
			print "Adding to fstab:", line, auto

		entry = { 'src' : src }
		if type != "file": entry['type'] = type
		if opts: entry['options'] = opts
		if creds: entry['credentials'] = creds
		if options.auto: entry['auto'] = True

		try:
			fstab.set(label, entry)
			fstab.save()
		except IOError as error:
			print >>sys.stderr, "Fstab update failed: " + str(error)
			sys.exit(2)

	if options.verbose:
		print "Mounting", line + "..."

	try:
		sid = Connector().mount(src, label, type=type, options=opts,
			credentials=creds)
	except IOError as error:
		print >>sys.stderr, "Mount failed: " + str(error)
		sys.exit(2)

	if options.verbose:
		try:
			rev = Connector().lookupDoc(sid, [sid]).rev(sid)
			with Connector().peek(sid, rev) as r:
				name = r.getData("/org.peerdrive.annotation/title")
		except IOError:
			name = "Unnamed store"
		print "Mounted '%s' (%s, '%s')" % (label, sid.encode('hex'), name)
	else:
		print "Mounted '%s'" % label

