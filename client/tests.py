import unittest
import time
import subprocess
from hotchpotch import Connector
from hotchpotch import connector
from hotchpotch import struct

STORE1 = 'rem1'
STORE2 = 'rem2'

class CommonParts(unittest.TestCase):

	class Watch(connector.Watch):
		def __init__(self, typ, doc, event):
			connector.Watch.__init__(self, typ, doc)
			self.__event = event
			self.__received = False
			Connector().watch(self)

		def close(self):
			Connector().unwatch(self)

		def triggered(self, cause):
			if self.__event == cause:
				self.__received = True

		def reset(self):
			self.__received = False

		def waitForWatch(self, maxSec=3):
			latest = time.time() + maxSec
			while not self.__received and not (time.time() > latest):
				Connector().process(100)
			return self.__received

	def setUp(self):
		if not Connector().enum().isMounted(STORE1):
			Connector().mount(STORE1)
		self.store1 = Connector().enum().doc(STORE1)
		if not Connector().enum().isMounted(STORE2):
			Connector().mount(STORE2)
		self.store2 = Connector().enum().doc(STORE2)

		self._disposeHandles = []
		self._disposeWatches = []

	def tearDown(self):
		for handle in self._disposeHandles:
			handle.close()
		for watch in self._disposeWatches:
			watch.close()

	def disposeHandle(self, handle):
		self._disposeHandles.append(handle)

	def disposeWatch(self, watch):
		self._disposeWatches.append(watch)

	def create(self, type, creator, stores):
		w = Connector().create(type, creator, stores)
		self.disposeHandle(w)
		return w

	def fork(self, rev, creator, stores=[]):
		w = Connector().fork(rev, "test.bar", stores)
		self.disposeHandle(w)
		return w

	def assertRevContent(self, rev, content):
		with Connector().peek(rev) as r:
			for (part, data) in content.items():
				revData = r.readAll(part)
				self.assertEqual(revData, data)

		s = Connector().stat(rev)
		for part in s.parts():
			self.assertTrue(part in content)

	def watchDoc(self, doc, event):
		w = CommonParts.Watch(connector.Watch.TYPE_DOC, doc, event)
		self.disposeWatch(w)
		return w

	def watchRev(self, rev, event):
		w = CommonParts.Watch(connector.Watch.TYPE_REV, rev, event)
		self.disposeWatch(w)
		return w

	def erlCall(self, command):
		proc = subprocess.Popen(['erl_call', '-e', '-n', 'hotchpotch'],
			stdout=subprocess.PIPE, stdin=subprocess.PIPE)
		proc.stdin.write(command+'\n')
		proc.stdin.close()
		proc.wait()
		return proc.stdout.read()


class TestReadWrite(CommonParts):

	def test_readback(self):
		dataOrig = 'akjdfaqhfkjsalur\naqidahgajsoasoiga\n\nakhsfdlkaf\r\n'
		w = self.create("public.data", "test.foo", [self.store1])
		w.writeAll('FILE', dataOrig)
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		with Connector().peek(rev) as r:
			dataRead = r.readAll('FILE')

		self.assertEqual(dataOrig, dataRead)


class TestCreatorCode(CommonParts):

	def test_create(self):
		w = self.create("public.data", "test.foo", [self.store1])
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		s = Connector().stat(rev)
		self.assertEqual(s.creator(), "test.foo")

	def test_fork(self):
		c = Connector()
		w = self.create("public.data", "test.foo", [self.store1])
		w.commit()
		doc1 = w.getDoc()
		rev1 = w.getRev()

		w = self.fork(rev1, "test.bar")
		w.commit()
		doc2 = w.getDoc()
		rev2 = w.getRev()

		s = c.stat(rev1)
		self.assertEqual(s.creator(), "test.foo")
		s = c.stat(rev2)
		self.assertEqual(s.creator(), "test.bar")

	def test_update_change(self):
		c = Connector()
		w = self.create("public.data", "test.foo", [self.store1])
		w.commit()
		doc = w.getDoc()
		rev1 = w.getRev()

		with c.update(doc, rev1, "test.baz") as w:
			w.commit()
			rev2 = w.getRev()

		s = c.stat(rev1)
		self.assertEqual(s.creator(), "test.foo")
		s = c.stat(rev2)
		self.assertEqual(s.creator(), "test.baz")

	def test_update_keep(self):
		c = Connector()
		w = self.create("public.data", "test.foo", [self.store1])
		w.commit()
		doc = w.getDoc()
		rev1 = w.getRev()

		with c.update(doc, rev1) as w:
			w.write('FILE', 'update')
			w.commit()
			rev2 = w.getRev()

		s = c.stat(rev1)
		self.assertEqual(s.creator(), "test.foo")
		s = c.stat(rev2)
		self.assertEqual(s.creator(), "test.foo")


class TestTypeCode(CommonParts):

	def test_create(self):
		c = Connector()
		w = self.create("test.format", "test.ignore", [self.store1])
		self.assertEqual(w.getType(), "test.format")
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		s = c.stat(rev)
		self.assertEqual(s.type(), "test.format")

	def test_fork_keep(self):
		c = Connector()
		w = self.create("test.format.foo", "test.ignore", [self.store1])
		self.assertEqual(w.getType(), "test.format.foo")
		w.commit()
		doc1 = w.getDoc()
		rev1 = w.getRev()

		w = self.fork(rev1, "test.ignore")
		self.assertEqual(w.getType(), "test.format.foo")
		w.write('FILE', 'update')
		w.commit()
		doc2 = w.getDoc()
		rev2 = w.getRev()

		s = c.stat(rev1)
		self.assertEqual(s.type(), "test.format.foo")
		s = c.stat(rev2)
		self.assertEqual(s.type(), "test.format.foo")

	def test_fork_change(self):
		c = Connector()
		w = self.create("test.format.foo", "test.ignore", [self.store1])
		self.assertEqual(w.getType(), "test.format.foo")
		w.commit()
		doc1 = w.getDoc()
		rev1 = w.getRev()

		w = self.fork(rev1, "test.ignore")
		w.write('FILE', 'update')
		self.assertEqual(w.getType(), "test.format.foo")
		w.setType("test.format.bar")
		self.assertEqual(w.getType(), "test.format.bar")
		w.commit()
		doc2 = w.getDoc()
		rev2 = w.getRev()

		s = c.stat(rev1)
		self.assertEqual(s.type(), "test.format.foo")
		s = c.stat(rev2)
		self.assertEqual(s.type(), "test.format.bar")

	def test_update_keep(self):
		c = Connector()
		w = self.create("test.format.foo", "test.ignore", [self.store1])
		self.assertEqual(w.getType(), "test.format.foo")
		w.commit()
		doc = w.getDoc()
		rev1 = w.getRev()

		with c.update(doc, rev1, "test.ignore") as w:
			self.assertEqual(w.getType(), "test.format.foo")
			w.write('FILE', 'update')
			w.commit()
			rev2 = w.getRev()

		s = c.stat(rev1)
		self.assertEqual(s.type(), "test.format.foo")
		s = c.stat(rev2)
		self.assertEqual(s.type(), "test.format.foo")

	def test_update_change(self):
		c = Connector()
		w = self.create("test.format.foo", "test.ignore", [self.store1])
		self.assertEqual(w.getType(), "test.format.foo")
		w.commit()
		doc = w.getDoc()
		rev1 = w.getRev()

		with c.update(doc, rev1, "test.ignore") as w:
			self.assertEqual(w.getType(), "test.format.foo")
			w.write('FILE', 'update')
			w.setType("test.format.bar")
			self.assertEqual(w.getType(), "test.format.bar")
			w.commit()
			rev2 = w.getRev()

		s = c.stat(rev1)
		self.assertEqual(s.type(), "test.format.foo")
		s = c.stat(rev2)
		self.assertEqual(s.type(), "test.format.bar")


class TestSync(CommonParts):

	def test_already_same(self):
		c = Connector()
		stores = [self.store1, self.store2]
		w = self.create("public.data", "test.ignore", stores)
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		self.assertTrue(c.sync(doc) == rev)
		self.assertTrue(c.sync(doc, stores=stores) == rev)

	def test_good(self):
		c = Connector()
		stores = [self.store1, self.store2]
		w = self.create("public.data", "test.ignore", stores)
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		with c.update(doc, rev, stores=[self.store1]) as w:
			w.write('FILE', 'update')
			w.commit()
			rev = w.getRev()

		self.assertTrue(c.sync(doc) == rev)

		l = c.lookup_doc(doc)
		self.assertEqual(len(l.revs()), 1)
		self.assertEqual(l.rev(self.store1), rev)
		self.assertEqual(l.rev(self.store2), rev)

	def test_bad(self):
		c = Connector()
		stores = [self.store1, self.store2]
		w = self.create("public.data", "test.ignore", stores)
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		with c.update(doc, rev, stores=[self.store1]) as w:
			w.write('FILE', 'first')
			w.commit()
			rev1 = w.getRev()

		with c.update(doc, rev, stores=[self.store2]) as w:
			w.write('FILE', 'second')
			w.commit()
			rev2 = w.getRev()

		self.failIfEqual(rev, rev1)
		self.failIfEqual(rev, rev2)
		self.failIfEqual(rev1, rev2)

		self.assertRaises(IOError, c.sync, doc)

		l = c.lookup_doc(doc)
		self.assertEqual(len(l.revs()), 2)
		self.assertEqual(l.rev(self.store1), rev1)
		self.assertEqual(l.rev(self.store2), rev2)

	def test_merge(self):
		c = Connector()
		stores = [self.store1, self.store2]
		w = self.create("public.data", "test.ignore", stores)
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		with c.update(doc, rev, stores=[self.store1]) as w:
			w.write('FILE', 'first')
			w.commit()
			rev1 = w.getRev()

		with c.update(doc, rev, stores=[self.store2]) as w:
			w.write('FILE', 'second')
			w.commit()
			rev2 = w.getRev()

		with c.update(doc, rev1, stores=stores) as w:
			w.setParents([rev1, rev2])
			w.commit()
			rev3 = w.getRev()

		self.assertTrue(c.sync(doc) == rev3)

		l = c.lookup_doc(doc)
		self.assertEqual(l.revs(), [rev3])

		self.assertEqual(set(c.lookup_rev(rev)), set(stores))
		self.assertEqual(set(c.lookup_rev(rev1)), set(stores))
		self.assertEqual(set(c.lookup_rev(rev2)), set(stores))
		self.assertEqual(set(c.lookup_rev(rev3)), set(stores))


class TestPreRevs(CommonParts):

	def createSuspendDoc(self):
		c = Connector()
		w = self.create("test.format", "test.ignore", [self.store1])
		w.writeAll('FILE', 'ok')
		w.commit()
		doc = w.getDoc()
		rev1 = w.getRev()

		with c.update(doc, rev1) as w:
			w.writeAll('FILE', 'update')
			w.suspend()
			rev2 = w.getRev()

		return (doc, rev1, rev2)

	def test_suspend(self):
		c = Connector()
		(doc, rev1, rev2) = self.createSuspendDoc()

		l = c.lookup_doc(doc)
		self.assertEqual(l.revs(), [rev1])
		self.assertEqual(l.preRevs(), [rev2])
		self.assertRevContent(rev1, {'FILE' : 'ok'})
		self.assertRevContent(rev2, {'FILE' : 'update'})

	def test_suspend_multi(self):
		c = Connector()
		(doc, rev1, rev_s1) = self.createSuspendDoc()

		with c.update(doc, rev1) as w:
			w.writeAll('FILE', 'forward')
			w.commit()
			rev2 = w.getRev()

		with c.update(doc, rev2) as w:
			w.writeAll('FILE', 'Hail to the king, baby!')
			w.suspend()
			rev_s2 = w.getRev()

		l = c.lookup_doc(doc)
		self.assertEqual(l.revs(), [rev2])
		self.assertEqual(len(l.preRevs()), 2)
		self.assertTrue(rev_s1 in l.preRevs())
		self.assertTrue(rev_s2 in l.preRevs())

		s = c.stat(rev_s1)
		self.assertEqual(s.parents(), [rev1])
		s = c.stat(rev_s2)
		self.assertEqual(s.parents(), [rev2])

		self.assertRevContent(rev1, {'FILE' : 'ok'})
		self.assertRevContent(rev_s1, {'FILE' : 'update'})
		self.assertRevContent(rev2, {'FILE' : 'forward'})
		self.assertRevContent(rev_s2, {'FILE' : 'Hail to the king, baby!'})

	def test_resume_wrong(self):
		c = Connector()
		(doc, rev1, rev2) = self.createSuspendDoc()
		self.assertRaises(IOError, c.resume, doc, rev1)

		l = c.lookup_doc(doc)
		self.assertEqual(l.revs(), [rev1])
		self.assertEqual(l.preRevs(), [rev2])
		self.assertRevContent(rev1, {'FILE' : 'ok'})
		self.assertRevContent(rev2, {'FILE' : 'update'})

	def test_resume_abort(self):
		c = Connector()
		(doc, rev1, rev2) = self.createSuspendDoc()

		with c.resume(doc, rev2) as w:
			w.writeAll('FILE', 'Hail to the king, baby!')

			l = c.lookup_doc(doc)
			self.assertEqual(l.revs(), [rev1])
			self.assertEqual(l.preRevs(), [rev2])

			w.close()

		l = c.lookup_doc(doc)
		self.assertEqual(l.revs(), [rev1])
		self.assertEqual(l.preRevs(), [rev2])
		self.assertRevContent(rev1, {'FILE' : 'ok'})
		self.assertRevContent(rev2, {'FILE' : 'update'})

	def test_resume_commit(self):
		c = Connector()
		(doc, rev1, rev2) = self.createSuspendDoc()

		with c.resume(doc, rev2) as w:
			w.writeAll('FILE', 'What are you waiting for, christmas?')
			w.commit()
			rev3 = w.getRev()

		l = c.lookup_doc(doc)
		self.assertEqual(l.revs(), [rev3])
		self.assertEqual(len(l.preRevs()), 0)

		s = c.stat(rev3)
		self.assertEqual(s.parents(), [rev1])
		self.assertRevContent(rev1, {'FILE' : 'ok'})
		self.assertRevContent(rev3, {'FILE' : 'What are you waiting for, christmas?'})

	def test_resume_suspend_orig(self):
		c = Connector()
		(doc, rev1, rev2) = self.createSuspendDoc()

		with c.resume(doc, rev2) as w:
			w.suspend()
			rev3 = w.getRev()

		l = c.lookup_doc(doc)
		self.assertEqual(l.revs(), [rev1])
		self.assertEqual(l.preRevs(), [rev3])

		s = c.stat(rev3)
		self.assertEqual(s.parents(), [rev1])

		self.assertRevContent(rev1, {'FILE' : 'ok'})
		self.assertRevContent(rev3, {'FILE' : 'update'})

	def test_resume_suspend_mod(self):
		c = Connector()
		(doc, rev1, rev2) = self.createSuspendDoc()

		with c.resume(doc, rev2) as w:
			w.writeAll('FILE', 'What are you waiting for, christmas?')
			w.suspend()
			rev3 = w.getRev()

		l = c.lookup_doc(doc)
		self.assertEqual(l.revs(), [rev1])
		self.assertEqual(l.preRevs(), [rev3])

		s = c.stat(rev3)
		self.assertEqual(s.parents(), [rev1])

		self.assertRevContent(rev1, {'FILE' : 'ok'})
		self.assertRevContent(rev3, {'FILE' : 'What are you waiting for, christmas?'})

	def test_forget(self):
		c = Connector()
		(doc, rev1, rev_s1) = self.createSuspendDoc()

		with c.update(doc, rev1) as w:
			w.writeAll('FILE', 'forward')
			w.commit()
			rev2 = w.getRev()

		with c.update(doc, rev2) as w:
			w.writeAll('FILE', 'Hail to the king, baby!')
			w.suspend()
			rev_s2 = w.getRev()

		self.assertRaises(IOError, c.forget, doc, rev1)
		c.forget(doc, rev_s1)

		l = c.lookup_doc(doc)
		self.assertEqual(l.revs(), [rev2])
		self.assertEqual(l.preRevs(), [rev_s2])


class TestGarbageCollector(CommonParts):

	def gc(self, store):
		guid = store.encode('hex')
		result = self.erlCall(
			"""case volman:store(<<16#"""+guid+""":128>>) of
				{ok, Pid} -> file_store:gc(Pid);
				error     -> {error, enoent}
			end.""")
		self.assertEqual(result, '{ok, ok}')

	def test_collect(self):
		c = Connector()

		# deliberately close handle after creating!
		with c.create("public.data", "test.foo", [self.store1]) as w:
			w.commit()
			doc = w.getDoc()
			rev = w.getRev()

		# perform a GC cycle
		self.gc(self.store1)

		l = c.lookup_doc(doc)
		self.assertEqual(l.revs(), [])
		self.assertEqual(l.preRevs(), [])
		self.assertRaises(IOError, c.stat, rev)

	def test_create_keep_handle(self):
		c = Connector()

		with c.create("public.data", "test.foo", [self.store1]) as w:
			w.commit()
			doc = w.getDoc()
			rev = w.getRev()

			# perform a GC cycle
			self.gc(self.store1)

			l = c.lookup_doc(doc)
			self.assertEqual(l.revs(), [rev])
			self.assertEqual(l.preRevs(), [])

			c.stat(rev)

	def test_fork_keep_handle(self):
		c = Connector()

		w = self.create("test.format.foo", "test.ignore", [self.store1])
		self.assertEqual(w.getType(), "test.format.foo")
		w.commit()
		doc1 = w.getDoc()
		rev1 = w.getRev()

		with c.fork(rev1, "test.ignore") as w:
			w.write('FILE', 'update')
			w.commit()
			doc2 = w.getDoc()
			rev2 = w.getRev()

			# perform a GC cycle
			self.gc(self.store1)

			l = c.lookup_doc(doc2)
			self.assertEqual(l.revs(), [rev2])
			self.assertEqual(l.preRevs(), [])
			c.stat(rev2)

	def test_transitive_keep(self):
		with Connector().create("test.format.foo", "test.foo", [self.store1]) as w1:
			with Connector().create("test.format.bar", "test.foo", [self.store1]) as w2:
				w2.write('FILE', 'test')
				w2.commit()
				doc2 = w2.getDoc()
				rev2 = w2.getRev()

				# create a reference from w1 to w2
				w1.write('HPSD', struct.dumps([struct.DocLink(doc2)]))
				w1.commit()
				doc1 = w1.getDoc()
				rev1 = w1.getRev()

			# w2 is closed now, w1 still open, should prevent gc
			self.gc(self.store1)

			l = Connector().lookup_doc(doc1)
			self.assertEqual(l.revs(), [rev1])
			self.assertEqual(l.preRevs(), [])
			self.assertEqual(Connector().lookup_rev(rev1), [self.store1])
			l = Connector().lookup_doc(doc2)
			self.assertEqual(l.revs(), [rev2])
			self.assertEqual(l.preRevs(), [])
			self.assertEqual(Connector().lookup_rev(rev2), [self.store1])


class TestReplicator(CommonParts):

	def test_sticky(self):
		s = struct.Set()
		# create sticky contianer on two stores
		with s.create("foo", [self.store1, self.store2]) as dummy:
			# create document on first store
			with Connector().create("test.format.foo", "test.ignore", [self.store1]) as w:
				w.commit()
				doc = w.getDoc()
				rev = w.getRev()

				watch1 = self.watchDoc(doc, connector.Watch.EVENT_REPLICATED)
				watch2 = self.watchRev(rev, connector.Watch.EVENT_REPLICATED)

				# add to sticky container
				s['dummy'] = struct.DocLink(doc)
				s.save()

			# wait for sticky replicatin to happen
			self.assertTrue(watch1.waitForWatch())
			self.assertTrue(watch2.waitForWatch())

			# check doc (with rev) to exist on all stores
			l = Connector().lookup_doc(doc)
			self.assertEqual(l.revs(), [rev])
			self.assertEqual(len(l.stores(rev)), 2)
			self.assertTrue(self.store1 in l.stores(rev))
			self.assertTrue(self.store2 in l.stores(rev))

			l = Connector().lookup_rev(rev)
			self.assertEqual(len(l), 2)
			self.assertTrue(self.store1 in l)
			self.assertTrue(self.store2 in l)


class TestSynchronization(CommonParts):

	def setUp(self):
		# make sure stores are unmounted to kill sync
		if Connector().enum().isMounted(STORE1):
			Connector().unmount(STORE1)
		if Connector().enum().isMounted(STORE2):
			Connector().unmount(STORE2)
		CommonParts.setUp(self)

	def tearDown(self):
		CommonParts.tearDown(self)
		# make sure stores are unmounted to kill sync
		if Connector().enum().isMounted(STORE1):
			Connector().unmount(STORE1)
		if Connector().enum().isMounted(STORE2):
			Connector().unmount(STORE2)

	def startSync(self, mode, fromStore, toStore):
		fromGuid = fromStore.encode('hex')
		toGuid = toStore.encode('hex')
		result = self.erlCall("synchronizer:sync(" + mode +
			", <<16#"+fromGuid+":128>>, <<16#"+toGuid+":128>>).")
		self.assertTrue(result.startswith('{ok, {ok,'))

	def createFastForward(self):
		w = self.create("public.data", "test.foo", [self.store1, self.store2])
		w.commit()
		doc = w.getDoc()
		rev1 = w.getRev()

		with Connector().update(doc, rev1, stores=[self.store1]) as w:
			w.writeAll('FILE', 'forward')
			w.commit()
			rev2 = w.getRev()

		return (doc, rev2)

	def createMerge(self, type, base, left, right):
		w = self.create(type, "test.foo", [self.store1, self.store2])
		for (part, data) in base.items():
			w.writeAll(part, data)
		w.commit()
		doc = w.getDoc()
		rev1 = w.getRev()

		with Connector().update(doc, rev1, stores=[self.store1]) as w:
			for (part, data) in left.items():
				w.writeAll(part, data)
			w.commit()
			rev2 = w.getRev()

		with Connector().update(doc, rev1, stores=[self.store2]) as w:
			for (part, data) in right.items():
				w.writeAll(part, data)
			w.commit()
			rev3 = w.getRev()

		return (doc, rev2, rev3)

	def performSync(self, doc, strategy):
		watch = self.watchDoc(doc, connector.Watch.EVENT_MODIFIED)

		self.startSync(strategy, self.store1, self.store2)

		# first wait until the doc gets changed
		while True:
			watch.reset()
			self.assertTrue(watch.waitForWatch())
			l = Connector().lookup_doc(doc)
			if len(l.revs()) == 1:
				break

		self.assertEqual(len(l.stores()), 2)
		self.assertTrue(self.store1 in l.stores())
		self.assertTrue(self.store2 in l.stores())

		# wait until sync_worker moved on
		result = self.erlCall(
			"""sync_locks:lock(<<16#"""+doc.encode('hex')+""":128>>),
			sync_locks:unlock(<<16#"""+doc.encode('hex')+""":128>>).""")
		self.assertEqual(result, '{ok, ok}')

		return l

	# Checks if a document is synchronized via fast-forward
	def test_sync_ff_ok(self):
		(doc, rev) = self.createFastForward()
		l = self.performSync(doc, 'ff')
		self.assertEqual(l.rev(self.store1), rev)
		self.assertEqual(l.rev(self.store2), rev)

	# Checks that the fast-forward sync does not synchronize documents which
	# need a real merge
	def test_sync_ff_err(self):
		(doc, rev1, rev2) = self.createMerge("public.data", {}, {'FILE' : "left"},
			{'FILE' : "right"})

		watch = self.watchDoc(doc, connector.Watch.EVENT_MODIFIED)

		self.startSync('ff', self.store1, self.store2)
		self.startSync('ff', self.store2, self.store1)

		self.assertFalse(watch.waitForWatch(1))

		# check that doc is not synced
		l = Connector().lookup_doc(doc)
		self.assertEqual(len(l.revs()), 2)
		self.assertEqual(l.rev(self.store1), rev1)
		self.assertEqual(l.rev(self.store2), rev2)

	# Checks if the 'latest' strategy makes an 'ours' merge and replicates that
	# to both stores
	def test_sync_latest(self):
		(doc, rev1, rev2) = self.createMerge("public.data", {}, {'FILE' : "left"},
			{'FILE' : "right"})
		l = self.performSync(doc, 'latest')

		rev = l.revs()[0]
		s = Connector().stat(rev)
		self.assertEqual(len(s.parents()), 2)
		self.assertTrue(rev1 in s.parents())
		self.assertTrue(rev2 in s.parents())
		self.assertRevContent(rev, {'FILE' : 'left'})

	# Check that 'latest' recognizes a fast-forward contition correctly
	def test_sync_latest_fallback(self):
		(doc, rev) = self.createFastForward()
		l = self.performSync(doc, 'latest')
		self.assertEqual(l.rev(self.store1), rev)
		self.assertEqual(l.rev(self.store2), rev)

	# Checks that the 'merge' strategy falls back to 'latest' in case of an
	# unknown document
	def test_sync_merge_fallback(self):
		(doc, rev1, rev2) = self.createMerge("public.data", {}, {'FILE' : "left"},
			{'FILE' : "right"})
		l = self.performSync(doc, 'merge')

		rev = l.revs()[0]
		s = Connector().stat(rev)
		self.assertEqual(len(s.parents()), 2)
		self.assertTrue(rev1 in s.parents())
		self.assertTrue(rev2 in s.parents())
		self.assertRevContent(rev, {'FILE' : 'left'})

	# Checks that the 'merge' strategy really merges
	def test_sync_merge(self):
		(doc, rev1, rev2) = self.createMerge("org.hotchpotch.dict",
			{'HPSD' : struct.dumps({"a":1}) },
			{'HPSD' : struct.dumps({"a":1, "b":2}) },
			{'HPSD' : struct.dumps({"a":1, "c":3}) })
		l = self.performSync(doc, 'merge')

		rev = l.revs()[0]
		s = Connector().stat(rev)
		self.assertEqual(len(s.parents()), 2)
		self.assertTrue(rev1 in s.parents())
		self.assertTrue(rev2 in s.parents())

		# all revs on all stores?
		l = Connector().lookup_rev(rev1)
		self.assertTrue(self.store1 in l)
		self.assertTrue(self.store2 in l)
		l = Connector().lookup_rev(rev2)
		self.assertTrue(self.store1 in l)
		self.assertTrue(self.store2 in l)

		# see if merge was ok
		with Connector().peek(rev) as r:
			hpsd = struct.loads(r.readAll('HPSD'))
			self.assertEqual(hpsd, {"a":1, "b":2, "c":3})

if __name__ == '__main__':
	unittest.main()

