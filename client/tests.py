import unittest
import time
import subprocess
import datetime
from peerdrive import Connector
from peerdrive import connector
from peerdrive import struct

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

		def triggered(self, cause, store):
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

	def create(self, store, type="public.data", creator="org.peerdrive.test-py"):
		w = Connector().create(store, type, creator)
		self.disposeHandle(w)
		return w

	def createCommon(self, stores, type="public.data", creator="org.peerdrive.test-py", data={}):
		leadStore = stores.pop()
		w = self.create(leadStore, type, creator)
		for (part, blob) in data.items():
			w.writeAll(part, blob)
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()
		for store in stores:
			w = self.create(store)
			w.writeAll('PDSD', struct.dumps([struct.DocLink(store, doc)]))
			w.commit()
			Connector().replicateDoc(leadStore, doc, store)

		# verify the common document on all stores
		l = Connector().lookupDoc(doc)
		self.assertEqual(l.revs(), [rev])
		for store in stores:
			self.assertTrue(store in l.stores())

		return (doc, rev)

	def fork(self, store, rev, creator="org.peerdrive.test-py"):
		w = Connector().fork(store, rev, creator)
		self.disposeHandle(w)
		return w

	def assertRevContent(self, store, rev, content):
		with Connector().peek(store, rev) as r:
			for (part, data) in content.items():
				revData = r.readAll(part)
				self.assertEqual(revData, data)

		s = Connector().stat(rev, [store])
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
		proc = subprocess.Popen(['erl_call', '-e', '-n', 'peerdrive'],
			stdout=subprocess.PIPE, stdin=subprocess.PIPE)
		proc.stdin.write(command+'\n')
		proc.stdin.close()
		proc.wait()
		return proc.stdout.read()


class TestReadWrite(CommonParts):

	def test_readback(self):
		dataOrig = 'akjdfaqhfkjsalur\naqidahgajsoasoiga\n\nakhsfdlkaf\r\n'
		w = self.create(self.store1)
		w.writeAll('FILE', dataOrig)
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		with Connector().peek(self.store1, rev) as r:
			dataRead = r.readAll('FILE')

		self.assertEqual(dataOrig, dataRead)

	def test_readback_big(self):
		dataOrig = 'abcdefghijklmnopqrstuvwxyz' * 1024
		w = self.create(self.store1)
		w.writeAll('FILE', dataOrig)
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		with Connector().peek(self.store1, rev) as r:
			dataRead = r.readAll('FILE')

		self.assertEqual(dataOrig, dataRead)

	def test_mtime(self):
		w = self.create(self.store1)
		w.writeAll('FILE', "fubar")
		w.commit()
		rev = w.getRev()
		s = Connector().stat(rev)
		now = datetime.datetime.now()
		self.assertTrue(s.mtime() <= now)
		self.assertTrue(s.mtime() > now - datetime.timedelta(seconds=3))


class TestFlags(CommonParts):

	def test_create(self):
		w = self.create(self.store1)
		w.commit()
		self.assertEqual(w.getFlags(), set())
		rev = w.getRev()

		s = Connector().stat(rev)
		self.assertEqual(s.flags(), set())

	def test_update_change(self):
		w = self.create(self.store1)
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		with Connector().update(self.store1, doc, rev) as w:
			w.setFlags([0, 3, 8])
			w.commit()
			rev = w.getRev()

		s = Connector().stat(rev)
		self.assertEqual(s.flags(), set([0, 3, 8]))

	def test_update_keep(self):
		w = self.create(self.store1)
		w.setFlags([3, 6, 12])
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		s = Connector().stat(rev)
		self.assertEqual(s.flags(), set([3, 6, 12]))

		with Connector().update(self.store1, doc, rev) as w:
			w.writeAll('FILE', "asdfafd")
			w.commit()
			self.assertEqual(w.getFlags(), set([3, 6, 12]))
			rev = w.getRev()

		s = Connector().stat(rev)
		self.assertEqual(s.flags(), set([3, 6, 12]))

	def test_fork(self):
		w = self.create(self.store1)
		w.setFlags([1, 2, 3])
		w.commit()
		rev1 = w.getRev()

		w = self.fork(self.store1, rev1)
		w.writeAll('FILE', "asdfafd")
		self.assertEqual(w.getFlags(), set([1, 2, 3]))
		w.commit()
		rev2 = w.getRev()

		s = Connector().stat(rev1)
		self.assertEqual(s.flags(), set([1, 2, 3]))
		s = Connector().stat(rev2)
		self.assertEqual(s.flags(), set([1, 2, 3]))


class TestCreatorCode(CommonParts):

	def test_create(self):
		w = self.create(self.store1, creator="test.foo")
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		s = Connector().stat(rev)
		self.assertEqual(s.creator(), "test.foo")

	def test_fork(self):
		w = self.create(self.store1, creator="test.foo")
		w.commit()
		doc1 = w.getDoc()
		rev1 = w.getRev()

		w = self.fork(self.store1, rev1, "test.bar")
		w.commit()
		doc2 = w.getDoc()
		rev2 = w.getRev()

		s = Connector().stat(rev1)
		self.assertEqual(s.creator(), "test.foo")
		s = Connector().stat(rev2)
		self.assertEqual(s.creator(), "test.bar")

	def test_update_change(self):
		w = self.create(self.store1, creator="test.foo")
		w.commit()
		doc = w.getDoc()
		rev1 = w.getRev()

		with Connector().update(self.store1, doc, rev1, "test.baz") as w:
			w.commit()
			rev2 = w.getRev()

		s = Connector().stat(rev1)
		self.assertEqual(s.creator(), "test.foo")
		s = Connector().stat(rev2)
		self.assertEqual(s.creator(), "test.baz")

	def test_update_keep(self):
		w = self.create(self.store1, creator="test.foo")
		w.commit()
		doc = w.getDoc()
		rev1 = w.getRev()

		with Connector().update(self.store1, doc, rev1) as w:
			w.write('FILE', 'update')
			w.commit()
			rev2 = w.getRev()

		s = Connector().stat(rev1)
		self.assertEqual(s.creator(), "test.foo")
		s = Connector().stat(rev2)
		self.assertEqual(s.creator(), "test.foo")


class TestTypeCode(CommonParts):

	def test_create(self):
		w = self.create(self.store1, "test.format")
		self.assertEqual(w.getType(), "test.format")
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		s = Connector().stat(rev)
		self.assertEqual(s.type(), "test.format")

	def test_fork_keep(self):
		w = self.create(self.store1, "test.format.foo")
		self.assertEqual(w.getType(), "test.format.foo")
		w.commit()
		doc1 = w.getDoc()
		rev1 = w.getRev()

		w = self.fork(self.store1, rev1)
		self.assertEqual(w.getType(), "test.format.foo")
		w.write('FILE', 'update')
		w.commit()
		doc2 = w.getDoc()
		rev2 = w.getRev()

		s = Connector().stat(rev1)
		self.assertEqual(s.type(), "test.format.foo")
		s = Connector().stat(rev2)
		self.assertEqual(s.type(), "test.format.foo")

	def test_fork_change(self):
		w = self.create(self.store1, "test.format.foo")
		self.assertEqual(w.getType(), "test.format.foo")
		w.commit()
		doc1 = w.getDoc()
		rev1 = w.getRev()

		w = self.fork(self.store1, rev1)
		w.write('FILE', 'update')
		self.assertEqual(w.getType(), "test.format.foo")
		w.setType("test.format.bar")
		self.assertEqual(w.getType(), "test.format.bar")
		w.commit()
		doc2 = w.getDoc()
		rev2 = w.getRev()

		s = Connector().stat(rev1)
		self.assertEqual(s.type(), "test.format.foo")
		s = Connector().stat(rev2)
		self.assertEqual(s.type(), "test.format.bar")

	def test_update_keep(self):
		w = self.create(self.store1, "test.format.foo")
		self.assertEqual(w.getType(), "test.format.foo")
		w.commit()
		doc = w.getDoc()
		rev1 = w.getRev()

		with Connector().update(self.store1, doc, rev1) as w:
			self.assertEqual(w.getType(), "test.format.foo")
			w.write('FILE', 'update')
			w.commit()
			rev2 = w.getRev()

		s = Connector().stat(rev1)
		self.assertEqual(s.type(), "test.format.foo")
		s = Connector().stat(rev2)
		self.assertEqual(s.type(), "test.format.foo")

	def test_update_change(self):
		w = self.create(self.store1, "test.format.foo")
		self.assertEqual(w.getType(), "test.format.foo")
		w.commit()
		doc = w.getDoc()
		rev1 = w.getRev()

		with Connector().update(self.store1, doc, rev1) as w:
			self.assertEqual(w.getType(), "test.format.foo")
			w.write('FILE', 'update')
			w.setType("test.format.bar")
			self.assertEqual(w.getType(), "test.format.bar")
			w.commit()
			rev2 = w.getRev()

		s = Connector().stat(rev1)
		self.assertEqual(s.type(), "test.format.foo")
		s = Connector().stat(rev2)
		self.assertEqual(s.type(), "test.format.bar")


class TestFastForward(CommonParts):

	def test_nop(self):
		(doc, rev) = self.createCommon([self.store1, self.store2])
		Connector().forwardDoc(self.store1, doc, rev, rev, self.store2)
		self.assertEqual(Connector().lookupDoc(doc).revs(), [rev])

	def test_forward(self):
		(doc, rev1) = self.createCommon([self.store1, self.store2])
		with Connector().update(self.store1, doc, rev1) as w:
			w.writeAll('FILE', 'update')
			w.commit()
			rev2 = w.getRev()
		Connector().forwardDoc(self.store2, doc, rev1, rev2, self.store1)
		self.assertEqual(Connector().lookupDoc(doc).revs(), [rev2])

	def test_not_updatable(self):
		(doc, rev1) = self.createCommon([self.store1, self.store2])
		with Connector().update(self.store1, doc, rev1) as w:
			w.writeAll('FILE', 'update')
			w.commit()
			rev2 = w.getRev()
		self.assertRaises(IOError, Connector().forwardDoc, self.store1,
			doc, rev1, rev2, self.store2)
		self.assertEqual(Connector().lookupDoc(doc).rev(self.store1), rev2)
		self.assertEqual(Connector().lookupDoc(doc).rev(self.store2), rev1)


class TestPreRevs(CommonParts):

	def createSuspendDoc(self):
		w = self.create(self.store1)
		w.writeAll('FILE', 'ok')
		w.commit()
		doc = w.getDoc()
		rev1 = w.getRev()

		with Connector().update(self.store1, doc, rev1) as w:
			w.writeAll('FILE', 'update')
			w.suspend()
			rev2 = w.getRev()

		return (doc, rev1, rev2)

	def test_suspend(self):
		(doc, rev1, rev2) = self.createSuspendDoc()

		l = Connector().lookupDoc(doc)
		self.assertEqual(l.revs(), [rev1])
		self.assertEqual(l.preRevs(), [rev2])
		self.assertRevContent(self.store1, rev1, {'FILE' : 'ok'})
		self.assertRevContent(self.store1, rev2, {'FILE' : 'update'})

	def test_suspend_multi(self):
		(doc, rev1, rev_s1) = self.createSuspendDoc()

		with Connector().update(self.store1, doc, rev1) as w:
			w.writeAll('FILE', 'forward')
			w.commit()
			rev2 = w.getRev()

		with Connector().update(self.store1, doc, rev2) as w:
			w.writeAll('FILE', 'Hail to the king, baby!')
			w.suspend()
			rev_s2 = w.getRev()

		l = Connector().lookupDoc(doc)
		self.assertEqual(l.revs(), [rev2])
		self.assertEqual(len(l.preRevs()), 2)
		self.assertTrue(rev_s1 in l.preRevs())
		self.assertTrue(rev_s2 in l.preRevs())

		s = Connector().stat(rev_s1)
		self.assertEqual(s.parents(), [rev1])
		s = Connector().stat(rev_s2)
		self.assertEqual(s.parents(), [rev2])

		self.assertRevContent(self.store1, rev1, {'FILE' : 'ok'})
		self.assertRevContent(self.store1, rev_s1, {'FILE' : 'update'})
		self.assertRevContent(self.store1, rev2, {'FILE' : 'forward'})
		self.assertRevContent(self.store1, rev_s2, {'FILE' : 'Hail to the king, baby!'})

	def test_resume_wrong(self):
		(doc, rev1, rev2) = self.createSuspendDoc()
		self.assertRaises(IOError, Connector().resume, self.store1, doc, rev1)

		l = Connector().lookupDoc(doc)
		self.assertEqual(l.revs(), [rev1])
		self.assertEqual(l.preRevs(), [rev2])
		self.assertRevContent(self.store1, rev1, {'FILE' : 'ok'})
		self.assertRevContent(self.store1, rev2, {'FILE' : 'update'})

	def test_resume_abort(self):
		(doc, rev1, rev2) = self.createSuspendDoc()

		with Connector().resume(self.store1, doc, rev2) as w:
			w.writeAll('FILE', 'Hail to the king, baby!')

			l = Connector().lookupDoc(doc)
			self.assertEqual(l.revs(), [rev1])
			self.assertEqual(l.preRevs(), [rev2])

			w.close()

		l = Connector().lookupDoc(doc)
		self.assertEqual(l.revs(), [rev1])
		self.assertEqual(l.preRevs(), [rev2])
		self.assertRevContent(self.store1, rev1, {'FILE' : 'ok'})
		self.assertRevContent(self.store1, rev2, {'FILE' : 'update'})

	def test_resume_commit(self):
		(doc, rev1, rev2) = self.createSuspendDoc()

		with Connector().resume(self.store1, doc, rev2) as w:
			w.writeAll('FILE', 'What are you waiting for, christmas?')
			w.commit()
			rev3 = w.getRev()

		l = Connector().lookupDoc(doc)
		self.assertEqual(l.revs(), [rev3])
		self.assertEqual(len(l.preRevs()), 0)

		s = Connector().stat(rev3)
		self.assertEqual(s.parents(), [rev1])
		self.assertRevContent(self.store1, rev1, {'FILE' : 'ok'})
		self.assertRevContent(self.store1, rev3, {'FILE' : 'What are you waiting for, christmas?'})

	def test_resume_suspend_orig(self):
		(doc, rev1, rev2) = self.createSuspendDoc()

		with Connector().resume(self.store1, doc, rev2) as w:
			w.suspend()
			rev3 = w.getRev()

		l = Connector().lookupDoc(doc)
		self.assertEqual(l.revs(), [rev1])
		self.assertEqual(l.preRevs(), [rev3])

		s = Connector().stat(rev3)
		self.assertEqual(s.parents(), [rev1])

		self.assertRevContent(self.store1, rev1, {'FILE' : 'ok'})
		self.assertRevContent(self.store1, rev3, {'FILE' : 'update'})

	def test_resume_suspend_mod(self):
		(doc, rev1, rev2) = self.createSuspendDoc()

		with Connector().resume(self.store1, doc, rev2) as w:
			w.writeAll('FILE', 'What are you waiting for, christmas?')
			w.suspend()
			rev3 = w.getRev()

		l = Connector().lookupDoc(doc)
		self.assertEqual(l.revs(), [rev1])
		self.assertEqual(l.preRevs(), [rev3])

		s = Connector().stat(rev3)
		self.assertEqual(s.parents(), [rev1])

		self.assertRevContent(self.store1, rev1, {'FILE' : 'ok'})
		self.assertRevContent(self.store1, rev3, {'FILE' : 'What are you waiting for, christmas?'})

	def test_forget(self):
		(doc, rev1, rev_s1) = self.createSuspendDoc()

		with Connector().update(self.store1, doc, rev1) as w:
			w.writeAll('FILE', 'forward')
			w.commit()
			rev2 = w.getRev()

		with Connector().update(self.store1, doc, rev2) as w:
			w.writeAll('FILE', 'Hail to the king, baby!')
			w.suspend()
			rev_s2 = w.getRev()

		self.assertRaises(IOError, Connector().forget, self.store1, doc, rev1)
		Connector().forget(self.store1, doc, rev_s1)

		l = Connector().lookupDoc(doc)
		self.assertEqual(l.revs(), [rev2])
		self.assertEqual(l.preRevs(), [rev_s2])


class TestGarbageCollector(CommonParts):

	def gc(self, store):
		guid = store.encode('hex')
		result = self.erlCall(
			"""case peerdrive_volman:store(<<16#"""+guid+""":128>>) of
				{ok, Pid} -> peerdrive_file_store:gc(Pid);
				error     -> {error, enoent}
			end.""")
		self.assertEqual(result, '{ok, ok}')

	def test_collect(self):
		# deliberately close handle after creating!
		with Connector().create(self.store1, "public.data", "test.ignore") as w:
			w.commit()
			doc = w.getDoc()
			rev = w.getRev()

		# perform a GC cycle
		self.gc(self.store1)

		l = Connector().lookupDoc(doc)
		self.assertEqual(l.revs(), [])
		self.assertEqual(l.preRevs(), [])
		self.assertRaises(IOError, Connector().stat, rev)

	def test_create_keep_handle(self):
		with Connector().create(self.store1, "public.data", "test.ignore") as w:
			w.commit()
			doc = w.getDoc()
			rev = w.getRev()

			# perform a GC cycle
			self.gc(self.store1)

			l = Connector().lookupDoc(doc)
			self.assertEqual(l.revs(), [rev])
			self.assertEqual(l.preRevs(), [])

			Connector().stat(rev)

	def test_fork_keep_handle(self):
		w = self.create(self.store1, "test.format.foo")
		self.assertEqual(w.getType(), "test.format.foo")
		w.commit()
		doc1 = w.getDoc()
		rev1 = w.getRev()

		with Connector().fork(self.store1, rev1, "test.ignore") as w:
			w.write('FILE', 'update')
			w.commit()
			doc2 = w.getDoc()
			rev2 = w.getRev()

			# perform a GC cycle
			self.gc(self.store1)

			l = Connector().lookupDoc(doc2)
			self.assertEqual(l.revs(), [rev2])
			self.assertEqual(l.preRevs(), [])
			Connector().stat(rev2)

	def test_transitive_keep(self):
		with Connector().create(self.store1, "public.data", "test.ignore") as w1:
			with Connector().create(self.store1, "public.data", "test.ignore") as w2:
				w2.write('FILE', 'test')
				w2.commit()
				doc2 = w2.getDoc()
				rev2 = w2.getRev()

				# create a reference from w1 to w2
				w1.write('PDSD', struct.dumps([struct.DocLink(self.store2, doc2)]))
				w1.commit()
				doc1 = w1.getDoc()
				rev1 = w1.getRev()

			# w2 is closed now, w1 still open, should prevent gc
			self.gc(self.store1)

			l = Connector().lookupDoc(doc1)
			self.assertEqual(l.revs(), [rev1])
			self.assertEqual(l.preRevs(), [])
			self.assertEqual(Connector().lookupRev(rev1), [self.store1])
			l = Connector().lookupDoc(doc2)
			self.assertEqual(l.revs(), [rev2])
			self.assertEqual(l.preRevs(), [])
			self.assertEqual(Connector().lookupRev(rev2), [self.store1])


class TestReplicator(CommonParts):

	def test_sticky(self):
		# create the document which should get replicated
		w = self.create(self.store1)
		w.writeAll('FILE', "foobar")
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		# create sticky contianer on first store
		s = struct.Folder()
		with s.create(self.store1, "foo") as dummy:
			s.append(struct.DocLink(self.store1, doc))
			s.save()
			contDoc = s.getDoc()

			# need a dummy folder on both stores
			self.createCommon([self.store1, self.store2], "org.peerdrive.folder",
				data={'PDSD' : struct.dumps([{'':struct.DocLink(self.store1, contDoc)}])})

		watch1 = self.watchDoc(doc, connector.Watch.EVENT_REPLICATED)
		watch2 = self.watchRev(rev, connector.Watch.EVENT_REPLICATED)

		# now replicate the folder to 2nd store
		Connector().replicateDoc(self.store1, contDoc, self.store2)

		# wait for sticky replicatin to happen
		self.assertTrue(watch1.waitForWatch())
		self.assertTrue(watch2.waitForWatch())

		# check doc (with rev) to exist on all stores
		l = Connector().lookupDoc(doc)
		self.assertEqual(l.revs(), [rev])
		self.assertEqual(len(l.stores(rev)), 2)
		self.assertTrue(self.store1 in l.stores(rev))
		self.assertTrue(self.store2 in l.stores(rev))

		l = Connector().lookupRev(rev)
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
		result = self.erlCall("peerdrive_sync_sup:start_sync(" + mode +
			", <<16#"+fromGuid+":128>>, <<16#"+toGuid+":128>>).")
		self.assertEqual(result, '{ok, ok}')

	def createFastForward(self):
		(doc, rev1) = self.createCommon([self.store1, self.store2])

		with Connector().update(self.store1, doc, rev1) as w:
			w.writeAll('FILE', 'forward')
			w.commit()
			rev2 = w.getRev()

		return (doc, rev2)

	def createMerge(self, type, base, left, right):
		(doc, rev1) = self.createCommon([self.store1, self.store2], type, data=base)

		with Connector().update(self.store1, doc, rev1) as w:
			for (part, data) in left.items():
				w.writeAll(part, data)
			w.commit()
			rev2 = w.getRev()

		with Connector().update(self.store2, doc, rev1) as w:
			for (part, data) in right.items():
				w.writeAll(part, data)
			w.commit()
			rev3 = w.getRev()

		# verify the merge condition
		l = Connector().lookupRev(rev1)
		self.assertTrue(self.store1 in l)
		self.assertTrue(self.store2 in l)
		self.assertEqual(Connector().lookupRev(rev2), [self.store1])
		self.assertEqual(Connector().lookupRev(rev3), [self.store2])
		self.assertEqual(Connector().stat(rev2).parents(), [rev1])
		self.assertEqual(Connector().stat(rev3).parents(), [rev1])

		return (doc, rev2, rev3)

	def performSync(self, doc, strategy):
		watch = self.watchDoc(doc, connector.Watch.EVENT_MODIFIED)

		self.startSync(strategy, self.store1, self.store2)

		# first wait until the doc gets changed
		while True:
			watch.reset()
			self.assertTrue(watch.waitForWatch())
			l = Connector().lookupDoc(doc)
			if len(l.revs()) == 1:
				break

		self.assertEqual(len(l.stores()), 2)
		self.assertTrue(self.store1 in l.stores())
		self.assertTrue(self.store2 in l.stores())

		# wait until sync_worker moved on
		result = self.erlCall(
			"""peerdrive_sync_locks:lock(<<16#"""+doc.encode('hex')+""":128>>),
			peerdrive_sync_locks:unlock(<<16#"""+doc.encode('hex')+""":128>>).""")
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
		(doc, rev1, rev2) = self.createMerge("public.data", {}, {'FILE' : "left1"},
			{'FILE' : "right1"})

		watch = self.watchDoc(doc, connector.Watch.EVENT_MODIFIED)

		self.startSync('ff', self.store1, self.store2)
		self.startSync('ff', self.store2, self.store1)

		self.assertFalse(watch.waitForWatch(1))

		# check that doc is not synced
		l = Connector().lookupDoc(doc)
		self.assertEqual(len(l.revs()), 2)
		self.assertEqual(l.rev(self.store1), rev1)
		self.assertEqual(l.rev(self.store2), rev2)

	# Checks if the 'latest' strategy makes an 'ours' merge and replicates that
	# to both stores
	def test_sync_latest(self):
		(doc, rev1, rev2) = self.createMerge("public.data", {}, {'FILE' : "left2"},
			{'FILE' : "right2"})
		l = self.performSync(doc, 'latest')

		rev = l.revs()[0]
		s = Connector().stat(rev)
		self.assertEqual(len(s.parents()), 2)
		self.assertTrue(rev1 in s.parents())
		self.assertTrue(rev2 in s.parents())
		self.assertRevContent(self.store1, rev, {'FILE' : 'left2'})

	# Check that 'latest' recognizes a fast-forward contition correctly
	def test_sync_latest_fallback(self):
		(doc, rev) = self.createFastForward()
		l = self.performSync(doc, 'latest')
		self.assertEqual(l.rev(self.store1), rev)
		self.assertEqual(l.rev(self.store2), rev)

	# Checks that the 'merge' strategy falls back to 'latest' in case of an
	# unknown document
	def test_sync_merge_fallback(self):
		(doc, rev1, rev2) = self.createMerge("public.data", {}, {'FILE' : "left3"},
			{'FILE' : "right3"})
		l = self.performSync(doc, 'merge')

		rev = l.revs()[0]
		s = Connector().stat(rev)
		self.assertEqual(len(s.parents()), 2)
		self.assertTrue(rev1 in s.parents())
		self.assertTrue(rev2 in s.parents())
		self.assertRevContent(self.store1, rev, {'FILE' : 'left3'})

	# Checks that the 'merge' strategy really merges
	def test_sync_merge(self):
		(doc, rev1, rev2) = self.createMerge("org.peerdrive.folder",
			{
				'META':struct.dumps({"a":1}),
				'PDSD':struct.dumps([{'':1}, {'':2}])
			},
			{
				'META':struct.dumps({"a":4, "b":2}),
				'PDSD':struct.dumps([{'':1}, {'':2}, {'':3}])
			},
			{
				'META':struct.dumps({"a":1, "c":3}),
				'PDSD':struct.dumps([{'':2}])
			})
		l = self.performSync(doc, 'merge')

		rev = l.revs()[0]
		s = Connector().stat(rev)
		self.assertEqual(len(s.parents()), 2)
		self.assertTrue(rev1 in s.parents())
		self.assertTrue(rev2 in s.parents())

		# all revs on all stores?
		l = Connector().lookupRev(rev1)
		self.assertTrue(self.store1 in l)
		self.assertTrue(self.store2 in l)
		l = Connector().lookupRev(rev2)
		self.assertTrue(self.store1 in l)
		self.assertTrue(self.store2 in l)

		# see if merge was ok
		with Connector().peek(self.store1, rev) as r:
			meta = struct.loads(self.store1, r.readAll('META'))
			if 'org.peerdrive.annotation' in meta:
				del meta['org.peerdrive.annotation']
			self.assertEqual(meta, {"a":4, "b":2, "c":3})
			pdsd = sorted(struct.loads(self.store1, r.readAll('PDSD')))
			self.assertEqual(pdsd, [{'':2},{'':3}])

if __name__ == '__main__':
	unittest.main()

