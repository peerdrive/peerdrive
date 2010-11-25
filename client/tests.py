import unittest
import time
from hotchpotch import HpConnector


class CommonParts(unittest.TestCase):

	def setUp(self):
		if not HpConnector().enum().isMounted('rem1'):
			HpConnector().mount('rem1')
		self.store1 = HpConnector().enum().doc('rem1')
		self.store1Id = 'rem1'
		if not HpConnector().enum().isMounted('rem2'):
			HpConnector().mount('rem2')
		self.store2 = HpConnector().enum().doc('rem2')
		self.store2Id = 'rem2'

		self._disposeHandles = []

	def tearDown(self):
		for handle in self._disposeHandles:
			handle.close()

	def disposeHandle(self, handle):
		self._disposeHandles.append(handle)

	def create(self, type, creator, stores):
		w = HpConnector().create(type, creator, stores)
		self.disposeHandle(w)
		return w

	def fork(self, rev, creator, stores=[]):
		w = HpConnector().fork(rev, "test.bar", stores)
		self.disposeHandle(w)
		return w

	def assertRevContent(self, rev, content):
		with HpConnector().peek(rev) as r:
			for (part, data) in content.items():
				revData = r.readAll(part)
				self.assertEqual(revData, data)

		s = HpConnector().stat(rev)
		for part in s.parts():
			self.assertTrue(part in content)



class TestCreatorCode(CommonParts):

	def test_create(self):
		w = self.create("public.data", "test.foo", [self.store1])
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		s = HpConnector().stat(rev)
		self.assertEqual(s.creator(), "test.foo")

	def test_fork(self):
		c = HpConnector()
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
		c = HpConnector()
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
		c = HpConnector()
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
		c = HpConnector()
		w = self.create("test.format", "test.ignore", [self.store1])
		self.assertEqual(w.getType(), "test.format")
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		s = c.stat(rev)
		self.assertEqual(s.type(), "test.format")

	def test_fork_keep(self):
		c = HpConnector()
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
		c = HpConnector()
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
		c = HpConnector()
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
		c = HpConnector()
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
		c = HpConnector()
		stores = [self.store1, self.store2]
		w = self.create("public.data", "test.ignore", stores)
		w.commit()
		doc = w.getDoc()
		rev = w.getRev()

		self.assertTrue(c.sync(doc) == rev)
		self.assertTrue(c.sync(doc, stores=stores) == rev)

	def test_good(self):
		c = HpConnector()
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
		c = HpConnector()
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
		c = HpConnector()
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
		c = HpConnector()
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
		c = HpConnector()
		(doc, rev1, rev2) = self.createSuspendDoc()

		l = c.lookup_doc(doc)
		self.assertEqual(l.revs(), [rev1])
		self.assertEqual(l.preRevs(), [rev2])
		self.assertRevContent(rev1, {'FILE' : 'ok'})
		self.assertRevContent(rev2, {'FILE' : 'update'})

	def test_suspend_multi(self):
		c = HpConnector()
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
		c = HpConnector()
		(doc, rev1, rev2) = self.createSuspendDoc()
		self.assertRaises(IOError, c.resume, doc, rev1)

		l = c.lookup_doc(doc)
		self.assertEqual(l.revs(), [rev1])
		self.assertEqual(l.preRevs(), [rev2])
		self.assertRevContent(rev1, {'FILE' : 'ok'})
		self.assertRevContent(rev2, {'FILE' : 'update'})

	def test_resume_abort(self):
		c = HpConnector()
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
		c = HpConnector()
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
		c = HpConnector()
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
		c = HpConnector()
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
		c = HpConnector()
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

	def test_collect(self):
		c = HpConnector()

		# deliberately close handle after creating!
		with c.create("public.data", "test.foo", [self.store1]) as w:
			w.commit()
			doc = w.getDoc()
			rev = w.getRev()

		# perform a GC cycle
		c.gc(self.store1Id)

		l = c.lookup_doc(doc)
		self.assertEqual(l.revs(), [])
		self.assertEqual(l.preRevs(), [])
		self.assertRaises(IOError, c.stat, rev)

	def test_create_keep_handle(self):
		c = HpConnector()

		with c.create("public.data", "test.foo", [self.store1]) as w:
			w.commit()
			doc = w.getDoc()
			rev = w.getRev()

			# perform a GC cycle
			c.gc(self.store1Id)

			l = c.lookup_doc(doc)
			self.assertEqual(l.revs(), [rev])
			self.assertEqual(l.preRevs(), [])

			c.stat(rev)

	def test_fork_keep_handle(self):
		c = HpConnector()

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
			c.gc(self.store1Id)

			l = c.lookup_doc(doc2)
			self.assertEqual(l.revs(), [rev2])
			self.assertEqual(l.preRevs(), [])
			c.stat(rev2)


if __name__ == '__main__':
	unittest.main()

