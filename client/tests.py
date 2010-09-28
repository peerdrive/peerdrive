import unittest
from hotchpotch import HpConnector


class CommonParts(unittest.TestCase):

	def setUp(self):
		if not HpConnector().enum().isMounted('rem1'):
			HpConnector().mount('rem1')
		self.store1 = HpConnector().enum().doc('rem1')
		if not HpConnector().enum().isMounted('rem2'):
			HpConnector().mount('rem2')
		self.store2 = HpConnector().enum().doc('rem2')

		self._disposeDocs = []
		self._disposeRevs = []

	def tearDown(self):
		for doc in self._disposeDocs:
			lookup = HpConnector().lookup(doc)
			for rev in lookup.revs():
				HpConnector().delete_doc(doc, rev)

		for rev in self._disposeRevs:
			HpConnector().delete_rev(rev)

	def disposeDoc(self, doc):
		self._disposeDocs.append(doc)

	def disposeRev(self, rev):
		self._disposeRevs.append(rev)


class TestCreatorCode(CommonParts):

	def test_create(self):
		c = HpConnector()
		with c.create("public.data", "test.foo", [self.store1]) as w:
			w.commit()
			doc = w.getDoc()
			rev = w.getRev()
			self.disposeDoc(doc)

		s = c.stat(rev)
		self.assertEqual(s.creator(), "test.foo")

	def test_fork(self):
		c = HpConnector()
		with c.create("public.data", "test.foo", [self.store1]) as w:
			w.commit()
			doc1 = w.getDoc()
			rev1 = w.getRev()
			self.disposeDoc(doc1)

		with c.fork(rev1, "test.bar") as w:
			w.commit()
			doc2 = w.getDoc()
			rev2 = w.getRev()
			self.disposeDoc(doc2)

		s = c.stat(rev1)
		self.assertEqual(s.creator(), "test.foo")
		s = c.stat(rev2)
		self.assertEqual(s.creator(), "test.bar")

	def test_update_change(self):
		c = HpConnector()
		with c.create("public.data", "test.foo", [self.store1]) as w:
			w.commit()
			doc = w.getDoc()
			rev1 = w.getRev()
			self.disposeDoc(doc)

		with c.update(doc, rev1, "test.baz") as w:
			w.commit()
			rev2 = w.getRev()

		s = c.stat(rev1)
		self.assertEqual(s.creator(), "test.foo")
		s = c.stat(rev2)
		self.assertEqual(s.creator(), "test.baz")

	def test_update_keep(self):
		c = HpConnector()
		with c.create("public.data", "test.foo", [self.store1]) as w:
			w.commit()
			doc = w.getDoc()
			rev1 = w.getRev()
			self.disposeDoc(doc)

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
		with c.create("test.format", "test.ignore", [self.store1]) as w:
			self.assertEqual(w.getType(), "test.format")
			w.commit()
			doc = w.getDoc()
			rev = w.getRev()
			self.disposeDoc(doc)

		s = c.stat(rev)
		self.assertEqual(s.type(), "test.format")

	def test_fork_keep(self):
		c = HpConnector()
		with c.create("test.format.foo", "test.ignore", [self.store1]) as w:
			self.assertEqual(w.getType(), "test.format.foo")
			w.commit()
			doc1 = w.getDoc()
			rev1 = w.getRev()
			self.disposeDoc(doc1)

		with c.fork(rev1, "test.ignore") as w:
			self.assertEqual(w.getType(), "test.format.foo")
			w.write('FILE', 'update')
			w.commit()
			doc2 = w.getDoc()
			rev2 = w.getRev()
			self.disposeDoc(doc2)

		s = c.stat(rev1)
		self.assertEqual(s.type(), "test.format.foo")
		s = c.stat(rev2)
		self.assertEqual(s.type(), "test.format.foo")

	def test_fork_change(self):
		c = HpConnector()
		with c.create("test.format.foo", "test.ignore", [self.store1]) as w:
			self.assertEqual(w.getType(), "test.format.foo")
			w.commit()
			doc1 = w.getDoc()
			rev1 = w.getRev()
			self.disposeDoc(doc1)

		with c.fork(rev1, "test.ignore") as w:
			w.write('FILE', 'update')
			self.assertEqual(w.getType(), "test.format.foo")
			w.setType("test.format.bar")
			self.assertEqual(w.getType(), "test.format.bar")
			w.commit()
			doc2 = w.getDoc()
			rev2 = w.getRev()
			self.disposeDoc(doc2)

		s = c.stat(rev1)
		self.assertEqual(s.type(), "test.format.foo")
		s = c.stat(rev2)
		self.assertEqual(s.type(), "test.format.bar")

	def test_update_keep(self):
		c = HpConnector()
		with c.create("test.format.foo", "test.ignore", [self.store1]) as w:
			self.assertEqual(w.getType(), "test.format.foo")
			w.commit()
			doc = w.getDoc()
			rev1 = w.getRev()
			self.disposeDoc(doc)

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
		with c.create("test.format.foo", "test.ignore", [self.store1]) as w:
			self.assertEqual(w.getType(), "test.format.foo")
			w.commit()
			doc = w.getDoc()
			rev1 = w.getRev()
			self.disposeDoc(doc)

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
		with c.create("public.data", "test.ignore", stores) as w:
			w.commit()
			doc = w.getDoc()
			rev = w.getRev()
			self.disposeDoc(doc)

		self.assertTrue(c.sync(doc))
		self.assertTrue(c.sync(doc, stores))


	def test_good(self):
		c = HpConnector()
		stores = [self.store1, self.store2]
		with c.create("public.data", "test.ignore", stores) as w:
			w.commit()
			doc = w.getDoc()
			rev = w.getRev()
			self.disposeDoc(doc)

		with c.update(doc, rev, stores=[self.store1]) as w:
			w.write('FILE', 'update')
			w.commit()
			rev = w.getRev()

		self.assertTrue(c.sync(doc))

		l = c.lookup(doc)
		self.assertEqual(len(l.revs()), 1)
		self.assertEqual(l.rev(self.store1), rev)
		self.assertEqual(l.rev(self.store2), rev)

	def test_bad(self):
		c = HpConnector()
		stores = [self.store1, self.store2]
		with c.create("public.data", "test.ignore", stores) as w:
			w.commit()
			doc = w.getDoc()
			rev = w.getRev()
			self.disposeDoc(doc)

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

		l = c.lookup(doc)
		self.assertEqual(len(l.revs()), 2)
		self.assertEqual(l.rev(self.store1), rev1)
		self.assertEqual(l.rev(self.store2), rev2)


if __name__ == '__main__':
	unittest.main()

