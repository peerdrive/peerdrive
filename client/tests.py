import unittest
from hotchpotch import HpConnector

class TestSync(unittest.TestCase):

	def setUp(self):
		if not HpConnector().enum().isMounted('rem1'):
			HpConnector().mount('rem1')
		self.store1 = HpConnector().enum().doc('rem1')
		if not HpConnector().enum().isMounted('rem2'):
			HpConnector().mount('rem2')
		self.store2 = HpConnector().enum().doc('rem2')

	def test_already_same(self):
		c = HpConnector()
		stores = [self.store1, self.store2]
		with c.create("public.data", "test", stores) as w:
			w.commit()
			doc = w.getDoc()
			rev = w.getRev()

		self.assertTrue(c.sync(doc))
		self.assertTrue(c.sync(doc, stores))

		c.delete_doc(doc, rev)


	def test_good(self):
		c = HpConnector()
		stores = [self.store1, self.store2]
		with c.create("public.data", "test", stores) as w:
			w.commit()
			doc = w.getDoc()
			rev = w.getRev()

		with c.update(doc, rev, stores=[self.store1]) as w:
			w.write('FILE', 'update')
			w.commit()
			rev = w.getRev()

		self.assertTrue(c.sync(doc))

		l = c.lookup(doc)
		self.assertEqual(len(l.revs()), 1)
		self.assertEqual(l.rev(self.store1), rev)
		self.assertEqual(l.rev(self.store2), rev)

		c.delete_doc(doc, rev)

	def test_bad(self):
		c = HpConnector()
		stores = [self.store1, self.store2]
		with c.create("public.data", "test", stores) as w:
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

		l = c.lookup(doc)
		self.assertEqual(len(l.revs()), 2)
		self.assertEqual(l.rev(self.store1), rev1)
		self.assertEqual(l.rev(self.store2), rev2)

		c.delete_doc(doc, rev1)
		c.delete_doc(doc, rev2)

if __name__ == '__main__':
	unittest.main()

