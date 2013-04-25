import unittest
import tarr.payload as m


class PayloadTests(object):

    INPUT = u'INPUT'
    KEYS = list()
    UNKNOWN_KEY = u'UNKNOWN KEY'

    payload = None  # should be supplied by setUp

    def test_input(self):
        self.assertIs(self.INPUT, self.payload.input)

    def test_keys(self):
        self.assertEqual(self.KEYS, list(self.payload.keys()))

    def test_accessing_unknown_key_raises_KeyError(self):
        with self.assertRaises(KeyError):
            self.payload[self.UNKNOWN_KEY]


class PayloadTestsWithExistingKey(PayloadTests):

    EXISTING_KEY = u'EXISTING KEY'
    EXISTING_VALUE = u'EXISTING VALUE'
    KEYS = [EXISTING_KEY]

    def test_existing_key_is_in_keys(self):
        self.assertIn(self.EXISTING_KEY, self.payload.keys())

    def test_existing_keys_value_can_be_retrieved(self):
        self.assertIs(self.EXISTING_VALUE, self.payload[self.EXISTING_KEY])


class Test_new(PayloadTests, unittest.TestCase):

    def setUp(self):
        self.payload = m.new(self.INPUT)

    def test_getitem(self):
        with self.assertRaises(KeyError):
            self.payload[0]


class Test_with_new_result(PayloadTestsWithExistingKey, unittest.TestCase):

    def setUp(self):
        self.payload = (
            m.new(self.INPUT)
            .with_new_result(u'WNR', self.EXISTING_KEY, self.EXISTING_VALUE))

    def test_optional_parameter_new_input_sets_input(self):
        NEW_INPUT = u'NEW INPUT'

        self.assertNotEqual(NEW_INPUT, self.INPUT)

        payload = (
            m.new(self.INPUT)
            .with_new_result(
                u'WNR',
                self.EXISTING_KEY, self.EXISTING_VALUE,
                NEW_INPUT))

        self.assertIs(NEW_INPUT, payload.input)

    def test_redefine_existing_key_value_is_overwritten(self):
        new_value = object()
        overwritten_payload = self.payload.with_new_result(
            u'redefine existing', self.EXISTING_KEY, new_value)

        self.assertIs(new_value, overwritten_payload[self.EXISTING_KEY])

    def test_redefine_existing_key_keys_is_unchanged(self):
        new_value = object()
        overwritten_payload = self.payload.with_new_result(
            u'redefine existing', self.EXISTING_KEY, new_value)

        self.assertListEqual(
            sorted(self.payload.keys()),
            sorted(overwritten_payload.keys()))


class Test_with_key_removed(PayloadTestsWithExistingKey, unittest.TestCase):

    def setUp(self):
        REMOVED_KEY = u'REMOVED KEY'

        self.assertNotEqual(REMOVED_KEY, self.EXISTING_KEY)

        self.payload = (
            m.new(self.INPUT)
            .with_new_result(u'WNR', self.EXISTING_KEY, self.EXISTING_VALUE)
            .with_new_result(u'WNR2', REMOVED_KEY, u'SOME VALUE')
            .with_key_removed(u'WKR', REMOVED_KEY))


class Test_new_input(PayloadTestsWithExistingKey, unittest.TestCase):

    def setUp(self):
        OLD_INPUT = u'OLD INPUT'

        self.assertNotEqual(OLD_INPUT, self.INPUT)

        self.payload = (
            m.new(OLD_INPUT)
            .with_new_result(u'WNR', self.EXISTING_KEY, self.EXISTING_VALUE)
            .with_new_input(u'WNI', self.INPUT))
