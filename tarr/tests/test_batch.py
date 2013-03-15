import unittest
import mock

import tarr.batch as m
from tarr.language import RETURN_TRUE
import tarr
from tarr.data import Data


class NoTransformBatch(m.Batch):

    def get_tarr_transform(self):
        return [RETURN_TRUE]


@tarr.rule
def duplicate(data):
    return (data, data)


class DuplicateInputBatch(m.Batch):

    def get_tarr_transform(self):
        return [
            duplicate,
            RETURN_TRUE
        ]


class TestBatch_process(unittest.TestCase):

    def setUp(self):
        # get_reader() & get_writer() is mocked out
        # to return self.reader and self.writer respectively
        # self.reader will return self.data1, self.data2, self.data3
        # self.written will hold the data written to self.writer

        self.batch = NoTransformBatch()
        self.batch.get_reader = mock.Mock(spec=self.batch.get_reader)
        self.reader = mock.MagicMock(spec=m.Reader(u''))
        self.data1 = Data(1, mock.sentinel.a)
        self.data2 = Data(2, mock.sentinel.b)
        self.data3 = Data(3, mock.sentinel.c)
        data = [self.data1, self.data2, self.data3]
        self.reader.__iter__.return_value = iter(data)
        self.batch.get_reader.return_value = self.reader

        self.batch.get_writer = mock.Mock(spec=self.batch.get_writer)
        self.written = []

        def store_data(data):
            self.written.append(data)

        self.writer = mock.Mock(spec=m.Writer(u''))
        self.writer.write.side_effect = store_data
        self.batch.get_writer.return_value = self.writer

    def test_instantiates_a_reader(self):
        self.batch.process(u'input', u'output')

        self.batch.get_reader.assert_called_once_with(u'input')

    def test_instantiates_a_writer(self):
        self.batch.process(u'input', u'output')

        self.batch.get_writer.assert_called_once_with(u'output')

    def test_data_read_from_reader_are_written_to_writer(self):
        self.batch.process(u'input', u'output')

        self.assertListEqual(
            [self.data1, self.data2, self.data3],
            self.written)

    def test_closes_reader(self):
        self.batch.process(u'input', u'output')

        self.reader.close.assert_called_once_with()

    def test_closes_writer(self):
        self.batch.process(u'input', u'output')

        self.writer.close.assert_called_once_with()

    def test_data_written_is_transformed_by_program(self):
        batch = DuplicateInputBatch()
        batch.get_reader = self.batch.get_reader
        batch.get_writer = self.batch.get_writer

        batch.process(u'input', u'output')

        self.assertEqual(3, len(self.written))
        self.assertEqual(
            (mock.sentinel.a, mock.sentinel.a),
            self.written[0].payload)

    def test_exception_in_transform_is_handled(self):
        self.batch.transform = mock.Mock(spec=self.batch.transform)
        self.batch.transform.run = mock.Mock(side_effect=[Exception])

        self.batch.process(u'input', u'output')

        self.assertEqual(3, self.batch.transform.run.call_count)
        self.assertEqual(
            [self.data1, self.data2, self.data3],
            self.written)
