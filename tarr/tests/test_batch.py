import unittest
import mock

import tarr.batch as m


class TestBatch_process(unittest.TestCase):

    BATCH_CLASS = m.Batch

    def setUp(self):
        # get_reader() & get_writer() is mocked out
        # to return self.reader and self.writer respectively
        # self.reader will return self.data1, self.data2, self.data3
        # self.written will hold the data written to self.writer

        self.batch = self.BATCH_CLASS()
        self.batch.get_reader = mock.Mock(spec=self.batch.get_reader)
        self.reader = mock.MagicMock(spec=m.Reader(u''))
        self.data1 = mock.sentinel.a
        self.data2 = mock.sentinel.b
        self.data3 = mock.sentinel.c
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
        def duplicate(x):
            return (x, x)

        self.batch.transform = mock.Mock(
            self.batch.transform, side_effect=duplicate)
        self.batch.process(u'input', u'output')

        self.assertEqual(3, len(self.written))
        self.assertEqual([
            (mock.sentinel.a, mock.sentinel.a),
            (mock.sentinel.b, mock.sentinel.b),
            (mock.sentinel.c, mock.sentinel.c)],
            self.written)


class TestTarrBatch_process(TestBatch_process):

    BATCH_CLASS = m.TarrBatch

    def test_exception_in_tarr_transform_is_handled(self):
        self.batch.transformation = mock.Mock(spec=self.batch.transformation)
        self.batch.transformation.run = mock.Mock(side_effect=[Exception])

        self.batch.process(u'input', u'output')

        self.assertEqual(
            3, self.batch.transformation.run.call_count)
        self.assertEqual(
            [self.data1, self.data2, self.data3],
            self.written)
