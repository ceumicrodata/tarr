import unittest
import mock

from tarr import container as m # odule
from tarr import ProcessorFailed
from tarr.processor import Processor
from tarr.data import Data


class A_Processor(Processor):

    pass


class TestContainer_initialize(unittest.TestCase):

    def test_initialize(self):
        processor_container = m.ProcessorContainer()
        processor_container.name = 'name'
        processor_container.impl = 'tarr.test_container.A_Processor'

        processor_container.initialize()

        self.assertTrue(isinstance(processor_container.processor, A_Processor))


class TestContainer_process(unittest.TestCase):

    PAYLOAD = 'payload'

    @property
    def data(self, id=1, payload=PAYLOAD):
        return Data(id=id, payload=payload)

    mock_process = None

    def get_container_with_mock_processor_process(self, **mockspec):
        processor_container = m.ProcessorContainer()
        processor_container.name = 'name'
        processor_container.impl = 'tarr.test_container.A_Processor'
        processor_container.initialize()
        self.mock_process = mock.Mock(spec=Processor.process, **mockspec)
        processor_container.processor.process = self.mock_process
        return processor_container

    def test_calls_processors_process_with_payload(self):
        processor_container = self.get_container_with_mock_processor_process(side_effect=lambda data: data)

        processor_container.process(self.data)

        self.mock_process.assert_called_once_with(self.PAYLOAD)

    def test_catches_exception_in_processors_process(self):
        msg = 'oops, processor died'
        processor_container = self.get_container_with_mock_processor_process(side_effect=Exception(msg))

        processor_container.process(self.data)

    def test_processor_dies_counts_as_failure(self):
        processor_container = self.get_container_with_mock_processor_process(side_effect=Exception)

        processor_container.process(self.data)

        self.assertEqual(m.ProcessorContainer.FAILURE, processor_container.status)

    def test_keeps_id_and_new_payload_on_success(self):
        processor_container = self.get_container_with_mock_processor_process(side_effect=lambda data: data * 2)

        processor_container.process(Data(id=3, payload=14))

        self.assertEqual(m.ProcessorContainer.SUCCESS, processor_container.status)
        self.assertEqual(3, processor_container.data.id)
        self.assertEqual(28, processor_container.data.payload)

    def test_keeps_id_and_old_payload_on_failure(self):
        processor_container = self.get_container_with_mock_processor_process(side_effect=ProcessorFailed)

        processor_container.process(Data(id=3, payload=14))

        self.assertEqual(m.ProcessorContainer.FAILURE, processor_container.status)
        self.assertEqual(3, processor_container.data.id)
        self.assertEqual(14, processor_container.data.payload)

    # statistics

    count_sentinel = -98

    def test_process_increments_count(self):
        processor_container = self.get_container_with_mock_processor_process()
        processor_container.count = self.count_sentinel

        processor_container.process(self.data)

        self.assertEqual(self.count_sentinel + 1, processor_container.count)

    def test_success_increments_success_count(self):
        processor_container = self.get_container_with_mock_processor_process()
        processor_container.success_count = self.count_sentinel

        processor_container.process(self.data)

        self.assertEqual(self.count_sentinel + 1, processor_container.success_count)

    def test_failure_increments_failure_count(self):
        processor_container = self.get_container_with_mock_processor_process(side_effect=ProcessorFailed)
        processor_container.failure_count = self.count_sentinel

        processor_container.process(self.data)

        self.assertEqual(self.count_sentinel + 1, processor_container.failure_count)

    def test_time_is_increased(self):
        processor_container = self.get_container_with_mock_processor_process()
        time_in_process = processor_container.time_in_process

        processor_container.process(self.data)
        time_in_process2 = processor_container.time_in_process

        self.assertLess(time_in_process, time_in_process2)

        processor_container.process(self.data)
        time_in_process3 = processor_container.time_in_process

        self.assertLess(time_in_process2, time_in_process3)
