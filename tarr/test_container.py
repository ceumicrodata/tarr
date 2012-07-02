import unittest
import mock

from tarr import container as m # odule
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
        def fail(data):
            processor_container.fail()
        processor_container = self.get_container_with_mock_processor_process(side_effect=fail)

        processor_container.process(Data(id=3, payload=14))

        self.assertEqual(m.ProcessorContainer.FAILURE, processor_container.status)
        self.assertEqual(3, processor_container.data.id)
        self.assertEqual(14, processor_container.data.payload)

    def test_after_fail_can_not_succeed(self):
        def fail(data):
            processor_container.fail()
            return 99
        processor_container = self.get_container_with_mock_processor_process(side_effect=fail)

        processor_container.process(Data(id=3, payload=14))

        self.assertEqual(m.ProcessorContainer.FAILURE, processor_container.status)
        self.assertEqual(3, processor_container.data.id)
        self.assertEqual(14, processor_container.data.payload)

    def test_after_needs_human_can_not_succeed(self):
        def fail(data):
            processor_container.need_human()
            return 99
        processor_container = self.get_container_with_mock_processor_process(side_effect=fail)

        processor_container.process(Data(id=3, payload=14))

        self.assertEqual(m.ProcessorContainer.NEED_HUMAN, processor_container.status)
        self.assertEqual(3, processor_container.data.id)
        self.assertEqual(14, processor_container.data.payload)
