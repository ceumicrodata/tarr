import unittest
import mock
import os
from tarr.test_case import TarrApplicationTestCase

import logging
from contextlib import contextmanager

import tarr.application as m # odule
import tarr.model
from datetime import datetime, timedelta


def make_app(cls=m.Application):
    app = cls()
    app.session = mock.Mock()
    app.program_config_hash = mock.Mock(app.program_config_hash, return_value=mock.sentinel.program_config_hash)
    return app


def create_job(app, name='test', program_config='', source='', partitioning_name='', description=''):
    app.create_job(name=name, program_config=program_config, source=source, partitioning_name=partitioning_name, description=description)
    app.session.commit.reset_mock()


def uncompleted_batch(source):
    batch = tarr.model.Batch()
    batch.source = source
    return batch

def completed_batch(source, time_completed=mock.sentinel.time_completed, program_config_hash=mock.sentinel.program_config_hash):
    batch = uncompleted_batch(source=source)
    batch.time_completed = time_completed
    batch.program_config_hash = program_config_hash
    return batch


class Bpplication(m.Application):

    pass


class Test_create_job(unittest.TestCase):

    def create_job(self, app, name='', program_config='', source='', partitioning_name='', description=''):
        app.create_job(name=name, program_config=program_config, source=source, partitioning_name=partitioning_name, description=description)

    def test_created(self):
        app = make_app()

        self.create_job(app)

        self.assertIsNotNone(app.job)

    def test_application(self):
        app = make_app()

        self.create_job(app)

        self.assertEqual('tarr.application.Application', app.job.application)

    def test_bpplication(self):
        app = make_app(cls=Bpplication)

        self.create_job(app)

        self.assertEqual('tarr.test_application.Bpplication', app.job.application)

    def test_added_to_session(self):
        app = make_app()

        self.create_job(app)

        app.session.add.assert_called_once_with(app.job)

    def test_creates_batches(self):
        app = make_app()
        app.create_batches = mock.Mock()

        self.create_job(app)

        app.create_batches.assert_called_once_with()

    def test_changes_committed(self):
        app = make_app()

        self.create_job(app)

        app.session.commit.assert_called_once_with()

    def test_parameters_stored(self):
        app = make_app()

        ms = mock.sentinel
        app.create_job(name=ms.name, program_config=ms.program_config, source=ms.source, partitioning_name=ms.partitioning_name, description=ms.description)

        self.assertEqual(ms.name, app.job.job_name)
        self.assertEqual(ms.program_config, app.job.program_config)
        self.assertEqual(ms.source, app.job.source)
        self.assertEqual(ms.partitioning_name, app.job.partitioning_name)
        self.assertEqual(ms.description, app.job.description)

    def test_program_config_hash_is_called_for_hash(self):
        app = make_app()

        self.create_job(app, program_config='tarr.nonexisting_program_config_for_hash')

        self.assertEqual(mock.sentinel.program_config_hash, app.job.program_config_hash)


class Test_program_config_file(unittest.TestCase):

    def test(self):
        app = make_app()
        program_config = 'tarr.fixtures.program'
        app.create_job(name='', program_config=program_config, source='', partitioning_name='', description='')

        self.assertRegexpMatches(app.program_config_file(), os.path.join('tarr', 'fixtures', 'program.py$'))


class Test_program_config_hash(unittest.TestCase):

    def test(self):
        app = m.Application()
        app.job = mock.Mock()
        app.job.program_config = 'tarr.fixtures.program_for_hash'

        self.assertEqual('48bae2445873f256cd8fa0793674bae315b67adb', app.program_config_hash())


class Test_load_program(unittest.TestCase):

    def test_program_is_available(self):
        app = make_app()
        app.job = mock.Mock()
        app.job.program_config = 'tarr.fixtures.program'

        app.load_program()

        self.assertIsNotNone(app.program)


class Test_process_job(unittest.TestCase):

    def test(self):
        processed_batches = []
        class Application(m.Application):
            def process_batch(self):
                processed_batches.append(self.batch.source)

        app = make_app(cls=Application)
        create_job(app)

        app.job.batches.append(uncompleted_batch(source='1'))
        app.job.batches.append(completed_batch(source='2'))
        app.job.batches.append(uncompleted_batch(source='3'))

        app.process_job()

        self.assertEqual(['1', '3'], processed_batches)


class Test_process_batch(unittest.TestCase):

    def mock_app(self):
        cls = m.Application
        app = make_app(cls=cls)
        create_job(app)
        batch = uncompleted_batch(source='1')
        app.job.batches.append(batch)
        app.batch = batch

        app.load_data_items = mock.Mock(
            cls.load_data_items,
            side_effect=lambda: [app.batch.source, app.batch.source * 2])

        app.process_data_item = mock.Mock(
            cls.process_data_item,
            side_effect=lambda data_item: [
                mock.sentinel.processed_data_item1,
                mock.sentinel.processed_data_item2
                ][len(data_item)-1])

        app.save_data_items = mock.Mock(
            cls.load_data_items,
            side_effect=lambda data_items: None)

        app.save_batch_statistics = mock.Mock(
            cls.save_batch_statistics)

        return app

    def test_load_data_items_called_once(self):
        app = self.mock_app()

        app.process_batch()

        app.load_data_items.assert_called_once_with()

    def test_process_data_item_called(self):
        app = self.mock_app()

        app.process_batch()

        self.assertTrue(app.process_data_item.called)
        self.assertEqual(2, app.process_data_item.call_count)
        app.process_data_item.assert_called_with('11')

    def test_save_data_items_called_once(self):
        app = self.mock_app()

        app.process_batch()

        app.save_data_items.assert_called_once_with(
            [mock.sentinel.processed_data_item1,
            mock.sentinel.processed_data_item2])

    def test_save_batch_statistics_called_once(self):
        app = self.mock_app()

        app.process_batch()

        app.save_batch_statistics.assert_called_once_with()

    def test_batch_is_completed(self):
        app = self.mock_app()

        app.process_batch()

        self.assertTrue(app.batch.is_processed)

    def test_config_hash_stored(self):
        app = self.mock_app()
        app.process_batch()

        self.assertEqual(mock.sentinel.program_config_hash, app.batch.program_config_hash)

    def test_time_completed_set(self):
        app = self.mock_app()

        time_before = datetime.now()
        app.process_batch()
        time_after = datetime.now()

        self.assertTrue(time_before <= app.batch.time_completed <= time_after)

    def test_commit_called_once(self):
        app = self.mock_app()
        app.session.commit = mock.Mock()

        app.process_batch()

        app.session.commit.assert_called_once_with()


# FIXME: RecordCollectorHandler and temporary_log_handler() should be moved out into a log testing lib
class RecordCollectorHandler(logging.Handler):

    def __init__(self):
        super(RecordCollectorHandler, self).__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record.getMessage())

@contextmanager
def temporary_log_handler(logger, handler):
    logger.addHandler(handler)
    yield
    logger.removeHandler(handler)


class Test_process_data_item(unittest.TestCase):

    def make_app(self, process):
        app = make_app()
        self.assertIsNone(app.program)
        app.program = mock.Mock(m.Program)
        app.program.run = mock.Mock(m.Program.run, side_effect=process)
        return app

    def test_works_with_program(self):
        app = self.make_app(process=lambda data_item: mock.sentinel.processed)

        output = app.process_data_item(mock.sentinel.input)

        app.program.run.assert_called_once_with(mock.sentinel.input)
        self.assertEqual(mock.sentinel.processed, output)

    def raise_exception_process(self, data_item):
        data_item['processed'] = True
        raise Exception('This should be caught by Application.process_data_item!')

    def test_exception_is_handled(self):
        app = self.make_app(self.raise_exception_process)

        data_item = {'something': 'svalue'}

        self.assertEqual({'something': 'svalue', 'processed': True}, app.process_data_item(data_item))

    def test_exception_logged(self):
        app = self.make_app(self.raise_exception_process)

        data_item = {'something': 'svalue'}

        record_collector = RecordCollectorHandler()

        with temporary_log_handler(m.log, record_collector):
            app.process_data_item(data_item)

        self.assertIn('''process_data_item({'something': 'svalue', 'processed': True})''', record_collector.records)

    def process_with_logging_error(self, record_collector, data_item):
        app = self.make_app(self.raise_exception_process)

        class CruelHandler(logging.Handler):
            def emit(self, record):
                if record.args:
                    raise Exception("Will not do it this time!")

        with temporary_log_handler(m.log, CruelHandler()), temporary_log_handler(m.log, record_collector):
            return app.process_data_item(data_item)

    def test_internal_logging_errors_are_not_propagated(self):
        data_item = {'something2': 'svalue2'}

        # should not get any exception - from logging
        output = self.process_with_logging_error(RecordCollectorHandler(), data_item)

        # we still get back the modified data
        self.assertEqual({'something2': 'svalue2', 'processed': True}, output)

    def test_on_internal_logging_error_something_is_still_logged(self):
        data_item = {'something2': 'svalue2'}

        record_collector = RecordCollectorHandler()

        self.process_with_logging_error(record_collector, data_item)

        self.assertIn('process_data_item - can not log data_item!', record_collector.records)


class DeleteJobFixture:

    app = None
    batch_1 = None
    batch_2 = None

    def __init__(self):
        self.app = make_app()
        create_job(self.app)
        self.batch_1 = uncompleted_batch(source=mock.sentinel.batch_1)
        self.batch_2 = completed_batch(source=mock.sentinel.batch_2)
        self.app.job.batches.append(self.batch_1)
        self.app.job.batches.append(self.batch_2)


class Test_delete_job(unittest.TestCase):

    def test_delete_batch_called(self):
        f = DeleteJobFixture()
        # collect app.batch.sources when delete_batch is called
        delete_batch_sources = []
        def record_app_batch_source():
            delete_batch_sources.append(f.app.batch.source)
        f.app.delete_batch = mock.Mock(f.app.delete_batch, side_effect=record_app_batch_source)

        f.app.delete_job()

        self.assertEqual([mock.sentinel.batch_1, mock.sentinel.batch_2], delete_batch_sources)

    def test_job_removed_from_session(self):
        f = DeleteJobFixture()
        job = f.app.job
        f.app.delete_batch = mock.Mock(f.app.delete_batch)

        f.app.delete_job()

        f.app.session.delete.assert_called_once_with(job)

    def test_changes_committed(self):
        f = DeleteJobFixture()

        f.app.delete_job()

        f.app.session.commit.assert_called_once_with()

    def test_job_is_set_to_None(self):
        f = DeleteJobFixture()

        f.app.delete_job()

        self.assertIsNone(f.app.job)


class DeleteBatchFixture:

    app = None
    batch = None

    def __init__(self):
        self.app = make_app()
        self.batch = uncompleted_batch(source=mock.sentinel.batch)
        self.app.batch = self.batch


class Test_delete_batch(unittest.TestCase):

    def test_batch_removed_from_session(self):
        f = DeleteBatchFixture()
        f.app.delete_batch()

        f.app.session.delete.assert_called_once_with(f.batch)

    def test_batch_is_set_to_None(self):
        f = DeleteBatchFixture()
        f.app.delete_batch()

        self.assertIsNone(f.app.batch)


class Test_statistics(TarrApplicationTestCase):

    def make_app(self):
        app = m.Application()
        app.session = self.session

        app.create_job('name', 'tarr.fixtures.program', 'source', 'partitioning_name', 'description')
        app.load_program()
        app.job.create_batch(source='1')
        app.batch = app.job.batches[0]

        # set statistics on nodes
        app.program.runner.ensure_statistics(2)
        stat1, stat2 = self.stats(app)

        stat1.item_count = 10
        stat1.success_count = 1
        stat1.failure_count = 9
        stat1.run_time = timedelta(1, 1, 1)

        stat2.item_count = 20
        stat2.success_count = 9
        stat2.failure_count = 11
        stat2.run_time = timedelta(2, 2, 2)
        return app

    def stats(self, app):
        return app.program.statistics[:2]

    def reload_app(self):
        # forget in-memory statistics objects
        self.session.flush()
        self.session.expunge_all()

        app = m.Application()
        app.session = self.session
        app.job = self.session.query(tarr.model.Job).one()
        app.load_program()
        app.batch = app.job.batches[0]
        return app

    def test_persistence(self):
        app = self.make_app()

        app.save_batch_statistics()

        # in a fresh app instance test loading statistics
        app = self.reload_app()

        app.merge_batch_statistics()

        # check statistics on nodes
        stat1, stat2 = self.stats(app)
        self.assertEqual(
            ((10, 1, 9, timedelta(1, 1, 1)),
                (20, 9, 11, timedelta(2, 2, 2))),
            ((stat1.item_count, stat1.success_count, stat1.failure_count, stat1.run_time),
                (stat2.item_count, stat2.success_count, stat2.failure_count, stat2.run_time)))

    def test_merge_batch_statistics_is_additive(self):
        app = self.make_app()

        app.save_batch_statistics()

        # in a fresh app instance test loading statistics
        app = self.reload_app()

        app.merge_batch_statistics()
        # and again - should multiply previous results by 2!
        app.merge_batch_statistics()

        # check statistics on nodes
        stat1, stat2 = self.stats(app)
        self.assertEqual(
            ((20, 2, 18, timedelta(2, 2, 2)),
                (40, 18, 22, timedelta(4, 4, 4))),
            ((stat1.item_count, stat1.success_count, stat1.failure_count, stat1.run_time),
                (stat2.item_count, stat2.success_count, stat2.failure_count, stat2.run_time)))
