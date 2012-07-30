import unittest
import tarr.db_model as m # odule
import tarr.application


class TApp(tarr.application.Application):
    pass


class Test_Job_get_application_instance(unittest.TestCase):

    def test_class(self):
        job = m.Job()
        job.application = 'tarr.test_db_model.TApp'

        app = job.get_application_instance()

        self.assertIsInstance(app, TApp)

    def test_application_job(self):
        job = m.Job()
        job.application = 'tarr.test_db_model.TApp'

        app = job.get_application_instance()

        self.assertEqual(job, app.job)


class Job_create_batch_fixture(unittest.TestCase):

    def __init__(self):
        self.job = m.Job()
        self.batch = self.job.create_batch(source='Test_Job_create_batch')


class Test_Job_create_batch(unittest.TestCase):

    def test_batch_added_to_job(self):
        f = Job_create_batch_fixture()
        self.assertEqual(1, len(f.job.batches))
        self.assertEqual(f.batch, f.job.batches[0])

    def test_batch_source_set(self):
        self.assertEqual('Test_Job_create_batch', Job_create_batch_fixture().batch.source)

    def test_job_is_referenced_from_batch(self):
        f = Job_create_batch_fixture()
        self.assertEqual(f.job, f.batch.job)