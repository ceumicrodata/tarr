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