import unittest
import tarr.cli as m # odule


class Test_parse_args(unittest.TestCase):

    def test_create_job(self):
        args = m.parse_args(
            (
                'create_job jobname --app=location.clean.Application --dag=dag-config --source=complex:rovat_13:pm'
                ' --partitioning_name=every_200'
            ).split()
            + ['--description=a description'])

        self.assertEqual('create_job', args.command)
        self.assertEqual('jobname', args.name)
        self.assertEqual('location.clean.Application', args.application)
        self.assertEqual('dag-config', args.dag_config)
        self.assertEqual('complex:rovat_13:pm', args.source)
        self.assertEqual('every_200', args.partitioning_name)
        self.assertEqual('a description', args.description)

    def test_delete_job(self):
        args = m.parse_args('delete_job jobname'.split())

        self.assertEqual('delete_job', args.command)
        self.assertEqual('jobname', args.name)

    def test_process_job(self):
        args = m.parse_args('process_job jobname'.split())

        self.assertEqual('process_job', args.command)
        self.assertEqual('jobname', args.name)

    def test_process_batch(self):
        args = m.parse_args('process_batch batch_id'.split())

        self.assertEqual('process_batch', args.command)
        self.assertEqual('batch_id', args.batch_id)
