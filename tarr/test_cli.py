import unittest
import mock
import tarr.cli as m # odule
import tarr.application
from tarr.model import Job, Batch
from db.db_test import TestConnection, SqlTestCase


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


class Test_main(unittest.TestCase):

    def test_init_db_called(self):
        command_mock = mock.Mock()
        command_mock.init_db = mock.Mock(m.Command.init_db)
        command_class_mock = mock.Mock(m.Command, return_value=command_mock)
        commands = dict(process_job=command_class_mock)
        args = 'process_job first'.split()

        m.main(commands, args)

        command_mock.init_db.assert_called_once_with(m.parse_args(args))

    def test_run_called(self):
        command_mock = mock.Mock()
        command_mock.run = mock.Mock(m.Command.run)
        command_class_mock = mock.Mock(m.Command, return_value=command_mock)
        commands = dict(process_job=command_class_mock)
        args = 'process_job first'.split()

        m.main(commands, args)

        command_mock.run.assert_called_once_with(m.parse_args(args))

    def test_shutdown_called(self):
        command_mock = mock.Mock()
        command_mock.shutdown = mock.Mock(m.Command.shutdown)
        command_class_mock = mock.Mock(m.Command, return_value=command_mock)
        commands = dict(process_job=command_class_mock)
        args = 'process_job first'.split()

        m.main(commands, args)

        command_mock.shutdown.assert_called_once_with()


class Test_main_integration(SqlTestCase):

    def test_create_job(self):
        args_list = (
            TestConnection().as_args_list()
            + (
                'create_job jobname --app=tarr.application.Application'
                ' --dag=fixtures/test_dag_config'
                ' --source=complex:rovat_13:pm').split())

        m.main(args=args_list)

        self.assert_rows('SELECT job_name FROM tarr.job', [['job_name'], ['jobname']])


def make_db_safe(command):
    command.session = mock.Mock()
    command.init_db = mock.Mock(command.init_db)
    command.shutdown = mock.Mock(command.shutdown)
    return command


def use_application(command, app):
    command.application = app

def mock_get_application(command, app):
    command.get_application = mock.Mock(command.get_application, side_effect=lambda application: use_application(command, app))

def mock_get_application_from_jobname(command, app):
    command.get_application_from_jobname = mock.Mock(command.get_application_from_jobname, side_effect=lambda jobname: use_application(command, app))

def mock_get_application_from_batchid(command, app):
    command.get_application_from_batchid = mock.Mock(command.get_application_from_batchid, side_effect=lambda jobname: use_application(command, app))

def args_mock():
    return mock.sentinel


class Test_CreateJobCommand(unittest.TestCase):

    def get_command(self):
        command = make_db_safe(m.CreateJobCommand())
        application_mock = mock.Mock(tarr.application.Application)
        application_mock.create_job = mock.Mock(tarr.application.Application.create_job)
        mock_get_application(command, application_mock)
        return command

    def test_application_loading(self):
        command = self.get_command()

        command.run(args_mock())

        command.get_application.assert_called_once_with(mock.sentinel.application)

    def test_create_job(self):
        command = self.get_command()

        command.run(args_mock())

        command.application.create_job.assert_called_once_with(
            name=mock.sentinel.name,
            dag_config=mock.sentinel.dag_config,
            source=mock.sentinel.source,
            partitioning_name=mock.sentinel.partitioning_name,
            description=mock.sentinel.description)


class Test_DeleteJobCommand(unittest.TestCase):

    def get_command(self):
        command = make_db_safe(m.DeleteJobCommand())
        application_mock = mock.Mock(tarr.application.Application)
        application_mock.delete_job = mock.Mock(tarr.application.Application.delete_job)
        mock_get_application_from_jobname(command, application_mock)
        return command

    def test_job_loading(self):
        command = self.get_command()

        command.run(args_mock())

        command.get_application_from_jobname.assert_called_once_with(mock.sentinel.name)

    def test_delete_job(self):
        command = self.get_command()

        command.run(args_mock())

        command.application.delete_job.assert_called_once_with()


class Test_ProcessJobCommand(unittest.TestCase):

    def get_command(self):
        command = make_db_safe(m.ProcessJobCommand())
        application_mock = mock.Mock(tarr.application.Application)
        application_mock.process_job = mock.Mock(tarr.application.Application.process_job)
        mock_get_application_from_jobname(command, application_mock)
        return command

    def test_get_application_from_jobname(self):
        command = self.get_command()

        command.run(args_mock())

        command.get_application_from_jobname.assert_called_once_with(mock.sentinel.name)

    def test_load_dag(self):
        command = self.get_command()

        command.run(args_mock())

        command.application.load_dag.assert_called_once_with()

    def test_process_job(self):
        command = self.get_command()

        command.run(args_mock())

        command.application.process_job.assert_called_once_with()


class Test_ProcessBatchCommand(unittest.TestCase):

    def get_command(self):
        command = make_db_safe(m.ProcessBatchCommand())
        application_mock = mock.Mock(tarr.application.Application)
        application_mock.process_job = mock.Mock(tarr.application.Application.process_job)
        mock_get_application_from_batchid(command, application_mock)
        return command

    def test_process_batch(self):
        command = self.get_command()
        command.process_batch = mock.Mock(command.process_batch)

        command.run(args_mock())

        command.process_batch.assert_called_once_with(mock.sentinel.batch_id)

    def test_get_application_from_batchid(self):
        command = self.get_command()

        command.process_batch(mock.sentinel.batch_id)

        command.get_application_from_batchid.assert_called_once_with(mock.sentinel.batch_id)

    def test_load_dag(self):
        command = self.get_command()

        command.process_batch(mock.sentinel.batch_id)

        command.application.load_dag.assert_called_once_with()

    def test_app_process_batch(self):
        command = self.get_command()

        command.process_batch(mock.sentinel.batch_id)

        command.application.process_batch.assert_called_once_with()


def session_query_one_mock(return_value):
    query = mock.Mock()
    query.return_value = query
    query.filter = query
    query.one = mock.Mock(return_value=return_value)
    return query


class Test_Command_get_application_from_jobname(unittest.TestCase):

    def test(self):
        command = make_db_safe(m.Command())

        mock_application = mock.Mock(tarr.application.Application)
        mock_job = mock.Mock(Job)
        mock_job.get_application_instance = mock.Mock(Job.get_application_instance, return_value=mock_application)

        command.session.query = session_query_one_mock(return_value=mock_job)

        command.get_application_from_jobname('jobname')

        command.session.query.one.assert_called_once_with()
        mock_job.get_application_instance.assert_called_once_with()
        self.assertEqual(command.session, command.application.session)


class Test_Command_get_application_from_batchid(unittest.TestCase):

    def test(self):
        command = make_db_safe(m.Command())

        mock_application = mock.Mock(tarr.application.Application)
        mock_job = mock.Mock(Job)
        mock_job.get_application_instance = mock.Mock(Job.get_application_instance, return_value=mock_application)

        mock_batch = mock.Mock(Batch)
        mock_batch.job = mock_job

        command.session.query = session_query_one_mock(return_value=mock_batch)

        command.get_application_from_batchid(batch_id=None)

        command.session.query.one.assert_called_once_with()
        mock_job.get_application_instance.assert_called_once_with()
        self.assertEqual(mock_batch, command.application.batch)
        self.assertEqual(command.session, command.application.session)
