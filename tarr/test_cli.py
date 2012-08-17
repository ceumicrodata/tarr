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

        self.assertEqual('command_create_job', args.command)
        self.assertEqual('jobname', args.name)
        self.assertEqual('location.clean.Application', args.application)
        self.assertEqual('dag-config', args.dag_config)
        self.assertEqual('complex:rovat_13:pm', args.source)
        self.assertEqual('every_200', args.partitioning_name)
        self.assertEqual('a description', args.description)

    def test_delete_job(self):
        args = m.parse_args('delete_job jobname'.split())

        self.assertEqual('command_delete_job', args.command)
        self.assertEqual('jobname', args.name)

    def test_process_job(self):
        args = m.parse_args('process_job jobname'.split())

        self.assertEqual('command_process_job', args.command)
        self.assertEqual('jobname', args.name)

    def test_process_batch(self):
        args = m.parse_args('process_batch batch_id'.split())

        self.assertEqual('command_process_batch', args.command)
        self.assertEqual('batch_id', args.batch_id)


class Test_Cli_integration(SqlTestCase):

    def test_create_job(self):
        args_list = TestConnection().as_args_list() + 'create_job jobname --app=tarr.application.Application --dag=fixtures/test_dag_config --source=complex:rovat_13:pm'.split()

        cli = m.Cli()
        cli.main(args_list)

        self.assert_rows('SELECT job_name FROM tarr.job', [['job_name'], ['jobname']])


def db_safe_cli():
    cli = m.Cli()
    def set_session(args):
        cli.session = mock.Mock()
    cli.init_db = mock.Mock(cli.init_db, side_effect=set_session)
    cli.shutdown = mock.Mock(cli.shutdown)
    return cli


def use_application(cli, app):
    cli.application = app

def mock_get_application(cli, app):
    cli.get_application = mock.Mock(cli.get_application, side_effect=lambda application: use_application(cli, app))

def mock_get_application_from_jobname(cli, app):
    cli.get_application_from_jobname = mock.Mock(cli.get_application_from_jobname, side_effect=lambda jobname: use_application(cli, app))

def mock_get_application_from_batchid(cli, app):
    cli.get_application_from_batchid = mock.Mock(cli.get_application_from_batchid, side_effect=lambda jobname: use_application(cli, app))

def args_mock():
    return mock.sentinel


class CliCommandDbUsageChecker(object):

    def test_init_db_called(self):
        cli = self.get_cli()

        args = args_mock()
        self.call_cli_command(cli, args)

        cli.init_db.assert_called_once_with(args)

    def test_shutdown_called(self):
        cli = self.get_cli()

        args = args_mock()
        self.call_cli_command(cli, args)

        cli.shutdown.assert_called_once_with()


class Test_Cli_command_create_job(unittest.TestCase, CliCommandDbUsageChecker):

    def get_cli(self):
        cli = db_safe_cli()
        application_mock = mock.Mock(tarr.application.Application)
        application_mock.create_job = mock.Mock(tarr.application.Application.create_job)
        mock_get_application(cli, application_mock)
        return cli

    def call_cli_command(self, cli, args):
        cli.command_create_job(args)

    def test_application_loading(self):
        cli = self.get_cli()

        self.call_cli_command(cli, args_mock())

        cli.get_application.assert_called_once_with(mock.sentinel.application)

    def test_create_job(self):
        cli = self.get_cli()

        self.call_cli_command(cli, args_mock())

        cli.application.create_job.assert_called_once_with(
            name=mock.sentinel.name,
            dag_config=mock.sentinel.dag_config,
            source=mock.sentinel.source,
            partitioning_name=mock.sentinel.partitioning_name,
            description=mock.sentinel.description)


class Test_Cli_command_delete_job(unittest.TestCase, CliCommandDbUsageChecker):

    def get_cli(self):
        cli = db_safe_cli()
        application_mock = mock.Mock(tarr.application.Application)
        application_mock.delete_job = mock.Mock(tarr.application.Application.delete_job)
        mock_get_application_from_jobname(cli, application_mock)
        return cli

    def call_cli_command(self, cli, args):
        cli.command_delete_job(args)

    def test_job_loading(self):
        cli = self.get_cli()

        self.call_cli_command(cli, args_mock())

        cli.get_application_from_jobname.assert_called_once_with(mock.sentinel.name)

    def test_delete_job(self):
        cli = self.get_cli()

        self.call_cli_command(cli, args_mock())

        cli.application.delete_job.assert_called_once_with()


class Test_Cli_command_process_job(unittest.TestCase, CliCommandDbUsageChecker):

    def get_cli(self):
        cli = db_safe_cli()
        application_mock = mock.Mock(tarr.application.Application)
        application_mock.process_job = mock.Mock(tarr.application.Application.process_job)
        mock_get_application_from_jobname(cli, application_mock)
        return cli

    def call_cli_command(self, cli, args):
        cli.command_process_job(args)

    def test_get_application_from_jobname(self):
        cli = self.get_cli()

        self.call_cli_command(cli, args_mock())

        cli.get_application_from_jobname.assert_called_once_with(mock.sentinel.name)

    def test_load_dag(self):
        cli = self.get_cli()

        self.call_cli_command(cli, args_mock())

        cli.application.load_dag.assert_called_once_with()

    def test_process_job(self):
        cli = self.get_cli()

        self.call_cli_command(cli, args_mock())

        cli.application.process_job.assert_called_once_with()


class Test_Cli_command_process_batch(unittest.TestCase, CliCommandDbUsageChecker):

    def get_cli(self):
        cli = db_safe_cli()
        application_mock = mock.Mock(tarr.application.Application)
        application_mock.process_job = mock.Mock(tarr.application.Application.process_job)
        mock_get_application_from_batchid(cli, application_mock)
        return cli

    def call_cli_command(self, cli, args):
        cli.command_process_batch(args)

    def test_get_application_from_batchid(self):
        cli = self.get_cli()

        self.call_cli_command(cli, args_mock())

        cli.get_application_from_batchid.assert_called_once_with(mock.sentinel.batch_id)

    def test_load_dag(self):
        cli = self.get_cli()

        self.call_cli_command(cli, args_mock())

        cli.application.load_dag.assert_called_once_with()

    def test_process_batch(self):
        cli = self.get_cli()

        self.call_cli_command(cli, args_mock())

        cli.application.process_batch.assert_called_once_with()


def session_query_one_mock(return_value):
    query = mock.Mock()
    query.return_value = query
    query.filter = query
    query.one = mock.Mock(return_value=return_value)
    return query


class Test_cli_get_application_from_jobname(unittest.TestCase):

    def test(self):
        cli = db_safe_cli()
        cli.init_db(args_mock())

        mock_application = mock.Mock(tarr.application.Application)
        mock_job = mock.Mock(Job)
        mock_job.get_application_instance = mock.Mock(Job.get_application_instance, return_value=mock_application)

        cli.session.query = session_query_one_mock(return_value=mock_job)

        cli.get_application_from_jobname('jobname')

        cli.session.query.one.assert_called_once_with()
        mock_job.get_application_instance.assert_called_once_with()
        self.assertEqual(cli.session, cli.application.session)


class Test_cli_get_application_from_batchid(unittest.TestCase):

    def test(self):
        cli = db_safe_cli()
        cli.init_db(args_mock())

        mock_application = mock.Mock(tarr.application.Application)
        mock_job = mock.Mock(Job)
        mock_job.get_application_instance = mock.Mock(Job.get_application_instance, return_value=mock_application)

        mock_batch = mock.Mock(Batch)
        mock_batch.job = mock_job

        cli.session.query = session_query_one_mock(return_value=mock_batch)

        cli.get_application_from_batchid(batch_id=None)

        cli.session.query.one.assert_called_once_with()
        mock_job.get_application_instance.assert_called_once_with()
        self.assertEqual(mock_batch, cli.application.batch)
        self.assertEqual(cli.session, cli.application.session)
