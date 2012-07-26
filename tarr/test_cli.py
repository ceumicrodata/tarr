import unittest
import mock
import tarr.cli as m # odule
import tarr.application
from db.db_test import TestConnection, SqlTestCase


class Test_Cli_parse_args(unittest.TestCase):

    def test_create_job(self):
        cli = m.Cli()
        args = cli.parse_args(
            (
                'create_job jobname --app=location.clean.Application --dag=dag-config --source=complex:rovat_13:pm'
                ' --partitioning_name=every_200'
            ).split()
            + ['--description=a description'])

        self.assertEqual(cli.command_create_job, args.command)
        self.assertEqual('jobname', args.name)
        self.assertEqual('location.clean.Application', args.application)
        self.assertEqual('dag-config', args.dag_config)
        self.assertEqual('complex:rovat_13:pm', args.source)
        self.assertEqual('every_200', args.partitioning_name)
        self.assertEqual('a description', args.description)

    def test_delete_job(self):
        cli = m.Cli()
        args = cli.parse_args('delete_job jobname'.split())

        self.assertEqual(cli.command_delete_job, args.command)
        self.assertEqual('jobname', args.name)

    def test_process_job(self):
        cli = m.Cli()
        args = cli.parse_args('process_job jobname'.split())

        self.assertEqual(cli.command_process_job, args.command)
        self.assertEqual('jobname', args.name)

    def test_process_batch(self):
        cli = m.Cli()
        args = cli.parse_args('process_batch batch_id'.split())

        self.assertEqual(cli.command_process_batch, args.command)
        self.assertEqual('batch_id', args.batch_id)


class Test_Cli_main(unittest.TestCase):

    def test(self):
        cli = m.Cli()
        command_mock = mock.Mock()
        mock_args = mock.Mock()
        mock_args.command = command_mock

        cli.parse_args = mock.Mock(cli.parse_args, return_value=mock_args)
        cli.main('a s d'.split())

        command_mock.assert_called_once_with(mock_args)


class Test_Cli_integration(SqlTestCase):

    def test_create_job(self):
        args_list = TestConnection().as_args_list() + 'create_job jobname --app=tarr.application.Application --dag=fixtures/test_dag_config --source=complex:rovat_13:pm'.split()

        cli = m.Cli()
        cli.main(args_list)

        self.assert_rows('SELECT job_name FROM tarr.job', [['job_name'], ['jobname']])


def db_safe_cli():
    cli = m.Cli()
    cli.init_db = mock.Mock(cli.init_db)
    cli.shutdown = mock.Mock(cli.shutdown)
    return cli


def use_application(cli, app):
    def set_application(cli, application):
        cli.application = application
    cli.get_application = mock.Mock(cli.get_application, side_effect=lambda application: set_application(cli, app))


def args_mock():
    args_mock = mock.Mock()
    args_mock.name = mock.sentinel.jobname
    args_mock.application = mock.sentinel.application
    args_mock.dag_config = mock.sentinel.dag_config
    args_mock.source = mock.sentinel.source
    args_mock.partitioning_name = mock.sentinel.partitioning_name
    args_mock.description = mock.sentinel.description
    return args_mock


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
        use_application(cli, application_mock)
        return cli

    def call_cli_command(self, cli, args):
        cli.command_create_job(args)

    def test(self):
        cli = self.get_cli()

        self.call_cli_command(cli, args_mock())

        cli.application.create_job.assert_called_once_with(
            name=mock.sentinel.jobname,
            dag_config=mock.sentinel.dag_config,
            source=mock.sentinel.source,
            partitioning_name=mock.sentinel.partitioning_name,
            description=mock.sentinel.description)
