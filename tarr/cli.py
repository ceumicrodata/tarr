import argparse
from tarr import model
import sqlalchemy
from zope.dottedname.resolve import resolve as dottedname_resolve
import itertools
import multiprocessing # http://pypi.python.org/pypi/billiard is a fork with bugfixes
from ConfigParser import ConfigParser


def add_connection_options_to(parser):
    parser.add_argument('--ini', '--config',
        dest='config', default='tarr.ini',
        help='Config file defining the database connection, ... (%(default)s)')
    parser.add_argument('--connection',
        dest='tarr_connection', default='connection-tarr',
        help='Section name in config file defining the database connection (%(default)s)')


class Command(object):

    application = None
    session = None

    def add_arguments(self, parser, defaults):
        pass

    def get_application(self, application):
        app_class = dottedname_resolve(application)
        self.application = app_class()
        self.application.session = self.session

    def get_application_from_jobname(self, job_name):
        job = self.session.query(model.Job).filter(model.Job.job_name==job_name).one()
        self.application = job.get_application_instance()
        self.application.session = self.session

    def get_application_from_batchid(self, batch_id):
        batch = self.session.query(model.Batch).filter(model.Batch.batch_id==batch_id).one()
        self.application = batch.job.get_application_instance()
        self.application.batch = batch
        self.application.session = self.session

    def init_db(self, args):
        config = ConfigParser()
        config.read(args.config)
        connection_config = dict(config.items(args.tarr_connection))
        model.init(sqlalchemy.engine_from_config(connection_config))
        self.session = model.Session()

    def shutdown(self):
        model.shutdown()

    def run(self, args):
        pass


class JobCommandBase(Command):

    def add_job_name_argument(self, parser):
        parser.add_argument('name', help='job name')

    def add_arguments(self, parser, defaults):
        self.add_job_name_argument(parser)


class InitCommand(Command):

    def run(self, args):
        model.init_meta_with_schema(model.meta)


class CreateJobCommand(JobCommandBase):

    def add_arguments(self, parser, defaults):
        self.add_job_name_argument(parser)
        parser.add_argument('--application', help='Application class reference - knows how to load and save data')
        parser.add_argument('--program', help='python module having a TARR_PROGRAM')
        parser.add_argument('--source', help='data to work on - application specific!')
        parser.add_argument('--partitioning_name', default=None, help='partitioning used by batch creation (%(default)s)')
        parser.add_argument('--description', default=None, help='words differentiating this job from others on the same data')

    def run(self, args):
        self.get_application(args.application)
        self.application.setup()

        self.application.create_job(
            name=args.name,
            program_config=args.program,
            source=args.source,
            partitioning_name=args.partitioning_name,
            description=args.description)


class DeleteJobCommand(JobCommandBase):

    def run(self, args):
        self.get_application_from_jobname(args.name)

        self.application.delete_job()


class ProcessJobCommand(JobCommandBase):

    def run(self, args):
        self.get_application_from_jobname(args.name)

        self.application.load_program()
        self.application.process_job()


class StatisticsCommand(JobCommandBase):

    def add_arguments(self, parser, defaults):
        self.add_job_name_argument(parser)
        parser.add_argument('--dot', dest='output_format', action='store_const', const='dot', help='''output in GraphViz's DOT language''')
        parser.add_argument('--text', dest='output_format', default='text', action='store_const', const='text', help='''output in text (default)''')

    def run(self, args):
        self.get_application_from_jobname(args.name)

        self.application.load_program()
        for batch in self.application.job.batches:
            self.application.batch = batch
            self.application.merge_batch_statistics()

        if args.output_format == 'dot':
            stat = self.application.program.to_dot(with_statistics=True)
        else:
            stat = self.application.program.to_text(with_statistics=True)
        print stat


class JobsCommand(Command):

    def run(self, args):
        for job in self.session.query(model.Job):
            print job.job_name


class ProcessBatchCommand(Command):

    def add_arguments(self, parser, defaults):
        parser.add_argument('batch_id', help='batch identifier')

    def process_batch(self, batch_id):
        self.get_application_from_batchid(batch_id)

        self.application.load_program()
        self.application.process_batch()

    def run(self, args):
        # process_batch should do everything,
        # except working with the command line arguments
        # which contains only the connection information
        # in case of parallel runs!
        self.process_batch(args.batch_id)


class ParallelProcessJobCommand(JobCommandBase):

    def run(self, args):
        # FIXME: ParallelProcessJobCommand is untested
        self.get_application_from_jobname(args.name)
        batch_ids = [batch.batch_id
            for batch in self.application.job.batches
            if not batch.is_processed]

        model.shutdown()

        pool = multiprocessing.Pool(maxtasksperchild=1)
        pool.map(
            process_batch_parallel,
            zip(batch_ids, itertools.repeat(args)),
            chunksize=1)
        pool.close()
        pool.join()


def _process_batch_parallel(parallel_arg):
    batch_id, connection_args = parallel_arg

    # XXX almost duplicate of main() internals
    command = ProcessBatchCommand()
    command.init_db(connection_args)
    try:
        command.process_batch(batch_id)
    finally:
        command.shutdown()

def process_batch_parallel(parallel_arg):
    try:
        _process_batch_parallel(parallel_arg)
    except:
        import traceback
        traceback.print_exc()
        raise


class Cli(object):

    description = 'TARR Command line tool'
    prog = 'python -m tarr'

    def __init__(self):
        self.parser = argparse.ArgumentParser(prog=self.prog, description=self.description)

        add_connection_options_to(self.parser)

        subparsers = self.parser.add_subparsers()

        for (subcmd, command_class, description) in self.get_commands():
            subparser = subparsers.add_parser(subcmd, description=description)
            subparser.set_defaults(command=subcmd)
            command_class().add_arguments(subparser, self.get_defaults())

    def get_defaults(self):
        return dict()

    def get_commands(self):
        return [
            ('init', InitCommand, 'Create initial TARR DB Schema (only if not already done)'),

            ('jobs', JobsCommand, 'List existing jobs'),
            ('create_job', CreateJobCommand, 'Create a new job'),
            ('delete_job', DeleteJobCommand, 'Delete an existing job'),
            ('process_job', ParallelProcessJobCommand, 'Start or continue processing an existing job'),
            ('sequential_process_job', ProcessJobCommand, 'Start or continue processing an existing job - batches are processed one after another'),
            ('parallel_process_job', ParallelProcessJobCommand, 'Start or continue processing an existing job - batches are processed in parallel'),

            ('process_batch', ProcessBatchCommand, 'Process a single batch'),
            ('statistics', StatisticsCommand, 'Print job statistics per processor'),
            ]

    def get_command(self, command_name):
        for (subcmd, command_class, description) in self.get_commands():
            if subcmd == command_name:
                return command_class()

    def parse_args(self, args=None):
        return self.parser.parse_args(args)

    def main(self, args=None):
        parsed_args = self.parse_args(args)

        command = self.get_command(parsed_args.command)

        command.init_db(parsed_args)
        try:
            command.run(parsed_args)
        finally:
            command.shutdown()


def main():
    Cli().main()


if __name__ == '__main__':
    main()
