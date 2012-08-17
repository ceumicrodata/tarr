import argparse
import tarr.model as db # FIXME: db -> model
from db.connection import add_connection_options_to # FIXME: db.connection is external to TARR!
from zope.dottedname.resolve import resolve as dottedname_resolve
import itertools
from lib.parallel import map_parallel



def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description='TARR Command line tool')

    add_connection_options_to(parser)

    subparsers = parser.add_subparsers()
    def subparser(name, description=None):
        p = subparsers.add_parser(name, description=description)
        p.set_defaults(command=name)
        return p

    p = subparser('create_job', description='Create a new job')
    p.add_argument('name', help='job name')
    p.add_argument('--application', help='Application class reference - knows how to load and save data')
    p.add_argument('--dag_config', help='config file describing the processing nodes')
    p.add_argument('--source', help='data to work on - application specific!')
    p.add_argument('--partitioning_name', default=None, help='partitioning used by batch creation (%(default)s)')
    p.add_argument('--description', default=None, help='words differentiating this job from others on the same data')

    p = subparser('delete_job', description='Delete an existing job')
    p.add_argument('name', help='job name')

    p = subparser('process_job', description='Start or continue processing an existing job')
    p.add_argument('name', help='job name')

    p = subparser('parallel_process_job', description='Start or continue processing an existing job in parallel')
    p.add_argument('name', help='job name')

    p = subparser('process_batch', description='Process a single batch')
    p.add_argument('batch_id', help='batch identifier')

    return parser.parse_args(args)


class Command(object):

    application = None
    session = None

    def get_application(self, application):
        app_class = dottedname_resolve(application)
        self.application = app_class()
        self.application.session = self.session

    def get_application_from_jobname(self, job_name):
        job = self.session.query(db.Job).filter(db.Job.job_name==job_name).one()
        self.application = job.get_application_instance()
        self.application.session = self.session

    def get_application_from_batchid(self, batch_id):
        batch = self.session.query(db.Batch).filter(db.Batch.batch_id==batch_id).one()
        self.application = batch.job.get_application_instance()
        self.application.batch = batch
        self.application.session = self.session

    def init_db(self, args):
        db.init_from(args)
        self.session = db.Session()

    def shutdown(self):
        db.shutdown()

    def run(self, args):
        pass


class CreateJobCommand(Command):

    def run(self, args):
        self.get_application(args.application)
        self.application.setup()

        self.application.create_job(
            name=args.name,
            dag_config=args.dag_config,
            source=args.source,
            partitioning_name=args.partitioning_name,
            description=args.description)


class DeleteJobCommand(Command):

    def run(self, args):
        self.get_application_from_jobname(args.name)

        self.application.delete_job()


class ProcessJobCommand(Command):

    def run(self, args):
        self.get_application_from_jobname(args.name)

        self.application.load_dag()
        self.application.process_job()


class ProcessBatchCommand(Command):

    def process_batch(self, batch_id):
        self.get_application_from_batchid(batch_id)

        self.application.load_dag()
        self.application.process_batch()

    def run(self, args):
        # process_batch should do everything,
        # except working with the command line arguments
        # which contains only the connection information
        # in case of parallel runs!
        self.process_batch(args.batch_id)


class ParallelProcessJobCommand(Command):

    def run(self, args):
        # FIXME: ParallelProcessJobCommand is untested
        self.get_application_from_jobname(args.name)
        batch_ids = [batch.batch_id
            for batch in self.application.job.batches
            if not batch.is_processed]
        map_parallel(process_batch_parallel,
            zip(batch_ids, itertools.repeat(args)))


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


COMMANDS = dict(
        create_job=CreateJobCommand,
        delete_job=DeleteJobCommand,
        process_job=ProcessJobCommand,
        parallel_process_job=ParallelProcessJobCommand,
        process_batch=ProcessBatchCommand)


def main(commands=None, args=None):
    parsed_args = parse_args(args)
    commands = commands or COMMANDS
    command_class = commands[parsed_args.command]

    command = command_class()
    command.init_db(parsed_args)
    try:
        command.run(parsed_args)
    finally:
        command.shutdown()


if __name__ == '__main__':
    main()
