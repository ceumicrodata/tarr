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

    def run(self, args):
        self.get_application_from_batchid(args.batch_id)

        self.application.load_dag()
        self.application.process_batch()


COMMANDS = dict(
        create_job=CreateJobCommand,
        delete_job=DeleteJobCommand,
        process_job=ProcessJobCommand,
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
