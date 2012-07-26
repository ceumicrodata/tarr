import argparse
import tarr.db_model as db
from db.connection import add_connection_options_to # FIXME: db.connection is external to TARR!
from zope.dottedname.resolve import resolve as dottedname_resolve


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description='TARR Command line tool')

    add_connection_options_to(parser)

    subparsers = parser.add_subparsers()
    def subparser(name, command, description=None):
        p = subparsers.add_parser(name, description=description)
        p.set_defaults(command=command)
        return p

    p = subparser('create_job', command_create_job, description='Create a new job')
    p.add_argument('name', help='job name')
    p.add_argument('--application', help='Application class reference - knows how to load and save data')
    p.add_argument('--dag_config', help='config file describing the processing nodes')
    p.add_argument('--source', help='data to work on - application specific!')
    p.add_argument('--partitioning_name', default=None, help='partitioning used by batch creation (%(default)s)')
    p.add_argument('--description', default='', help='words differentiating this job from others on the same data')

    p = subparser('delete_job', command_delete_job, description='Delete an existing job')
    p.add_argument('name', help='job name')

    p = subparser('process_job', command_process_job, description='Start or continue processing an existing job')
    p.add_argument('name', help='job name')

    p = subparser('process_batch', command_process_batch, description='Process a single batch')
    p.add_argument('batch_id', help='batch identifier')

    return parser.parse_args(args)


def command_create_job(args):
    app_class = dottedname_resolve(args.application)
    app = app_class()

    db.init_from(args)
    app.session = db.Session()

    app.create_job(
        name=args.name,
        dag_config=args.dag_config,
        source=args.source,
        partitioning_name=args.partitioning_name,
        description=args.description)

    db.shutdown()


def command_delete_job(args):
    pass


def command_process_job(args):
    pass


def command_process_batch(args):
    pass


def main():
    parse_args()


if __name__ == '__main__':
    main()
