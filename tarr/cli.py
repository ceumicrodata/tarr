import argparse
from tarr.db import Job


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description='TARR Command line tool')

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
    p.add_argument('--description', default='', help='words differentiating this job from others on the same data')

    p = subparser('delete_job', description='Delete an existing job')
    p.add_argument('name', help='job name')

    p = subparser('process_job', description='Start or continue processing an existing job')
    p.add_argument('name', help='job name')

    p = subparser('process_batch', description='Process a single batch')
    p.add_argument('batch_id', help='batch identifier')

    return parser.parse_args(args)


def main():
    parse_args()


if __name__ == '__main__':
    main()
