'''
This is a demo of tarr implementation - also used for automated testing.

What it does is totally artifical, however it does show all major parts of
writing a and provides a valid implementation to test.

What does the demo do?

- create batches (fixed number: NUM_BATCHES)
- generates varying amount of identifiable data for a batch (instead of loading them from db/file)
- store data so that it can be examined after processing (into files)

Also provided is a demo TARR_PROGRAM to use with the demo tarr implementation,
that does a trivial modification on the input:
- turns False into True

In a real-world implementation this TARR_PROGRAM would live in a separate module.

-----


HOW to use:

When creating a job give the destination directory in the source parameter.
E.g.:

# Make a directory containing the output
$ mkdir /tmp/x

# create tarr job, specifying the output directory by --source
$ python -m tarr.cli create_job --application tarr.demo_app.DemoApp --program tarr.demo_app --source /tmp/x demo_app_1

# process all batches in the job
$ python -m tarr.cli process_job demo_app_1

# /tmp/x/ should contain the output files
$ ls -1 /tmp/x
file_00
file_01
..
file_63

(using Job.source for destination here is counter-intuitive,
but can be argued that destination is supposed to be
1. a well known place (e.g. database table, directory known from e.g an ini file)
2. relative to source)

-----


Contents/specific meaning of tarr attributes:

Data:
 id: (filename, index)
 payload: True or False, with the meaning "is this data already processed?"

Job:
 source: temporary directory name

Batch:
 source: name of unique temporary file within job's temporary directory

'''
import os
import tarr.application
import tarr.compiler
from tarr.data import Data


@tarr.rule
def set_processed(payload):
    return True

@tarr.branch
def is_processed(processed):
    return processed


TARR_PROGRAM = [
    # NOTE: wrap `set_processed` in `is_processed`s to enable testing of statistics
    is_processed,   # statistics should have failure_count, not success_count
    set_processed,  # statistics should have item_count
    is_processed,   # statistics should have success_count, not failure_count
    tarr.compiler.RETURN
]


NUM_BATCHES = 9


class DemoApp(tarr.application.Application):

    def create_batches(self):
        '''Create NUM_BATCHES batches for the current job
        '''

        for i in range(NUM_BATCHES):
            self.job.create_batch(source='file_{0:02d}'.format(i))

    def load_data_items(self):
        '''(Job, Batch) -> list of data items

        Where a data item is a Data with
         id: (filename, index)
         payload: True or False, with the meaning "is this data already processed?"
        '''

        filename = os.path.join(self.job.source, self.batch.source)
        processed = False
        count = self.batch.batch_id % 5

        return [Data((filename, i), processed) for i in range(count)]

    def save_data_items(self, data_items):
        '''Store each data items into a file they reference in their `id`
        '''

        for data in data_items:
            filename, index = data.id
            with open(filename, 'a') as f:
                f.write('[{}:{}]'.format(index, data.payload))
