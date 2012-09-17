from tarr.model import Job
from tarr.runner import Runner

import hashlib
from datetime import datetime

from lib.file import ResourceLocator # FIXME: ResourceLocator is external to tarr & it is only a temporary solution

import logging


log = logging.getLogger(__name__)


class Application(ResourceLocator):
    ''' Facade of operations of batch data processing using DAG of processors.

    This class is intended to be subclassed for defining the concrete operations.

    Batch: amount of data that can be kept in memory at once for processing.
    Job: collection of Batches, also defines the data source

    '''

    session = None

    dag_runner = None
    job = None
    batch = None

    def setup(self):
        '''Create application specific schema here'''
        pass

    def create_job(self, name, dag_config, source, partitioning_name, description):
        self.job = Job()

        self.job.job_name = name

        cls = self.__class__
        self.job.application = '{0}.{1}'.format(cls.__module__, cls.__name__)

        self.job.dag_config = dag_config
        self.job.dag_config_hash = self.dag_config_hash()
        self.job.source = source
        self.job.partitioning_name = partitioning_name
        self.job.description = description

        self.session.add(self.job)

        self.create_batches()

        self.session.commit()

    def dag_config_file(self):
        ''' .job.dag_config -> file name '''

        return self.relative_path(self.job.dag_config)

    def dag_config_content(self):
        with open(self.dag_config_file()) as f:
            return f.read()

    def dag_config_hash(self):
        hash = hashlib.sha1()
        hash.update(self.dag_config_content())
        return hash.hexdigest()

    def create_batches(self):
        '''Create batch objects for the current job

        As batches are data source specific there is no default implementation
        '''

        pass

    def process_job(self):
        for self.batch in self.job.batches:
            if not self.batch.is_processed:
                self.process_batch()

    def process_batch(self):
        data_items = self.load_data_items()
        processed_data_items = [self.process_data_item(item) for item in data_items]
        self.save_data_items(processed_data_items)

        self.batch.time_completed = datetime.now()
        self.batch.dag_config_hash = self.dag_config_hash()

        self.save_batch_statistics()

        self.session.commit()

    def load_data_items(self):
        '''(Job, Batch) -> list of data items

        The output should be [tarr.data.Data], i.e. the items must at least contain:
            an id:     constant identifier of the data
            a payload: the real data with or without identifiers, all of them can be potentially modified when processed
        they can contain any more contextual information if needed
        '''

        pass

    def process_data_item(self, data_item):
        try:
            return self.dag_runner.process(data_item)
        except:
            try:
                log.exception('process_data_item(%s)', repr(data_item))
            except:
                log.exception('process_data_item - can not log data_item!')
            return data_item

    def save_data_items(self, data_items):
        '''Extract output from data items and store them.

        data_items are like those of returned by load_data_items()
        '''

        pass

    def save_batch_statistics(self):
        self.batch.save_statistics(self.dag)

    def merge_batch_statistics(self):
        self.batch.merge_statistics_into(self.dag)

    def delete_job(self):
        for self.batch in self.job.batches:
            self.delete_batch()

        self.session.delete(self.job)
        self.session.commit()
        self.job = None

    def delete_batch(self):
        self.session.delete(self.batch)
        self.batch = None

    @property
    def dag(self):
        '''Data processing logic in the format of Directed Acyclic Graph of Processors'''

        return self.dag_runner.dag

    def load_program(self):
        '''Loads the job's DAG - the data processing logic'''

        self.dag_runner = Runner(self.dag_config_content())
