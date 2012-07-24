from tarr.db import Job
from tarr.runner import Runner

import pkg_resources
import hashlib
from datetime import datetime


class Application:
    ''' Facade of operations of batch data processing using DAG of processors.

    This class is intended to be subclassed for defining the concrete operations.

    Batch: amount of data that can be kept in memory at once for processing.
    Job: collection of Batches, also defines the data source

    '''

    session = None

    dag_runner = None
    job = None
    batch = None

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

        return pkg_resources.resource_filename(self.__class__.__module__, self.job.dag_config)

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

        self.store_batch_statistics()

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
        '''Processes a single data item'''
        return self.dag_runner.process(data_item)

    def save_data_items(self, data_items):
        '''Extract output from data items and store them.

        data_items are like those of returned by load_data_items()
        '''

        pass

    def store_batch_statistics(self):
        pass

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

    def load_dag(self):
        '''Loads the job's DAG - the data processing logic'''

        self.dag_runner = Runner(self.dag_config_content())
