import tarr.dag
from tarr.processor import ProcessorFailed
from zope.dottedname.resolve import resolve as dottedname_resolve
import traceback
from datetime import datetime, timedelta


class ProcessorContainer(tarr.dag.Node):

    SUCCESS, FAILURE = range(2)

    status = None
    data = None
    processor = None

    success_count = 0
    failure_count = 0
    item_count = 0
    time_in_process = timedelta() # FIXME: rename to run_time

    def initialize(self):
        processor_class = dottedname_resolve(self.impl)
        self.processor = processor_class(container=self)

    def process(self, data):
        self.status = self.SUCCESS
        self.data = data
        self.item_count += 1
        before = datetime.now()

        try:
            self.data.payload = self.processor.process(data.payload)
            self.success_count += 1
        except ProcessorFailed:
            self.status = self.FAILURE
            self.failure_count += 1
        except Exception as e:
            self.status = self.FAILURE
            self.failure_count += 1
            print traceback.format_exc(e)

        after = datetime.now()
        self.time_in_process += after - before
