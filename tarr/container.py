import tarr.dag
from tarr.processor import ProcessorFailed, BranchProcessor
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
    run_time = timedelta()

    def initialize(self):
        processor_instantiator = dottedname_resolve(self.impl)
        self.processor = processor_instantiator.instantiate(container=self)

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
        self.run_time += after - before

    def dot_label_for_success(self):
        return "S: %d" % self.success_count

    def dot_label_for_failure(self):
        return "F: %d" % self.failure_count

    def dot_shape(self):
        return "rectangle" if isinstance(self.processor, BranchProcessor) else "Mrecord"

    def dot_color(self):
        return "deeppink" if isinstance(self.processor, BranchProcessor) else "white"

