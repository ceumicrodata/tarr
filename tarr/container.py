import tarr.dag
from tarr.processor import ProcessorFailed, ProcessorNeedsHuman
from zope.dottedname.resolve import resolve as dottedname_resolve
import traceback


class ProcessorContainer(tarr.dag.Node):

    SUCCESS, FAILURE, NEED_HUMAN = range(3)

    status = None
    data = None
    processor = None

    def initialize(self):
        processor_class = dottedname_resolve(self.impl)
        self.processor = processor_class(container=self)

    def process(self, data):
        self.status = self.SUCCESS
        self.data = data
        try:
            self.data.payload = self.processor.process(data.payload)
        except ProcessorFailed:
            self.status = self.FAILURE
        except ProcessorNeedsHuman:
            self.status = self.NEED_HUMAN
        except Exception as e:
            self.status = self.FAILURE
            print traceback.format_exc(e)
