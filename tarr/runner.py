from tarr import dag
from tarr.container import ProcessorContainer # Node


class ProcessorDAG(dag.DAG):

    def initialize(self):
        for container in self.nodes:
            container.initialize()


class DagConfigReader(dag.DagConfigReader):

    def new_node(self):
        return ProcessorContainer()

    def new_dag(self):
        return ProcessorDAG()


class Runner(object):

    dag = None

    def __init__(self, dag_config):
        super(Runner, self).__init__()
        self.dag = self.build_dag(dag_config)
        self.dag.initialize()

    def build_dag(self, dag_config):
        return DagConfigReader().from_string(dag_config)

    def process(self, data):
        processor = self.dag.start_node
        while True:
            processor.process(data)
            data = processor.data
            if processor.status == processor.SUCCESS:
                next_processor_name = processor.nn_success
            elif processor.status == processor.FAILURE:
                next_processor_name = processor.nn_fail
            else:
                next_processor_name = processor.nn_human

            if next_processor_name:
                processor = self.dag.node_by_name(next_processor_name)
            else:
                return data