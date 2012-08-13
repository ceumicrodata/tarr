import unittest
from tarr import runner as m # odule
from tarr.processor import Processor
from tarr.data import Data
import tarr


class Processor_1plus(Processor):

    def process(self, data):
        return 1 + data


class Processor_2div(Processor):

    def process(self, data):
        return 2.0 / data


# same as above, but just defining the process method directly:
@tarr.rule
def processor_erase(data):
    return 'Oops'


TEST_CONFIG = '''
plus_one: tarr.test_runner.Processor_1plus
two_div: tarr.test_runner.Processor_2div
    S -> STOP
erase: tarr.test_runner.processor_erase
'''


class TestRunner(unittest.TestCase):

    def test_create(self):
        runner = m.Runner(TEST_CONFIG)
        plus_one = runner.dag.node_by_name('plus_one')
        two_div = runner.dag.node_by_name('two_div')
        erase = runner.dag.node_by_name('erase')

        self.assertTrue(isinstance(plus_one.processor, Processor_1plus))
        self.assertTrue(isinstance(two_div.processor, Processor_2div))
        # self.assertTrue(isinstance(erase.processor, processor_erase))

    def test_process(self):
        runner = m.Runner(TEST_CONFIG)

        self.assertEqual(2, runner.process(Data(1, 0)).payload)
        self.assertEqual(1, runner.process(Data(1, 1)).payload)
        self.assertEqual('Oops', runner.process(Data(1, -1)).payload)