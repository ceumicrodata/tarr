import unittest
from tarr.runner import Runner
from tarr.data import Data
import tarr


@tarr.branch
def is_animal(data):
    return data in ('fish', 'cat', 'dog')


@tarr.rule
def animal(data):
    return 'ANIMAL'

@tarr.rule
def other(data):
    return 'something else'


TEST_CONFIG = '''
is_animal: tarr.test.is_animal
    F -> other

animal: tarr.test.animal
    SF -> STOP

other: tarr.test.other
'''


class TestDecorators(unittest.TestCase):

    def test_decorators(self):
        runner = Runner(TEST_CONFIG)

        self.assertEqual('ANIMAL', runner.process(Data(1, 'fish')).payload)
        self.assertEqual('ANIMAL', runner.process(Data(1, 'cat')).payload)
        self.assertEqual('ANIMAL', runner.process(Data(1, 'dog')).payload)
        self.assertEqual('something else', runner.process(Data(1, 'flower')).payload)
        self.assertEqual('something else', runner.process(Data(1, 'rock')).payload)