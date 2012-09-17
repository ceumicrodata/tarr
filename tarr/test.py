import unittest
from tarr.data import Data
import tarr
from tarr.compiler import IF, ELSE, ENDIF, RETURN, Program


@tarr.branch
def is_animal(data):
    return data in ('fish', 'cat', 'dog')


@tarr.rule
def animal(data):
    return 'ANIMAL'

@tarr.rule
def other(data):
    return 'something else'


PROGRAM = [
    IF (is_animal),
        animal,
    ELSE,
        other,
    ENDIF,
    RETURN
]


class TestDecorators(unittest.TestCase):

    def test_decorators(self):
        program = Program(PROGRAM)

        self.assertEqual('ANIMAL', program.run(Data(1, 'fish')).payload)
        self.assertEqual('ANIMAL', program.run(Data(1, 'cat')).payload)
        self.assertEqual('ANIMAL', program.run(Data(1, 'dog')).payload)
        self.assertEqual('something else', program.run(Data(1, 'flower')).payload)
        self.assertEqual('something else', program.run(Data(1, 'rock')).payload)
