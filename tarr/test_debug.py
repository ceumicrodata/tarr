import unittest
import os.path
import tempdir
import tarr.debug as m
from tarr.compiler import Program, RETURN
from tarr.data import Data


class Test_WRITE_TO_FILE(unittest.TestCase):

    def program(self, tempfile):
        return Program([m.WRITE_TO_FILE(tempfile), RETURN])

    def test_writes_data_as_id_and_payload(self):
        with tempdir.TempDir() as d:
            tempfile = os.path.join(d.name, 'tempfile')
            p = self.program(tempfile)
            p.run(Data('id', 'payload'))
            p.run(Data(1, 'Data'))

            with open(tempfile) as f:
                self.assertEqual(
                    ['id: payload\n',
                    '1: Data\n'],
                    f.readlines())

    def test_returns_data_as_is(self):
        with tempdir.TempDir() as d:
            tempfile = os.path.join(d.name, 'tempfile')
            p = self.program(tempfile)

            d = Data('id', 'payload')
            self.assertEqual(d, p.run(d))
