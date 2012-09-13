import unittest
import tarr.compiler as m
from tarr.data import Data
import tarr.test_compiler_base


Noop = m.Instruction()

@m.rule
def add1(n):
    return n + 1

@m.rule
def const_odd(any):
    return 'odd'

@m.rule
def const_even(any):
    return 'even'

@m.branch
def odd(n):
    return n % 2 == 1


class Test_Program(tarr.test_compiler_base.Test_Program):

    # verify, that the functionality of the parent is intact - Liskov's substitution principle

    # NOTE: this test class will pull in all tests from tarr.test_compiler_base.Test_Program but run with
    # the compiler.Program class, not with compiler_base.Program class

    PROGRAM_CLASS = m.Program


class Test_Program_statistics(unittest.TestCase):

    def prog(self, condition=None):
        RETURN_MAP = {
            True: m.RETURN_TRUE,
            False: m.RETURN_FALSE,
            None: m.RETURN
        }
        prog = m.Program([Noop, RETURN_MAP[condition], Noop, Noop, m.RETURN])
        prog.runner.ensure_statistics(1)
        return prog

    def test_statistics_created(self):
        prog = m.Program([Noop, m.RETURN, Noop, Noop, m.RETURN])
        prog.run(None)

        self.assertEqual(2, len(prog.statistics))

    COUNT_SENTINEL = -98

    def test_process_increments_count(self):
        prog = self.prog()
        stat = prog.statistics[1]
        stat.item_count = self.COUNT_SENTINEL

        prog.run(None)

        self.assertEqual(self.COUNT_SENTINEL + 1, stat.item_count)

    def test_success_increments_success_count(self):
        prog = self.prog(True)
        stat = prog.statistics[1]
        stat.success_count = self.COUNT_SENTINEL

        prog.run(None)

        self.assertEqual(self.COUNT_SENTINEL + 1, stat.success_count)

    def test_failure_increments_failure_count(self):
        prog = self.prog(False)
        stat = prog.statistics[1]
        stat.failure_count = self.COUNT_SENTINEL

        prog.run(None)

        self.assertEqual(self.COUNT_SENTINEL + 1, stat.failure_count)

    def test_time_is_increased(self):
        prog = self.prog()
        stat = prog.statistics[1]
        run_time = stat.run_time

        prog.run(None)
        run_time2 = stat.run_time

        self.assertLess(run_time, run_time2)

        prog.run(None)
        run_time3 = stat.run_time

        self.assertLess(run_time2, run_time3)


class Test_decorators(unittest.TestCase):

    def assertEqualData(self, expected, actual):
        self.assertEqual(expected.id, actual.id)
        self.assertEqual(expected.payload, actual.payload)

    def test_rule(self):
        prog = m.Program([add1, m.RETURN])

        self.assertEqualData(Data(id, 1), prog.run(Data(id, 0)))
        self.assertEqualData(Data(id, 2), prog.run(Data(id, 1)))

    def test_branch(self):
        # program: convert an odd number to string 'odd', an even number to 'even'
        prog = m.Program([
            m.IF (odd),
                const_odd,
            m.ELSE,
                const_even,
            m.ENDIF,
            m.RETURN])

        self.assertEqualData(Data(id, 'even'), prog.run(Data(id, 0)))
        self.assertEqualData(Data(id, 'odd'), prog.run(Data(id, 1)))

    def test_multiple_use_of_instructions(self):
        # program: convert an odd number to string 'odd', an even number to 'even'
        prog = m.Program([
            odd,
            m.IF (odd),
                odd,
                const_even,
                const_odd,
            m.ELSE,
                odd,
                const_odd,
                const_even,
            m.ENDIF,
            m.RETURN])

        self.assertEqualData(Data(id, 'even'), prog.run(Data(id, 0)))
        self.assertEqualData(Data(id, 'odd'), prog.run(Data(id, 1)))
