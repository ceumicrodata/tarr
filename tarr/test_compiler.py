import unittest
import tarr.compiler as m
from tarr.data import Data


Noop = m.Instruction()


class Test_Program_statistics(unittest.TestCase):

    def prog(self, condition=None):
        RETURN_MAP = {
            True: m.RETURN_TRUE,
            False: m.RETURN_FALSE,
            None: m.RETURN
        }
        prog = m.compile([Noop, RETURN_MAP[condition], Noop, Noop, m.RETURN])
        prog.runner.ensure_statistics(1)
        return prog

    def test_statistics_created(self):
        prog = m.compile([Noop, m.RETURN, Noop, Noop, m.RETURN])
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


class Test_decorators(unittest.TestCase):

    def assertEqualData(self, expected, actual):
        self.assertEqual(expected.id, actual.id)
        self.assertEqual(expected.payload, actual.payload)

    def test_rule(self):
        prog = m.compile([add1, m.RETURN])

        self.assertEqualData(Data(id, 1), prog.run(Data(id, 0)))
        self.assertEqualData(Data(id, 2), prog.run(Data(id, 1)))

    def test_branch(self):
        # program: convert an odd number to string 'odd', an even number to 'even'
        prog = m.compile([
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
        prog = m.compile([
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


# FIXME: derive from compiler_base.Program and test only new functionality + reuse original tests (Liskov), thus remove this duplicate
class Test_Program_sub_programs(unittest.TestCase):

    def test_sub_programs(self):
        prog = m.compile([
            add1,
            m.RETURN,
            m.DEF ('x1'),
            m.RETURN,
            m.DEF ('x2'),
            m.RETURN,
            m.DEF ('x3'),
            add1,
            add1,
            m.RETURN,
            ])

        sub_programs = iter(prog.sub_programs())
        sub_program = sub_programs.next()
        self.assertEqual(None, sub_program[0])
        self.assertEqual(2, len(sub_program[1])) # add1, RETURN

        sub_program = sub_programs.next()
        self.assertEqual('x1', sub_program[0])
        self.assertEqual(1, len(sub_program[1])) # RETURN

        sub_program = sub_programs.next()
        self.assertEqual('x2', sub_program[0])
        self.assertEqual(1, len(sub_program[1])) # RETURN

        sub_program = sub_programs.next()
        self.assertEqual('x3', sub_program[0])
        self.assertEqual(3, len(sub_program[1])) # add1, add1, RETURN

        with self.assertRaises(StopIteration):
            sub_programs.next()
