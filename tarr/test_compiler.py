import unittest
import tarr.compiler as m


Noop = m.Instruction()


class Test_Statistics(unittest.TestCase):

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
