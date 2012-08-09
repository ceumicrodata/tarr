import unittest
from .compiler_base import (
    Instruction, BranchingInstruction,
    RETURN, RETURN_TRUE, RETURN_FALSE,
    define, do,
    DuplicateLabelError, UndefinedLabelError, BackwardReferenceError, FallOverOnDefineError, UnclosedProgramError,
    Compiler)


class Add1(Instruction):

    def run(self, state):
        return state + 1

Add1 = Add1()


class Mul2(Instruction):

    def run(self, state):
        return state * 2

Mul2 = Mul2()

class IsOdd(BranchingInstruction):

    def run(self, state):
        self.condition.value = (state % 2 == 1)
        return state

IsOdd = IsOdd()

class Die(Instruction):

    def run(self, state):
        raise Exception('')

Die = Die()

class Noop(Instruction):
    pass

Noop = Noop()


def compile(program_spec):
    return Compiler().compile(program_spec)


class Test_Compiler(unittest.TestCase):

    def test_instruction_sequence(self):
        prog = compile([Add1, Mul2, RETURN])
        self.assertEqual(8, prog.run(3))

    def test_condition_output1(self):
        prog = compile([IsOdd, RETURN])
        self.assertEqual(3, prog.run(3))
        self.assertTrue(prog.condition.value)

    def test_condition_output2(self):
        prog = compile([IsOdd, RETURN])
        self.assertEqual(4, prog.run(4))
        self.assertFalse(prog.condition.value)

    def test_duplicate_definition_of_label_is_not_compilable(self):
        self.assertRaises(DuplicateLabelError, compile, [RETURN, define('label'), Noop, RETURN, define('label'), Noop])

    def test_incomplete_program_is_not_compilable(self):
        self.assertRaises(UndefinedLabelError, compile, [Noop, RETURN, define('label'), RETURN, define('label2')])

    def test_incomplete_before_label_is_not_compilable(self):
        self.assertRaises(FallOverOnDefineError, compile, [Noop, define('label'), Noop, RETURN])

    def test_program_without_closing_return_is_not_compilable(self):
        self.assertRaises(UnclosedProgramError, compile, [Noop])

    def test_backward_reference_is_not_compilable(self):
        self.assertRaises(BackwardReferenceError, compile, [RETURN, define('label'), Noop, do('label')])

    def test_branch_on_yes(self):
        prog = compile([IsOdd.on_no('add2'), Add1, RETURN, define('add2'), Add1, Add1, RETURN])
        self.assertEqual(4, prog.run(3))
        self.assertEqual(6, prog.run(4))

    def test_branch_on_no(self):
        prog = compile([IsOdd.on_no('add1'), Add1, Add1, RETURN, define('add1'), Add1, RETURN])
        self.assertEqual(5, prog.run(4))
        self.assertEqual(5, prog.run(3))

    def test_multiple_labels(self):
        prog = compile([IsOdd.on_no('no'), do('yes'), RETURN, define('yes', 'no'), Add1, RETURN])
        self.assertEqual(4, prog.run(3))
        self.assertEqual(5, prog.run(4))

    def test_do(self):
        prog = compile([
            do('+1'), do('+2'), RETURN,

            define('+2'), do('+1'), do('+1'), RETURN,
            define('+1'), Add1, RETURN
            ])
        self.assertEqual(3, prog.run(0))

    def test_macro_return_yes(self):
        prog = compile(
            [do('odd?').on_no('even'), Add1, RETURN,

            define('even'), RETURN,

            define('odd?'),
                IsOdd.on_no('odd?: no'),
                        RETURN_TRUE,
                    define('odd?: no'),
                        RETURN_FALSE])

        self.assertEqual(2, prog.run(1))
        self.assertEqual(4, prog.run(3))

    def test_macro_return_no(self):
        prog = compile(
            [do('odd?').on_no('even'), Add1, RETURN,

            define('even'), RETURN,

            define('odd?'),
                IsOdd.on_no('odd?: no'),
                        RETURN_TRUE,
                    define('odd?: no'),
                        RETURN_FALSE])

        self.assertEqual(2, prog.run(2))
        self.assertEqual(4, prog.run(4))

    # used in the next 2 tests
    complex_prog_spec = [
        do('even?').on_no('odd'), RETURN,

        define('odd'), Add1, RETURN,

        define('even?'),
            do('odd?').on_no('even? even'),
                    RETURN_FALSE,
                define('even? even'), RETURN_TRUE,

        define('odd?'),
            IsOdd, RETURN,
    ]

    def test_macro_return(self):
        prog = compile(self.complex_prog_spec)

        self.assertEqual(2, prog.run(1))
        self.assertEqual(2, prog.run(2))
        self.assertEqual(4, prog.run(3))
        self.assertEqual(4, prog.run(4))

    def test_instruction_index(self):
        prog = compile(self.complex_prog_spec)

        indices = [i.index for i in prog.instructions]
        self.assertEqual(range(len(prog.instructions)), indices)