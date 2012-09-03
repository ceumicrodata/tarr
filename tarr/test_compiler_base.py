import unittest
import tarr.compiler_base as m
from tarr.compiler_base import (
    Instruction, BranchingInstruction,
    RETURN, RETURN_TRUE, RETURN_FALSE,
    DEF, IF, ELSE, ENDIF,
    DuplicateLabelError, UndefinedLabelError, BackwardReferenceError, FallOverOnDefineError, UnclosedProgramError, MissingEndIfError, MultipleElseError,
    Compiler, Runner)


class Add1(Instruction):

    def run(self, state):
        result = state + 1
        print result
        return result

Add1 = Add1()


class Div2(Instruction):

    def run(self, state):
        return state / 2

Div2 = Div2()


class IsOdd(BranchingInstruction):

    def run(self, state):
        self.condition.value = (state % 2 == 1)
        print self.condition.value
        return state

IsOdd = IsOdd()


class Die(Instruction):

    def run(self, state):
        raise Exception('')

Die = Die()


class Noop(Instruction):
    pass

Noop = Noop()


class Test_Path(unittest.TestCase):

    def test_NewPathAppender(self):
        # append
        i1 = m.Instruction()
        i2 = m.Instruction()

        path = m.Path()
        path.append(i1)
        path.append(i2)

        self.assertEqual(i2, i1.next_instruction)

    def test_InstructionAppender(self):
        # append
        i0 = m.Instruction()
        i1 = m.Instruction()
        i2 = m.Instruction()

        path = m.Path(m.InstructionAppender(i0))
        path.append(i1)
        path.append(i2)

        self.assertEqual(i1, i0.next_instruction)

    def test_join(self):
        # p1 p1i1   p1i2
        # p2 p2i1 /
        p1i1 = m.Instruction()
        p1i2 = m.Instruction()
        p2i1 = m.Instruction()

        path1 = m.Path()
        path1.append(p1i1)
        path2 = m.Path()
        path2.append(p2i1)

        path1.join(path2)

        path1.append(p1i2)

        self.assertEqual(p1i2, p1i1.next_instruction)
        self.assertEqual(p1i2, p2i1.next_instruction)

    def test_join_a_joined_path(self):
        # p1 p1i1   p1i2
        # p2 p2i1 /
        # p3 p3i1 /
        # or
        # p2.join(p3)
        # p1.join(p2)
        p1i1 = m.Instruction()
        p1i2 = m.Instruction()
        p2i1 = m.Instruction()
        p3i1 = m.Instruction()

        path1 = m.Path()
        path1.append(p1i1)
        path2 = m.Path()
        path2.append(p2i1)
        path3 = m.Path()
        path3.append(p3i1)

        path2.join(path3)
        path1.join(path2)

        path1.append(p1i2)

        self.assertEqual(p1i2, p1i1.next_instruction)
        self.assertEqual(p1i2, p2i1.next_instruction)
        self.assertEqual(p1i2, p3i1.next_instruction)

    def test_TrueBranchAppender(self):
        bi = m.BranchingInstruction()
        i1 = m.Instruction()

        path = m.Path()
        path.set_appender(m.TrueBranchAppender(path, bi))
        path.append(i1)

        self.assertEqual(i1, bi.instruction_on_yes)

    def test_TrueBranchAppender_does_not_touch_no_path(self):
        sentinel = object()
        bi = m.BranchingInstruction()
        bi.instruction_on_no = sentinel
        i1 = m.Instruction()
        i2 = m.Instruction()

        path = m.Path()
        path.set_appender(m.TrueBranchAppender(path, bi))
        path.append(i1)
        path.append(i2)

        self.assertEqual(sentinel, bi.instruction_on_no)

    def test_TrueBranchAppender_resets_appender(self):
        bi = m.BranchingInstruction()
        i1 = m.Instruction()
        i2 = m.Instruction()

        path = m.Path()
        path.set_appender(m.TrueBranchAppender(path, bi))
        path.append(i1)
        path.append(i2)

        self.assertEqual(i1, bi.instruction_on_yes)
        self.assertEqual(i2, i1.next_instruction)

    def test_FalseBranchAppender(self):
        bi = m.BranchingInstruction()
        i1 = m.Instruction()

        path = m.Path()
        path.set_appender(m.FalseBranchAppender(path, bi))
        path.append(i1)

        self.assertEqual(i1, bi.instruction_on_no)

    def test_FalseBranchAppender_does_not_touch_yes_path(self):
        sentinel = object()
        bi = m.BranchingInstruction()
        bi.instruction_on_yes = sentinel
        i1 = m.Instruction()
        i2 = m.Instruction()

        path = m.Path()
        path.set_appender(m.FalseBranchAppender(path, bi))
        path.append(i1)
        path.append(i2)

        self.assertEqual(sentinel, bi.instruction_on_yes)

    def test_FalseBranchAppender_resets_appender(self):
        bi = m.BranchingInstruction()
        i1 = m.Instruction()
        i2 = m.Instruction()

        path = m.Path()
        path.set_appender(m.FalseBranchAppender(path, bi))
        path.append(i1)
        path.append(i2)

        self.assertEqual(i1, bi.instruction_on_no)
        self.assertEqual(i2, i1.next_instruction)


def compile(program_spec):
    program = Compiler().compile(program_spec)
    program.register_runner(Runner())
    return program


class Test_Compiler(unittest.TestCase):

    def test_instruction_sequence(self):
        prog = compile([Add1, Div2, RETURN])
        self.assertEqual(2, prog.run(3))

    def test_condition_output1(self):
        prog = compile([IsOdd, RETURN])
        self.assertEqual(3, prog.run(3))
        self.assertTrue(prog.condition.value)

    def test_condition_output2(self):
        prog = compile([IsOdd, RETURN])
        self.assertEqual(4, prog.run(4))
        self.assertFalse(prog.condition.value)

    def test_duplicate_definition_of_label_is_not_compilable(self):
        self.assertRaises(DuplicateLabelError, compile, [RETURN, DEF('label'), Noop, RETURN, DEF('label'), Noop])

    def test_incomplete_program_is_not_compilable(self):
        self.assertRaises(UndefinedLabelError, compile, ['label', RETURN])

    def test_incomplete_before_label_is_not_compilable(self):
        self.assertRaises(FallOverOnDefineError, compile, [Noop, DEF('label'), Noop, RETURN])

    def test_label_definition_within_label_def_is_not_compilable(self):
        self.assertRaises(FallOverOnDefineError, compile, [RETURN, DEF('label'), DEF('label2'), RETURN])

    def test_program_without_closing_return_is_not_compilable(self):
        self.assertRaises(UnclosedProgramError, compile, [Noop])

    def test_backward_reference_is_not_compilable(self):
        self.assertRaises(BackwardReferenceError, compile, [RETURN, DEF('label'), Noop, 'label'])

    def test_branch_on_yes(self):
        prog = compile([
            IF (IsOdd),
                Add1,
            ELSE,
                'add2',
            ENDIF,
            RETURN,

            DEF('add2'),
                Add1,
                Add1,
            RETURN
            ])
        self.assertEqual(4, prog.run(3))
        self.assertEqual(6, prog.run(4))

    def test_branch_on_no(self):
        prog = compile([
            IF (IsOdd),
                Add1,
                Add1,
            ELSE,
                'add1',
            ENDIF,
            RETURN,

            DEF('add1'),
                Add1,
            RETURN
            ])
        self.assertEqual(5, prog.run(4))
        self.assertEqual(5, prog.run(3))

    def test_string_as_call_symbol(self):
        prog = compile([
            '+1', '+2', RETURN,

            DEF('+2'), '+1', '+1', RETURN,
            DEF('+1'), Add1, RETURN
            ])
        self.assertEqual(3, prog.run(0))

    def test_compilation_with_missing_ENDIF_is_not_possible(self):
        self.assertRaises(MissingEndIfError, compile, [IF (IsOdd), RETURN])

    def test_compilation_with_multiple_ELSE_is_not_possible(self):
        self.assertRaises(MultipleElseError, compile, [IF (IsOdd), ELSE, ELSE, ENDIF, RETURN])

    def test_IF(self):
        prog = compile([
            IF (IsOdd),
                Add1,
            ENDIF,
            Add1,
            RETURN])

        self.assertEqual(1, prog.run(0))
        self.assertEqual(3, prog.run(1))
        self.assertEqual(3, prog.run(2))

    def test_ELSE(self):
        prog = compile([
            IF (IsOdd),
            ELSE,
                Add1,
            ENDIF,
            Add1,
            RETURN])

        self.assertEqual(2, prog.run(0))
        self.assertEqual(2, prog.run(1))
        self.assertEqual(4, prog.run(2))

    def test_IF_ELSE(self):
        prog = compile([
            IF (IsOdd),
                Add1,
            ELSE,
                Add1,
                Add1,
            ENDIF,
            Add1,
            RETURN])

        self.assertEqual(3, prog.run(0))
        self.assertEqual(3, prog.run(1))
        self.assertEqual(5, prog.run(2))

    def test_embedded_IFs(self):
        prog = compile([
            IF (IsOdd),
                Add1,
                Div2,
                IF (IsOdd),
                    Add1,
                ENDIF,
            ELSE,
                Add1,
                Add1,
            ENDIF,
            Div2,
            RETURN])

        self.assertEqual(1, prog.run(0))
        self.assertEqual(1, prog.run(1))
        self.assertEqual(2, prog.run(2))
        self.assertEqual(1, prog.run(3))
        self.assertEqual(3, prog.run(4))
        self.assertEqual(2, prog.run(5))

    def test_macro_return_yes(self):
        prog = compile(
            [
            IF ('odd?'),
                Add1,
            ENDIF,
            RETURN,

            DEF ('odd?'),
                IF (IsOdd),
                    RETURN_TRUE,
                ELSE,
                    RETURN_FALSE,
                ENDIF
            ])

        self.assertEqual(2, prog.run(1))
        self.assertEqual(4, prog.run(3))

    def test_macro_return_no(self):
        prog = compile(
            [
            IF ('odd?'),
                Add1,
            ENDIF,
            RETURN,

            DEF ('odd?'),
                IF (IsOdd),
                    RETURN_TRUE,
                ELSE,
                    RETURN_FALSE,
                ENDIF
            ])

        self.assertEqual(2, prog.run(2))
        self.assertEqual(4, prog.run(4))

    # used in the next 2 tests
    complex_prog_spec = [
        IF ('even?'),
            RETURN,
        ELSE,
            Add1,
        ENDIF,
        RETURN,

        DEF ('even?'),
            IF ('odd?'),
                RETURN_FALSE,
            ELSE,
                RETURN_TRUE,
            ENDIF,

        DEF ('odd?'),
            IsOdd,
        RETURN,
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
