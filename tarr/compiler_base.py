class DuplicateLabelError(Exception):
    pass

class UndefinedLabelError(Exception):
    pass

class BackwardReferenceError(Exception):
    pass

class FallOverOnDefineError(Exception):
    pass

class UnclosedProgramError(Exception):
    pass


class Compilable(object):

    def compile(self, compiler):
        pass


class Instruction(Compilable):

    next_instruction = None

    def run(self, state):
        return state

    def compile(self, compiler):
        compiler.add_instruction(self.clone())

    def clone(self):
        return self.__class__()


class ConditionalInstruction(Instruction):

    condition = None

    def register_condition(self, condition):
        self.condition = condition


class Return(ConditionalInstruction):

    return_value = None
    def __init__(self, return_value=None):
        self.return_value = return_value

    @property
    def next_instruction(self):
        if self.return_value is not None:
            self.condition.value = self.return_value
        return None

    @next_instruction.setter
    def next_instruction(self, instruction):
        pass

    def compile(self, compiler):
        super(Return, self).compile(compiler)
        compiler.would_fall_over = False

    def clone(self):
        return self.__class__(self.return_value)


RETURN = Return()
RETURN_TRUE = Return(return_value=True)
RETURN_FALSE = Return(return_value=False)


class Macro(Compilable):

    def __init__(self, *instructions):
        self.instructions = instructions

    def compile(self, compiler):
        for instruction in self.instructions:
            instruction.compile(compiler)


class BranchingInstruction(ConditionalInstruction):

    instruction_on_yes = None
    instruction_on_no = None

    @property
    def next_instruction(self):
        if self.condition.value:
            return self.instruction_on_yes
        return self.instruction_on_no

    @next_instruction.setter
    def next_instruction(self, instruction):
        self.instruction_on_yes = instruction
        if self.instruction_on_no is None:
            self.instruction_on_no = instruction

    def set_on_no(self, instruction):
        self.instruction_on_no = instruction

    # compiler
    def on_no(self, label):
        return Macro(self, OnNo(label))


class OnNo(Compilable):

    label = None

    def __init__(self, label):
        self.label = label

    def compile(self, compiler):
        compiler.register_fix(self.label, compiler.last_instruction.set_on_no)


class Define(Compilable):

    labels = None

    def __init__(self, labels):
        self.labels = labels

    def compile(self, compiler):
        if compiler.would_fall_over:
            raise FallOverOnDefineError

        compiler.add_labels(self.labels)
        compiler.would_fall_over = True

def define(*labels):
    return Define(set(labels))


class Runner(object):

    def run_instruction(self, instruction, state):
        return instruction.run(state)

    def run(self, start_instruction, condition, state):
        condition.value = True
        instruction = start_instruction

        while instruction:
            state = self.run_instruction(instruction, state)
            instruction = instruction.next_instruction

        return state


class Runnable(object):

    start_instruction = None
    condition = None
    runner = None

    def register_runner(self, runner):
        self.runner = runner

    def run(self, state):
        return self.runner.run(self.start_instruction, self.condition, state)


class Call(Runnable, BranchingInstruction):

    label = None

    def __init__(self, label):
        self.label = label

    def compile(self, compiler):
        super(Call, self).compile(compiler)
        compiler.register_fix(self.label, compiler.last_instruction.set_start_instruction)

    def set_start_instruction(self, instruction):
        self.start_instruction = instruction

    def clone(self):
        return self.__class__(self.label)

def do(label):
    return Call(label)


class Condition(object):

    value = True


class Program(Runnable):

    instructions = None

    def __init__(self, instructions, runner=None):
        self.instructions = instructions
        self.start_instruction = instructions[0]
        self.condition = Condition()
        self.register_condition()
        self.register_runner(runner or Runner())

    def set_runner(self, runner):
        super(Program, self).set_runner(runner)
        self.register_runner(runner)

    def register_runner(self, runner):
        self.runner = runner
        def noop(runner):
            pass
        for instruction in self.instructions:
            register = getattr(instruction, 'register_runner', noop)
            register(self.runner)

    def register_condition(self):
        def noop(condition):
            pass
        for instruction in self.instructions:
            register = getattr(instruction, 'register_condition', noop)
            register(self.condition)


class Compiler(object):

    instructions = None
    previous_labels = None
    labels = None
    fixes = None
    would_fall_over = True

    @property
    def last_instruction(self):
        return self.instructions[-1]

    def compile(self, program_spec):
        self.instructions = list()
        self.labels = set()
        self.previous_labels = set()
        self.fixes = dict()

        for compilable in program_spec:
            compilable.compile(self)

        if self.fixes or self.labels:
            raise UndefinedLabelError(set(self.fixes.keys()).union(self.labels))

        if self.would_fall_over:
            raise UnclosedProgramError

        return Program(self.instructions)

    def add_instruction(self, instruction):
        if self.instructions:
            self.instructions[-1].next_instruction = instruction
        self.instructions.append(instruction)
        self.fix_labels(instruction)

    def fix_labels(self, instruction):
        self.previous_labels.update(self.labels)
        for label in self.labels:
            if label in self.fixes:
                for fix in self.fixes[label]:
                    fix(instruction)
                del self.fixes[label]
        self.labels = set()

    def add_labels(self, labels):
        if not labels.isdisjoint(self.previous_labels):
            raise DuplicateLabelError

        self.labels = labels

    def register_fix(self, label, fix):
        if label in self.previous_labels:
            raise BackwardReferenceError

        self.fixes.setdefault(label, []).append(fix)


def compile(program_spec):
    return Compiler().compile(program_spec)

# ---------------------------------------------------------------------------

import unittest


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
            [do('odd?').on_no('even'),
                    Add1, RETURN,
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

    def test_macro_return(self):
        prog = compile(
            [do('even?').on_no('odd'), RETURN,

            define('odd'), Add1, RETURN,

            define('even?'),
                do('odd?').on_no('even? even'),
                        RETURN_FALSE,
                    define('even? even'), RETURN_TRUE,

            define('odd?'),
                IsOdd, RETURN,
                ])

        self.assertEqual(2, prog.run(1))
        self.assertEqual(2, prog.run(2))
        self.assertEqual(4, prog.run(3))
        self.assertEqual(4, prog.run(4))


if __name__ == '__main__':
    unittest.main()