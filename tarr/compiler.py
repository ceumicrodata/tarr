class DuplicateLabelError(Exception):
    pass

class UndefinedLabelError(Exception):
    pass

class BackwardReferenceError(Exception):
    pass


class Compilable(object):

    def compile(self, compiler):
        pass


class Instruction(Compilable):

    next_instruction = None

    def run(self, state):
        return state

    def compile(self, compiler):
        compiler.add_instruction(self.__class__())


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
        compiler.add_instruction(self.__class__(self.return_value))


RETURN = Return()
RETURN_TRUE = Return(return_value=True)
RETURN_FALSE = Return(return_value=False)


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
        if self.instruction_on_yes is None:
            self.instruction_on_yes = instruction
        if self.instruction_on_no is None:
            self.instruction_on_no = instruction
    def set_on_yes(self, instruction):
        self.instruction_on_yes = instruction

    def set_on_no(self, instruction):
        self.instruction_on_no = instruction


# FIXME: continue_at() should be removed
class ContinueAt(Instruction):

    label = None
    continue_at = None

    def __init__(self, label):
        self.label = label

    @property
    def next_instruction(self):
        return self.continue_at

    @next_instruction.setter
    def next_instruction(self, instruction):
        pass

    def run(self, state):
        return state

    def compile(self, compiler):
        compiler.add_instruction(self)
        compiler.register_fix(self.label, self.fix_label)

    def fix_label(self, instruction):
        self.continue_at = instruction


def continue_at(label):
    ''' = goto, should turn out to be unneeded, will be removed '''
    return ContinueAt(label)


class Label(Compilable):

    label = None

    def __init__(self, label):
        self.label = label

    def compile(self, compiler):
        compiler.add_label(self.label)

def define(label):
    return Label(label)


# FIXME: on_yes() should be removed, when_not() should be enough!
class OnYes(Compilable):

    label = None

    def __init__(self, label):
        self.label = label

    def compile(self, compiler):
        compiler.register_fix(self.label, compiler.last_instruction.set_on_yes)

def on_yes(label):
    return OnYes(label)

class OnNo(OnYes):

    def compile(self, compiler):
        compiler.register_fix(self.label, compiler.last_instruction.set_on_no)

def on_no(label):
    return OnNo(label)

when_not = on_no

class Runnable(object):

    def run(self, state):
        self.condition.value = True
        instruction = self.start_instruction

        while instruction:
            state = instruction.run(state)
            instruction = instruction.next_instruction

        return state


class InsertMacro(Runnable, BranchingInstruction):

    label = None
    start_instruction = None

    def __init__(self, label):
        self.label = label

    def compile(self, compiler):
        instruction = self.__class__(self.label)
        compiler.add_instruction(instruction)
        compiler.register_fix(self.label, instruction.set_start_instruction)

    def set_start_instruction(self, instruction):
        self.start_instruction = instruction

def insert(label):
    return InsertMacro(label)


class Condition(object):

    value = True


class Program(Runnable):

    instructions = None
    condition = None

    @property
    def start_instruction(self):
         return self.instructions[0]

    def __init__(self, instructions):
        self.instructions = instructions
        self.condition = Condition()
        self.register_condition()

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
            raise UndefinedLabelError

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

    def add_label(self, label):
        if label in self.previous_labels:
            raise DuplicateLabelError

        self.labels.add(label)

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
        prog = compile([Add1, Mul2])
        self.assertEqual(8, prog.run(3))

    def test_condition_output1(self):
        prog = compile([IsOdd])
        self.assertEqual(3, prog.run(3))
        self.assertTrue(prog.condition.value)

    def test_condition_output2(self):
        prog = compile([IsOdd])
        self.assertEqual(4, prog.run(4))
        self.assertFalse(prog.condition.value)

    def test_continue_at(self):
        prog = compile([continue_at('q'), Die, define('q'), Add1])
        self.assertEqual(5, prog.run(4))

    def test_duplicate_definition_of_label_is_not_compilable(self):
        self.assertRaises(DuplicateLabelError, compile, [define('label'), Noop, define('label'), Noop])

    def test_incomplete_program_is_not_compilable(self):
        self.assertRaises(UndefinedLabelError, compile, [define('label'), Noop, define('label2')])

    def test_backward_reference_prevents_compilation(self):
        self.assertRaises(BackwardReferenceError, compile, [define('label'), Noop, continue_at('label')])

    def test_branch_on_yes(self):
        prog = compile([IsOdd, on_yes('just1'), Add1, define('just1'), Add1])
        self.assertEqual(4, prog.run(3))
        self.assertEqual(6, prog.run(4))

    def test_branch_on_no(self):
        prog = compile([IsOdd, on_no('just1'), Add1, define('just1'), Add1])
        self.assertEqual(5, prog.run(4))
        self.assertEqual(5, prog.run(3))

    def test_branch(self):
        prog = compile([IsOdd, on_no('just1'), on_yes('just1'), Die, define('just1'), Add1])
        self.assertEqual(4, prog.run(3))
        self.assertEqual(5, prog.run(4))

    def test_insert(self):
        prog = compile([insert('+1'), insert('+2'), define('+2'), Add1, define('+1'), Add1])
        self.assertEqual(5, prog.run(0))

    def test_macro_return_yes(self):
        prog = compile(
            [insert('odd?'), when_not('even'),
                    Add1, RETURN,
                define('even'), RETURN,

            define('odd?'),
                IsOdd, on_yes('odd?: yes'),
                        RETURN_FALSE,
                    define('odd?: yes'),
                        RETURN_TRUE])

        self.assertEqual(2, prog.run(1))
        self.assertEqual(4, prog.run(3))

    def test_macro_return_no(self):
        prog = compile(
            [insert('odd?'), when_not('even'),
                    Add1, RETURN,
                define('even'), RETURN,

            define('odd?'),
                IsOdd, on_yes('odd?: yes'),
                        RETURN_FALSE,
                    define('odd?: yes'),
                        RETURN_TRUE])

        self.assertEqual(2, prog.run(2))
        self.assertEqual(4, prog.run(4))

    def test_macro_return(self):
        prog = compile(
            [insert('even?'), when_not('odd'),
                    RETURN,
                define('odd'), Add1, RETURN,

            define('even?'),
                insert('odd?'), when_not('even? even'),
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