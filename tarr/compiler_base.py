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

    index = None

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
        return None

    @next_instruction.setter
    def next_instruction(self, instruction):
        pass

    def run(self, state):
        if self.return_value is not None:
            self.condition.value = self.return_value

        return state

    def compile(self, compiler):
        super(Return, self).compile(compiler)
        compiler.would_fall_over = False

    def clone(self):
        return self.__class__(self.return_value)


# FIXME: RETURN -> RETURN_WITH_CURRENT_CONDITION
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
    # FIXME: on_no -> on_no_return_with
    def on_no(self, label):
        return Macro(self, OnNo(label))


class OnNo(Compilable):

    label = None

    def __init__(self, label):
        self.label = label

    def compile(self, compiler):
        compiler.register_linker(self.label, compiler.last_instruction.set_on_no)


class Define(Compilable):

    label = None

    def __init__(self, label):
        self.label = label

    def compile(self, compiler):
        if compiler.would_fall_over:
            raise FallOverOnDefineError

        compiler.add_label(self.label)
        compiler.would_fall_over = True

def DEF(label):
    return Define(label)


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
        compiler.register_linker(self.label, compiler.last_instruction.set_start_instruction)

    def set_start_instruction(self, instruction):
        self.start_instruction = instruction

    def clone(self):
        return self.__class__(self.label)

def DO(label):
    return Call(label)


class Condition(object):

    value = True


class Program(Runnable):

    instructions = None

    def __init__(self, instructions):
        self.instructions = instructions
        self.start_instruction = instructions[0]
        self.condition = Condition()
        self.register_condition()

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
    label = None
    linkers = None
    would_fall_over = True

    @property
    def last_instruction(self):
        return self.instructions[-1]

    def compile(self, program_spec):
        self.instructions = list()
        self.label = None
        self.previous_labels = set()
        self.linkers = dict()

        for compilable in program_spec:
            if isinstance(compilable, basestring):
                compilable = Call(compilable)
            compilable.compile(self)

        if self.linkers or self.label:
            raise UndefinedLabelError(set(self.linkers.keys()).union(set([self.label])))

        if self.would_fall_over:
            raise UnclosedProgramError

        for i, instruction in enumerate(self.instructions):
            instruction.index = i

        return Program(self.instructions)

    def add_instruction(self, instruction):
        if self.instructions:
            self.instructions[-1].next_instruction = instruction
        self.instructions.append(instruction)
        self.fix_labels(instruction)

    def fix_labels(self, instruction):
        self.previous_labels.add(self.label)
        if self.label in self.linkers:
            for linker in self.linkers[self.label]:
                linker(instruction)
            del self.linkers[self.label]
        self.label = None

    def add_label(self, label):
        if label in self.previous_labels:
            raise DuplicateLabelError

        self.label = label

    def register_linker(self, label, linker):
        if label in self.previous_labels:
            raise BackwardReferenceError

        self.linkers.setdefault(label, []).append(linker)
