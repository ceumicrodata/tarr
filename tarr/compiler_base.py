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

class MissingEndIfError(Exception):
    pass

class MultipleElseError(Exception):
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
        compiler.path.close()

    def clone(self):
        return self.__class__(self.return_value)


# FIXME: RETURN -> RETURN_WITH_CURRENT_CONDITION
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
        self.instruction_on_yes = instruction
        if self.instruction_on_no is None:
            self.instruction_on_no = instruction

    def set_instruction_on_yes(self, instruction):
        self.instruction_on_yes = instruction

    def set_instruction_on_no(self, instruction):
        self.instruction_on_no = instruction


class Define(Compilable):

    label = None

    def __init__(self, label):
        self.label = label

    def compile(self, compiler):
        if compiler.path.is_open:
            raise FallOverOnDefineError

        compiler.start_define_label(self.label)

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


class CompileIf(Compilable):

    def __init__(self, branch_instruction):
        self.branch_instruction = branch_instruction

    def compile(self, compiler):
        branch_instruction = compiler.compilable(self.branch_instruction)
        branch_instruction.compile(compiler)
        false_path = compiler.path.split(compiler.last_instruction)
        compiler.control_stack.append(IfElseControlFrame(compiler.path, false_path))

def IF(branch_instruction):
    return CompileIf(branch_instruction)


class Else(Compilable):

    def compile(self, compiler):
        frame = compiler.control_stack[-1]
        if frame.else_used:
            raise MultipleElseError
        compiler.path = frame.false_path
        frame.else_used = True

ELSE = Else()


class EndIf(Compilable):

    def compile(self, compiler):
        frame = compiler.control_stack.pop()
        compiler.path = frame.true_path
        compiler.path.join(frame.false_path)

ENDIF = EndIf()


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


class Appender(object):
    '''Knows how to continue a path

    Continue = how to append a new instruction to
    '''

    def append(self, instruction):
        pass


class InstructionAppender(Appender):
    '''Appends to previous instruction
    '''

    def __init__(self, instruction):
        self.last_instruction = instruction

    def append(self, instruction):
        self.last_instruction.next_instruction = instruction
        self.last_instruction = instruction


class NewPathAppender(Appender):
    '''Appends to empty path
    '''

    def __init__(self, path):
        self.path = path

    def append(self, instruction):
        self.path.set_appender(InstructionAppender(instruction))


class DefineAppender(Appender):
    '''Defines the label when appending an instruction
    '''

    def __init__(self, compiler, path, label):
        self.compiler = compiler
        self.path = path
        self.label = label

    def append(self, instruction):
        self.path.set_appender(InstructionAppender(instruction))
        self.compiler.complete_define_label(self.label, instruction)


class TrueBranchAppender(Appender):
    '''Appends to True side of a branch instruction
    '''

    def __init__(self, path, branch_instruction):
        self.path = path
        self.branch_instruction = branch_instruction

    def append(self, instruction):
        self.branch_instruction.set_instruction_on_yes(instruction)
        self.path.set_appender(InstructionAppender(instruction))


class FalseBranchAppender(Appender):
    '''Appends to False side of a branch instruction
    '''

    def __init__(self, path, branch_instruction):
        self.path = path
        self.branch_instruction = branch_instruction

    def append(self, instruction):
        self.branch_instruction.set_instruction_on_no(instruction)
        self.path.set_appender(InstructionAppender(instruction))


class JoinAppender(Appender):

    def __init__(self, path, merged_path):
        self.path = path
        self.orig_appender = path.appender
        self.merged_path = merged_path

    def append(self, instruction):
        self.merged_path.append(instruction)
        self.path.set_appender(InstructionAppender(instruction))
        self.orig_appender.append(instruction)


class Path(object):
    '''
    An execution path.

    Instructions can be appended to it and other paths can be joined in.
    Real work happens in appenders, which are changed as needed.
    '''

    def __init__(self, appender=None):
        self.appender = appender or NewPathAppender(self)
        self._is_closed = False

    def append(self, instruction):
        self.appender.append(instruction)

    def split(self, branch_instruction):
        self.set_appender(TrueBranchAppender(self, branch_instruction))
        false_path = Path()
        false_path.set_appender(FalseBranchAppender(false_path, branch_instruction))
        return false_path

    def join(self, path):
        self.appender = JoinAppender(self, path)

    def set_appender(self, appender):
        self.appender = appender

    @property
    def is_open(self):
        return not self._is_closed

    @property
    def is_closed(self):
        return self._is_closed

    def close(self):
        self._is_closed = True


class IfElseControlFrame(object):

    def __init__(self, true_path, false_path):
        self.true_path = true_path
        self.false_path = false_path
        self.else_used = False


class Compiler(object):

    instructions = None
    control_stack = None
    path = None

    previous_labels = None
    linkers = None

    @property
    def last_instruction(self):
        return self.instructions[-1]

    def compile(self, program_spec):
        self.control_stack = []
        self.path = Path()
        self.instructions = list()
        self.previous_labels = set()
        self.linkers = dict()

        for instruction in program_spec:
            compilable = self.compilable(instruction)
            compilable.compile(self)

        if self.control_stack:
            raise MissingEndIfError

        if self.linkers:
            raise UndefinedLabelError(set(self.linkers.keys()))

        if self.path.is_open:
            raise UnclosedProgramError

        return Program(self.instructions)

    def compilable(self, instruction):
        if isinstance(instruction, basestring):
            return Call(instruction)

        return instruction

    def add_instruction(self, instruction):
        self.path.append(instruction)
        instruction.index = len(self.instructions)
        self.instructions.append(instruction)

    def start_define_label(self, label):
        if label in self.previous_labels:
            raise DuplicateLabelError

        self.path = Path()
        # can not resolve label references yet, as the content (first instruction) is not known yet
        self.path.set_appender(DefineAppender(self, self.path, label))

    def complete_define_label(self, label, instruction):
        self.previous_labels.add(label)
        if label in self.linkers:
            for linker in self.linkers[label]:
                linker(instruction)
            del self.linkers[label]

    def register_linker(self, label, linker):
        if label in self.previous_labels:
            raise BackwardReferenceError

        self.linkers.setdefault(label, []).append(linker)
