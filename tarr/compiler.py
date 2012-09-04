from tarr import compiler_base
from tarr import model
from datetime import datetime, timedelta


from .compiler_base import (
    Instruction, BranchingInstruction,
    RETURN, RETURN_TRUE, RETURN_FALSE,
    DEF, IF, ELSE, ENDIF)


class StatisticsCollectorRunner(compiler_base.Runner):

    statistics = None
    condition = None

    def __init__(self, condition):
        self.condition = condition
        self.statistics = []

    def run_instruction(self, instruction, state):
        self.ensure_statistics(instruction.index)

        before = datetime.now()
        stat = self.statistics[instruction.index]
        stat.item_count += 1

        state = instruction.run(state)

        if self.condition.value:
            stat.success_count += 1
        else:
            stat.failure_count += 1

        after = datetime.now()
        stat.run_time += after - before

        return state

    def ensure_statistics(self, index):
        while index >= len(self.statistics):
            stat = model.NodeStatistic()
            stat.init(index)
            self.statistics.append(stat)


class Program(object):

    program = None
    runner = None

    def __init__(self, program, runner):
        self.program = program
        self.runner = runner

    def run(self, state):
        return self.program.run(state)

    @property
    def statistics(self):
        return self.runner.statistics


def compile(program_spec):
    ''' program_spec -> program object with .run method, that will collect statistics '''
    program = compiler_base.Compiler().compile(program_spec)
    runner = StatisticsCollectorRunner(program.condition)
    program.register_runner(runner)
    return Program(program, runner)


# decorators to make simple functions into a Instruction

class TarrRuleInstruction(Instruction):

    def __init__(self, func):
        self.func = func

    def run(self, data):
        data.payload = self.func(data.payload)
        return data

    def clone(self):
        return self.__class__(self.func)


def rule(func):
    '''
    Decorator, enable function to be used as an instruction in a Tarr program.

    Usage:

    @rule
    def func(data):
        ...
        return data
    '''
    func.compile = TarrRuleInstruction(func).compile
    return func


class TarrBranchInstruction(BranchingInstruction):

    def __init__(self, func):
        self.func = func

    def run(self, data):
        self.condition.value = self.func(data.payload)
        return data

    def clone(self):
        return self.__class__(self.func)


def branch(func):
    '''
    Decorator, enable function to be used as a condition in a Tarr program.

    Usage:

    @branch
    def cond(data):
        ...
        return {True | False}
    '''
    func.compile = TarrBranchInstruction(func).compile
    return func
