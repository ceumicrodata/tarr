from tarr import compiler_base
from tarr import model
from datetime import datetime


from .compiler_base import (
    Instruction, BranchingInstruction,
    RETURN, RETURN_TRUE, RETURN_FALSE,
    DEF, IF, ELSE, ENDIF)


class StatisticsCollectorRunner(compiler_base.Runner):

    statistics = None

    def __init__(self):
        self.statistics = []

    def run_instruction(self, instruction, state):
        self.ensure_statistics(instruction.index)

        before = datetime.now()
        stat = self.statistics[instruction.index]
        stat.item_count += 1

        state = instruction.run(self, state)

        if self.exit_status:
            stat.success_count += 1
        else:
            stat.failure_count += 1

        after = datetime.now()
        stat.run_time += after - before

        return state

    def ensure_statistics(self, index):
        while index >= len(self.statistics):
            stat = model.NodeStatistic()
            stat.init(len(self.statistics))
            self.statistics.append(stat)


class ToTextVisitor(compiler_base.ProgramVisitor):

    def __init__(self, formatter):
        self.lines = []
        self.formatter = formatter

    def text(self):
        return '\n'.join(self.lines)

    def addline(self, line, is_comment=False):
        self.lines.append(line)

    def addcomment(self, line):
        self.addline('     ' + line)

    def addcode(self, instruction, line):
        self.addline('{0:4d} {1}'.format(instruction.index, line))

    def format_branch(self, instruction, name):
        self.formatter.format_branch(self, instruction, name)

    def format_instruction(self, instruction, name):
        self.formatter.format_instruction(self, instruction, name)

    def enter_subprogram(self, label, instructions):
        if label is not None:
            self.addline('')
            self.addline('DEF ("{0}")'.format(label))

    def leave_subprogram(self, label):
        if label is None:
            self.addline('END OF MAIN PROGRAM')
        else:
            self.addline('END # {0}'.format(label))

    def visit_call(self, i_call):
        self.format_branch(i_call, 'CALL "{0}"'.format(i_call.label))

    def visit_return(self, i_return):
        if i_return.return_value is None:
            self.format_instruction(i_return, 'RETURN')
        else:
            self.format_instruction(i_return, 'RETURN {0}'.format(i_return.return_value))

    def visit_instruction(self, instruction):
        self.addcode(instruction, instruction.instruction_name)

    def visit_branch(self, i_branch):
        self.format_branch(i_branch, i_branch.instruction_name)


class TextFormatter(object):

    def format_branch(self, collector, instruction, name):
        collector.addcode(instruction, name)
        on_success = instruction.next_instruction(exit_status=True).index
        on_failure = instruction.next_instruction(exit_status=False).index
        collector.addcomment('  # True  -> {0}'.format(on_success))
        collector.addcomment('  # False -> {0}'.format(on_failure))

    def format_instruction(self, collector, instruction, name):
        collector.addcode(instruction, name)


class TextFormatterWithStatistics(TextFormatter):

    def __init__(self, statistics):
        self.statistics = statistics

    def format_branch(self, collector, instruction, name):
        statistics = self.statistics[instruction.index]
        collector.addcode(instruction, name)
        on_success = instruction.next_instruction(exit_status=True).index
        on_failure = instruction.next_instruction(exit_status=False).index
        collector.addcomment('  # True  -> {0}   (*{1.success_count})'.format(on_success, statistics))
        collector.addcomment('  # False -> {0}   (*{1.failure_count})'.format(on_failure, statistics))

    def format_instruction(self, collector, instruction, name):
        statistics = self.statistics[instruction.index]
        collector.addcode(instruction, '{0}   (*{1.item_count})'.format(name, statistics))


class ToDotVisitor(compiler_base.ProgramVisitor):

    def __init__(self, formatter):
        self.lines = ['digraph {', '', 'compound = true;']
        self.formatter = formatter
        self.inter_cluster_edges = []

    def text(self):
        extras = []
        if self.inter_cluster_edges:
            extras.extend(
                ['',
                '// inter-cluster-edges'
                ])
            extras.extend(self.inter_cluster_edges)
        extras.append('}')
        return '\n'.join(self.lines + extras)

    def addline(self, line, is_comment=False):
        self.lines.append(line)

    def format_branch(self, instruction, name):
        self.formatter.format_branch(self, instruction, name)

    def format_instruction(self, instruction, name):
        self.formatter.format_instruction(self, instruction, name)

    def cluster_name(self, label):
        return self.formatter.cluster_name(label)

    def node_name(self, instruction):
        return self.formatter.node_name(instruction)

    def add_node(self, instruction, name):
        self.formatter.add_node(self, instruction, name)

    def add_return_node(self, instruction, name):
        self.formatter.add_return_node(self, instruction, name)

    def add_edge(self, instruction1, instruction2, label):
        self.formatter.add_edge(self, instruction1, instruction2, label)

    def add_inter_cluster_edge(self, instruction1, instruction2, label):
        self.inter_cluster_edges.append(self.formatter.format_edge(instruction1, instruction2, label))

    def enter_subprogram(self, label, instructions):
        self.addline('')
        self.addline('subgraph {} {{'.format(self.cluster_name(label)))
        if label is not None:
            self.addline('    label = "{}";'.format(self.formatter.escape(label)))
            self.addline('')

    def leave_subprogram(self, label):
        self.addline('}')

    def visit_call(self, i_call):
        self.add_inter_cluster_edge(i_call, i_call.start_instruction, '')
        self.format_branch(i_call, 'CALL {0}'.format(i_call.label))

    def visit_return(self, i_return):
        if i_return.return_value is None:
            node_name = 'RETURN'
        else:
            node_name = 'RETURN {0}'.format(i_return.return_value)
        self.add_return_node(i_return, node_name)

    def visit_instruction(self, instruction):
        self.format_instruction(instruction, instruction.instruction_name)

    def visit_branch(self, i_branch):
        self.format_branch(i_branch, i_branch.instruction_name)


class DotFormatter(object):

    def cluster_name(self, label):
        return '"cluster_{}"'.format(self.escape(label))

    def node_name(self, instruction):
        return 'node_{}'.format(instruction.index)

    def escape(self, label):
        return str(label).replace('"', r'\"')

    def add_node(self, collector, instruction, name):
        node = self.node_name(instruction)
        collector.addline('    {} [label="{}"];'.format(node, self.escape(name)))

    def add_return_node(self, collector, instruction, name):
        self.add_node(collector, instruction, name)

    def format_edge(self, instruction1, instruction2, label):
        node1 = self.node_name(instruction1)
        node2 = self.node_name(instruction2)
        attrs = dict()
        if label:
            attrs['label'] = '"{}"'.format(self.escape(label))
        if attrs:
            formatted_attrs = ' [{}]'.format(
                ','.join(
                    '{}={}'.format(attr, value)
                    for (attr, value) in attrs.iteritems()))
        else:
            formatted_attrs = ''

        return '    {} -> {}{};'.format(node1, node2, formatted_attrs)

    def add_edge(self, collector, instruction1, instruction2, label):
        collector.addline(self.format_edge(instruction1, instruction2, label))

    def format_branch(self, collector, instruction, name):
        collector.add_node(instruction, name)
        on_success = instruction.next_instruction(exit_status=True)
        on_failure = instruction.next_instruction(exit_status=False)
        collector.add_edge(instruction, on_success, label='True')
        collector.add_edge(instruction, on_failure, label='False')

    def format_instruction(self, collector, instruction, name):
        collector.add_node(instruction, name)
        next_instruction = instruction.next_instruction(exit_status=True)
        collector.add_edge(instruction, next_instruction, label='')


class DotFormatterWithStatistics(DotFormatter):

    def __init__(self, statistics):
        self.statistics = statistics

    def add_return_node(self, collector, instruction, name):
        statistics = self.statistics[instruction.index]
        self.add_node(collector, instruction, '{0}: {1.item_count}'.format(name, statistics))

    def format_branch(self, collector, instruction, name):
        collector.add_node(instruction, name)
        on_success = instruction.next_instruction(exit_status=True)
        on_failure = instruction.next_instruction(exit_status=False)
        statistics = self.statistics[instruction.index]
        collector.add_edge(instruction, on_success, label='True: {0.success_count}'.format(statistics))
        collector.add_edge(instruction, on_failure, label='False: {0.failure_count}'.format(statistics))


class Program(compiler_base.Program):

    # FIXME: Program.__init__ should initialize statistic as well by calling runner.ensure_statistics
    def make_runner(self):
        return StatisticsCollectorRunner()

    @property
    def statistics(self):
        return self.runner.statistics

    def to_text(self, with_statistics=False):
        if with_statistics:
            formatter = TextFormatterWithStatistics(self.statistics)
        else:
            formatter = TextFormatter()
        v = ToTextVisitor(formatter)
        self.accept(v)
        return v.text()

    def to_dot(self, with_statistics=False):
        if with_statistics:
            formatter = DotFormatterWithStatistics(self.statistics)
        else:
            formatter = DotFormatter()
        v = ToDotVisitor(formatter)
        self.accept(v)
        return v.text()


# decorators to make simple functions into Instructions

class TarrInstructionBase(object):

    def __init__(self, func):
        self.func = func

    def clone(self):
        return self.__class__(self.func)

    @property
    def instruction_name(self):
        return self.func.__name__


class TarrRuleInstruction(TarrInstructionBase, Instruction):

    def run(self, runner, data):
        data.payload = self.func(data.payload)
        return data


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


class TarrBranchInstruction(TarrInstructionBase, BranchingInstruction):

    def run(self, runner, data):
        runner.set_exit_status(self.func(data.payload))
        return data


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
