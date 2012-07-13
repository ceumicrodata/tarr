'''
A restricted DAG, where each node can have only 3 output edges:
    S: for success
    F: for failure

Node definitions syntax:
    :nodename(nodeimpl) [{O[O[O]]:nodename-o1 [O[O[O]]:nodename-o2 [...]]}]

where
 O is either S or F,
 nodename-oN is either some not-yet defined nodename or the special nodename STOP.

The output definitions are optional, if missing
  S: and F: defaults to next node defined
'''

import itertools
from pyparsing import Word, Literal, Or, Optional, ZeroOrMore, OneOrMore, StringEnd, alphanums
from pyparsing import ParseFatalException


class Node(object):

    name = None
    impl = None

    # only 3 outgoing edges supported:
    # they contain node names or None for STOP:
    nn_success = None
    nn_fail = None

    def to_dot(self):
        '''Create DOT output - see GraphViz'''

        # parallel arrays
        names = []
        labels = []

        def addlabel(name, label):
            if name is None:
                return

            try:
                labels[names.index(name)] += label
            except ValueError:
                # new node
                names.append(name)
                labels.append(label)

        addlabel(self.nn_success, 'S')
        addlabel(self.nn_fail, 'F')

        if self.nn_success and self.nn_success == self.nn_fail:
            return '{0} -> {1}'.format(self.name, self.nn_success)

        return ' '.join(
            '{0} -> {1} [label={2}]'.format(self.name, name, label)
            for (name, label) in zip(names, labels))


class DAG(object):

    start_node = None
    name2node = None

    def node_by_name(self, name):
        return self.name2node[name]

    @property
    def nodes(self):
        return self.name2node.itervalues()

    def to_dot(self):
        '''Create DOT output - see GraphViz'''

        return '\n'.join(
            itertools.chain(
                ['digraph {', 'node [ordering=out,shape=Mrecord]'],
                (n.to_dot() for n in self.nodes),
                ['}']))


class DagConfigReader(object):

    STOP_NODE_NAME = 'STOP'

    # nodes | futures
    # edges can be created only from nodes to futures

    nodes = None
    futures = None
    nodenames = None
    IMPLICIT_NEXT = object()

    def new_node(self):
        return Node()

    def new_dag(self):
        return DAG()

    def __init__(self):
        self.nodes = []
        self.futures = set()
        self.nodenames = set()

    @property
    def nodecount(self):
        return len(self.nodenames)

    def define(self, name, impl):
        if name in self.nodenames:
            raise Exception('Duplicate definition of {0}'.format(name))

        node = self.new_node()
        node.name = name
        node.impl = impl
        node.nn_success = self.IMPLICIT_NEXT
        node.nn_fail = self.IMPLICIT_NEXT

        self.fix_implicit_edges_with(name)
        self.nodes.append(node)
        self.nodenames.add(name)
        self.futures.discard(name)
        return node

    def fix_implicit_edges_with(self, nodename):
        if not self.nodes:
            return

        node_to_fix = self.nodes[-1]
        if node_to_fix.nn_success == self.IMPLICIT_NEXT:
            node_to_fix.nn_success = nodename
        if node_to_fix.nn_fail == self.IMPLICIT_NEXT:
            node_to_fix.nn_fail = nodename

    def add_edge(self, label, destnodename):
        assert self.nodes
        if destnodename in self.nodenames:
            raise Exception('{0} already defined, config is not monotone'.format(destnodename))

        node = self.nodes[-1]

        if destnodename:
            self.futures.add(destnodename)

        if label == 'S':
            node.nn_success = destnodename
        else:
            assert label == 'F'
            node.nn_fail = destnodename

    def from_string(self, string):
        # Grammar for config:
        wordchars = alphanums + '_'
        nodename = Word(wordchars)
        nodeimpl = Word(wordchars + '.')
        outputlabel = Or([Literal('S'), Literal('F')])
        output_edgedef = OneOrMore(outputlabel) + Literal('->') + nodename
        output_edgedefs = ZeroOrMore(output_edgedef)
        nodedef = nodename + Literal(':') + nodeimpl + Optional(output_edgedefs)
        nodedefs = OneOrMore(nodedef) + StringEnd()

        # Actions to do build a DAG at parse time
        nodename.setParseAction(self.handle_nodename)
        nodedef.setParseAction(self.handle_nodedef)
        output_edgedef.setParseAction(self.handle_edgedef)

        parseresults = nodedefs.parseString(string)
        if parseresults:
            # see handle_nodedef: only exceptions are kept in the list!
            # this hack is due to the inability to properly raise an exception in a parseAction
            # (it would be masked by another - obscure - exception,
            # see http://stackoverflow.com/questions/10177276/pyparsing-setparseaction-function-is-getting-no-arguments)
            raise parseresults[0]
        # fix last node - all edges should be STOP
        self.fix_implicit_edges_with(None)
        if self.futures:
            raise ParseFatalException('Undefined nodes: {0}'.format(self.futures))

        dag = self.new_dag()
        dag.name2node = dict((n.name, n) for n in self.nodes)
        dag.start_node = self.nodes[0]

        return dag

    def handle_nodename(self, s, loc, toks):
        nodename, = toks
        if nodename == self.STOP_NODE_NAME:
            return [None]

    def handle_nodedef(self, s, loc, toks):
        try:
            nodename, _, nodeimpl = toks[:3]
            if nodename is None:
                raise Exception('{0} is reserved name for marking the place to STOP'.format(self.STOP_NODE_NAME))
            self.define(nodename, nodeimpl)
            if len(toks) > 3:
                output_edgedefs = toks[3:]
                for (label, destnodename) in output_edgedefs:
                    self.add_edge(label, destnodename)
        except Exception as e:
            return [ParseFatalException(s, loc, msg=str(e))]
        return []

    def handle_edgedef(self, s, loc, toks):
        nodename = toks[-1]
        outputlabels = toks[:-2]
        return [(outputlabel, nodename) for outputlabel in outputlabels]