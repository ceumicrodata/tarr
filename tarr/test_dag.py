import unittest

from tarr import dag as m # odule


TEST_GOOD_CONFIG = '''
node1 : impl
    F -> node_fail
    H -> node_fail

node_success : impl_success
    SH -> STOP

node_fail : impl_fail
'''

TEST_CONFIG_MISSING_DEFS = '''
node : impl
    F -> missing
'''

TEST_CONFIG_BACKREF = '''
node1 : impl
node2 : impl
    F -> node1
'''

TEST_CONFIG_STOP_REDEFINED = '''
STOP: whatever
'''


class DagConfigReader(m.DagConfigReader):

    call_count_new_node = 0
    call_count_new_dag = 0

    def new_node(self):
        self.call_count_new_node += 1
        return super(DagConfigReader, self).new_node()

    def new_dag(self, name2node):
        self.call_count_new_dag += 1
        return super(DagConfigReader, self).new_dag(name2node)

    def node_by_name(self, name):
        for node in self.nodes:
            if node.name == name:
                return node


class TestDagConfig(unittest.TestCase):

    def test_define(self):
        reader = DagConfigReader()

        node = reader.define('node', 'package.module.class')

        self.assertEqual('node', node.name)
        self.assertEqual('package.module.class', node.impl)
        self.assertEqual(1, reader.nodecount)
        self.assertEqual(set(['node']), reader.nodenames)

    def test_duplicate_name_can_not_be_created(self):
        reader = DagConfigReader()
        reader.define('nodename', 'impl')

        self.assertRaises(Exception, lambda: reader.define('nodename'))

    def test_node_by_name(self):
        reader = DagConfigReader()
        reader.define('nodename', 'impl')

        node = reader.node_by_name('nodename')
        self.assertEqual('nodename', node.name)

    def test_node_by_name_return_real_node(self):
        reader = DagConfigReader()
        reader.define('nodename', 'impl')
        node = reader.node_by_name('nodename')
        self.assertEqual('impl', node.impl)
        node.impl = 'nodeimpl'

        node = reader.node_by_name('nodename')

        self.assertEqual('nodeimpl', node.impl)

    def test_add_edge(self):
        reader = DagConfigReader()
        reader.define('nodename', 'impl')

        reader.add_edge('S', 'success')
        self.assertEqual('success', reader.node_by_name('nodename').nn_success)

    def test_dest_name_already_defined__add_edge__dies(self):
        reader = DagConfigReader()
        reader.define('node1', 'impl')
        reader.define('node2', 'impl')

        self.assertRaises(Exception, lambda: reader.add_edge('S', 'node2'))
        self.assertRaises(Exception, lambda: reader.add_edge('S', 'node1'))

    def test_from_string(self):
        reader = DagConfigReader()

        dag = reader.from_string(TEST_GOOD_CONFIG)

        self.assertEqual(3, len(dag.name2node))
        self.assertEqual(set(['node1', 'node_success', 'node_fail']), set(dag.name2node.keys()))

        node1 = dag.node_by_name('node1')
        node_fail = dag.node_by_name('node_fail')
        node_success = dag.node_by_name('node_success')

        self.assertEqual(node1, dag.start_node)

        self.assertEqual('impl', node1.impl)
        self.assertEqual('node_success', node1.nn_success)
        self.assertEqual('node_fail', node1.nn_human)
        self.assertEqual('node_fail', node1.nn_fail)

        self.assertEqual('impl_success', node_success.impl)
        self.assertEqual(None, node_success.nn_success)
        self.assertEqual(None, node_success.nn_human)
        self.assertEqual('node_fail', node_success.nn_fail)

        self.assertEqual('impl_fail', node_fail.impl)
        self.assertEqual(None, node_fail.nn_success)
        self.assertEqual(None, node_fail.nn_fail)
        self.assertEqual(None, node_fail.nn_human)

    def assert_from_string_fails(self, config, *messages):
        reader = DagConfigReader()

        try:
            reader.from_string(config)
            self.fail()
        except Exception as e:
            emsg = str(e).lower()
            for msg in messages:
                self.assertIn(msg, emsg)

    def test_missing_node__from_string__dies(self):
        self.assert_from_string_fails(TEST_CONFIG_MISSING_DEFS, 'undefined nodes')

    def test_backref_in_config__from_string__dies(self):
        self.assert_from_string_fails(TEST_CONFIG_BACKREF, 'already defined', 'not monotone')

    def test_node_definition_for_STOP__raises_exception(self):
        self.assert_from_string_fails(TEST_CONFIG_STOP_REDEFINED, 'reserved name', 'stop')

    def test_from_string_calls_new_node(self):
        reader = DagConfigReader()

        reader.from_string(TEST_GOOD_CONFIG)
        self.assertEqual(3, reader.call_count_new_node)

    def test_from_string_calls_new_dag(self):
        reader = DagConfigReader()

        reader.from_string(TEST_GOOD_CONFIG)
        self.assertEqual(1, reader.call_count_new_dag)
