import unittest
import dag as m # odule


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

class DagConfigReader(m.DagConfigReader):

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
        self.assertEqual('success', reader.node_by_name('nodename').success)

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

        self.assertEqual('impl', node1.impl)
        self.assertEqual('node_success', node1.success)
        self.assertEqual('node_fail', node1.human)
        self.assertEqual('node_fail', node1.fail)

        self.assertEqual('impl_success', node_success.impl)
        self.assertEqual(None, node_success.success)
        self.assertEqual(None, node_success.human)
        self.assertEqual('node_fail', node_success.fail)

        self.assertEqual('impl_fail', node_fail.impl)
        self.assertEqual(None, node_fail.success)
        self.assertEqual(None, node_fail.fail)
        self.assertEqual(None, node_fail.human)

    def test_missing_node__from_string__dies(self):
        reader = DagConfigReader()

        self.assertRaises(Exception, lambda: reader.from_string(TEST_CONFIG_MISSING_DEFS))

    def test_backref_in_config__from_string__dies(self):
        reader = DagConfigReader()

        self.assertRaises(Exception, lambda: reader.from_string(TEST_CONFIG_BACKREF))
