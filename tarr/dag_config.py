import tarr.dag


# special objects used in place of node names with special meaning
NEXT = tarr.dag.IMPLICIT_NEXT
STOP = None


class Node(object):

    name = None
    impl = None

    # only 3 outgoing edges supported:
    # they contain node names or None for STOP:
    nn_success = None
    nn_fail = None

    def __init__(self, name, impl, on_success=NEXT, on_fail=NEXT):
        self.name = name
        self.impl = impl
        self.nn_success = on_success
        self.nn_fail = on_fail

