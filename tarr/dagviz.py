import sys
from tarr.runner import DagConfigReader

config, = sys.argv[1:]

dag = DagConfigReader().from_string(open(config).read())
dag.initialize()
print dag.to_dot()
