import sys
from tarr.dag import DagConfigReader

config, = sys.argv[1:]

dag = DagConfigReader().from_string(open(config).read())
print dag.to_dot()
