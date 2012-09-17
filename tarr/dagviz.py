import sys
from tarr.compiler import Program

name, = sys.argv[1:]

module = __import__(name, fromlist=[True])

program = Program(module.TARR_PROGRAM)
print program.to_dot()
