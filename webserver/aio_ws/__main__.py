import sys

import os, sys, inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
libdir = os.path.join(parentdir, 'lib')
sys.path.insert(0, libdir)


from aio_ws.main import main


main(sys.argv[1:])
