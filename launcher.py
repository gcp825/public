#!/usr/bin/python3

import sys
from importlib import import_module

mod = import_module(sys.argv[1] + '.' + sys.argv[1])
run = getattr(mod,'run')

run(True)
