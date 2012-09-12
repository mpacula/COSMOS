#!/usr/bin/python
"""
Prints the first argument to stdout, and the reverse to stderr
"""
import sys

out = sys.argv[1]

print out
print >> sys.stderr, out[::-1]