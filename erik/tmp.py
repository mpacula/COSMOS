
import sys
with open('/tmp/test','w') as f:
  for line in sys.stdin:
    f.write(line)
