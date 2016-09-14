"""
Used to generate the simple dataset for testing.
Generates a dataset with 1000 tuples, 2 attributes A & B.
Each attribute takes values randomly from 1 to 1000.
"""

import os
import random

if __name__ == "__main__":
  dir_path = os.path.dirname(os.path.realpath(__file__))
  os.chdir(dir_path)
  if not os.path.isdir("simple"):
    os.system("mkdir simple")

  os.chdir("simple")
  rands = ["%d|%d" % (random.randint(1,1000), random.randint(1,1000)) for i in xrange(0,1000)]
  open("simple.txt", "w").write("\n".join(rands))

