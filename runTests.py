import subprocess
import time
import sys

num_processes = 1

interval = time.time_ns()

for i in range(num_processes):
    while True:

        if (time.time_ns() - interval) / 1000000000 > 2.5:
            #Open processes a 3 seconds apart
            subprocess.Popen(['python', 'P2PHashTableClient.py'])
            interval = time.time_ns()
            break

sys.exit(0)