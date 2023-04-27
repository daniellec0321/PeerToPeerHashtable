import subprocess
import time
import sys

num_processes = 2

interval = time.time_ns()

for i in range(num_processes):
    while True:

        if (time.time_ns() - interval) / 1000000000 > 0.5:
            #Open processes a half of a second apart
            subprocess.Popen(['python', 'P2PHashTableClient.py'])
            interval = time.time_ns()
            break

sys.exit(0)