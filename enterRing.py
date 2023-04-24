import socket
import json
from P2PHashTableClient import P2PHashTableClient
import sys

clean_exit = True
if len(sys.argv) > 1:
    clean_exit = bool(int(sys.argv[1]))

client = P2PHashTableClient(clean_exit)
client.enterRing('begloff-project')

# After entering the ring feel free to test any other items
# client.sendUpdateNext([1, '129.74.152.142', 38867],[3.8192902168798724, '129.74.152.142', 38867])
# client.sendUpdatePrev([1, '129.74.152.142', 38867],[3.8192902168798724, '129.74.152.142', 38867])
