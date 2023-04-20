from P2PHashTableClient import P2PHashTableClient
from FingerTable import FingerTable

ft = FingerTable()
ft.addNode((29, 'student11.cse.nd.edu', 40000))

ht = P2PHashTableClient()
ht.fingerTable = ft
ht.next = (9, 'student10.cse.nd.edu', 40000)
ht.prev = (34, 'student10.cse.nd.edu', 40000)
ht.ipAddress = 'student12.cse.nd.edu'
ht.port = 40000
ht.high = 0
ht.low = 35

msg = {'hi': 1}
ht.forwardMessage(msg, 19)
