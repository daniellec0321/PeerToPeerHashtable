from P2PHashTableClient import P2PHashTableClient

ht = P2PHashTableClient()
ht.next = (9, 'student10.cse.nd.edu', 40000)
ht.prev = (9, 'student10.cse.nd.edu', 40000)
ht.ipAddress = 'student12.cse.nd.edu'
ht.port = 40000
ht.high = 19
ht.low = 10

ht.sanityCheck()
