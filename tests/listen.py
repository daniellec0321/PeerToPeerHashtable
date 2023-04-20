import socket
import json
from P2PHashTableClient import P2PHashTableClient

ht = P2PHashTableClient()
ht.ipAddress = 'student10.cse.nd.edu'

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(('', 40000))
port = sock.getsockname()[1]
sock.listen()
print('Listening on port {}...'.format(port))

ht.port = port
ht.prev = (19, 'student12.cse.nd.edu', 40000)
ht.next = (19, 'student12.cse.nd.edu', 40000)
ht.high = 9
ht.low = 0

while True:

    conn, addr = sock.accept()
    msg_len = int.from_bytes(conn.recv(4), byteorder='big')
    json_msg = conn.recv(msg_len).decode()
    ret = json.loads(json_msg)

    print('Received ', end='')
    print(ret)
    print('Handle an update here')

    msg = {'method': 'acknowledgement'}
    print('Sending back ', end='')
    print('CRASH')
    exit()
    '''
    print(msg)
    json_msg = json.dumps(msg)
    msg_len = len(json_msg).to_bytes(4, byteorder='big')
    conn.sendall(msg_len + json_msg.encode())

    conn.close()
    '''
