import requests
import math
import socket
import json
import select
import sys
import time
from FingerTable import FingerTable

class P2PHashTableClient:
    def __init__(self):
        self.ipAddress = None # IP of client --> where it can be reached
        self.port = None # What port the client can be reached at

        # TODO: where to put open socket connections
        self.sock = None
        self.conn = None
        self.stdinDesc = None
        
        self.prev = None # prev node in the ring
        self.next = None # next node in the ring
        self.highRange = None # highest radian number client is responsible for
        self.lowRange = None # lowest radian number client is responsible for
        self.fingerTable = FingerTable() # client's finger table
        self.projectName = None # project name to find in naming service

        # TODO: run enter ring here
    
    def enterRing(self, projectName):
        # To enter ring, need to check naming service to verify there is or isn't an existing client
        
        #Fetch IP and store in ipAddress variable
        ip = requests.get('http://icanhazip.com')
        self.ipAddress = ip.text[:-1] #Need to remove newline char
        
        #Store passed projectName in project name var
        self.projectName = projectName
        
        #Use project name to locate server and test communication
        
        details = self.locateServer()
        
        if not details:
            self.startP2P()
        else:
            #Details contains the information for sockets in the name server
            #TODO: Connect to Ring when there are other nodes in the ring --> contact first socket that connects requesting entry
            port = 0
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.bind((self.ipAddress,port))
            self.sock.listen()
            
            self.port = self.sock.getsockname()[1]
            
            #Details contains a socket that you need to send a message to --> Send connection message
            
            if self.sendJoinRequest(details):
                #If join request succeeds, send to name server and start reading messages
                self.sendToNameServer()
                self.readMessages()
                        
        
    def locateServer(self):
        
        # Get json data from naming service and try to find existing clients
        data = requests.get("http://catalog.cse.nd.edu:9097/query.json")
        data = data.json()

        data = list(filter( lambda x: "type" in x and "project" in x and x["type"] == "p2phashtable" and x["project"] == self.projectName, data))
        
        if not data:
            #No entries in the ring, so need to start 
            return False
        
        #TODO: Check to see if any servers are available
        for entry in data:
            #Contact each of these servers to see if they are available/in the ring
            newSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                newSock.connect((entry['address'], entry['port']))
                
                #TODO: If connect succeeds, talk to connecting and get inserted into the ring
                #      When connect succeeds, locate server will return socket
                newSock.close()
                
                #Gives dest_addr format --> don't have highRange yet
                return (None, entry['address'], entry['port'])
                
            except:
                #If error --> socket is no longer in ring, so continue to next iteration
                continue
        
        #None of the nameserver sockets are in the ring
        return False
        
        
    def startP2P(self):
        #This method creates the P2P system --> First Hash the IP addr
        
        hashedIP = self.hashKey(self.ipAddress)
        
        # Max Hash for djb2 is 2^32 - 1
        # Calculate spot on circle by dividing hashedIP by (2^32 - 1)
        ratio = hashedIP / (pow(2,32) - 1) 
        
        #Multiply ratio by 2pi radians to find its exact spot on the ring
        location = ratio * 2 * math.pi

        #Both ranges will be approx equal
        self.highRange = location
        
        #Differentiates the range by the smallest fraction
        self.lowRange = location + (1 / pow(2,52) )
        
        #Start listening socket
        port = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.ipAddress,port))
        self.sock.listen()
        
        self.port = self.sock.getsockname()[1]
        
        self.sendToNameServer()
        self.readMessages()
        
    def readMessages(self):
        
        self.stdinDesc = sys.stdin.fileno()
        
        print(f"Listening on Port {self.sock.getsockname()[1]}")
        self.sock.settimeout(60)
        listen_list = [self.sock, self.stdinDesc]
        write_list = []
        exception_list = []
        
        while True:
            try:
                read_sockets, write_sockets, error_sockets = select.select(listen_list, write_list, exception_list,0)
                for sock in read_sockets:
                    
                    if sock == self.sock: #MasterSocket ready for reading
                
                        conn, addr = self.sock.accept()
                        
                        listen_list.append(conn)
                        
                    elif sock == self.stdinDesc:
                        #This flag is set off when there is keyboard input and enter is pressed
                        print(input())
                        #TODO: Call function that handles user input
                        pass
                        
                    else:
                        
                        #Updates self.conn to be current connection
                        self.conn = sock
                        
                        # while True:
                        #Accept incoming connection and read JSON sent over --> typically where you accept

                        #TODO: Compact transaction log if necessary and update checkpoint

                        # See if socket is still connected to client, otherwise break and start a new connection
                        try:
                            # data = self.conn.recv(size,socket.MSG_PEEK)
                            msg_length = int.from_bytes(self.conn.recv(4), byteorder='big')
                            json_msg = self.conn.recv(msg_length).decode()
                            stream = json.loads(json_msg)
                        except: #If error ask for new connection: Client quits 
                            #If client has left, remove from sockets dictionary
                            listen_list.remove(sock)
                            self.conn.close()
                            break

                        if not stream: #Issue: getting stuck here if client ends naturally
                            listen_list.remove(sock) #If no data when checking the stream --> delete socket
                            self.conn.close()
                            continue

                        #Parse Data
                        if(stream):
                            #TODO: Define Way to parse stream
                            print(stream)
                            self.parseStream(stream, msg_length)
                                
            except TimeoutError: #This exception is taken on timeout
                #TODO: Define Exception for timeout
                        
                # self.sendToNameServer()
                pass
        
    def parseStream(self, stream, msg_length):
        #After receiving message need to check that 2 lengths match, then extract fields from stream
        if msg_length != len(str(stream)):
            #Encountered malformed stream
            return False
        
        #Two different types of methods--> ack and requests
        
        
        if stream['method'] == 'join':
            #Handle adding node to the ring
            msg = self.addToRing(stream['from'])
            #Need to send message back
            print(self.send_msg(msg, stream['from']))
        elif stream['method'] == 'updateNext':
            #Handle updating next node
            pass
        elif stream['method'] == 'updatePrev':
            #Handle updating prev node
            pass
        elif stream['method'] == 'updateRange':
            #Handle updatingRange
            pass
        elif stream['method'] == 'ack':
            #Handle acknowledgement
            pass
        
    def addToRing(self, details):
        #To add node to ring need to hashIP
        hashedIP = self.hashKey(details[1])
        
        # Max Hash for djb2 is 2^32 - 1
        # Calculate spot on circle by dividing hashedIP by (2^32 - 1)
        ratio = hashedIP / (pow(2,32) - 1)
        
        #Multiply ratio by 2pi radians to find its exact spot on the ring
        location = ratio * 2 * math.pi
        
        highRange = location
        
        #If fingertable is empty, then this is the second node joining so can just add to it
        if len(self.fingerTable.ft) == 0:
            self.fingerTable.addNode((highRange, details[1], details[2]))
            
            #After adding finger table, need to get low range for new node and update your own low range
            self.lowRange = highRange + (1 / pow(2,52) )
            
            lowRange = self.highRange + (1 / pow(2,52))
            
            #Set next and prev as this node
            prev = (self.highRange, self.ipAddress, self.port)
            next = (self.highRange, self.ipAddress, self.port)
            self.prev = (highRange, details[1], details[2])
            self.next = (highRange, details[1], details[2])
            
        #TODO: Adding into ring when there are more than 2 members
                
                

        #Once you have high Range --> get low range by consulting finger table
        # It will send the new node’s position in the ring, a copy of the process’ finger table, the new node’s previous process, and the new node’s next process
        
        # Need to send back to the node highRange, lowRange, next, prev
        return {'status': 'success', 'next': next, 'prev': prev, 'highRange': highRange, 'lowRange': lowRange}
    
    def hashKey(self, key):
        #This hashing algorithm is djb2 source: http://www.cse.yorku.ca/~oz/hash.html
        
        # NOTE: Max Hash -->  2^{32} - 1 = 4,294,967,295
        # print(key)
        
        #Need to modify hashing algorithm to more evenly distribute these nodes
        
        try:
        
            hashedKey = 5381
            
            for x in key:
                hashedKey = (( hashedKey << 5) + hashedKey) + ord(x)
            
            a = hashedKey & 0xFFFFFFFF
            
            #Self entered to try and diversify ip hashes
            a = a * (int(key[0]) + 1) * (int(key[-1]) * 9999 + 1 )
            a = a % (pow(2,32) - 1)
            return a

        except: #Catch non strings and record as errors
            return False



    # In a successful case, return the message received. Otherwise, need to decide what semantics we will have for failure messages
    # msg: json message to send
    # dest_args: a tuple representing where the message should go
    # ack: a bool specifying whether we are sending an acknowledgement or not--if sending acknowledgement, don't need to wait for response
    # adj: a bool specifying if the destination is adjancent to the sender in the ring. If it is, then a failed response means there is a crash.
    # RETURNS A JSON MESSAGE
    def send_msg(self, msg, dest_args, ack=False):

        # msg MUST be a dictionary already ready for sending
        if not type(msg) is dict:
            return {'status': 'failure', 'message': 'message sent to function was not a dictionary'}

        # connect to destination
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((dest_args[1], dest_args[2]))
        except:
            # try 5 times, then send failure
            success = False
            wait = 0.05
            while wait <= 0.8:
                time.sleep(wait)
                try:
                    sock.connect((dest_args[1], dest_args[2]))
                    success = True
                    break
                except:
                    pass
                wait *= 2
            # handle a failure to respond
            if success == False:
                self.fingerTable.delNode(dest_args[1])
                return {'status': 'failure', 'message': 'destination not responding'}

        # send message
        json_msg = json.dumps(msg)
        msg_length = len(json_msg).to_bytes(4, byteorder='big')
        try:
            sock.sendall(msg_length + json_msg.encode())
        except:
            # try 5 times, then send failure
            success = False
            wait = 0.05
            while wait <= 0.8:
                time.sleep(wait)
                try:
                    sock.sendall(msg_length + json_msg.encode())
                    success = True
                    break
                except:
                    pass
                wait *= 2
            # handle a failure to respond
            if success == False:
                self.fingerTable.delNode(dest_args[1])
                return {'status': 'failure', 'message': 'destination not responding'}

        # should receive a message back (unless an acknowledgement)
        msg_length = 0
        json_msg = None
        if ack:
            return {'status': 'success', 'message': 'acknowledgement sent'}
        try:
            msg_length = int.from_bytes(sock.recv(4), byteorder='big')
            json_msg = sock.recv(msg_length).decode() # include a way to test for timeout here
            ret = json.loads(json_msg)
        except:
            # try 5 times, then send failure
            success = False
            wait = 0.05
            while wait <= 0.8:
                time.sleep(wait)
                try:
                    msg_length = int.from_bytes(sock.recv(4), byteorder='big')
                    json_msg = sock.recv(msg_length).decode() # include a way to test for timeout here
                    ret = json.loads(json_msg)
                    success = True
                    break
                except:
                    pass
                wait *= 2
            # handle a failure to respond
            if success == False:
                self.fingerTable.delNode(dest_args[1])
                return {'status': 'failure', 'message': 'destination not responding'}

        # return
        sock.close()
        return {'status': 'success', 'message': ret}



    def sendUpdateNext(self, next_args, dest_args):

        msg = {'method': 'updateNext', 'next': next_args, 'from': (self.highRange, self.ipAddress, self.port)}
        ret_msg = self.send_msg(msg, dest_args)
        # Need to check contents of ret_msg to decide whether to return 'Success' or 'Failure'
        if ret_msg:
            return True
        return False

    def sendUpdatePrev(self, prev_args, dest_args):

        msg = {'method': 'updatePrev', 'prev': prev_args, 'from': (self.highRange, self.ipAddress, self.port)}
        ret_msg = self.send_msg(msg, dest_args)
        # Need to check contents of ret_msg to decide whether to return 'Success' or 'Failure'
        if ret_msg:
            return True
        return False
        
    def sendUpdateRange(self, high, low, dest_args):

        msg = {'method': 'updateRange', 'high': high, 'low': low, 'from': (self.highRange, self.ipAddress, self.port)}
        ret_msg = self.send_msg(msg, dest_args)
        # Need to check contents of ret_msg to decide whether to return 'Success' or 'Failure'
        if ret_msg:
            return True
        return False
    
    def sendJoinRequest(self, dest_args):
        
        msg = {'method': 'join', 'from': (self.highRange, self.ipAddress, self.port)}
        ret_msg = self.send_msg(msg, dest_args)
        
        return True

    def sendToNameServer(self):
        #Send an update to the name server describing server
            
        #Define message
        jsonMessage = dict()
        jsonMessage["type"] = "p2phashtable"
        jsonMessage["owner"] = "begloff"
        jsonMessage["port"] = self.port
        jsonMessage["project"] = self.projectName
        
        jsonMessage = str(json.dumps(jsonMessage))


        h = socket.gethostbyname("catalog.cse.nd.edu")
        
        nameServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        #FOR SOME REASON 9098 works but 9097 doesn't??????
        nameServer.connect((h, 9097 + 1))
        #Send Desc to name server
        nameServer.sendall(bytes(jsonMessage, encoding='utf-8'))



if __name__ == '__main__':
    client = P2PHashTableClient()
    client.enterRing('begloff-project')
        
    
