import requests
import math
import socket
import json
import select
import sys

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
        self.fingerTable = None # client's finger table
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
            
            #Details contains a socket that you need to send a message to --> Send connection message
            # details.sendall(b'Yo, let me join')
            pass
                        
        
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
                return newSock
                
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
        self.lowRange = location + (1 / (pow(2,32) - 1) )
        
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
                read_sockets, write_sockets, error_sockets = select.select(listen_list, [], [],0)
                for sock in read_sockets:
                    
                    if sock == self.sock: #MasterSocket ready for reading
                
                        conn, addr = self.sock.accept()
                        
                        listen_list.append(conn)
                        
                    elif sock == self.stdinDesc:
                        #This flag is set off when there is keyboard input and enter is pressed
                        
                        #TODO: Call function that handles user input
                        pass
                        
                    else:
                        
                        #Updates self.conn to be current connection
                        self.conn = sock
                        
                        # while True:
                        #Accept incoming connection and read JSON sent over --> typically where you accept

                        #TODO: Compact transaction log if necessary and update checkpoint

                        size = 1024 #Read in a KB at a time

                        # See if socket is still connected to client, otherwise break and start a new connection
                        try:
                            data = self.conn.recv(size,socket.MSG_PEEK)
                        except: #If error ask for new connection: Client quits 
                            #If client has left, remove from sockets dictionary
                            listen_list.remove(sock)
                            self.conn.close()
                            break

                        if not data: #Issue: getting stuck here if client ends naturally
                            listen_list.remove(sock) #If no data when checking the stream --> delete socket
                            self.conn.close()
                            continue

                        stream = ''

                        #Read Loop --> loop while buffer still has anything in it
                        while True:
                            try:
                                data = self.conn.recv(size)
                            except:
                                break

                            if not data:
                                break
                            stream += data.decode('utf-8')
                            if len(data) < size:
                                break

                        #Parse Data
                        if(stream):
                            #TODO: Define Way to parse stream
                            print(stream)
                                
            except TimeoutError: #This exception is taken on timeout
                #TODO: Define Exception for timeout
                        
                # self.sendToNameServer()
                pass
        

    
    def hashKey(self, key):
        #This hashing algorithm is djb2 source: http://www.cse.yorku.ca/~oz/hash.html
        
        # NOTE: Max Hash -->  2^{32} - 1 = 4,294,967,295
        
        try:
        
            hashedKey = 5381
            
            for x in key:
                hashedKey = (( hashedKey << 5) + hashedKey) + ord(x)
            
            return hashedKey & 0xFFFFFFFF

        except: #Catch non strings and record as errors
            return False



    # In a successful case, return the message received. Otherwise, need to decide what semantics we will have for failure messages
    def send_msg(self, msg, dest_args, ack=False):

        # msg MUST be a dictionary already ready for sending
        if not type(msg) is dict:
            return None

        # connect to destination
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # sock.connect(('student11.cse.nd.edu', 98907))
            sock.connect((dest_args[1], dest_args[2]))
        except:
            # add the ability to retry and decide when to remove from finger table
            return False

        # send message
        json_msg = json.dumps(msg)
        msg_length = len(json_msg).to_bytes(4, byteorder='big')
        try:
            sock.sendall(msg_length + json_msg.encode())
        except:
            # like above, add ability to retry and decide when to remove from FT
            return False

        # should receive a message back (unless an acknowledgement)
        if ack:
            return True
        try:
            msg_length = int.from_bytes(sock.recv(4), byteorder='big')
            json_msg = sock.recv(msg_length).decode() # include a way to test for timeout here
            ret = json.loads(json_msg)
            sock.close()
        except:
            # retry and decide when to remove from FT
            return False

        return ret



    def sendUpdateNext(self, next_args, dest_args):

        msg = {'method': 'updateNext', 'next': prev_args, 'from': (self.highRange, self.ipAddress, self.port)}
        ret_msg = send_msg(msg, dest_args)
        # Need to check contents of ret_msg to decide whether to return 'Success' or 'Failure'
        if ret_msg:
            return True
        return False

    def sendUpdatePrev(self, prev_args, dest_args):

        msg = {'method': 'updatePrev', 'prev': prev_args, 'from': (self.highRange, self.ipAddress, self.port)}
        ret_msg = send_msg(msg, dest_args)
        # Need to check contents of ret_msg to decide whether to return 'Success' or 'Failure'
        if ret_msg:
            return True
        return False
        
    def sendUpdateRange(self, high, low, dest_args):

        msg = {'method': 'updateRange', 'high': high, 'low': low, 'from': (self.highRange, self.ipAddress, self.port)}
        ret_msg = send_msg(msg, dest_args)
        # Need to check contents of ret_msg to decide whether to return 'Success' or 'Failure'
        if ret_msg:
            return True
        return False

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
        
    
