import requests
import math
import socket
import json
import select
import sys
import time
from FingerTable import FingerTable
from HashTable import HashTable

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
        self.ht = HashTable()

        # TODO: run enter ring here

    def __del__(self):
        # send update next to previous
        if self.next and self.next[1] != self.ipAddress:
            self.sendUpdateNext(self.next, self.prev)
            self.sendUpdatePrev(self.prev, self.next)
        # perform a bunch of send inserts
        for key in self.ht.hash:
            print('{}: {}'.format(key, self.ht.hash[key]))

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
        
        self.prev = [self.highRange, self.ipAddress, self.port]
        self.next = [self.highRange, self.ipAddress, self.port]
        
        self.sendToNameServer()
        self.readMessages()

    # check if next and previous are still the next and previous
    def sanityCheck(self):

        # send updatePrev to next
        prev_args = [self.highRange, self.ipAddress, self.port]
        dest_args = self.next
        ret = self.sendUpdatePrev(prev_args, dest_args)
        if ret == False:
            self.handleCrash(dest_args, 'next')

        # send updateNext to prev
        next_args = [self.highRange, self.ipAddress, self.port]
        dest_args = self.prev
        ret = self.sendUpdateNext(next_args, dest_args)
        if ret == False:
            self.handleCrash(dest_args, 'prev')



    # crash_args: the tuple of the destination that crashed
    # position: a string that is either 'prev' or 'next' and references whether the crashed node is the next or previous of ourselves.
    def handleCrash(self, crash_args, position):

        # TODO: handle the case where there were only two processes in the ring and one of them crashed
        
        # If the process that crashed is the next:
        if position == 'next':
            success = False
            while success == False:
                # Find crash's next node using finger table
                crashNextNode = self.findProcess(crash_args, 'next')
                # Send update prev to crash's next node. The prev will be ourselves.
                success = self.sendUpdatePrev([self.highRange, self.ipAddress, self.port], crashNextNode)
            # Update our next to be the crash's next node
            self.next = crashNextNode
            # Update our range of values to cover for the crashed node
            self.highRange = crash_args[0]

        # If the process that crashed is the prev:
        elif position == 'prev':
            success = False
            while success == False:
                # Find crash's prev node using finger table
                crashPrevNode = self.findProcess(crash_args, 'prev')
                # Send update next to the crash's prev node. The next will be ourselves
                success = self.sendUpdateNext([self.highRange, self.ipAddress, self.port], crashPrevNode)
            # Update our previous to be the crash's prev node
            self.prev = crashPrevNode
            # Send an update range to crash's prev
            success = self.sendUpdateRange(crash_args[0], -1, crashPrevNode)



    # This function tries to find the adjacent process to a destination. In this function, we are assuming we cannot talk to the destination
    # dest_args: tuple containing the silent destination
    # position: a string that is either 'prev' or 'next' and references which relative process we're trying to locate
    def findProcess(self, dest_args, position):

        # check if the dest args are your position
        if position == 'prev':
            if (dest_args[1] == self.next[1]) and (dest_args[2] == self.next[2]):
                return (self.highRange, self.ipAddress, self.port)

            # Contact finger table to find process to contact
            # Trying to find dest_args previous so need to undershoot
            ret = self.fingerTable.findProcess(dest_args[0], False)

            # TODO: function to tell a process to find another process

            # for now, assume we found process
            return process

        else:
            if (dest_args[1] == self.prev[1]) and (dest_args[2] == self.prev[2]):
                return (self.highRange, self.ipAddress, self.port)

            # Contact finger table to find process to contact
            # Trying to find dest_args next so need to overshoot
            ret = self.fingerTable.findProcess(dest_args[0], True)
            
            # TODO: function to tell a process to find another process

            # for now, assume we found process
            return process



    # use finger table or next and prev pointers to take a message to a process
    # msg: dictionary of the message to send
    # position: position on the ring where this message is trying to go
    def forwardMessage(self, msg, position):
        
        print("IN")
        print('FT: ', self.fingerTable.ft)

        while len(self.fingerTable.ft) > 0:

            # find where to send process on finger table
            proc = self.fingerTable.findProcess(position)
            print(proc)

            # check if this is closer than if you send to previous or next
            dis_ft = abs(proc[0] - position)
            dis_prev = abs(self.prev[0] - position)
            dis_next = abs(self.next[0] - position)

            # send with finger table
            if (dis_ft <= dis_prev) and (dis_ft <= dis_next):
                print('1')
                res = self.send_msg(msg, proc, True)
                if (res['status'] == 'failure') and (res['message'] == 'destination not responding'):
                    continue
                else:
                    return True

            # send to previous
            elif (dis_prev <= dis_ft) and (dis_prev <= dis_next):
                print('2')
                res = self.send_msg(msg, self.prev, True)
                while (res['status'] == 'failure') and (res['message'] == 'destination not responding'):
                    self.handleCrash(self.prev, 'prev')
                    res = self.send_msg(msg, self.prev, True)
                return True

            # send to next
            else:
                print('3')
                res = self.send_msg(msg, self.next, True)
                while (res['status'] == 'failure') and (res['message'] == 'destination not responding'):
                    self.handleCrash(self.next, 'next')
                    res = self.send_msg(msg, self.next, True)
                return True

        # nothing in finger table, so test next and prev
        else:
            dis_prev = abs(self.prev[0] - position)
            dis_next = abs(self.next[0] - position)

            # send to previous
            if dis_prev <= dis_next:
                res = self.send_msg(msg, self.prev, True)
                while (res['status'] == 'failure') and (res['message'] == 'destination not responding'):
                    self.handleCrash(self.prev, 'prev')
                    res = self.send_msg(msg, self.prev, True)
                return True

            # send to next
            else:
                res = self.send_msg(msg, self.next, True)
                while (res['status'] == 'failure') and (res['message'] == 'destination not responding'):
                    self.handleCrash(self.next, 'next')
                    res = self.send_msg(msg, self.next, True)
                return True




    def readMessages(self):
        
        self.stdinDesc = sys.stdin.fileno()
        
        print(f"Listening on Port {self.sock.getsockname()[1]}")
        self.sock.settimeout(60)
        listen_list = [self.sock, self.stdinDesc]
        write_list = []
        exception_list = []

        # variable to keep track of when to do sanity checks
        last_time = time.time()
        
        while True:

            # check if 60 seconds have passed and perform sanity check if necessary
            curr_time = time.time()
            if (curr_time - last_time) > 15:
                last_time = curr_time
                # self.sanityCheck()
                self.debug()

            try:
                read_sockets, write_sockets, error_sockets = select.select(listen_list, write_list, exception_list,0)
                for sock in read_sockets:
                    
                    if sock == self.sock: #MasterSocket ready for reading
                
                        conn, addr = self.sock.accept()
                        
                        listen_list.append(conn)
                        
                    elif sock == self.stdinDesc:
                        #This flag is set off when there is keyboard input and enter is pressed
                        i = input()
                        
                        if i == '1':
                            self.sendUpdateNext((1, self.ipAddress, self.port), self.next)
                        if i == 'insert':
                            msg = {'method': 'insert', 'key': 'what', 'value': 'bruh', 'from': (self.highRange, self.ipAddress, self.port)}
                            self.send_msg(msg, self.next)
                            
                        if i == 'lookup':
                            msg = {'method': 'lookup', 'key': 'what', 'from': (self.highRange, self.ipAddress, self.port)}
                            self.send_msg(msg, self.next)

                        if i == 'remove':
                            msg = {'method': 'remove', 'key': 'what', 'from': (self.highRange, self.ipAddress, self.port)}
                            self.send_msg(msg, self.next)

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
                            print('Message Recieved: ', stream)
                            self.conn.close()
                            self.parseStream(stream, msg_length)
                            listen_list.remove(sock)
                                
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
        
        if 'method' in stream:
            if stream['method'] == 'joinReq':
                #Handle adding node to the ring
                msg = self.addToRing(stream['from'], stream)
                #Need to send message back
                if msg:
                    self.send_msg(msg, stream['from'], True)
            elif stream['method'] == 'join':
                self.next = stream['next']
                self.prev = stream['prev']
                self.highRange = stream['highRange']
                self.lowRange = stream['lowRange']
                # JSON dumps converts [()] to [[]] --> need to convert
                self.fingerTable.ft = stream['ft']
                
                self.fingerTable.addNode(stream['from'])
                
                msg = {'method': 'ack', 'message': 'Successfully joined ring'}
                self.send_msg(msg, stream['from'], True)
                
            elif stream['method'] == 'updateNext':
                #Handle updating next node --> need to send ack
                self.next = stream['next']
                print('Updated Next', self.prev)
                msg = {'method': 'ack', 'message': 'Successfully updated next pointer'}
                self.send_msg(msg, stream['from'], True)
                
            elif stream['method'] == 'updatePrev':
                #Handle updating prev node --> need to send ack
                self.prev = stream['prev']
                msg = {'method': 'ack', 'message': 'Successfully updated prev pointer'}
                self.send_msg(msg, stream['from'], True)
                
            elif stream['method'] == 'updateRange':
                #Handle updatingRange --> need to send ack
                if stream['low'] < 0 and stream['high'] > 0:
                    #Don't update low, but update high
                    self.highRange = stream['high']
                    
                elif stream['low'] > 0 and stream['high'] < 0:
                    #Dont update high, but update low
                    self.lowRange = stream['low']
                    
                elif stream['low'] > 0 and stream['high'] > 0:
                    self.lowRange = stream['low']
                    self.highRange = stream['high']
                    
                msg = {'method': 'ack', 'message': 'Successfully updated range'}
                self.send_msg(msg, stream['from'], True)
                
            elif stream['method'] == 'getLow':
                #Just need to return prev info
                msg = {'method': 'ack', 'prev': self.prev, 'message': 'successfully retrieved prev info'}
                self.send_msg(msg, stream['from'], True)

            elif stream['method'] == 'insert':
                ret = self.updateHashTable('insert', stream['key'], stream['value'])
                msg = {'method': 'ack', 'message': 'Successfully inserted value'}
                self.send_msg(msg, stream['from'], True)
            elif stream['method'] == 'remove':
                ret = self.updateHashTable('remove', stream['key'])
                msg = {'method': 'ack', 'message': 'Successfully removed value'}
                self.send_msg(msg, stream['from'], True)
            elif stream['method'] == 'lookup':
                ret = self.updateHashTable('lookup', stream['key'])
                msg = {'method': 'ack', 'message': ret}
                self.send_msg(msg, stream['from'], True)
        
    def addToRing(self, details, msg):
        #To add node to ring need to hashIP
        hashedIP = self.hashKey(details[1])
        
        # Max Hash for djb2 is 2^32 - 1
        # Calculate spot on circle by dividing hashedIP by (2^32 - 1)
        ratio = hashedIP / (pow(2,32) - 1)
        
        #Multiply ratio by 2pi radians to find its exact spot on the ring
        location = ratio * 2 * math.pi
        
        highRange = location
        
        #If fingertable is empty, then this is the second node joining so can just add to it
        if self.consultFingerTable(highRange,msg):
            self.fingerTable.addNode((highRange, details[1], details[2]))
            
            #After adding finger table, need to get low range by communicating with assigned nextNode which will be who sends you the message
            
            if self.prev != [self.highRange, self.ipAddress, self.port]:
                self.lowRange = highRange + (1 / pow(2,52) )
                
                next = [self.highRange, self.ipAddress, self.port]
                
                prev = self.prev
                
                lowRange = self.prev[0]
                
                self.prev = [highRange, details[1], details[2]]
                #More than 2 members
                #Need to update prev next
                self.sendUpdateNext([highRange, details[1], details[2]], self.prev)
            else: #2 members
                self.lowRange = highRange + (1 / pow(2,52) )
                
                lowRange = self.highRange + (1 / pow(2,52))
                
                #Set next and prev as this node
                prev = self.prev
                next = [self.highRange, self.ipAddress, self.port]
                self.prev = [highRange, details[1], details[2]]
            
            
                self.next = [highRange, details[1], details[2]]
                
            
            return {'method': 'join', 'next': next, 'prev': prev, 'highRange': highRange, 'lowRange': lowRange, 'ft': self.fingerTable.ft, 'from': (self.highRange, self.ipAddress, self.port)}
        
        else:
            return False
            
        #TODO: Adding into ring when there are more than 2 members
        #consultFingerTable will send this message to the appropriate source
                
        #Once you have high Range --> get low range by consulting finger table
        # It will send the new node’s position in the ring, a copy of the process’ finger table, the new node’s previous process, and the new node’s next process
        
        # Need to send back to the node highRange, lowRange, next, prev
    

    
    def consultFingerTable(self, position, msg):
        #In this function, consult your own finger table and see if you are responsible for message: other wise forward to other node
        
        print('CHECKING',position, self.highRange, self.lowRange, self.lowRange <= position <= self.highRange)
        
        #FIRST SEE IF YOU ARE RESPONSIBLE
        if self.highRange < self.lowRange:
            print('CHECKING',position, self.highRange <= position <= 0, 0 <= position <= self.lowRange)
            #Need to check between high & 0 and 0 & low
            if 0 <= position <= self.highRange:
                return True
            elif self.lowRange <= position <= 2 * math.pi:
                return True
            else:
                if self.forwardMessage(msg,position):
                    return False
            
        elif self.lowRange <= position <= self.highRange:
                return True
            
        else:
            #YOU ARE NOT RESPONSIBLE FOR THIS INSERT/JOIN --> Call forwardMessage
            if self.forwardMessage(msg,position):
                return False
    
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
            wait = 0.01
            while wait <= 0.1:
                time.sleep(wait)
                try:
                    sock.connect((dest_args[1], dest_args[2]))
                    success = True
                    break
                except:
                    pass
                wait += 0.01
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
            wait = 0.01
            while wait <= 0.1:
                time.sleep(wait)
                try:
                    sock.sendall(msg_length + json_msg.encode())
                    success = True
                    break
                except:
                    pass
                wait += 0.01
            # handle a failure to respond
            if success == False:
                self.fingerTable.delNode(dest_args[1])
                return {'status': 'failure', 'message': 'destination not responding'}

        # should receive a message back (unless an acknowledgement)
        '''
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
            wait = 0.01
            while wait <= 0.1:
                time.sleep(wait)
                try:
                    msg_length = int.from_bytes(sock.recv(4), byteorder='big')
                    json_msg = sock.recv(msg_length).decode() # include a way to test for timeout here
                    ret = json.loads(json_msg)
                    success = True
                    break
                except:
                    pass
                wait += 0.01
            # handle a failure to respond
            if success == False:
                self.fingerTable.delNode(dest_args[1])
                return {'status': 'failure', 'message': 'destination not responding'}

        '''

        # return
        sock.close()
        return {'status': 'success'}



    # next_args: a tuple containing the arguments that the destination should set as its next
    # dest_args: a tuple containing the arguments of where the message should be sent
    # Returns a boolean of whether the update succeeded or not
    def sendUpdateNext(self, next_args, dest_args):
        msg = {'method': 'updateNext', 'next': next_args, 'from': [self.highRange, self.ipAddress, self.port]}
        ret_msg = self.send_msg(msg, dest_args)
        if ret_msg['status'] == 'failure':
            return False
        return True

    # prev_args: a tuple containing the arguments that the destination should set as its prev
    # dest_args: a tuple containing the arguments of where the message should be sent
    # Returns a boolean of whether the update succeeded or not
    def sendUpdatePrev(self, prev_args, dest_args):

        msg = {'method': 'updatePrev', 'prev': prev_args, 'from': [self.highRange, self.ipAddress, self.port]}
        ret_msg = self.send_msg(msg, dest_args)
        if ret_msg['status'] == 'failure':
            return False
        return True
        
    # high: a number containing the value of the 'high' range
    # low: a number containing the value of the 'low' range
    # dest_args: a tuple containing the arguments of where the message should be sent
    # Returns a boolean of whether the update succeeded or not
    def sendUpdateRange(self, high, low, dest_args):

        msg = {'method': 'updateRange', 'high': high, 'low': low, 'from': [self.highRange, self.ipAddress, self.port]}
        ret_msg = self.send_msg(msg, dest_args)
        if ret_msg['status'] == 'failure':
            return False
        return True

    def sendJoinRequest(self, dest_args):
        
        msg = {'method': 'joinReq', 'from': [self.highRange, self.ipAddress, self.port]}
        ret_msg = self.send_msg(msg, dest_args, True)
        return ret_msg

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

    def debug(self):
        print(f'DEBUG: prev: {self.prev}, next: {self.next}, FT: {self.fingerTable.ft}, highRange: {self.highRange}, lowRange: {self.lowRange}')


    def updateHashTable(self, method, key, value=None):

        if method == 'insert':
            self.ht.insert(key, value)
            return True
        elif method == 'lookup':
            return self.ht.lookup(key)
        elif method == 'remove':
            self.ht.remove(key)
            return True
        else:
            return False



if __name__ == '__main__':
    client = P2PHashTableClient()
    client.enterRing('begloff-project')
        
    
