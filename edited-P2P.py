import requests
import math
import socket
import json
import select
import sys
import time
from FingerTable import FingerTable
from HashTable import HashTable
from faker import Faker
import random
import os

# Units of difference between spots on the ring
UNIT = (2 * math.pi) / pow(2, 32)

class P2PHashTableClient:

    def __init__(self, projectName='begloff-project', clean_exit=True):

        # IP address and port of the client
        self.ipAddress = None
        self.port = None

        # Flag specifying if a node is in ring or not
        self.inRing = False

        # Open socket connections
        self.sock = None
        self.conn = None
        self.stdinDesc = None
        
        # Project name to find in naming service
        self.projectName = projectName
        
        # Node's internal hashtable
        self.ht = HashTable()

        # Internal ring structure
        self.prev = None # Previous node in the ring
        self.next = None # Next node in the ring
        self.highRange = None # Highest radian number client is responsible for
        self.lowRange = None # Lowest radian number client is responsible for
        self.fingerTable = FingerTable() # Client's finger table
        
        # For debugging: whether the node should exit cleanly or "crash"
        self.clean_exit = clean_exit

        # Internal Hashtable to allow for rebalancing
        self.TEMP = dict()

        # Internal variables for testing
        self.testInput = []
        self.counter = 0
        self.testFile = None
        self.finalResult = None
        self.runTests = False
        self.messageCount = 0
        self.exit = False

        # Begin the ring
        self.enterRing()



    # What the client runs when it wants to exit
    def __del__(self):

        print('\nExiting program...')
        if self.clean_exit == False:
            print('Program crashed.')
            return

        # Alert the next node
        if self.next and self.next[1] != self.ipAddress:
            self.sendUpdatePrev(self.prev, self.next)
            self.sendUpdateRange(-1, self.lowRange, self.next)

        # Alert the previous node
        if self.prev and self.prev[1] != self.ipAddress:
            self.sendUpdateNext(self.next, self.prev)

        # Reset ranges
        self.highRange = 1001
        self.lowRange = 1000

        # Send out data to the ring
        if self.next and self.prev and (self.next[1] != self.ipAddress or self.next[2] != self.port) and (self.prev[1] != self.ipAddress or self.prev[2] != self.port):
            for key in self.ht.hash:
                value = self.ht.hash[key]
                userStream = 'insert {} {}'.format(key, value)
                self.performInsert(userStream=userStream)
        print('Program finished exiting.')
    
  

    # Updating a node's current hashtable
    def updateHashTable(self, method, key, value=None):
        if method == 'insert':
            self.ht.insert(key, value)
            return True
        elif method == 'remove':
            self.ht.remove(key)
            return True
        elif method == 'lookup':
            return self.ht.lookup(key)
        else:
            return False



    # Given a key or IP, provide a hash between 0 and 2pi
    # This hashing algorithm is djb2 source: http://www.cse.yorku.ca/~oz/hash.html
    # Max Hash -->  2^{32} - 1 = 4,294,967,295
    def hashKey(self, key, ip=False, port=None):
        try:
            hashedKey = 5381
            for x in key:
                hashedKey = (( hashedKey << 5) + hashedKey) + ord(x)
            a = hashedKey & 0xFFFFFFFF
            key0 = None
            key1 = None
            try:
                key0 = int(key[0])
            except:
                key0 = ord(key[0])
            try:
                keyn1 = int(key[-1])
            except:
                keyn1 = ord(key[-1])
            a = a * (key0 + 1) * (keyn1 * 9999 + 1 )
            if ip:
                a = a * port * 2499
            a = a % (pow(2,32) - 1)
            a = a / (pow(2, 32) - 1)
            a = a * 2 * math.pi
            return a
        # Catch non-strings as errors
        except:
            return False



    # Printing to the user how to use the system
    def usage(self):
        print('\nP2PHashTable Usage:')
        print('  Insert [key] [value]')
        print('  Lookup [key]')
        print('  Remove [key]')
        print('  Exit/Ctrl-c to quit\n')



    # Print status of a node to the terminal for debugging purposes
    def debug(self):
        print(f'DEBUG: prev: {self.prev}, next: {self.next}, FT: {self.fingerTable.ft}, highRange: {self.highRange}, lowRange: {self.lowRange}, ip address: {self.ipAddress}')
        print('my hashtable is:')
        for key in self.ht.hash:
            print('{}: {}'.format(key, self.ht.hash[key]))



    # Finding a valid node on the ND naming service
    def locateServer(self):
        
        # Contact and parse naming service
        data = requests.get("http://catalog.cse.nd.edu:9097/query.json")
        data = data.json()
        data = list(filter( lambda x: "type" in x and "project" in x and x["type"] == "p2phashtable" and x["project"] == self.projectName, data))
        
        # No valid entries in name server
        if not data:
            return False
        
        # Loop through nodes and attempt to connect
        for entry in data:
            newSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                # If connect succeeds, record contact information and return
                newSock.connect((entry['address'], entry['port']))
                newSock.close()
                return [None, entry['address'], entry['port']]
            # If there is an error, then that node is no longer in the ring
            except:
                continue
        
        # All nodes in the nameserver are invalid
        return False



    # Send project to the name server
    def sendToNameServer(self):
        jsonMessage = dict()
        jsonMessage["type"] = "p2phashtable"
        jsonMessage["owner"] = "begloff"
        jsonMessage["port"] = self.port
        jsonMessage["project"] = self.projectName
        jsonMessage = str(json.dumps(jsonMessage))
        h = socket.gethostbyname("catalog.cse.nd.edu")
        nameServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        nameServer.connect((h, 9097 + 1))
        nameServer.sendall(bytes(jsonMessage, encoding='utf-8'))



    # Initializes a P2P System
    def startP2P(self):
        
        # Start listening socket and get port
        port = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.ipAddress,port))
        self.sock.listen()
        self.port = self.sock.getsockname()[1]
        
        # Hash IP Address + port to get position on ring
        hashedIP = self.hashKey(self.ipAddress, True, self.port)
        self.highRange = hashedIP
        self.lowRange = hashedIP + UNIT
        
        # Set previous and next be ourselves
        self.prev = [self.highRange, self.ipAddress, self.port]
        self.next = [self.highRange, self.ipAddress, self.port]

        # We are now in the ring
        self.inRing = True
        
        # Send to name server and begin listening for messages
        self.sendToNameServer()
        self.readMessages()



    # When a node has requested to join the ring
    def enterRing(self, projectName='begloff-project'):
        
        # Fetch IP and store in ipAddress variable
        ip = requests.get('http://icanhazip.com')
        self.ipAddress = ip.text[:-1]
        
        # Store projectName
        self.projectName = projectName
        
        # Find if ring is established or not
        details = self.locateServer()
        
        # Ring is not established--start your own
        if not details:
            self.startP2P()
        # Ring already established and need to join
        else:
            port = 0
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.bind((self.ipAddress,port))
            self.sock.listen()
            self.port = self.sock.getsockname()[1]
            
            # Send a join request to node found on naming server
            result = self.sendJoinRequest(details)

            # If join request succeeds, send to name server and start reading messages
            if result:
                self.sendToNameServer()
                self.readMessages()



    # Send a request to join the ring
    def sendJoinRequest(self, dest_args):
        msg = {'method': 'joinReq', 'from': [self.highRange, self.ipAddress, self.port]}
        ret_msg = self.send_msg(msg, dest_args)
        return ret_msg

        

    # Function to add a node to the ring
    def addToRing(self, details, msg):

        # Hash the given IP to find the position
        hashedIP = self.hashKey(details[1], True, details[2])
        highRange = hashedIP
        
        # Check if this node is my responsibility
        if self.consultFingerTable(highRange,msg):

            # Add the node to our fing
            self.fingerTable.addNode([highRange, details[1], details[2]])
            
            # For the case where there are two or more nodes in the ring
            if self.prev != [self.highRange, self.ipAddress, self.port]:

                # Set incoming node's parameters
                next = [self.highRange, self.ipAddress, self.port]
                prev = self.prev
                lowRange = self.prev[0] + UNIT

                # Update my internal variables
                self.lowRange = highRange + UNIT
                self.prev = [highRange, details[1], details[2]]
                self.sendUpdateNext([highRange, details[1], details[2]], prev)

            # There is currently only one member in the ring
            else:
                
                # Set incoming node parameters
                lowRange = self.highRange + UNIT
                prev = self.prev
                next = [self.highRange, self.ipAddress, self.port]

                # Update my parameters
                self.lowRange = highRange + UNIT
                self.fingerTable.addNode(self.prev)
                self.next = [highRange, details[1], details[2]]
                self.fingerTable.addNode(self.next)
                
            # Return message with information to join
            return {'method': 'join', 'next': next, 'prev': prev, 'highRange': highRange, 'lowRange': lowRange, 'ft': self.fingerTable.ft, 'from': [self.highRange, self.ipAddress, self.port]}
        
        # Not my responsibility, so return false
        else:
            return False
            
   

    # Given a message and a destination, send a message
    def send_msg(self, msg, dest_args):

        # Check if message is not a dictionary
        if not type(msg) is dict:
            return {'status': 'failure', 'message': 'message sent to function was not a dictionary'}

        # Check if dest args is not specified
        if not dest_args:
            return {'status': 'failure', 'message': 'dest_args not specified'}

        # Attempt to connect to destination
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((dest_args[1], dest_args[2]))
        except:
            # If unsuccessful connection, try again while waiting
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
            # If never got to connection, return failure
            if success == False:
                self.fingerTable.delNode(dest_args[1])
                return {'status': 'failure', 'message': 'destination not responding'}

        # Attempt to send message
        json_msg = json.dumps(msg)
        msg_length = len(json_msg).to_bytes(4, byteorder='big')
        try:
            sock.sendall(msg_length + json_msg.encode())
        except:
            # If unsuccessful send, try again while waiting
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

        # Close and return success
        sock.close()
        return {'status': 'success'}



    # Function to continuously listen for messages from other processes
    def readMessages(self):
        
        # Set variable for user input
        self.stdinDesc = sys.stdin.fileno()
        
        # Initialize listening variables
        print(f"Listening on Port {self.sock.getsockname()[1]}")
        self.usage()
        self.sock.settimeout(60)
        listen_list = [self.sock, self.stdinDesc]

        # Initialize variables for testing
        write_list = []
        exception_list = []
        x = 0
        output = None
        startTest = time.time()
        if self.runTests:
            startTest = time.time_ns()
        intervalTimer = time.time()
        if self.runTests:
            intervalTimer = time.time_ns()

        # Keep track of timing for sanity checks and updating name server
        sanity_last_time = time.time()
        ns_last_time = time.time()

        # Loop indefinitely while listening
        while True:

            # Perform sanity check, if necessary
            sanity_curr_time = time.time()
            if ((sanity_curr_time - sanity_last_time) > 3) and self.inRing:
                sanity_last_time = sanity_curr_time
                self.sanityCheck()
            
            # Send to name server, if necessary
            ns_curr_time = time.time()
            if (ns_curr_time - ns_last_time) > 60:
                ns_last_time = ns_curr_time
                self.sendToNameServer()

            # Attempt to read from open sockets
            try:

                read_sockets, write_sockets, error_sockets = select.select(listen_list, write_list, exception_list,0)
                
                # Functionality for running tests
                if self.runTests:
                    if not read_sockets and x == 0 and (time.time_ns() - startTest) / 1000000000 > 60:
                        x = 1
                        self.testSystem()
                    elif not read_sockets and x == 1 and self.counter < 500:
                        if self.counter == 0:
                            start = time.time_ns()
                        self.performInsert(userStream=f'insert {self.testInput[self.counter][0]} {self.testInput[self.counter][1]}')
                        self.counter += 1
                        if self.counter == 500:
                            print(f'500 Copies Successfully Inserted\n')
                    elif self.counter == 500 and (time.time_ns() - intervalTimer) / 1000000000 > 0.5:
                        #Writing is complete
                        intervalTimer = time.time_ns()
                        #From here lookup the last result and see how long it takes to get self.lastResult[1]
                        ret = self.performLookup(userStream=f'lookup {self.finalResult[0]}')
                        if 'value' in ret and ret['value'] == self.finalResult[1]:
                            self.exit = True
                    elif self.exit and output == None:
                        #Kill all processes 60 seconds after inserts are complete to avoid rebalancing messing with measurements
                        #Write to file regarding how long this took
                        end = time.time_ns()
                        output = f'Inserting 500 elements completed & verified - Time Elapsed: {(end - start) / 1000000000} seconds - Message Count: {self.messageCount}\n'
                        print(output)
                        self.testFile.write(output)
                        self.testFile.flush()
                        os.fsync(self.testFile.fileno())
                    if self.exit and (time.time_ns() - start) / 1000000000 > 30:
                        sys.exit(0)

                # Loop through sockets that are ready for reading
                for sock in read_sockets:
                    
                    # Master socket --> add open socket to listening list
                    if sock == self.sock:
                        conn, addr = self.sock.accept()
                        listen_list.append(conn)
                        
                    # Open socket is from user input --> run specified function based on input
                    elif sock == self.stdinDesc:
                        i = input()
                        
                        if i.rstrip().split()[0].lower() == 'insert':
                            self.performInsert(userStream=i)

                        elif i.rstrip().split()[0].lower() == 'lookup':
                            ret = self.performLookup(userStream=i)
                            if ret['status'] == 'success' and ret['value'] is not None:
                                print('{}: {}'.format(i.rstrip().split()[1], ret['value']))
                            if ret['status'] == 'success' and ret['value'] is None:
                                print('Key {} does not exist in table.'.format(i.rstrip().split()[1]))

                        elif i.rstrip().split()[0].lower() == 'remove':
                            self.performRemove(userStream=i)

                        elif i.lower() == 'debug':
                            self.debug()
                            
                        elif i.lower() == 'usage':
                            self.usage()
                            
                        elif i.lower() == 'exit':
                            sys.exit(0)
                            
                        elif i.lower() == 'test':
                            self.test = True

                        elif i == 'sanity check':
                            self.sanityCheck()
                            
                    # Reading message from another process
                    else:
                        
                        # Updates self.conn to be current connection
                        self.conn = sock
                        
                        # Attempt to read json message from connection
                        try:
                            msg_length = int.from_bytes(self.conn.recv(4), byteorder='big')
                            json_msg = self.conn.recv(msg_length).decode()
                            stream = json.loads(json_msg)
                        # Client left, remove from listen list
                        except: 
                            listen_list.remove(sock)
                            self.conn.close()
                            break

                        # If nothing to read, then client left naturally
                        if not stream:
                            listen_list.remove(sock)
                            self.conn.close()
                            continue

                        # Parse stream passed through socket
                        if (stream):
                            self.messageCount += 1
                            self.conn.close()
                            self.parseStream(stream, msg_length)
                            self.messageCount += 1
                            listen_list.remove(sock)
                                
            # If a timeout error, just ignore
            except TimeoutError:
                pass


        
    # Given a stream from a process, parse through it and act on it
    def parseStream(self, stream, msg_length):

        # Check if we have encountered a malformed stream
        if msg_length != len(str(stream)):
            return False

        # If method not in stream, just return
        if 'method' not in stream:
            return False
        
        # Command to store hashtable in TEMP and perform removes
        if stream['method'] == 'saveAndRemove':
            self.TEMP = dict()
            for key in self.ht.hash:
                self.TEMP[key] = self.ht.hash[key]
            for key in self.TEMP:
                self.performRemove(userStream='remove {}'.format(key))
            msg = {'method': 'saveAndRemoveAck', 'from': [self.highRange, self.ipAddress, self.port], 'toForward': stream['toForward']}
            self.send_msg(msg, stream['from'])

        # Acknowledgement from a save and remove, which means a waiting node is now authorized to enter the ring
        elif stream['method'] == 'saveAndRemoveAck':
            author = stream['toForward']['from']
            msg = self.addToRing(author, stream['toForward'])
            if msg:
                self.send_msg(msg, author)

        # Command to rebalance the ring after a crash
        elif stream['method'] == 'crashRebalance':
            for key in self.ht.hash:
                self.performInsert(userStream='insert {} {}'.format(key, self.ht.hash[key]))
            msg = {'method': 'ack', 'message': 'Successfully rebalanced', 'from': [self.highRange, self.ipAddress, self.port]}
            self.send_msg(msg, stream['from'])

        # Request from node to join the ring
        elif stream['method'] == 'joinReq':

            # If only one node in ring, can handle a join with no rebalancing
            if not self.next or not self.prev or (self.prev[1] == self.ipAddress and self.prev[2] == self.port) or (self.next[1] == self.ipAddress and self.next[2] == self.port):
                msg = self.addToRing(stream['from'], stream)
                if msg:
                    self.send_msg(msg, stream['from'])

            # Multiple nodes in ring, so must perform some rebalancing or forward message
            else:
                hashedIP = self.hashKey(stream['from'][1], True, stream['from'][2])
                # Message is meant for me, so perform rebalancing
                if self.consultFingerTable(hashedIP, stream):
                    self.TEMP = dict()
                    for key in self.ht.hash:
                        self.TEMP[key] = self.ht.hash[key]
                    for key in self.TEMP:
                        self.performRemove(userStream='remove {}'.format(key))
                    # Tell previous node to also rebalance
                    msg = {'method': 'saveAndRemove', 'from': [self.highRange, self.ipAddress, self.port], 'toForward': stream}
                    self.send_msg(msg, self.prev)
                # Message is not meant for me, which means it has been forwarded
                else:
                    pass

        # Message from node with instructions of how to join ring
        elif stream['method'] == 'join':

            # Set internal information from stream
            self.next = stream['next']
            self.fingerTable.addNode(stream['next'])
            self.prev = stream['prev']
            self.fingerTable.addNode(stream['prev'])
            self.highRange = stream['highRange']
            self.lowRange = stream['lowRange']
            self.fingerTable.ft = stream['ft']
            self.fingerTable.addNode(stream['from'])
            self.inRing = True

            # Send a rebalance message to previous and next
            msg = {'method': 'insertFromTemp', 'from': [self.highRange, self.ipAddress, self.port]}
            self.send_msg(msg, self.prev)
            self.send_msg(msg, self.next)

        # Received a command to rebalance
        elif stream['method'] == 'insertFromTemp':
            for key in self.ht.hash:
                self.performInsert(userStream='insert {} {}'.format(key, self.ht.hash[key]))
            for key in self.TEMP:
                self.performInsert(userStream='insert {} {}'.format(key, self.TEMP[key]))
            self.TEMP = dict()

        # Command to find a an adjacent process to a process that crashed
        elif stream['method'] == 'findProcess':
            self.performFindProcess(stream)
            
        # Command to update next pointer
        elif stream['method'] == 'updateNext':
            self.next = stream['next']
            self.fingerTable.addNode(stream['next'])
            msg = {'method': 'ack', 'message': 'Successfully updated next pointer'}
            self.send_msg(msg, stream['from'])
            
        # Command to update previous pointer
        elif stream['method'] == 'updatePrev':
            self.prev = stream['prev']
            self.fingerTable.addNode(stream['prev'])
            msg = {'method': 'ack', 'message': 'Successfully updated prev pointer'}
            self.send_msg(msg, stream['from'])
                
        # Command to update range of control
        elif stream['method'] == 'updateRange':
            if stream['low'] >= 0:
                self.lowRange = stream['low']
            if stream['high'] >= 0:
                self.highRange = stream['high']
            msg = {'method': 'ack', 'message': 'Successfully updated range'}
            self.send_msg(msg, stream['from'])
            
        # Send information of previous pointer
        elif stream['method'] == 'getLow':
            msg = {'method': 'ack', 'prev': self.prev, 'message': 'successfully retrieved prev info'}
            self.send_msg(msg, stream['from'])

        # Command to insert a value
        elif stream['method'] == 'insert':
            self.performInsert(processStream=stream)
            
        # Command to insert a copy of a value
        elif stream['method'] == 'insertCopy':
            ret = self.updateHashTable('insert', stream['key'], stream['value'])
            if ret:
                msg = {'method': 'ack', 'message': 'Successful insert of copy'}
            else:
                msg = {'method': 'ack', 'message': 'Error on insertion of copy'}
            self.send_msg(msg, stream['from'])

        # Command to lookup a value
        elif stream['method'] == 'lookup':
            self.performLookup(processStream=stream)

        # Command to remove a value
        elif stream['method'] == 'remove':
            self.performRemove(processStream=stream)

        # Command to remove a copy of a value
        elif stream['method'] == 'removeCopy':
            ret = self.updateHashTable('remove', stream['key'])
            if ret:
                msg = {'method': 'ack', 'message': 'Successful removal of copy'}
            else:
                msg = {'method': 'ack', 'message': 'Error on removal of copy'}    
            self.send_msg(msg, stream['from'])

        # Got an acknowledgement
        elif stream['method'] == 'ack':
            # If returning from a lookup, need to pring result
            if stream['message'] == 'Result of lookup' and stream['value'] is not None:
                print('{}: {}'.format(stream['key'], stream['value']))
                if self.runTests and stream['value'] == self.finalResult[1]:
                    self.exit = True
            elif stream['message'] == 'Result of lookup' and stream['value'] is None and 'next' in stream:
                msg = {'method': 'lookup', 'key': stream['key'], 'triedNext': True, 'from': [self.highRange, self.ipAddress, self.port]}
                self.send_msg(msg,stream['next'])
            elif stream['message'] == 'Result of lookup' and stream['value'] is None:
                print('Key {} does not exist in table.'.format(stream['key']))

        # Received acknowledge from crash's adjacent node, need to perform more actions
        elif stream['method'] == 'crashAcknowledge':
            if stream['todo'] == 'updatePrevAndRange':
                self.prev = stream['from']
                self.lowRange = stream['from'][0] + UNIT
            else:
                self.next = stream['from']
            for key in self.ht.hash:
                userStream = 'insert {} {}'.format(key, self.ht.hash[key])
                self.performInsert(userStream=userStream)
            # Tell sender to also perform a rebalance
            msg = {'method': 'crashRebalance', 'from': [self.highRange, self.ipAddress, self.port]}
            self.send_msg(msg, stream['from'])



    # A function to determine whether a given message is your responsibility
    def consultFingerTable(self, position, msg, overshoot=True):
        
        # Just in case: add previous and next to finger table and delete yourself
        self.fingerTable.addNode(self.next)
        self.fingerTable.addNode(self.prev)
        self.fingerTable.delNode(self.ipAddress)

        # If finger table is empty, then you are responsible
        if len(self.fingerTable.ft) <= 0:
            return True

        elif self.highRange < self.lowRange:
            if 0 <= position <= self.highRange:
                return True
            elif self.lowRange <= position <= 2 * math.pi:
                return True
            # Not responsible for this, so forward message
            else:
                return not self.forwardMessage(msg, position, overshoot)
            
        elif self.lowRange <= position <= self.highRange:
            return True
            
        elif self.lowRange == self.highRange:
            return True
            
        # Not responsible for this, so forward message
        else:
            return not self.forwardMessage(msg, position, overshoot)
   


    # Forward a message to another node
    def forwardMessage(self, msg, position, overshoot=True):
        
        # If finger table is empty, then there is an error
        if len(self.fingerTable.ft) <= 0:
            print('An error occurred.')
            sys.exit(0)

        # Try to send message while there are processes in the ring
        while len(self.fingerTable.ft) > 0:
            proc = self.fingerTable.findProcess(position, overshoot)
            ret = self.send_msg(msg, proc)
            # Check return status of send message
            if ret['status'] == 'failure':
                continue
            else:
                break
        # Finger table is empty, so nowhere to forward
        else:
            return False

        # Sent message successfully
        return True



    # Given a stream, perform an insert on hashtable
    def performInsert(self, userStream=None, processStream=None):

        # Command to insert came from user
        if userStream:
            args = userStream.rstrip().split()
            if len(args) != 3:
                print('Usage: $ insert [key] [value]')
                return False
            key = args[1]
            hashedKey = self.hashKey(key)
            msg = {'method': 'insert', 'key': key, 'value': args[2], 'next': False, 'from': [self.highRange, self.ipAddress, self.port]}
            # If key is in my range, then perform insert on my own table
            if self.consultFingerTable(hashedKey, msg):
                msg = {'method': 'insertCopy', 'key': key, 'value': args[2], 'from': [self.highRange, self.ipAddress, self.port]}
                self.send_msg(msg,self.next)
                return self.updateHashTable('insert', key, args[2])
            # Key is not meant for me, so message has been forwarded
            else:
                pass

        # Command to insert came from another process
        if processStream:
            hashedKey = self.hashKey(processStream['key'])
            # Check if key is in my range
            if self.consultFingerTable(hashedKey, processStream):
                ret = self.updateHashTable('insert', processStream['key'], processStream['value'])
                msg = {'method': 'insertCopy', 'key': processStream['key'], 'value': processStream['value'], 'from': [self.highRange, self.ipAddress, self.port]}
                self.send_msg(msg,self.next)
                if ret:
                    msg = {'method': 'ack', 'message': 'Successful insert'}
                else:
                    msg = {'method': 'ack', 'message': 'Error on insertion'}
                self.send_msg(msg, processStream['from'])
            # If key is not in range, then message has been forwarded
            else:
                pass

    

    # Given a stream, perform a lookup on hashtable
    def performLookup(self, userStream=None, processStream=None):

        # Perform lookup has a little bit of different semantics since you can either return the looked up value in the function or wait for it from another process.
        # Perform lookup will always return a dictionary with a key called 'status'. If status is set to forwarded, then you need to wait get the value from another process. If status is set to 'success', then there will be another element in the dictionary containing the value.

        # Request to lookup came from user
        if userStream:
            args = userStream.rstrip().split()
            if len(args) != 2:
                print('Usage: $ lookup [key]')
                msg = {'status': 'failure'}
                return msg
            key = args[1]
            hashedKey = self.hashKey(key)
            msg = {'method': 'lookup', 'key': key, 'next': self.next, 'triedNext': False, 'from': [self.highRange, self.ipAddress, self.port]}
            # Check if key is contained in my own hashtable
            if self.consultFingerTable(hashedKey, msg):
                ret = self.updateHashTable('lookup', key)
                msg = {'status': 'success', 'value': ret}
                return msg
            # Otherwise, message has been forwarded
            else:
                return {'status': 'forwarded'}
                
        # Request to lookup came from another process
        if processStream:
            hashedKey = self.hashKey(processStream['key'])
            # Check if key is contained in my own hashtable
            if self.consultFingerTable(hashedKey, processStream):
                ret = self.updateHashTable('lookup', processStream['key'])
                msg = {'method': 'ack', 'message': 'Result of lookup', 'key': processStream['key'], 'value': ret}
                if processStream['triedNext'] == False:
                    msg['next'] = self.next
                    
                self.send_msg(msg, processStream['from'])
                return {'status': 'returned'}
            # Otherwise, message has been forwarded
            else:
                return {'status': 'forwarded'}



    # Given a stream, perform a remove
    def performRemove(self, userStream=None, processStream=None):
        
        # Command to remove came from user
        if userStream:
            args = userStream.rstrip().split()
            if len(args) != 2:
                print('Usage: $ remove [key]')
                return False
            key = args[1]
            hashedKey = self.hashKey(key)
            msg = {'method': 'remove', 'key': key, 'from': [self.highRange, self.ipAddress, self.port]}
            # Check if key is contained in my own hashtable
            if self.consultFingerTable(hashedKey, msg):
                ret = self.updateHashTable('remove', key)
                msg = {'method': 'removeCopy', 'key': key, 'from': [self.highRange, self.ipAddress, self.port]}
                self.send_msg(msg,self.next)
                return ret
            # Otherwise, message has been forwarded
            else:
                pass

        # Command to remove came from another process
        if processStream:
            hashedKey = self.hashKey(processStream['key'])
            # Check if key is contained in my own hashtable
            if self.consultFingerTable(hashedKey, processStream):
                ret = self.updateHashTable('remove', processStream['key'])
                msg = {'method': 'removeCopy', 'key': processStream['key'], 'from': [self.highRange, self.ipAddress, self.port]}
                self.send_msg(msg,self.next)
                if ret:
                    msg = {'method': 'ack', 'message': 'Successful remove'}
                else:
                    msg = {'method': 'ack', 'message': 'Error on removal'}
                self.send_msg(msg, processStream['from'])
            # Otherwise, message has been forwarded
            else:
                pass

        
    
    # Send a message to a destination to update their next pointer
    def sendUpdateNext(self, next_args, dest_args):
        msg = {'method': 'updateNext', 'next': next_args, 'from': [self.highRange, self.ipAddress, self.port]}
        ret_msg = self.send_msg(msg, dest_args)
        if ret_msg['status'] == 'failure':
            return False
        return True



    # Send a message to a destination to update their previous pointer
    def sendUpdatePrev(self, prev_args, dest_args):
        msg = {'method': 'updatePrev', 'prev': prev_args, 'from': [self.highRange, self.ipAddress, self.port]}
        ret_msg = self.send_msg(msg, dest_args)
        if ret_msg['status'] == 'failure':
            return False
        return True
        


    # Send a message to a destination to update their range of control
    def sendUpdateRange(self, high, low, dest_args):
        msg = {'method': 'updateRange', 'high': high, 'low': low, 'from': [self.highRange, self.ipAddress, self.port]}
        ret_msg = self.send_msg(msg, dest_args)
        if ret_msg['status'] == 'failure':
            return False
        return True



    # Check if previous and next are still alive
    def sanityCheck(self):

        # Return if no other node in ring
        if not self.prev or not self.next or (self.prev[1] == self.ipAddress and self.prev[2] == self.port) or (self.next[1] == self.ipAddress and self.next[2] == self.port):
            return

        # Sending dummy update previous to next
        prev_args = [self.highRange, self.ipAddress, self.port]
        dest_args = self.next
        ret = self.sendUpdatePrev(prev_args, dest_args)
        # If no response, then crash occurred
        if ret == False:
            self.handleCrash(dest_args, 'next')

        # Sending dummy update next to previous
        next_args = [self.highRange, self.ipAddress, self.port]
        dest_args = self.prev
        ret = self.sendUpdateNext(next_args, dest_args)
        # If no response, then crash occurred
        if ret == False:
            self.handleCrash(dest_args, 'prev')



    # Function to run when a crash is discovered
    def handleCrash(self, crash_args, position):

        print('Crash discovered, handling...')

        # If there were only two nodes in the ring prior to crash
        if not self.next or not self.prev or self.next[1] == self.prev[1] or self.next[1] == self.ipAddress or self.prev[1] == self.ipAddress:
            self.lowRange = self.highRange + UNIT
            self.next = [self.highRange, self.ipAddress, self.port]
            self.prev = [self.highRange, self.ipAddress, self.port]
            self.fingerTable.ft = list()
            return

        # Remove crashed process from finger table
        self.fingerTable.delNode(crash_args[1])

        # Find position of crashed node
        hashedIP = self.hashKey(crash_args[1], True, crash_args[2])

        # Create message to send to crash's adjacent process
        msg = {}
        if position == 'prev':
            msg = {'method': 'findProcess', 'next': crash_args, 'from': [self.highRange, self.ipAddress, self.port], 'toForward': [{'method': 'updateNext', 'next': [self.highRange, self.ipAddress, self.port], 'from': [self.highRange, self.ipAddress, self.port]}]}
        else:
            msg = {'method': 'findProcess', 'prev': crash_args, 'from': [self.highRange, self.ipAddress, self.port], 'toForward': [{'method': 'updatePrev', 'prev': [self.highRange, self.ipAddress, self.port], 'from': [self.highRange, self.ipAddress, self.port]}, {'method': 'updateRange', 'low': self.highRange + UNIT, 'high': -1, 'from': [self.highRange, self.ipAddress, self.port]}]}

        # Send message
        self.consultFingerTable(hashedIP, msg)



    # Find the next/previous of a crashed process
    def performFindProcess(self, processArgs):

        # processArgs is a json message with this structure:
            # method: findProcess
            # prev/next: [position, ipAddress, port] --> should be the arguments of a process that crashed
            # from: [position, ipAddress, port]
            # toForward: [{method: updatePrev/Next, args...}, {method: updateRange, args...}, ...] --> instructions to be forwarded

        # Set a variable specifying the process we are trying to find
        p = None
        if 'next' in processArgs:
            p = 'next'
        else:
            p = 'prev'

        # Remove the crashed process from the finger table
        self.fingerTable.delNode(processArgs[p])

        # Check if the crashed process is adjacent to us
        if (p == 'next' and processArgs[p][1] == self.next[1]) or (p == 'prev' and processArgs[p][1] == self.prev[1]):

            # Perform the functions specified in 'toForward'
            for func in processArgs['toForward']:

                # Update next node
                if func['method'] == 'updateNext':
                    self.next = func['next']
                    self.fingerTable.addNode(func['next'])
                    msg = {}
                    # Send acknowledgement message to sender with instructions on what to do next
                    if p == 'next':
                        msg = {'method': 'crashAcknowledge', 'message': 'Successfully updated next pointer', 'from': [self.highRange, self.ipAddress, self.port], 'todo': 'updatePrevAndRange'}
                    else:
                        msg = {'method': 'crashAcknowledge', 'message': 'Successfully updated next pointer', 'from': [self.highRange, self.ipAddress, self.port], 'todo': 'updateNext'}
                    self.send_msg(msg, processArgs['from'])

                # Update previous node
                if func['method'] == 'updatePrev':
                    self.prev = func['prev']
                    self.fingerTable.addNode(func['prev'])
                    msg = {}
                    # Send acknowledgement message to sender with instructions on what to do next
                    if p == 'next':
                        msg = {'method': 'crashAcknowledge', 'message': 'Successfully updated prev pointer', 'from': [self.highRange, self.ipAddress, self.port], 'todo': 'updatePrevAndRange'}
                    else:
                        msg = {'method': 'crashAcknowledge', 'message': 'Successfully updated prev pointer', 'from': [self.highRange, self.ipAddress, self.port], 'todo': 'updateNext'}
                    self.send_msg(msg, processArgs['from'])

                # Update my range
                if func['method'] == 'updateRange':
                    if func['low'] >= 0:
                        self.lowRange = func['low']
                    if func['high'] >= 0:
                        self.highRange = func['high']
                    msg = {}
                    # Send acknowledgement message to sender with instructions on what to do next
                    if p == 'next':
                        msg = {'method': 'crashAcknowledge', 'message': 'Successfully updated range', 'from': [self.highRange, self.ipAddress, self.port], 'todo': 'updatePrevAndRange'}
                    else:
                        msg = {'method': 'crashAcknowledge', 'message': 'Successfully updated range', 'from': [self.highRange, self.ipAddress, self.port], 'todo': 'updateNext'}
                    self.send_msg(msg, processArgs['from'])

        # Forward a message that is not meant for me
        else:
            hashedIP = self.hashKey(processArgs[p][1], True, processArgs[p][2])
            if p == 'next':
                self.consultFingerTable(hashedIP, processArgs, overshoot=False)
            else:
                self.consultFingerTable(hashedIP, processArgs, overshoot=True)



    # Function to test the latency/throughput of the system
    def testSystem(self):
        f = Faker()
        for i in range(500):
            l = [f.text().replace(' ','').replace('\n',''), f.name().replace(' ','')]
            self.testInput.append(l)
        self.finalResult = self.testInput[-1]
        numClients = 10
        self.testFile = open(f'tests/{self.ipAddress}/outputs-{numClients}.txt', 'a')



# Main Function
if __name__ == '__main__':
    cleanExit = True
    if len(sys.argv) == 2:
        try:
            cleanExit = bool(int(sys.argv[1]))
        except:
            print(f'{sys.argv[1]} is not a valid flag for \'cleanExit\'.')
            sys.exit(1)
    projectName = 'dcroft-project'
    client = P2PHashTableClient(clean_exit=cleanExit, projectName=projectName)
