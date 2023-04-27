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

UNIT = (2 * math.pi) / pow(2, 32)

class P2PHashTableClient:
    def __init__(self, clean_exit=True):
        self.ipAddress = None # IP of client --> where it can be reached
        self.port = None # What port the client can be reached at

        self.inRing = False

        # Where to put open socket connections
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
        
        self.testInput = []
        self.counter = 0

        self.clean_exit = clean_exit
        self.TEMP = dict()

        # TODO: run enter ring here

    def __del__(self):
        print('\nExiting program...')
        if self.clean_exit == False:
            print('Program crashed.')
            return
        # update next node
        if self.next and self.next[1] != self.ipAddress:
            self.sendUpdatePrev(self.prev, self.next)
            self.sendUpdateRange(-1, self.lowRange, self.next)
        # update previous node
        if self.prev and self.prev[1] != self.ipAddress:
            self.sendUpdateNext(self.next, self.prev)
        # reset low and high range
        self.highRange = 1001
        self.lowRange = 1000
        # rebalance data
        if self.next and self.prev and (self.next[1] != self.ipAddress or self.next[2] != self.port) and (self.prev[1] != self.ipAddress or self.prev[2] != self.port):
            for key in self.ht.hash:
                value = self.ht.hash[key]
                userStream = 'insert {} {}'.format(key, value)
                self.performInsert(userStream=userStream)
        print('Program finished exiting.')
    
    
    
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
            # Connect to Ring when there are other nodes in the ring --> contact first socket that connects requesting entry
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
        
        # Check to see if any servers are available
        for entry in data:
            #Contact each of these servers to see if they are available/in the ring
            newSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                newSock.connect((entry['address'], entry['port']))
                
                # If connect succeeds, talk to connecting and get inserted into the ring
                #      When connect succeeds, locate server will return socket
                newSock.close()
                
                #Gives dest_addr format --> don't have highRange yet
                return [None, entry['address'], entry['port']]
                
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
        self.lowRange = location + UNIT
        
        #Start listening socket
        port = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.ipAddress,port))
        self.sock.listen()
        
        self.port = self.sock.getsockname()[1]
        
        self.prev = [self.highRange, self.ipAddress, self.port]
        self.next = [self.highRange, self.ipAddress, self.port]
        self.inRing = True
        
        self.sendToNameServer()
        self.readMessages()

    # check if next and previous are still the next and previous
    def sanityCheck(self):

        # if it is only you in the ring, just return
        if not self.prev or not self.next or (self.prev[1] == self.ipAddress and self.prev[2] == self.port) or (self.next[1] == self.ipAddress and self.next[2] == self.port):
            return

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



    def performFindProcess(self, processArgs):

        # processArgs is a json message with this structure:
            # method: findProcess
            # prev/next: [position, ipAddress, port] --> should be the arguments of a process that crashed
            # from: [position, ipAddress, port]
            # toForward: [{method: updatePrev/Next, args...}, {method: updateRange, args...}, ...] --> instructions to be forwarded

        p = None
        if 'next' in processArgs:
            p = 'next'
        else:
            p = 'prev'

        # remove this process from finger table
        self.fingerTable.delNode(processArgs[p])

        # check if the target process is my prev or next
        if (p == 'next' and processArgs[p][1] == self.next[1]) or (p == 'prev' and processArgs[p][1] == self.prev[1]):
            # need to perform the functions in the thingy
            for func in processArgs['toForward']:
                # func is a json msg

                if func['method'] == 'updateNext':
                    self.next = func['next']
                    self.fingerTable.addNode(func['next'])
                    msg = {}
                    if p == 'next':
                        msg = {'method': 'crashAcknowledge', 'message': 'Successfully updated next pointer', 'from': [self.highRange, self.ipAddress, self.port], 'todo': 'updatePrevAndRange'}
                    else:
                        msg = {'method': 'crashAcknowledge', 'message': 'Successfully updated next pointer', 'from': [self.highRange, self.ipAddress, self.port], 'todo': 'updateNext'}
                    self.send_msg(msg, processArgs['from'], True)

                if func['method'] == 'updatePrev':
                    self.prev = func['prev']
                    self.fingerTable.addNode(func['prev'])
                    msg = {}
                    if p == 'next':
                        msg = {'method': 'crashAcknowledge', 'message': 'Successfully updated prev pointer', 'from': [self.highRange, self.ipAddress, self.port], 'todo': 'updatePrevAndRange'}
                    else:
                        msg = {'method': 'crashAcknowledge', 'message': 'Successfully updated prev pointer', 'from': [self.highRange, self.ipAddress, self.port], 'todo': 'updateNext'}
                    self.send_msg(msg, processArgs['from'], True)

                if func['method'] == 'updateRange':
                    if func['low'] >= 0:
                        self.lowRange = func['low']
                    if func['high'] >= 0:
                        self.highRange = func['high']
                    msg = {}
                    if p == 'next':
                        msg = {'method': 'crashAcknowledge', 'message': 'Successfully updated range', 'from': [self.highRange, self.ipAddress, self.port], 'todo': 'updatePrevAndRange'}
                    else:
                        msg = {'method': 'crashAcknowledge', 'message': 'Successfully updated range', 'from': [self.highRange, self.ipAddress, self.port], 'todo': 'updateNext'}
                    self.send_msg(msg, processArgs['from'], True)

        # Forward a message if it is not for you
        else:
            hashedIP = self.hashKey(processArgs[p][1])
            if p == 'next':
                self.consultFingerTable(hashedIP, processArgs, overshoot=False)
            else:
                self.consultFingerTable(hashedIP, processArgs, overshoot=True)



    # crash_args: the tuple of the destination that crashed
    # position: a string that is either 'prev' or 'next' and references whether the crashed node is the next or previous of ourselves.
    def handleCrash(self, crash_args, position):

        print('Crash discovered, handling...')

        # case where there is only 1 node in the ring
        if not self.next or not self.prev or self.next[1] == self.prev[1] or self.next[1] == self.ipAddress or self.prev[1] == self.ipAddress:
            self.lowRange = self.highRange + UNIT
            self.next = [self.highRange, self.ipAddress, self.port]
            self.prev = [self.highRange, self.ipAddress, self.port]
            self.fingerTable.ft = list()
            return

        # remove crash_args from fingertable
        self.fingerTable.delNode(crash_args[1])

        # hash ipaddress to find position
        hashedIP = self.hashKey(crash_args[1])

        # Different functionalities for whether the crash is next or previous
        msg = {}
        if position == 'prev':
            # send update next AND REBALANCE to the next process
            msg = {'method': 'findProcess', 'next': crash_args, 'from': [self.highRange, self.ipAddress, self.port], 'toForward': [{'method': 'updateNext', 'next': [self.highRange, self.ipAddress, self.port], 'from': [self.highRange, self.ipAddress, self.port]}]}
        else:
            # send update prev and range to next process
            msg = {'method': 'findProcess', 'prev': crash_args, 'from': [self.highRange, self.ipAddress, self.port], 'toForward': [{'method': 'updatePrev', 'prev': [self.highRange, self.ipAddress, self.port], 'from': [self.highRange, self.ipAddress, self.port]}, {'method': 'updateRange', 'low': self.highRange+UNIT, 'high': -1, 'from': [self.highRange, self.ipAddress, self.port]}]}

        if self.consultFingerTable(hashedIP, msg):
            pass



    # use finger table or next and prev pointers to take a message to a process
    # msg: dictionary of the message to send
    # position: position on the ring where this message is trying to go
    def forwardMessage(self, msg, position, overshoot=True):
        
        # check length of finger table
        if len(self.fingerTable.ft) <= 0:
            print('An error occurred.')
            sys.exit(0)

        # get where to send the process
        while len(self.fingerTable.ft) > 0:
            proc = self.fingerTable.findProcess(position, overshoot)
            ret = self.send_msg(msg, proc, True)
            # try again if needed
            if ret['status'] == 'failure':
                continue
            else:
                break
        else:
            # you are the only one in the ring, return failure
            return False

        # successfully sent message, return True
        return True



    def readMessages(self):
        
        self.stdinDesc = sys.stdin.fileno()
        
        print(f"Listening on Port {self.sock.getsockname()[1]}")
        self.usage()
        self.sock.settimeout(60)
        listen_list = [self.sock, self.stdinDesc]
        write_list = []
        exception_list = []
        x = 0
        readTest = False

        # variable to keep track of when to do sanity checks
        sanity_last_time = time.time()

        # variable to keep track of sending to name server
        ns_last_time = time.time()

        while True:

            # check if 3 seconds have passed and perform sanity check if necessary
            sanity_curr_time = time.time()
            if ((sanity_curr_time - sanity_last_time) > 3) and self.inRing:
                sanity_last_time = sanity_curr_time
                self.sanityCheck()
            
            ns_curr_time = time.time()
            if (ns_curr_time - ns_last_time) > 60:
                ns_last_time = ns_curr_time
                self.sendToNameServer()

            try:
                read_sockets, write_sockets, error_sockets = select.select(listen_list, write_list, exception_list,0)
                
                '''
                if not read_sockets and x == 0:
                    x = 1
                    self.testSystem()
                    
                elif not read_sockets and x == 1 and self.counter < 50:
                    print(f'insert {self.testInput[self.counter][0]} {self.testInput[self.counter][1]}')
                    
                    self.performInsert(userStream=f'insert {self.testInput[self.counter][0]} {self.testInput[self.counter][1]}')
                    self.counter += 1
                '''
                
                for sock in read_sockets:
                    
                    if sock == self.sock: #MasterSocket ready for reading
                
                        conn, addr = self.sock.accept()
                        
                        listen_list.append(conn)
                        
                    elif sock == self.stdinDesc:
                        #This flag is set off when there is keyboard input and enter is pressed
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

                        elif i == 'sanity check':
                            self.sanityCheck()
                            
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
                            # print('Message Recieved: ', stream)
                            # print(stream)
                            self.conn.close()
                            self.parseStream(stream, msg_length)
                            listen_list.remove(sock)
                                
            except TimeoutError: #This exception is taken on timeout
                # self.sendToNameServer()
                pass
        
    def parseStream(self, stream, msg_length):
        #After receiving message need to check that 2 lengths match, then extract fields from stream
        if msg_length != len(str(stream)):
            #Encountered malformed stream
            return False
        
        #Two different types of methods--> ack and requests
        
        if 'method' in stream:

            if stream['method'] == 'saveAndRemove':
                # put everything in your temp and then remove everything
                self.TEMP = dict()
                for key in self.ht.hash:
                    self.TEMP[key] = self.ht.hash[key]
                for key in self.TEMP:
                    self.performRemove(userStream='remove {}'.format(key))
                # send acknowledgement
                msg = {'method': 'saveAndRemoveAck', 'from': [self.highRange, self.ipAddress, self.port], 'toForward': stream['toForward']}
                self.send_msg(msg, stream['from'])

            elif stream['method'] == 'saveAndRemoveAck':
                # node can now join the the thing
                author = stream['toForward']['from']
                msg = self.addToRing(author, stream['toForward'])
                #Need to send message back
                if msg:
                    self.send_msg(msg, author, True)

            elif stream['method'] == 'crashRebalance':
                for key in self.ht.hash:
                    self.performInsert(userStream='insert {} {}'.format(key, self.ht.hash[key]))
                msg = {'method': 'ack', 'message': 'Successfully rebalanced', 'from': [self.highRange, self.ipAddress, self.port]}
                self.send_msg(msg, stream['from'])

            elif stream['method'] == 'joinReq':

                # check if I am the only node, and if so, just handle ring
                if not self.next or not self.prev or (self.prev[1] == self.ipAddress and self.prev[2] == self.port) or (self.next[1] == self.ipAddress and self.next[2] == self.port):
                    #Handle adding node to the ring
                    msg = self.addToRing(stream['from'], stream)
                    #Need to send message back
                    if msg:
                        # TODO: failure check this send msg
                        self.send_msg(msg, stream['from'], True)


                else:
                    # Hash IP from join req.
                    hashedIP = self.hashKey(stream['from'][1])
                    # if it for me:
                    if self.consultFingerTable(hashedIP, stream):
                        # Remove everything from hashtable.
                        self.TEMP = dict()
                        for key in self.ht.hash:
                            self.TEMP[key] = self.ht.hash[key]
                        for key in self.TEMP:
                            self.performRemove(userStream='remove {}'.format(key))
                        # send message to previous to remove everything
                        msg = {'method': 'saveAndRemove', 'from': [self.highRange, self.ipAddress, self.port], 'toForward': stream}
                        self.send_msg(msg, self.prev)
                    # if it isnt:
                    else:
                        # message has been forwarded
                        pass

            elif stream['method'] == 'join':
                self.next = stream['next']
                self.fingerTable.addNode(stream['next'])
                self.prev = stream['prev']
                self.fingerTable.addNode(stream['prev'])
                self.highRange = stream['highRange']
                self.lowRange = stream['lowRange']
                self.fingerTable.ft = stream['ft']
                
                self.fingerTable.addNode(stream['from'])
                
                self.inRing = True

                # send an insert from temp to next and prev
                msg = {'method': 'insertFromTemp', 'from': [self.highRange, self.ipAddress, self.port]}
                self.send_msg(msg, self.prev)
                self.send_msg(msg, self.next)

            elif stream['method'] == 'insertFromTemp':
                # reinsert all values from temp
                for key in self.ht.hash:
                    self.performInsert(userStream='insert {} {}'.format(key, self.ht.hash[key]))
                for key in self.TEMP:
                    self.performInsert(userStream='insert {} {}'.format(key, self.TEMP[key]))
                self.TEMP = dict()

            elif stream['method'] == 'findProcess':
                self.performFindProcess(stream)
                
            elif stream['method'] == 'updateNext':
                #Handle updating next node --> need to send ack
                self.next = stream['next']
                self.fingerTable.addNode(stream['next'])
                msg = {'method': 'ack', 'message': 'Successfully updated next pointer'}
                self.send_msg(msg, stream['from'], True)
                
            elif stream['method'] == 'updatePrev':
                #Handle updating prev node --> need to send ack
                self.prev = stream['prev']
                self.fingerTable.addNode(stream['prev'])
                msg = {'method': 'ack', 'message': 'Successfully updated prev pointer'}
                self.send_msg(msg, stream['from'], True)
                
            elif stream['method'] == 'updateRange':
                if stream['low'] >= 0:
                    self.lowRange = stream['low']
                if stream['high'] >= 0:
                    self.highRange = stream['high']
                    
                msg = {'method': 'ack', 'message': 'Successfully updated range'}
                self.send_msg(msg, stream['from'], True)
                
            elif stream['method'] == 'getLow':
                #Just need to return prev info
                msg = {'method': 'ack', 'prev': self.prev, 'message': 'successfully retrieved prev info'}
                self.send_msg(msg, stream['from'], True)

            elif stream['method'] == 'insert':
                self.performInsert(processStream=stream)
                
            elif stream['method'] == 'insertCopy':
                ret = self.updateHashTable('insert', stream['key'], stream['value'])
                if ret:
                    msg = {'method': 'ack', 'message': 'Successful insert of copy'}
                else:
                    msg = {'method': 'ack', 'message': 'Error on insertion of copy'}
                    
                self.send_msg(msg, stream['from'])

            elif stream['method'] == 'lookup':
                self.performLookup(processStream=stream)

            elif stream['method'] == 'remove':
                self.performRemove(processStream=stream)

            elif stream['method'] == 'removeCopy':
                ret = self.updateHashTable('remove', stream['key'])
                if ret:
                    msg = {'method': 'ack', 'message': 'Successful removal of copy'}
                else:
                    msg = {'method': 'ack', 'message': 'Error on removal of copy'}    
                self.send_msg(msg, stream['from'])

            elif stream['method'] == 'ack':
                # check if returning from a lookup
                if stream['message'] == 'Result of lookup' and stream['value'] is not None:
                    print('{}: {}'.format(stream['key'], stream['value']))
                elif stream['message'] == 'Result of lookup' and stream['value'] is None and 'next' in stream:
                    # Check next node to see if key is there
                    msg = {'method': 'lookup', 'key': stream['key'], 'triedNext': True, 'from': [self.highRange, self.ipAddress, self.port]}
                    self.send_msg(msg,stream['next'])
                elif stream['message'] == 'Result of lookup' and stream['value'] is None:
                    print('Key {} does not exist in table.'.format(stream['key']))

            elif stream['method'] == 'crashAcknowledge':
                if stream['todo'] == 'updatePrevAndRange':
                    # update next and range
                    self.prev = stream['from']
                    self.lowRange = stream['from'][0] + UNIT
                else:
                    # update next
                    self.next = stream['from']
                # need to rebalance after crash acknowledge
                for key in self.ht.hash:
                    userStream = 'insert {} {}'.format(key, self.ht.hash[key])
                    self.performInsert(userStream=userStream)
                msg = {'method': 'crashRebalance', 'from': [self.highRange, self.ipAddress, self.port]}
                self.send_msg(msg, stream['from'])



    def performInsert(self, userStream=None, processStream=None):

        # Hash given key
        if userStream:
            args = userStream.rstrip().split()
            if len(args) != 3:
                print('Usage: $ insert [key] [value]')
                return False
            key = args[1]
            hashedKey = self.hashKey(key)
            msg = {'method': 'insert', 'key': key, 'value': args[2], 'next': False, 'from': [self.highRange, self.ipAddress, self.port]}
            if self.consultFingerTable(hashedKey, msg):
                # perform insert --> also need to send message to next
                msg = {'method': 'insertCopy', 'key': key, 'value': args[2], 'from': [self.highRange, self.ipAddress, self.port]}
                self.send_msg(msg,self.next)
                return self.updateHashTable('insert', key, args[2])
            else:
                # message has been successfully forwarded
                pass

        if processStream:
            hashedKey = self.hashKey(processStream['key'])
            if self.consultFingerTable(hashedKey, processStream):
                ret = self.updateHashTable('insert', processStream['key'], processStream['value'])
                msg = {'method': 'insertCopy', 'key': processStream['key'], 'value': processStream['value'], 'from': [self.highRange, self.ipAddress, self.port]}
                self.send_msg(msg,self.next)
                # return ack
                if ret:
                    msg = {'method': 'ack', 'message': 'Successful insert'}
                else:
                    msg = {'method': 'ack', 'message': 'Error on insertion'}
                self.send_msg(msg, processStream['from'])
            else:
                # already been forwarded
                pass

    


    def performLookup(self, userStream=None, processStream=None):

        # Perform lookup has a little bit of different semantics since you can either return the looked up value in the function or wait for it from another process.
        # Perform lookup will always return a dictionary with a key called 'status'. If status is set to forwarded, then you need to wait get the value from another process. If status is set to 'success', then there will be another element in the dictionary containing the value.

        # lookup request from user
        if userStream:
            args = userStream.rstrip().split()
            if len(args) != 2:
                print('Usage: $ lookup [key]')
                msg = {'status': 'failure'}
                return msg
            key = args[1]
            hashedKey = self.hashKey(key)
            msg = {'method': 'lookup', 'key': key, 'next': self.next, 'triedNext': False, 'from': [self.highRange, self.ipAddress, self.port]}
            if self.consultFingerTable(hashedKey, msg):
                # perform lookup on my own table
                ret = self.updateHashTable('lookup', key)
                msg = {'status': 'success', 'value': ret}
                return msg
            else:
                # message has been successfully forwarded
                return {'status': 'forwarded'}
                
        if processStream:
            hashedKey = self.hashKey(processStream['key'])
            if self.consultFingerTable(hashedKey, processStream):
                ret = self.updateHashTable('lookup', processStream['key'])
                # return ack
                msg = {'method': 'ack', 'message': 'Result of lookup', 'key': processStream['key'], 'value': ret}
                if processStream['triedNext'] == False:
                    msg['next'] = self.next
                    
                self.send_msg(msg, processStream['from'])
                return {'status': 'returned'}
            else:
                # already been forwarded
                return {'status': 'forwarded'}



    def performRemove(self, userStream=None, processStream=None):
        
        if userStream:
            args = userStream.rstrip().split()
            if len(args) != 2:
                print('Usage: $ remove [key]')
                return False
            key = args[1]
            hashedKey = self.hashKey(key)
            msg = {'method': 'remove', 'key': key, 'from': [self.highRange, self.ipAddress, self.port]}
            if self.consultFingerTable(hashedKey, msg):
                # perform remove
                ret = self.updateHashTable('remove', key)
                msg = {'method': 'removeCopy', 'key': key, 'from': [self.highRange, self.ipAddress, self.port]}
                self.send_msg(msg,self.next)
                return ret
            else:
                # message has been successfully forwarded
                pass

        if processStream:
            hashedKey = self.hashKey(processStream['key'])
            if self.consultFingerTable(hashedKey, processStream):
                ret = self.updateHashTable('remove', processStream['key'])
                msg = {'method': 'removeCopy', 'key': processStream['key'], 'from': [self.highRange, self.ipAddress, self.port]}
                self.send_msg(msg,self.next)
                # return ack
                if ret:
                    msg = {'method': 'ack', 'message': 'Successful remove'}
                else:
                    msg = {'method': 'ack', 'message': 'Error on removal'}
                self.send_msg(msg, processStream['from'])
            else:
                # already been forwarded
                pass


        
    def addToRing(self, details, msg):
        #To add node to ring need to hashIP
        hashedIP = self.hashKey(details[1])
        highRange = hashedIP
        
        #If fingertable is empty, then this is the second node joining so can just add to it
        if self.consultFingerTable(highRange,msg):
            self.fingerTable.addNode((highRange, details[1], details[2]))
            
            #After adding finger table, need to get low range by communicating with assigned nextNode which will be who sends you the message
            
            if self.prev != [self.highRange, self.ipAddress, self.port]:
                self.lowRange = highRange + UNIT
                
                next = [self.highRange, self.ipAddress, self.port]
                
                prev = self.prev
                
                lowRange = self.prev[0] + UNIT
                
                self.prev = [highRange, details[1], details[2]]
                self.fingerTable.addNode(self.prev)
                #More than 2 members
                #Need to update prev next
                self.sendUpdateNext([highRange, details[1], details[2]], prev)

            else: #2 members
                self.lowRange = highRange + UNIT
                
                lowRange = self.highRange + UNIT
                
                #Set next and prev as this node
                prev = self.prev
                next = [self.highRange, self.ipAddress, self.port]
                self.prev = [highRange, details[1], details[2]]
                self.fingerTable.addNode(self.prev)
            
                self.next = [highRange, details[1], details[2]]
                self.fingerTable.addNode(self.next)
                
            
            return {'method': 'join', 'next': next, 'prev': prev, 'highRange': highRange, 'lowRange': lowRange, 'ft': self.fingerTable.ft, 'from': (self.highRange, self.ipAddress, self.port)}
        
        else:
            return False
            
        # Adding into ring when there are more than 2 members
        #consultFingerTable will send this message to the appropriate source
                
        #Once you have high Range --> get low range by consulting finger table
        # It will send the new node’s position in the ring, a copy of the process’ finger table, the new node’s previous process, and the new node’s next process
        
        # Need to send back to the node highRange, lowRange, next, prev
    

    
    def consultFingerTable(self, position, msg, overshoot=True):
        #In this function, consult your own finger table and see if you are responsible for message: other wise forward to other node
        
        # delete yourself, add your previous and next to finger table
        self.fingerTable.addNode(self.next)
        self.fingerTable.addNode(self.prev)
        self.fingerTable.delNode(self.ipAddress)

        # if finger table is empty, then you are responsible
        if len(self.fingerTable.ft) <= 0:
            return True

        #FIRST SEE IF YOU ARE RESPONSIBLE
        elif self.highRange < self.lowRange:
            #Need to check between high & 0 and 0 & low
            if 0 <= position <= self.highRange:
                return True
            elif self.lowRange <= position <= 2 * math.pi:
                return True
            else:
                return not self.forwardMessage(msg, position, overshoot)
            
        elif self.lowRange <= position <= self.highRange:
            return True
            
        elif self.lowRange == self.highRange:
            return True
            
        else:
            #YOU ARE NOT RESPONSIBLE FOR THIS INSERT/JOIN --> Call forwardMessage
            return not self.forwardMessage(msg, position, overshoot)
    
    def hashKey(self, key):
        #This hashing algorithm is djb2 source: http://www.cse.yorku.ca/~oz/hash.html
        # NOTE: Max Hash -->  2^{32} - 1 = 4,294,967,295
        
        #Need to modify hashing algorithm to more evenly distribute these nodes
        
        try:
        
            hashedKey = 5381
            
            for x in key:
                hashedKey = (( hashedKey << 5) + hashedKey) + ord(x)
            
            a = hashedKey & 0xFFFFFFFF
            
            #Self entered to try and diversify ip hashes
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
            a = a % (pow(2,32) - 1)
            a = a / (pow(2, 32) - 1)
            a = a * 2 * math.pi
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

        # if destargs is none, return failure
        if not dest_args:
            return {'status': 'failure', 'message': 'dest_args not specified'}

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
        print(f'DEBUG: prev: {self.prev}, next: {self.next}, FT: {self.fingerTable.ft}, highRange: {self.highRange}, lowRange: {self.lowRange}, ip address: {self.ipAddress}')
        print('my hashtable is:')
        for key in self.ht.hash:
            print('{}: {}'.format(key, self.ht.hash[key]))
            
    def usage(self):
        print('\nP2PHashTable Usage:')
        print('  Insert [key] [value]')
        print('  Lookup [key]')
        print('  Remove [key]')
        print('  Exit/Ctrl-c to quit\n')

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

    def testSystem(self):
        #Prepare arguments to be passed to client
        #
        f = Faker()
        for i in range(50):
            l = [f.name().replace(' ',''), f.name().replace(' ','')]
            self.testInput.append(l)

if __name__ == '__main__':
    client = P2PHashTableClient()
    client.enterRing('begloff-project')
        
    
