import requests
import math


class P2PHashTableClient:
    def __init__(self):
        self.ipAddress = None # IP of client --> where it can be reached
        self.port = None # What port the client can be reached at

        # TODO: where to put open socket connections
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
        if not self.locateServer():
            self.startP2P()
        
    def locateServer(self):
        
        # Get json data from naming service and try to find existing clients
        data = requests.get("http://catalog.cse.nd.edu:9097/query.json")
        data = data.json()

        data = list(filter( lambda x: "type" in x and "project" in x and x["type"] == "p2phashtable" and x["project"] == self.projectName, data))
        
        if not data:
            #No entries in the ring, so need to start 
            return False
        
        #TODO: Check to see if any servers are available
        
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
        self.lowRange = location + 0.0000000000000001

    
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
            if msg is not dict:
                return None

            # connect to destination
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
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



if __name__ == '__main__':
    client = P2PHashTableClient()
    client.enterRing('begloff-project')
        
    
