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



        def sendUpdateNext(self, next_args, dest_args):

            msg = {'method': 'updateNext', 'next': prev_args, 'from': (self.highRange, self.ipAddress, self.port)}
        
            # TODO: use ast.as_literal_eval() to parse messages 

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((dest_args[1], dest_args[2]))
                sock.sendall(msg)
                # TODO: should receive a message back
                ack = sock.recv(1024)
                sock.close()
            except:
                return False

        def sendUpdatePrev(self, prev_args, dest_args):

            msg = {'method': 'updatePrev', 'prev': prev_args, 'from': (self.highRange, self.ipAddress, self.port)}
        
            # TODO: use ast.as_literal_eval() to parse messages 

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((dest_args[1], dest_args[2]))
                sock.sendall(msg)
                # TODO: should receive a message back
                ack = sock.recv(1024)
                sock.close()
            except:
                return False

        def sendUpdateRange(self, high, low, dest_args):

            msg = {'method': 'updateRange', 'high': high, 'low': low, 'from': (self.highRange, self.ipAddress, self.port)}

            # TODO: use ast.literal_eval() to parse messages

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((dest_args[1], dest_args[2]))
                sock.sendall(msg)
                # TODO: should receive message back
                ack = sock.recv(1024)
                sock.close()
            except:
                return False


if __name__ == '__main__':
    client = P2PHashTableClient()
    client.enterRing('begloff-project')
        
    
