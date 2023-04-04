class HashTable:
    def __init__(self):
        self.hash = dict()

    def insert(self, key, value):

        #Ways insert can fail: 1. Invalid Key (Needs to be a string)

        self.hash[key] = value

    def lookup(self, key):

        #Ways lookup can fail: 1. Invalid Key (Needs to be a string)

        if key in self.hash:
            return self.hash[key]
    
    def remove(self, key):

        #Ways remove can fail: 1. Invalid Key (Needs to be a response)

        if key in self.hash:
            return self.hash.pop(key)

    def size(self):

        #Size cannot fail

        return len(self.hash)

    def query(self, subkey, subvalue):
        #Returns a list of (key,value) pairs where the value (if it is a dictionary) contains a `subkey` with the value `subvalue`.
        #Need to iterate through all values within dict

        #Ways query can fail: 1. Invalid Subkey ( Needs to be a string )

        for key in self.hash:
            if type(self.hash[key]) is dict:
                for item in self.hash[key]:
                    if item == subkey and self.hash[key][subkey] == subvalue:
                        return [self.hash[key]]