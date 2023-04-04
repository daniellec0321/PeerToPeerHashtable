class FingerTable():

    def __init__(self):
        self.ft = list()

    def addNode(self, args):
        # args is a tuple structured as (position, IP_ADDR, port)
        # position is the TOP RANGE of the range

        if not type(args) is tuple:
            return False

        # check that node is not already in table
        for node in self.ft:
            if node[1] == args[1]:
                return False

        self.ft.append(args)
        self.ft = sorted(self.ft)
        return True



    def delNode(self, IP_ADDR):
        # delete node with that address
        to_delete = -1
        for idx, node in enumerate(self.ft):
            if node[1] == IP_ADDR:
                to_delete = idx
                break

        if to_delete == -1:
            return False

        del self.ft[to_delete]
        return True



    def findProcess(self, position):
        # find the process with a position that is juuuuust higher than the given position and return it
        for node in self.ft:
            if node[0] >= position:
                return node

        # if could not find one, then return the first node in the list
        return self.ft[0]
