class FingerTable():

    def __init__(self):
        self.ft = list()

    def addNode(self, args):
        # args is a list structured as (position, IP_ADDR, port)
        # position is the TOP RANGE of the range

        if not type(args) is list:
            return False

        # check that node is not already in table
        for node in self.ft:
            if node[1] == args[1] and node[2] == args[2]:
                return False

        self.ft.append(args)
        self.ft = sorted(self.ft)
        return True



    def delNode(self, IP_ADDR):

        # sanity checks
        if (not self.ft) or len(self.ft) <= 0:
            return False

        # delete node with that address
        keep = list()
        for idx, node in enumerate(self.ft):
            if node[1] != IP_ADDR:
                keep.append(idx)

        if len(keep) == len(self.ft):
            return False

        temp = list()
        for idx in keep:
            temp.append(self.ft[idx])

        self.ft = temp

        return True



    # Overshoot should only be false when handling crashes
    def findProcess(self, position, overshoot=True):

        # Handle empty finger table
        if len(self.ft) <= 0:
            return None

        # find the process with a position that is juuuuust higher than the given position and return it
        if overshoot:
            for node in self.ft:
                if node[0] >= position:
                    return node
            # if could not find node, then just return first one
            return self.ft[0]

        # find process that is juuuuust below the position. Used for crashes
        else:
            for node in reversed(self.ft):
                if node[0] <= position:
                    return node
            # if could not find node, then just return last one
            return self.ft[-1]
