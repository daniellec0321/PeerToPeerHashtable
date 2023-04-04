class FingerTable():

    def __init__(self):
        self.ft = list()

    def addNode(self, args):
        # args is a tuple structured as (position, IP_ADDR, port)
        if not type(args) is tuple:
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
