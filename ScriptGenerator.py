import os
import random as rand
import optparse
import string

class ScriptGenerator:

    TxActionMin = None
    TxActionMax = None
    TxNumMin = None
    TxNumMax = None
    fileMin = None
    fileMax = None
    numNodes = None
    possibleActions = None
    nodeList = None
    validOnly = True

    def __init__(self):
        self.TxActionMin = 2
        self.TxActionMax = 6
        self.TxNumMin = 10
        self.TxNumMax = 20
        self.numNodes = 3
        self.nodeList = []
        
        
    def Generate(self):

        parser = optparse.OptionParser()
        parser.add_option('--out', action = 'store', type = 'string', dest = 'outfile', help = 'Input type')
	(option, args) =  parser.parse_args()
        scriptFile = open(option.outfile, "w")
        
        numTxActions = rand.randrange(self.TxActionMin, self.TxActionMax)
        numTxs = rand.randrange(self.TxNumMin, self.TxNumMax)

        for index in xrange(0, self.numNodes):
            print >> scriptFile, "start " + str(index)
            self.nodeList.append(Node(index))
        print >> scriptFile, "time"
        
        for tx in xrange(0, numTxs):
            node = rand.randrange(1, self.numNodes)
            
            print >> scriptFile, self.nodeList[node].buildCommand(node, "txstart")
            print >> scriptFile, "time"

            for action in xrange(0, numTxActions):

                nextAction = self.nodeList[node].getNextCommand(self.validOnly)
                print >> scriptFile, nextAction
                print >> scriptFile, "time"
                
            print >> scriptFile, self.nodeList[node].buildCommand(node, "txcommit")
            print >> scriptFile, "time"
        
class Node:

    Address = None
    knownFiles = None
    possibleActions = ["create", "put", "append", "get", "delete"]
    fileMin = None
    fileMax = None
    

    def __init__(self, add):
        self.address = add
        self.knownFiles = []
        self.fileMin = 1
        self.fileMax = 6

    def getNextCommand(self, validOnly):
        nextAction = rand.choice(self.possibleActions)
        nextFile = "f" + str(rand.randrange(self.fileMin, self.fileMax))
        truth_vals = [nextFile in self.knownFiles, validOnly]

        # I know this is shitty
        
        if ((nextAction == "put" or nextAction == "append") and all(truth_vals)):
            contents = ''.join(rand.choice(string.letters) for i in xrange(4))
            return self.buildCommand(self.address, nextAction, nextFile, contents)
        elif (nextAction == "create"):
            if all(truth_vals):
                return self.buildCommand(self.address, "delete", nextFile)
            else:
                return self.buildCommand(self.address, "create", nextFile)
        else: # it's a delete or an unknown file
            if nextFile not in self.knownFiles and validOnly:
                return self.buildCommand(self.address, "create", nextFile)
            else:
                return self.buildCommand(self.address, "delete", nextFile)
            
            

    def buildCommand(self, node, command, filename = "", contents = ""):
        outString = ' '.join([str(node), command, filename, contents])
        if (command == "create"):
            self.knownFiles.append(filename)
        elif (command == "delete"):
            self.knownFiles.remove(filename)
        return(outString.strip())


if __name__ == "__main__":
    ScriptGen = ScriptGenerator()
    ScriptGen.Generate()
