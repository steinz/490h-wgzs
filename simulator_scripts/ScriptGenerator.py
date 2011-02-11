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

    def __init__(self):
        self.TxActionMin = 2
        self.TxActionMax = 6
        self.TxNumMin = 30
        self.TxNumMax = 50
        self.fileMin = 1
        self.fileMax = 6
        self.numNodes = 3
        self.possibleActions = ["create", "put", "append", "get", "delete"]
        
        
    def Generate(self):

        parser = optparse.OptionParser()
        parser.add_option('--out', action = 'store', type = 'string', dest = 'outfile', help = 'Input type')
	(option, args) =  parser.parse_args()
        scriptFile = open(option.outfile, "w")
        
        numTxActions = rand.randrange(self.TxActionMin, self.TxActionMax)
        numTxs = rand.randrange(self.TxNumMin, self.TxNumMax)

        for index in xrange(0, self.numNodes):
            print >> scriptFile, "start " + str(index)
        print >> scriptFile, "time"
        
        for tx in xrange(0, numTxs):
            node = rand.randrange(1, self.numNodes)
            
            print >> scriptFile, self.buildCommand(node, "txstart") 
            for action in xrange(0, numTxActions):

                nextAction = rand.choice(self.possibleActions)
                nextFile = "f" + str(rand.randrange(self.fileMin, self.fileMax))
                if nextAction == "put" or nextAction == "append":
                    contents = ''.join(rand.choice(string.letters) for i in xrange(4))
                    print >> scriptFile, self.buildCommand(node, nextAction, nextFile, contents)
                else:
                    print >> scriptFile, self.buildCommand(node, nextAction, nextFile)
                for index in xrange(0,5):
                    print >> scriptFile, "time"
                
            print >> scriptFile, self.buildCommand(node, "txcommit")
        
    def buildCommand(self, node, command, filename = "", contents = ""):
        outString = ' '.join([str(node), command, filename, contents])
        return(outString.strip())
        


if __name__ == "__main__":
    ScriptGen = ScriptGenerator()
    ScriptGen.Generate()
