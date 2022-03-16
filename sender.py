import sys
import errno
import struct
import fcntl
import socket
import time
import random
import pickle

class flow():
    flowSizes = []
    flowWeights = []
    avgSearchSize = 1620
    avgDataMiningSize = 9500
    flowType = ""

    def __init__(self, filename):
        if (filename not in ["flows/websearch.txt", "flows/datamining.txt"]):
            raise ValueError('Incorrect input file')

        flowCDF = []

        with open(filename, 'r') as f:
            for l in f.readlines():
                flowCDF.append(([float(i) for i in str.split(l)]))

        self.flowType = filename.split('.')[0]

        prev = 0
        for size in flowCDF:
            self.flowSizes.append(int(size[0]))
            self.flowWeights.append(size[2] - prev)
            prev = size[2]

    """ This function is taken from http://eli.thegreenplace.net/2010/01/22/weighted-random-generation-in-python/ """

    def weightedChoice(self):
        totals = []
        runningTotal = 0

        for w in self.flowWeights:
            runningTotal += w
            totals.append(runningTotal)

        rnd = random.random() * runningTotal
        for i, total in enumerate(totals):
            if rnd < total:
                return i

    def randomSize(self):
        index = self.weightedChoice()
        return self.flowSizes[index]

    def meanSize(self):
        if self.flowType == "flows/websearch":
            return self.avgSearchSize
        else:
            return self.avgDataMiningSize

    def maxSize(self):
        return self.flowSizes[len(self.flowSizes) - 1]

    def getPriority(self, flowSize):
        maxSize = self.maxSize()
        res = (flowSize / (maxSize / 16)) + 1
        return res if res <= 16 else 16

class Sender(object):

    def __init__(self, sourceIP, flowSource = "flows/websearch.txt", cong = "mintcp", destList = [], destPort = 8000):
        self.IP = sourceIP
        self.flowSource = flowSource 
        self.destList = destList
        self.destPort = destPort
        self.cong = cong

        self.createPrioMap()
        self.removeSelfFromDestList()

    def removeSelfFromDestList(self):
        dests = []
        for IP in self.destList:
            if IP != self.IP:
                dests.append(IP)
        self.destList = dests

    def createFlowObj(self):
        self.flow = flow(self.flowSource)

    def setTimers(self, st, rt):
        self.starttime = st
        self.runtime = rt

    def createPrioMap(self):
        val = 65
        self.prioMap = {}
        for i in range(1,17):
            self.prioMap[i] = chr(val)
            val += 1

    def openTCPConnection(self, destIP, destPort):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect((destIP, destPort))
        return s


    def pickDest(self):
        random.seed()
        i = random.randrange(len(self.destList))
        dest = self.destList[i]
        return dest

    def getTCPUnacked(self,s):
        fmt = "B"*7+"I"*21
        x = struct.unpack(fmt, s.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 92))
        return int(x[11])

    def sendFlow(self, socket, destIP):
        flowSize = self.flow.randomSize()
        toSend = flowSize
        flowStartTime = time.time()

        while toSend > 0:
            if (time.time() - self.starttime) > self.runtime:
                return None

            priority = self.flow.getPriority(toSend)

            #first byte is the priority, rest of payload is just zeros
            payload = "0"*1023 
            packet = self.prioMap[priority] + payload

            socket.send(packet)
           
            toSend = toSend - 1 #decrement bytes left to send by 1kb

        numUnacked = self.getTCPUnacked(socket)
        while (numUnacked > 0):
            numUnacked = self.getTCPUnacked(socket)
            if (time.time() - self.starttime) > self.runtime:
                return None

        FCT = time.time() - flowStartTime
        return (flowSize, FCT)
            
    def sendRoutine(self):
        destIP = self.pickDest()  #pick random destination
        output = None
        s = self.openTCPConnection(destIP, self.destPort)  #open TCP connection to destination
        output = self.sendFlow(s, destIP) #send a random-sized flow to random destination

        s.close()
        return output


def main():
    load = float(sys.argv[1])
    runtime = float(sys.argv[2])
    output = sys.argv[3]
    numhosts = int(sys.argv[4])

    #DEBUG; set random seed to fixed value so sequence is deterministic
    random.seed()

    #open pickled sender (created by pfabric.py)
    sender = ""
    with open("sender.pkl", "rb") as f:
        sender = pickle.load(f)
    
    sender.createFlowObj()

    #debug; get some member variables
    meanFlowSize = (sender.flow).meanSize()
    
    newflow = sender.flow
    priomap = sender.prioMap
    
    outfile = "{}/load{}.txt".format(output, int(load*10))

    bw = 0.1 #bw is 0.1Gbps
    #calculate rate (lambda) of the Poisson process representing flow arrivals
    rate = (bw*load*(1000000000) / (numhosts*meanFlowSize*1000*8.0))
    start = time.time()
    sender.setTimers(start, runtime)
    while (time.time() - start) < runtime:
        #the inter-arrival time for a Poisson process of rate L is exponential with rate L
        waittime = random.expovariate(rate)
        time.sleep(waittime)

        output = sender.sendRoutine()
        if output is not None: 
            flowSize =  output[0]
            FCT = output[1]
     
            result = "{} {}\n".format(flowSize, FCT)

            #write flowSize and completion time to file named by 'load'
            with open(outfile, "a") as f:
                while True:
                    try:
                        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB) #lock the file
                        break
                    except IOError as e:
                        if e.errno != errno.EAGAIN:                    
                            raise
                        else:
                            time.sleep(0.1)
               
                f.write(result)
                fcntl.flock(f, fcntl.LOCK_UN)

        

if __name__== '__main__':
    main()


