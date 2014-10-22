#!/usr/bin/env python

import argparse

from twisted.spread import pb
from twisted.internet import reactor
from twisted.python import util

frequency = 0

def broken(reason):
    print reason.getErrorMessage()
    print reason.type
    reactor.stop()

def print_and_stop(status):
    print status
    reactor.stop()

def get_status(root):
    try:
        d = root.callRemote("status")
        if frequency == 0:
            d.addCallback(print_and_stop)
        else:
            d.addCallback(util.println)
            reactor.callLater(1, get_status, root)
        d.addErrback(broken)
    except:
        print "Connection to experiment server lost"
        reactor.stop()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', "--port", default=1234,
                        help = "Port on which to connect to server")
    parser.add_argument('-s', "--server", default="localhost",
                        help = "Server from which to pull design points")
    parser.add_argument('-f', "--frequency", default=0, type=float,
                        help = "Frequecy to poll for the server status.")
    args = parser.parse_args()

    factory = pb.PBClientFactory()
    reactor.connectTCP(args.server, args.port, factory)

    global frequency
    frequency = args.frequency

    d = factory.getRootObject()
    d.addCallback(get_status)
    d.addErrback(broken)

    reactor.run()

if __name__ == '__main__':
    main()
