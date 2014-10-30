#!/usr/bin/env python

import argparse

from twisted.spread import pb
from twisted.internet import reactor
from twisted.python import util

parameters = {}

def broken(reason):
    print reason.getErrorMessage()
    print reason.type
    reactor.stop()

def print_and_stop(msg):
    print msg
    reactor.stop()

def update_server(root):
    global parameters
    d = root.callRemote("update_server", parameters)
    d.addCallback(print_and_stop)
    d.addErrback(broken)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', "--port", default=1234,
                        help = "Port on which to connect to server")
    parser.add_argument('-s', "--server", default="localhost",
                        help = "Server from which to pull design points")
    parser.add_argument('-b', "--block", type=int,
                        help = "Minimum block size for dispatching work")
    parser.add_argument('-l', '--min-samples', type=int,
                        help = "Minimum number of samples for each design point.")
    parser.add_argument('-u', '--max-samples', type=int,
                        help = "Maximum number of samples for each design point.")
    parser.add_argument('-c', '--confidence', type=float,
                        help = "Confidence level for estimation.")
    parser.add_argument('-i', '--interval', type=float,
                        help = "Maximum confidence-interval length.")
    args = parser.parse_args()

    factory = pb.PBClientFactory()
    reactor.connectTCP(args.server, args.port, factory)

    global parameters
    if args.block:
        parameters['block_size'] = args.block
    if args.min_samples:
        parameters['min_samples'] = args.min_samples
    if args.max_samples:
        parameters['max_samples'] = args.max_samples
    if args.confidence:
        parameters['confidence'] = args.confidence
    if args.interval:
        parameters['max_ci'] = args.interval

    print parameters

    d = factory.getRootObject()
    d.addCallback(update_server)
    d.addErrback(broken)

    reactor.run()

if __name__ == '__main__':
    main()
