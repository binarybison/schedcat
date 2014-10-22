import argparse
import sys
from numpy import arange

from schedcat.distributor.rpc import Experiment, ExperimentManager

def myrange(start, end, inc):
    return [float(x) for x in arange(start, end+inc, inc)]

def valid(dp):
    return dp['sys_util'] <= dp['processors']

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', "--database", default = "results.db",
                        help = "sqlite3 database file")
    parser.add_argument('-p', "--port", default=1234,
                        help = "Port to listen on (default= 1234)")
    parser.add_argument('-s', "--seed", default="12345",
                        help = "Seed for the random-number generator.")
    parser.add_argument('-r', "--restart", action='store_true',
                        help = "Restart the experiment and wipe the database.")
    args = parser.parse_args()

    params = {}
    params['processors'] = [4]
    params['task_util'] = ['uni-light', 'uni-medium', 'uni-heavy']
    params['period'] = ['uni-short', 'uni-moderate', 'uni-long']
    params['sys_util'] = myrange(0.1, 4.0, 0.1)

    metrics = ["OMLP", "FMLP"]

    if args.restart:
        mode = 'n'
    else:
        mode = 'c'
    exp = Experiment(args.database, params, metrics, valid = valid, mode = mode)

    ExperimentManager(exp, args.seed,
                      block_size = 10,
                      min_samples = 100,
                      max_samples = 5000,
                      confidence = 0.95,
                      max_ci = 0.025).start(args.port)

if __name__ == '__main__':
    main()
