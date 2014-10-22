#!/usr/bin/env python

from __future__ import division

import sys
import random
import sqlite3
from itertools import product
from heapq import *

from twisted.spread import pb
from twisted.internet import reactor

from stats import BernoulliEstimator

def dict_to_key(d):
    return "".join([str(k)+str(d[k]) for k in sorted(d.keys())])

class Experiment(object):
    def __init__(self, dbfile, params, metrics, valid = lambda x: True, tblname = 'results', mode = 'c'):
        self.dbconn = sqlite3.connect(dbfile)
        self.db = self.dbconn.cursor()
        self.tblname = tblname

        self.params = params
        self.factors = params.keys()
        self.metrics = metrics
        self.cols = self.factors + metrics + ["samples", "elapsed"]
        
        self.design_points = filter(valid,
                    [DesignPoint(self, dict(zip(params.keys(),val)), metrics)
                        for val in product(*params.values())])
        self.dp_lookup = dict([(dict_to_key(dp.params), dp) for dp in self.design_points])

        if mode == 'n':
            self._drop_table()
            self._create_table()
        elif mode == 'c':
            self._create_table()
            self._resume()
        elif model == 'w':
            self._resume()
        else:
            print >> sys.stderr, "Invalid database mode"

    def _resume(self):
        for row in self.db.execute("SELECT {} FROM results".format(", ".join(self.cols))):
            data = dict(zip(self.cols, row))
            key = dict_to_key(dict([(k, data[k]) for k in self.factors]))
            metrics = dict([(k, data[k]) for k in self.metrics])
            samples = data['samples']
            elapsed = data['elapsed']

            if self.dp_lookup.has_key(key):
                dp = self.dp_lookup[key]
                dp.samples = samples
                dp.elapsed = elapsed
                for metric, stat in dp.data.iteritems():
                    stat.mean = metrics[metric]
                    stat.count = samples

    def _drop_table(self):
        self.db.execute("DROP TABLE IF EXISTS {}".format(self.tblname))

    def _create_table(self):
        cols_types = [("id", "INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL")]
        cols_types += [(col,self._determine_type(self.params[col])) for col in self.params.keys()]
        cols_types += [(m, "DOUBLE") for m in self.metrics]
        cols_types += [("samples", "INT"), ("elapsed", "DOUBLE")]
        cols_types = ", ".join([col + " " + typ for (col,typ) in cols_types])
        self.db.execute("CREATE TABLE IF NOT EXISTS {} ({})".format(self.tblname, cols_types))

        self.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS dp ON {} ({})".format(
            self.tblname, ", ".join(self.factors)))

    def _determine_type(self, lst):
        bytes = 0
        is_str = False
        for x in lst:
            bytes = max(len(str(x)), bytes)
            if type(x) is not type(lst[0]):
                print "WARNING: converting to string type"
                is_str = True

        if is_str or type(lst[0]) is str:
            return "CHARACTER({})".format(bytes)
        elif type(lst[0]) is int:
            return "INT"
        elif type(lst[0]) is float:
            return "DOUBLE"
        else:
            print >> sys.stderr, "ERROR: unsupported type {}".format(str(type(lst[0])))
            return None

    def quote(self, val):
        if type(val) is str:
            return "'{}'".format(val)
        else:
            return str(val)

    def update(self, dp):
        values = ",".join([self.quote(x) for x in dp.params.itervalues()] + 
                [str(x.mean) for x in dp.data.itervalues()] +
                [str(dp.samples), str(dp.elapsed)])
        query = "INSERT OR REPLACE INTO {} ({}) VALUES ({})".format(
                self.tblname, ", ".join(self.cols), values)
        self.db.execute(query)
        self.dbconn.commit()

    def close(self):
        self.dbconn.close()

class DesignPoint(object):
    def __init__(self, exp, params, metrics, elapsed = 0, samples = 0, id = None):
        self.exp = exp
        self.params = params
        self.data = dict([(m, BernoulliEstimator()) for m in metrics])
        self.elapsed = elapsed
        self.samples = samples
        self.id = id

    def __lt__(self, other):
        if self.samples == 0:
            return True
        elif other.samples == 0:
            False
        else:
            (self.elapsed / self.samples) > (other.elapsed / other.samples)
    
    def complete(self, min_samples, max_samples, confidence, max_ci):
        for metric, stat in self.data.iteritems():
            if stat.count < min_samples:
                return False
            elif stat.count > max_samples:
                return True
            elif stat.confidence_interval(confidence) > max_ci:
                return False
        return True

    def update(self, trials, elapsed, results):
        self.samples += trials

        self.elapsed += elapsed

        for k, v in results.iteritems():
            self.data[k].add_samples(v, trials)

        self.exp.update(self)

    def time_remaining(self, min_samples, max_samples, confidence, interval):
        if self.samples > 0:
            count = self.samples_remaining(min_samples,
                                           max_samples,
                                           confidence,
                                           interval)
            return count * (self.elapsed / self.samples)
        else:
            # this isn't accurate, but we have no way to estimate
            return 0

    def samples_remaining(self, min_samples, max_samples, confidence, interval):
        remaining = max(s.samples_remaining(confidence, interval) for s in self.data.itervalues())

        return max(min_samples - self.samples,
                   min(max_samples - self.samples, remaining))

    def __getitem__(self, item):
        return self.params[item]

class ExperimentManager(pb.Root):
    def __init__(self, experiment,
                       seed = 12345,
                       block_size = 30,
                       min_samples = 100,
                       max_samples = 1000,
                       confidence = 0.95,
                       max_ci = 0.025):
        self.experiment = experiment

        random.seed(seed)

        self.clients = 0
        
        self.block_size = block_size
        self.min_samples = min_samples
        self.max_samples = max_samples
        self.confidence = confidence
        self.max_ci = max_ci

        self.work_queue = []
        # save completed so that they can be later added back to the work queue
        # if the termination conditions are changed.
        self.completed = []

        for dp in self.experiment.design_points:
            if dp.complete(self.min_samples,
                            self.max_samples,
                            self.confidence,
                            self.max_ci):
                self.completed.append(dp)
            else:
                self.work_queue.append(dp)

        heapify(self.work_queue)

        self.outstanding = {}
    
    def start(self, port, reactor_running = False):
        reactor.listenTCP(port, pb.PBServerFactory(self))
        if not reactor_running:
            reactor.run()

    def remote_client_add(self):
        self.clients += 1

    def remote_client_leave(self):
        self.clients -= 1
        if self.clients == 0 and len(self.work_queue) == 0:
            print "Shutting down the server. Bye."
            reactor.callLater(1, reactor.stop)

    def remote_next_dp(self):
        if len(self.work_queue) > 0:
            dp = heappop(self.work_queue)
            self.outstanding[dict_to_key(dp.params)] = dp
            if dp.samples == 0:
                trials = self.min_samples
            else:
                remaining = dp.samples_remaining(self.min_samples,
                                           self.max_samples,
                                           self.confidence,
                                           self.max_ci)
                trials = max(remaining // 2, self.block_size)
            return dp.params, trials

        else:
            return None, 0

    def remote_record_data(self, dp, trials, elapsed, results):
        dpo = self.outstanding[dict_to_key(dp)]
        dpo.update(trials, elapsed, results)

        del(self.outstanding[dict_to_key(dp)])

        if dpo.complete(self.min_samples,
                        self.max_samples,
                        self.confidence,
                        self.max_ci):
            self.completed.append(dpo)
        else:
            heappush(self.work_queue, dpo)

    def estimate_time_remaining(self):
        if self.clients > 0:
            return sum(dp.time_remaining(self.min_samples,
                                         self.max_samples,
                                         self.confidence,
                                         self.max_ci) for dp in self.work_queue) / self.clients
        else:
            return sum(dp.time_remaining(self.min_samples,
                                         self.max_samples,
                                         self.confidence,
                                         self.max_ci) for dp in self.work_queue)

    def remote_status(self):
        untested = sum([1 for d in self.work_queue if d.samples == 0])
        retval  = "Remaining design points: {}\n".format(len(self.work_queue))
        retval += "Completed design points: {}\n".format(len(self.completed))
        retval += "Estimated time remaining: {}s\n".format(self.estimate_time_remaining())
        if untested > 0:
            retval += "Estimate may be innaccurate: {} design points have not been tested at all.".format(untested)
        return retval+"\n"

    def remote_update_server(self, entries):
        before = self.estimate_time_remaining()

        self.__dict__.update(entries)

        work_queue = []
        completed = []

        for dp in [d for (t, d) in self.work_queue] + self.completed:
            if dp.complete(self.min_samples,
                            self.max_samples,
                            self.confidence,
                            self.max_ci):
                completed.append(dp)
            else:
                work_queue.append(dp)

        heapify(work_queue)
        self.work_queue = work_queue
        self.completed = completed

        after = self.estimate_time_remaining()

        return "Estimated time remaining: before - {}s, after - {}s".format(before, after)

class SchedulabilityClient(object):
    def __init__(self, dpfactory, server, port, local_buffer = 2):
        self.dpfactory = dpfactory

        factory = pb.PBClientFactory()
        reactor.connectTCP(server, port, factory)

        d = factory.getRootObject()
        d.addCallback(self.set_root)
        d.addErrback(self.broken)

        self.count = 0
        self.backlog = 0
        self.local_buffer = local_buffer

    def set_root(self, root):
        self.manager = root
        self.manager.callRemote("client_add")
        self.fetch_first_work()

    def fetch_first_work(self):
        for _ in xrange(self.local_buffer):
            self.next()

    def next(self):
        next = self.manager.callRemote("next_dp")
        next.addCallback(self.process_design_point)
        next.addErrback(self.broken)
        self.backlog += 1

    def process_design_point(self, (dp_dict, trials)):
        self.backlog -= 1
        if dp_dict:
            dp = self.dpfactory.build_design_point(trials, **dp_dict)
            elapsed, results = dp.run()
            self.count += 1
            self.manager.callRemote("record_data", dp_dict, trials, elapsed, results)
            self.next()
        else:
            if self.backlog == 0:
                shutdown = self.manager.callRemote("client_leave")
                shutdown.addCallback(self.shutdown)

    def shutdown(self, clients):
        reactor.stop()
    
    def broken(self, reason):
        print reason.printTraceback()
        reactor.stop()

    def start(self, reactor_running = False):
        if not reactor_running:
            reactor.run()
