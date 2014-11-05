#!/usr/bin/env python

from __future__ import division

import sys
import random
from itertools import product
from heapq import *
from math import ceil

from twisted.spread import pb
from twisted.internet import reactor
from twisted.enterprise import adbapi

from stats import BernoulliEstimator

def dict_to_key(d):
    return "".join([str(k)+str(d[k]) for k in sorted(d.keys())])

class Experiment(object):
    def __init__(self, dbfile, params, metrics, checker,
                 valid = lambda x: True,
                 tblname = 'results',
                 mode = 'c'):

        self.dbpool = adbapi.ConnectionPool("sqlite3", dbfile, cp_max = 1, check_same_thread = False)
        self.tblname = tblname
        self.checker = checker

        self.params = params
        self.factors = params.keys()
        self.metrics = metrics
        self.cols = self.factors + metrics + ["samples", "elapsed"]
        
        self.design_points = filter(valid,
                    [DesignPoint(self, dict(zip(params.keys(),val)), metrics, checker)
                        for val in product(*params.values())])
        self.dp_lookup = dict([(dict_to_key(dp.params), dp) for dp in self.design_points])

        if mode == 'n':
            self.dbpool.runInteraction(self._reset_db)
        elif mode == 'c':
            self.dbpool.runInteraction(self._create_or_resume)
        elif model == 'w':
            self.dbpool.runInteraction(self._resume)
        else:
            print >> sys.stderr, "Invalid database mode"

    def _reset_db(self, txn):
        self._drop_table(txn)
        self._create_table(txn)
    
    def _drop_table(self, txn):
        txn.execute("DROP TABLE IF EXISTS {}".format(self.tblname))
    
    def _create_table(self, txn):
        cols_types = [("id", "INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL")]
        cols_types += [(col,self._determine_type(self.params[col])) for col in self.params.keys()]
        cols_types += [(m, "DOUBLE") for m in self.metrics]
        cols_types += [("samples", "INT"), ("elapsed", "DOUBLE")]
        cols_types = ", ".join([col + " " + typ for (col,typ) in cols_types])

        txn.execute("CREATE TABLE IF NOT EXISTS {} ({})".format(self.tblname, cols_types))
        txn.execute("CREATE UNIQUE INDEX IF NOT EXISTS dp ON {} ({})".format(
                    self.tblname, ", ".join(self.factors)))

    def _create_or_resume(self, txn):
        self._create_table(txn)
        self._resume(txn)

    def _resume(self, txn):
        for row in txn.execute("SELECT id, {} FROM results".format(", ".join(self.cols))):
            data = dict(zip(["id"] + self.cols, row))
            key = dict_to_key(dict([(k, data[k]) for k in self.factors]))
            metrics = dict([(k, data[k]) for k in self.metrics])
            samples = data['samples']
            elapsed = data['elapsed']
            id = data['id']

            if self.dp_lookup.has_key(key):
                dp = self.dp_lookup[key]
                dp.id = id
                dp.samples = samples
                dp.elapsed = elapsed
                for metric, stat in dp.data.iteritems():
                    stat.mean = metrics[metric]
                    stat.count = samples

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

    def _insert(self, txn, dp):
        values = ",".join([self.quote(dp.params[x]) for x in self.factors] +
                [str(dp.data[x].mean) for x in self.metrics] +
                [str(dp.samples), str(dp.elapsed)])
        query = "INSERT INTO {} ({}) VALUES ({})".format(
                self.tblname, ", ".join(self.cols), values)
        txn.execute(query)
        txn.execute("SELECT last_insert_rowid()")
        id = txn.fetchone()[0]
        dp.id = id
        return id

    def _update(self, dp):
        values = [(metric, dp.data[metric].mean) for metric in self.metrics]
        values += [("samples", dp.samples)]
        values += [("elapsed", dp.elapsed)]
        valstr = ", ".join(["{} = {}".format(lhs,rhs) for lhs,rhs in values])
        query = "UPDATE {} SET {} WHERE id = {}".format(
                self.tblname, valstr, dp.id)
        return self.dbpool.runQuery(query)

    def save(self, dp):
        if dp.id:
            return self._update(dp)
        else:
            return self.dbpool.runInteraction(self._insert, dp)

    def close(self):
        self.dbconn.close()

class CompletionChecker(object):

    def __init__(self, min_samples, confidence, max_ci):
        self.min_samples = min_samples
        self.confidence = confidence
        self.max_ci = max_ci
        self.max_samples = self._max_samples()

    # A few relevant examples:
    # confidence: 0.95, max_ci = 0.05  => 1540
    # confidence: 0.95, max_ci = 0.025 => 6149
    # confidence: 0.95, max_ci = 0.01  => 38418
    def _max_samples(self):
        import scipy.stats
        old = 0
        new = self.min_samples
        while abs(new - old) > 0.0001:
            old = new
            new = (scipy.stats.t.ppf((1+self.confidence)/2.0, old - 1) / self.max_ci) ** 2

        return int(ceil(new))
    
    def complete(self, dp):
        for metric, stat in dp.data.iteritems():
            if stat.count < self.min_samples:
                return False
            elif stat.confidence_interval(self.confidence) > self.max_ci:
                return False
        return True

    def remaining(self, dp):
        remaining = max(s.samples_remaining(self.confidence, self.max_ci)
                            for s in dp.data.itervalues())

        return max(self.min_samples - dp.samples,
                   min(self.max_samples - dp.samples, remaining))

class DesignPoint(object):

    total_elapsed = 0
    total_samples = 0

    def __init__(self, exp, params, metrics, checker, elapsed = 0, samples = 0, id = None):
        self.exp = exp
        self.params = params
        self.data = dict([(m, BernoulliEstimator()) for m in metrics])
        self.elapsed = elapsed
        self.samples = samples
        self.id = id

        DesignPoint.total_elapsed += elapsed
        DesignPoint.total_samples += samples

    def __lt__(self, other):
        if self.samples == 0:
            return True
        elif other.samples == 0:
            return False
        else:
            return (self.elapsed / self.samples) > (other.elapsed / other.samples)
    
    def update(self, trials, elapsed, results):
        self.samples += trials
        DesignPoint.total_samples += trials

        self.elapsed += elapsed
        DesignPoint.total_elapsed += elapsed

        for k, v in results.iteritems():
            self.data[k].add_samples(v, trials)

        self.exp.save(self)

    def time_remaining(self):
        if self.samples > 0:
            count = self.exp.checker.remaining(self)
            return count * (self.elapsed / self.samples)
        else:
            if DesignPoint.total_samples > 0:
                return (self.exp.checker.max_samples + self.exp.checker.min_samples)/2.0 *\
                        (DesignPoint.total_elapsed / DesignPoint.total_samples)
            else:
                return 0

    def __getitem__(self, item):
        return self.params[item]

    def __str__(self):
        retval = ", ".join(["{}: {}".format(k,v) for k,v in self.params.iteritems()])
        retval += "\n"
        retval += ", ".join(["{}: {}".format(k,v.mean) for k,v in self.data.iteritems()])
        retval += "\nSamples: {}, Elapsed: {}".format(self.samples, self.elapsed)
        return retval

class ExperimentManager(pb.Root):
    def __init__(self, experiment,
                       checker,
                       seed = 12345,
                       min_block_time = 1,
                       client_params = []):
        self.experiment = experiment
        self.client_params = client_params

        random.seed(seed)

        self.clients = 0

        self.min_block_time = min_block_time
        self.min_samples = checker.min_samples
        self.checker = checker
        
        self.work_queue = []
        # save completed so that they can be later added back to the work queue
        # if the termination conditions are changed.
        self.completed = []

        for dp in self.experiment.design_points:
            if self.checker.complete(dp):
                self.completed.append(dp)
            else:
                heappush(self.work_queue, dp)

        self.outstanding = {}
    
    def start(self, port, reactor_running = False):
        reactor.listenTCP(port, pb.PBServerFactory(self))
        if not reactor_running:
            reactor.run()

    def remote_client_add(self):
        self.clients += 1

    def remote_request_client_params(self):
        return self.client_params

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
                remaining = self.checker.remaining(dp)
                by_time = int((self.min_block_time * dp.samples) / dp.elapsed)
                trials = max(remaining // 2, by_time)
            return dp.params, trials

        else:
            return None, 0

    def remote_record_data(self, dp, trials, elapsed, results):
        dpo = self.outstanding[dict_to_key(dp)]
        dpo.update(trials, elapsed, results)

        del(self.outstanding[dict_to_key(dp)])

        if self.checker.complete(dpo):
            self.completed.append(dpo)
        else:
            heappush(self.work_queue, dpo)

    def estimate_time_remaining(self):
        if self.clients > 0:
            return sum(dp.time_remaining() for dp in self.work_queue) / self.clients
        else:
            return sum(dp.time_remaining() for dp in self.work_queue)

    def remote_status(self):
        untested = sum([1 for d in self.work_queue if d.samples == 0])
        retval = "Untested/Completed/Total {}/{}/{}\n".format(
                    untested,
                    len(self.completed),
                    len(self.experiment.design_points))
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
            if self.checker.complete(dp):
                completed.append(dp)
            else:
                heappush(work_queue, dp)

        self.work_queue = work_queue
        self.completed = completed

        after = self.estimate_time_remaining()

        return "Estimated time remaining: before - {}s, after - {}s".format(before, after)

class SchedulabilityClient(object):
    def __init__(self, DesignPointRunner, server, port, local_buffer, recv_client_params=None):
        self.DesignPointRunner = DesignPointRunner

        factory = pb.PBClientFactory()
        reactor.connectTCP(server, port, factory)

        d = factory.getRootObject()
        d.addCallback(self.set_root)
        d.addErrback(self.broken)

        self.count = 0
        self.backlog = 0
        self.local_buffer = local_buffer
        self.recv_client_params = recv_client_params

    def set_root(self, root):
        self.manager = root
        self.manager.callRemote("client_add")
        if self.recv_client_params is not None:
            client_params = self.manager.callRemote("request_client_params")
            client_params.addCallback(self.recv_client_params)
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
            dp = self.DesignPointRunner(trials, **dp_dict)
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
