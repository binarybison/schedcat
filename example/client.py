#!/usr/bin/env python

from __future__ import division

import argparse
import random
import math
import time

from twisted.spread import pb
from twisted.internet import reactor
from copy import deepcopy

import schedcat.mapping.binpack as bp
import schedcat.model.resources as resources
import schedcat.generator.tasks as tasks
import schedcat.locking.bounds as bounds
import schedcat.sched.fp as fp
from schedcat.model.tasks import SporadicTask, TaskSystem
from schedcat.generator.tasksets import NAMED_PERIODS, NAMED_UTILIZATIONS
from schedcat.distributor.stats import BernoulliEstimator
from schedcat.distributor.rpc import SchedulabilityClient

INFINITY = 999999

class DesignPointFactory(object):
    def build_design_point(self, trials, **levels):
        return DesignPointRunner(trials, **levels)

class DesignPointRunner(object):
    metrics = ["OMLP", "FMLP"]
    tests = []

    def __init__(self, trials, **levels):
        self.__dict__.update(levels)
        self.trials = trials 
        self.levels = levels
        self.data = dict([(m, BernoulliEstimator()) for m in DesignPointRunner.metrics])

    def run(self):
        start = time.clock()
        for _ in xrange(self.trials):
            org_ts = self.create_task_set()

            for test in DesignPointRunner.tests:
                ts = deepcopy(org_ts)
                test(self, ts)

        elapsed = time.clock() - start

        return (elapsed, dict([(k,v.mean) for (k,v) in self.data.iteritems()]))

    def fmlp(self, ts):
        # overapproximation needed for blocking-bound calculations
        for t in ts:
            t.response_time = t.period

        bounds.apply_part_fmlp_bounds(ts)
        res = fp.is_schedulable(1, ts)

        self.data["FMLP"].add_sample(res)
        return res

    tests.append(fmlp)

    def omlp(self, ts):
        # overapproximation needed for blocking-bound calculations
        for t in ts:
            t.response_time = t.period

        bounds.apply_clustered_omlp_bounds(ts, 1)
        res = fp.is_schedulable(1, ts)

        self.data["OMLP"].add_sample(res)
        return res

    tests.append(omlp)

    def create_task_set(self):
        tg = tasks.TaskGenerator(period = NAMED_PERIODS[self.period],
                                util   = NAMED_UTILIZATIONS[self.task_util])

        ts = tg.make_task_set(max_util = self.sys_util, squeeze = True)

        resources.initialize_resource_model(ts)

        for t in ts:
            t.resmodel[0].add_request(10)

        bounds.assign_fp_preemption_levels(ts)

        for i, partition in enumerate(bp.worst_fit(ts, self.processors,
            weight = SporadicTask.utilization,
            capacity = INFINITY,
            empty_bin = TaskSystem)):
            for t in partition:
                t.partition = i

        return ts

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', "--port", default=1234,
                        help = "Port on which to connect to server")
    parser.add_argument('-s', "--server", default="localhost",
                        help = "Server from which to pull design points")
    args = parser.parse_args()

    dpfactory = DesignPointFactory()
    SchedulabilityClient(dpfactory, args.server, args.port).start()

if __name__ == '__main__':
    main()
