import schedcat.locking.native as cpp

class Resource(object):
    def __init__(self, short):
        self.short = short

#Create a lock object for all independant locks. For example, each resource
#controlled by the global OMLP is a separate instance, where as a single Lock
#object should be created for all resource controlled by the C-OMLP.
class Lock(object):
    def __init__(self, type, resources = None):

        self.type = type

        if resources:
            self.resources = resources
        else:
            self.resoruces = {}

    def apply_blocking_bounds(self, *args, **kwargs):
        if hasattr(self, 'apply_'+self.type+'_bounds'):
            getattr(self, 'apply_'+self.type+'_bounds')(*args, **kwargs)
        else:
            raise TypeError("Unknown Lock type: %s" % self.type)

    def apply_suspension_oblivious(self, all_tasks, res):
        for i,t in enumerate(all_tasks):
            # s-oblivious <=> no suspension. We don't set t.suspended = 0,
            # because t.suspended may have been previously set by another lock.

            # all blocking, including arrival blocking
            if hasattr(t, "blocked"):
                t.blocked += res.get_blocking_term(i)
            else:
                t.blocked = res.get_blocking_term(i)

            if hasattr(t, "arrival_blocking"):
                t.arrival_blocking += res.get_arrival_blocking(i)
            else:
                t.arrival_blocking = res.get_arrival_blocking(i)

            t.cost += res.get_blocking_term(i)

    apply_spin_based = apply_suspension_oblivious

    def cpp_mutex_model(self, all_tasks, use_task_period = False):
        rsi = cpp.ResourceSharingInfo(len(all_tasks))
        for t in all_tasks:
            rsi.add_task(t.period,
                         t.period if use_task_period else t.response_time,
                         t.partition,
                         t.locking_prio)
            for id, resource in enumerate(t.resmodel[self]):
                req = t.resmodel[self][resource]
                rsi.add_request(id, req.max_requests, req.max_length)
        return rsi

    def apply_global_omlp_bounds(self, all_tasks, num_cpus,
            use_task_period = False):
        model = self.cpp_mutex_model(all_tasks, use_task_period)
        res = cpp.global_omlp_bounds(model, num_cpus)
        self.apply_suspension_oblivious(all_tasks, res)

    def apply_global_fmlp_soblivious_bounds(self, all_tasks,
            use_task_period = False):
        model = self.cpp_mutex_model(all_tasks, use_task_period)
        res = cpp.global_fmlp_bounds(model)
        self.apply_suspension_oblivious(all_tasks, res)

    #port a bunch of the code in locking/bounds.py here.

class ResourceRequirement(object):
    def __init__(self, res_id, num_writes=1, write_length=1,
                 num_reads=0, read_length=0, nest_short = False):
        self.res_id           = res_id
        self.max_writes       = num_writes
        self.max_reads        = num_reads
        self.max_write_length = write_length
        self.max_read_length  = read_length
        self.nest_short       = nest_short

    @property
    def max_requests(self):
        "Number of requests of any kind."
        return self.max_writes + self.max_reads

    @property
    def max_length(self):
        "Maximum request length (of any kind)."
        return max(self.max_write_length, self.max_read_length)

    def add_request(self, length, read=False):
        "Increase requirements."
        if read:
            self.max_reads += 1
            self.max_read_length = max(self.max_read_length, length)
        else:
            self.max_writes += 1
            self.max_write_length = max(self.max_write_length, length)


    def add_read_request(self, length):
        self.add_request(length, True)

    def add_write_request(self, length):
        self.add_request(length, False)

    def convert_reads_to_writes(self):
        self.max_writes = self.max_requests
        self.max_write_length = self.max_length
        self.max_reads = 0
        self.max_read_length = 0


class ResourceRequirements(dict):
    def __missing__(self, key):
        self[key] = ResourceRequirement(key, 0, 0, 0, 0)
        return self[key]

class LockRequests(dict):
    def __missing__(self, key):
        self[key] = ResourceRequirements()
        return self[key]

def initialize_resource_model(taskset):
    for t in taskset:
        # mapping of res_id to ResourceRequirement object
        t.resmodel   = LockRequests()

#def apply_blocking_bounds(taskset):
#    locks = set()
#    for t in taskset:
#        for lock in t.resmodel:
#            locks.add(lock)
#
#    for lock in locks:
#        lock.apply_blocking_bounds(taskset)
#
# There needs to be a better way of incorporating arrival blocking bounds.
# Arrival blocking is not necessarily additive between locks, but instead, a job
# can be arrival blocked by at most m jobs, though those jobs may be accessing
# different locks. For example, consider a system in which there are x < m jobs
# that require a C-OMLP resource, and there are a y other jobs that acquire a
# spin-lock such that x + y > m. Then a job can be arrival blocked by the
# longest m requests for either of these resources. However, the way things are
# accounted now, we count the top x + y requests. While this is a safe
# approximation, it can be improved upon.
