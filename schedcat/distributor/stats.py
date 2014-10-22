from __future__ import division
import scipy.stats
from math import sqrt, ceil

class BernoulliEstimator(object):
    def __init__(self, default_mean = 0, default_count = 0):
        self.mean = default_mean
        self.count = default_count

    def add_sample(self, sample):
        self.mean = (self.mean*self.count + sample)/(self.count+1)
        self.count+=1

    def add_samples(self, avg, samples):
        self.mean = (self.mean*self.count + avg*samples)/(self.count + samples)
        self.count += samples

    def confidence_interval(self, confidence = 0.95):
        if self.count == 0:
            return 1
        else:
            zalpha = scipy.stats.t.ppf((1+confidence)/2.0, self.count-1)
            return zalpha * sqrt(self.mean*(1-self.mean)/self.count)

    def samples_remaining(self, confidence = 0.95, interval = 0.025):
        if self.count > 1:
            zalpha = scipy.stats.t.ppf((1+confidence)/2.0, self.count - 1)
        else:
            zalpha = 2
        return int(ceil(zalpha**2 * self.mean * (1 - self.mean) / (interval**2)))
