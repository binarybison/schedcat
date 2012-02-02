#ifndef TASKS_H
#define TASKS_H

#ifndef SWIG

#include <vector>
#include <algorithm>

#include <gmpxx.h>

#include <math.h>

#endif

class Task
{
  private:
    unsigned long period;
    unsigned long wcet;
    unsigned long deadline;

  public:

    /* construction and initialization */
    void init(unsigned long wcet, unsigned long period, unsigned long deadline = 0);
    Task(unsigned long wcet = 0,
         unsigned long period = 0,
         unsigned long deadline = 0) { init(wcet, period, deadline); }

    /* getter / setter */
    unsigned long get_period() const { return period;   }
    unsigned long get_wcet() const   { return wcet;     }
    /* defaults to implicit deadline */
    unsigned long get_deadline() const {return deadline; }

    void set_period(unsigned long period)     { this->period   = period;   }
    void set_wcet(unsigned long wcet)         { this->wcet     = wcet;     }
    void set_deadline(unsigned long deadline) { this->deadline = deadline; }

    /* properties */
    bool has_implicit_deadline() const;
    bool has_constrained_deadline() const;
    bool is_feasible() const;


    void get_utilization(mpq_class &util) const;
    void get_density(mpq_class &density) const;

    // Demand bound function (DBF) and LOAD support.
    // This implements Fisher, Baker, and Baruah's PTAS

    unsigned long bound_demand(unsigned long time) const
    {
        if (time <= deadline)
            return 0;
        else
        {
            unsigned long jobs;

            time -= deadline;
            jobs = time / period; // implicit floor in integer division
            jobs += 1;
            return jobs * wcet;
        }
    }

    void bound_demand(const mpz_class &time, mpz_class &demand) const
    {
        if (time <= deadline)
            demand = 0;
        else
        {
            demand = time;
            demand -= deadline;

            demand /= period; // implicit floor in integer division
            demand += 1;
            demand *= wcet;
        }
    }

    void bound_load(const mpz_class &time, mpq_class &load) const
    {
        mpz_class demand;

        if (time > 0)
        {
            bound_demand(time, demand);
            load = demand;
            load /= time;
        }
        else
            load = 0;
    }

    unsigned long approx_demand(unsigned long time, unsigned int k) const
    {
        if (time < k * period + deadline)
            return bound_demand(time);
        else
        {
            double approx = time - deadline;
            approx *= wcet;
            approx /= period;

            return wcet + (unsigned long) ceil(approx);
        }
    }

    void approx_demand(const mpz_class &time, mpz_class &demand,
                       unsigned int k) const
    {
        if (time < k * period + deadline)
            bound_demand(time, demand);
        else
        {
            mpz_class approx;

            approx = time;
            approx -= deadline;
            approx *= wcet;

            mpz_cdiv_q_ui(demand.get_mpz_t(), approx.get_mpz_t(), period);

            demand += wcet;
        }
    }

    void approx_load(const mpz_class &time, mpq_class &load,
                     unsigned int k) const
    {
        mpz_class demand;

        if (time > 0)
        {
            approx_demand(time, demand, k);
            load = demand;
            load /= time;
        }
        else
            load = 0;
    }
};

typedef std::vector<Task> Tasks;

class TaskSet
{
  private:
    Tasks tasks;
  
    unsigned long k_for_epsilon(unsigned int idx, const mpq_class &epsilon) const;

  public:
    TaskSet();
    TaskSet(const TaskSet &original);
    virtual ~TaskSet();

    void add_task(unsigned long wcet, unsigned long period, unsigned long deadline = 0)
    {
        tasks.push_back(Task(wcet, period, deadline));
    }

    unsigned int get_task_count() const { return tasks.size(); }

    Task& operator[](int idx) { return tasks[idx]; }

    const Task& operator[](int idx) const { return tasks[idx]; }

    bool has_only_implicit_deadlines() const;
    bool has_only_constrained_deadlines() const;
    bool has_only_feasible_tasks() const;
    bool is_not_overutilized(unsigned int num_processors) const;

    void get_utilization(mpq_class &util) const;
    void get_density(mpq_class &density) const;
    void get_max_density(mpq_class &max_density) const;

    void approx_load(mpq_class &load, const mpq_class &epsilon = 0.1) const;

    /* wrapper for Python access */
    unsigned long get_period(unsigned int idx) const
    {
        return tasks[idx].get_period();
    }

    unsigned long get_wcet(unsigned int idx) const
    {
        return tasks[idx].get_wcet();
    }

    unsigned long get_deadline(unsigned int idx) const
    {
        return tasks[idx].get_deadline();
    }
};



#endif