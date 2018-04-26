# -*- coding: utf-8 -*-
"""
Created on Fri Apr 20 20:51:46 2018

@author: mitch
"""

from __future__ import division, print_function

from collections import defaultdict
from multiprocessing import Process, Queue, Pipe, Manager, cpu_count, freeze_support, Pool
import numpy as np
from abc import ABCMeta#if we need an abstract method, abstractmethod would use @abstractmethod on function
import pandas as pd
from functools import partial


REGULAR_RUN = 0
POOL_RUN = 1
PROCESS_RUN_MANAGER = 2
PROCESS_RUN_QUEUE = 3
PROCESS_RUN_PIPE = 4

class RunFactory(object):
    #__init__
    def __init__(self, run_type):
        self.run_type = run_type
        
    def factory(self):
        if self.run_type is REGULAR_RUN:
            return RegularRunType(self.run_type)
        elif self.run_type is POOL_RUN:
            return PoolTypeRun(self.run_type)
        elif self.run_type in [PROCESS_RUN_MANAGER, PROCESS_RUN_QUEUE, PROCESS_RUN_PIPE]:
            return ProcessTypeRun(self.run_type)
        ##else raise expection

##Would take in parent object for inheritence and use a__supoer__ in init            
class RegularRunType(object):
    def __init__(self, run_type):
        self.run_type = run_type
        
    def execute_runs(self, simulation_runs, number_months_retired, savings, monthly_allowance, stock_change_df):
        result_savings = defaultdict(list)
        for curr_index, allowance in enumerate(monthly_allowance):
                curr_perc = curr_index / len(monthly_allowance)
                print('Current Percentage Done Monthly Allowance Sims {}'.format(curr_perc))
                result_savings[allowance] = self.run_simulations(number_months_retired,
                                                                 savings,
                                                                 allowance,
                                                                 stock_change_df,
                                                                 simulation_runs)
        return result_savings

    def run_simulations(self, number_months_retired, savings, allowance, stock_change_df, simulation_runs): 
        num_rows,_ = stock_change_df.shape
        result_savings = []
        for num_runs in range(simulation_runs):
            local_savings = savings
            random_change = np.random.randint(0, num_rows, size=number_months_retired)
            for months in range(number_months_retired):
                monthly_change = stock_change_df.loc[random_change[months], 'MonthlyGainOrLoss']
                ##Apply Monthly Change
                local_savings += local_savings * monthly_change
                ##Take out our monthly allowance
                local_savings -= allowance
            result_savings.append(local_savings)
        return result_savings
    
class PoolTypeRun(object):
    def __init__(self, run_type):
        self.run_type = run_type
        
    def execute_runs(self, simulation_runs, number_months_retired, savings, monthly_allowance, stock_change_df):
        result_savings = defaultdict(list)
        for curr_index, allowance in enumerate(monthly_allowance):
            curr_perc = curr_index / len(monthly_allowance)
            print('Current Percentage Done Monthly Allowance Sims {}'.format(curr_perc))
            result_savings[allowance] = self.run_simulations(number_months_retired, 
                                                             savings, 
                                                             allowance,
                                                             stock_change_df, 
                                                             simulation_runs)
        return result_savings

    def run_simulations(self, number_months_retired, savings, allowance, stock_change_df, simulation_runs):
        ##provide run list as the iterable
        run_list = [runs for runs in range(simulation_runs)]
        pool = Pool()
        ##We cant pickle a function defined in our class, use one that is global
        partial_simulations_function = partial(simulation_run_format_pool,
                                               number_months_retired,
                                               savings,
                                               allowance,
                                               stock_change_df)
        results = (pool.map(partial_simulations_function, run_list))
        pool.close()
        pool.join()
        return results
##Needs to be defined outside class due to pool restrictions
def simulation_run_format_pool(number_months_retired, savings, allowance, stock_change_df, run_num = None):
    num_rows,_ = stock_change_df.shape
    random_change = np.random.randint(0, num_rows, size=number_months_retired)
    for months in range(number_months_retired):
        monthly_change = stock_change_df.loc[random_change[months], 'MonthlyGainOrLoss']
        ##Apply Monthly Change
        savings += savings * monthly_change
        # ##Take out our monthly allowance
        savings -= allowance
    return savings

class ProcessTypeRun(object):
    def __init__(self, run_type):
        self.run_type = run_type
    '''
    def run_test(self):
        Container = ContainerFactory(self.run_type).get_container()
        proc = Process(target=self.run_simulations_process_test,  args=(5, 1, 1, pd.read_csv(r'C:\Users\mitch\Desktop\Masters\DataMiningI\Python-Simulation\sp500.csv'), 5, Container))
        proc.start()
        proc.join()
        print(Container.get_output(1))
    '''
    #Execute process runs.
    def execute_runs(self, simulation_runs, number_months_retired, savings, monthly_allowance, stock_change_df):
        result_savings = defaultdict(list)
        for curr_index, allowance in enumerate(monthly_allowance):
            curr_perc = curr_index / len(monthly_allowance)
            print('Current Percentage Done Monthly Allowance Sims {}'.format(curr_perc))
            result_savings[allowance] = self.run_simulations(number_months_retired, savings, allowance, stock_change_df, simulation_runs)
        return result_savings

    def run_simulations(self, number_months_retired, savings, allowance, stock_change_df, simulation_runs):
        jobs = []
        Container = ContainerFactory(self.run_type).get_container()
        num_procs = cpu_count()
        split_run_size = simulation_runs // num_procs
        for num_splits in range(num_procs):
            proc = Process(target=self.simulation_run_format, args=(number_months_retired,
                                                                    savings,
                                                                    allowance,
                                                                    stock_change_df,
                                                                    split_run_size,
                                                                    Container))
            jobs.append(proc)
            proc.start()
        ##Pipe and Queue want jobs joined after the items are recieved, the Manager wants them joined before
        ##Let them handle the joining
        return_list = Container.get_output(num_procs, jobs)#, jobs)
        #print(return_list)
        return return_list

    def simulation_run_format(self, number_months_retired, savings, monthly_allowance, stock_change_df, split_run_size, Container):
        savings_list = []
        for runs in range(split_run_size):
            num_rows,_ = stock_change_df.shape
            random_change = np.random.randint(0, num_rows, size=number_months_retired)
            local_savings = savings
            for months in range(number_months_retired):
                monthly_change = stock_change_df.loc[random_change[months], 'MonthlyGainOrLoss']
                ##Apply Monthly Change
                local_savings += local_savings * monthly_change
                ##Take out our monthly allowance
                local_savings -= monthly_allowance
            savings_list.append(local_savings)
            #Container.update_output(local_savings)
            #append
        Container.update_output(savings_list)
 
class ContainerFactory(object):
    def __init__(self, run_type):
        #REGULAR_RUN = 0
        #POOL_RUN = 1
        self.run_type = run_type

    def get_container(self):
        if self.run_type is PROCESS_RUN_MANAGER:
            return ManagerContainer()
        elif self.run_type is PROCESS_RUN_QUEUE:
            return QueueContainer()
        elif self.run_type is PROCESS_RUN_PIPE:
            return PipeContainer()
            
class ContainerClass(object):
    def __init__(self):
        pass
    
    def update_output(self, savings):
        raise Exception("Not Implemented")
    
    def get_output(self, num_procs, jobs):
        raise Exception("Not Implemented")
    
class ManagerContainer(ContainerClass):
    def __init__(self):
        #print('building manager')
        self.list = Manager().list()
    
    def update_output(self, savings):
        self.list.append(savings)
        
    def get_output(self, num_procs, jobs):
        ##This implementation doesnt use num_procs
        for proc in jobs:
            proc.join()
        ##we have been appending lists to our list, return the reduced version of our list to make it 1d
        ##IE turn [[1,2],[2,3]] to [1,2,2,3]
        return reduce(lambda x,y: x + y, self.list)
        #if our list had just been a a single list we would have had to use list comprehension
        #The manager list would not work inline with other lists, or being stored as a dict
        #return [result for result in self.list]

class QueueContainer(ContainerClass):
    def __init__(self):
        #print('building queue')
        self.output_q = Queue()
    
    def update_output(self, savings_list):
        self.output_q.put(savings_list)
    
    def get_output(self, num_procs, jobs):
        result_list = []
        for num_runs in range(num_procs):
            result_list += self.output_q.get()
        for proc in jobs:
            proc.join()
        return result_list
    
class PipeContainer(ContainerClass):
    def __init__(self):
        #print('building pipe')
        self.output_p, self.input_p = Pipe()
        
    def update_output(self, savings_list):
        self.output_p.send(savings_list)
        
    def get_output(self, num_procs, jobs):
        result_list = []
        for num_runs in range(num_procs):
            result_list += self.input_p.recv()
        for proc in jobs:
            proc.join()            
        return result_list
'''
REGULAR_RUN = 0
POOL_RUN = 1
PROCESS_RUN_MANAGER = 2
PROCESS_RUN_QUEUE = 3
PROCESS_RUN_PIPE = 4
'''    
def _main():
    #multi_list = Manager().list()
    #proc = Process(target=run_simulations_process_test,  args=(1, 1, 1, pd.read_csv(r'C:\Users\mitch\Desktop\Masters\DataMiningI\Python-Simulation\sp500.csv'), 1, multi_list))
    #proc.start()
    #proc.join()
    #print([result for result in multi_list])
    ##2,3,4
    for run_type in [REGULAR_RUN, POOL_RUN, PROCESS_RUN_MANAGER, PROCESS_RUN_QUEUE, PROCESS_RUN_PIPE]:
        run_ver = RunFactory(run_type).factory()
        result  = run_ver.run_simulations(10, 1, 1, pd.read_csv(r'C:\Users\mitch\Desktop\Masters\DataMiningI\Python-Simulation\sp500.csv'), 5, 10)
        print(result)
        #result  = Process(target=self.run_simulations_process_test,  args=(5, 1, 1, pd.read_csv(r'C:\Users\mitch\Desktop\Masters\DataMiningI\Python-Simulation\sp500.csv'), 5, Container))
if __name__ == '__main__':
    freeze_support()
    _main()