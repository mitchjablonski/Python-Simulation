# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 20:50:05 2018

@author: mitch
"""
from __future__ import division, print_function
import pandas as pd
import numpy as np
from collections import defaultdict
from multiprocessing import Pool, Process, Manager, cpu_count
from functools import partial
import time

def run_simulations(number_months_retired, savings, monthly_allowance, stock_change_df, run_num=None):
     num_rows,_ = stock_change_df.shape
     random_change = np.random.randint(0, num_rows, size=number_months_retired)
     for months in range(number_months_retired):
         monthly_change = stock_change_df.loc[random_change[months], 'MonthlyGainOrLoss']
         ##Apply Monthly Change
         savings += savings * monthly_change
         ##Take out our monthly allowance
         savings -= monthly_allowance
     return savings
 
def run_simulations_process(number_months_retired, savings, monthly_allowance, stock_change_df, split_run_size, return_multi_process_list):
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
        return_multi_process_list.append(local_savings)
 
def determine_number_runs_for_confidence(number_months_retired, savings, stock_change_df, simulation_runs, confidence_allowance, run_type):
    ##We want to withdrawl 10k a month and have 95% confidence we won't run out of money, how much do we need?
    savings_for_monthly = []
    savings_df = pd.DataFrame()
    if run_type is POOL_RUN:
        savings_for_monthly = execute_pool_runs(number_months_retired,
                                                savings, 
                                                confidence_allowance, 
                                                stock_change_df, 
                                                simulation_runs)
    elif run_type is PROCESS_RUN_MANAGER:
        savings_for_monthly = execute_process_manager(simulation_runs, 
                                                      number_months_retired, 
                                                      savings, 
                                                      confidence_allowance,
                                                      stock_change_df)
    else:
        for num_runs in range(simulation_runs):
            savings_for_monthly.append(run_simulations(number_months_retired, 
                                                       savings, 
                                                       confidence_allowance, 
                                                       stock_change_df))
            
    savings_df['Savings'] = savings_for_monthly
    rows_eval, _ = savings_df.shape
    rows_pos, _ = savings_df.loc[savings_df['Savings'] >= 0].shape
    perc_confidence = (rows_pos / rows_eval) *100
    print('{} % confidence achieved with {} savings'.format(perc_confidence, savings))
    if perc_confidence >= 95:
        return savings, savings_for_monthly
    else:
        ##If more than 20% away, should probably use a larger step size, and 10000 step size when closer to 95
        ##this is also a function of the number of simulations being run though
        if ((95-perc_confidence) > 20):
            savings += 100000*5
        else:
            savings += 100000
        return determine_number_runs_for_confidence(number_months_retired, savings, stock_change_df, simulation_runs, confidence_allowance, run_type)
    
def eval_results_for_savings(result_savings):
    results_summary = defaultdict(list)
    for keys in result_savings.keys():
        temp_df = pd.DataFrame()
        temp_df[str(keys)] = result_savings[keys]
        rows_pos, _ = temp_df.loc[temp_df[str(keys)] >= 0].shape
        tot_rows, _ = temp_df.shape
        rows_neg,_ = temp_df.loc[temp_df[str(keys)] < 0].shape
        mean_money = temp_df[str(keys)].mean(axis = 0)
        results_summary['allowance'].append(keys)
        results_summary['positive_sim'].append(rows_pos)
        results_summary['negative_sim'].append(rows_neg)
        results_summary['mean_money'].append(mean_money)
        results_summary['percent_positive'].append(rows_pos/tot_rows)
    return pd.DataFrame(results_summary)

def regular_run(simulation_runs, number_months_retired, savings, monthly_allowance, stock_change_df):
    result_savings = defaultdict(list)
    for curr_index, allowance in enumerate(monthly_allowance):
        for num_runs in range(simulation_runs):
            if (num_runs == 0):
                curr_perc = curr_index / (len(monthly_allowance))
                print('Current Percentage Done Monthly Allowance Sims {}'.format(curr_perc))
            result_savings[allowance].append(run_simulations(number_months_retired, savings, allowance, stock_change_df))
    return result_savings

def execute_pool_runs(number_months_retired, savings, allowance, stock_change_df, simulation_runs):
    run_list = [runs for runs in range(simulation_runs)]
    pool = Pool()
    parital_simulations_func = partial(run_simulations, number_months_retired, savings, allowance, stock_change_df)
    return (pool.map(parital_simulations_func, run_list))

def pool_run(simulation_runs, number_months_retired, savings, monthly_allowance, stock_change_df):
    result_savings = defaultdict(list)
    for curr_index, allowance in enumerate(monthly_allowance):
        curr_perc = curr_index / (len(monthly_allowance))
        print('Current Percentage Done Monthly Allowance Sims {}'.format(curr_perc))
        result_savings[allowance] = execute_pool_runs(number_months_retired, savings, allowance, stock_change_df, simulation_runs)
    return result_savings

def execute_process_manager(simulation_runs, number_months_retired, savings, allowance, stock_change_df):
    jobs = []
    multi_proc_list = Manager().list()
    num_procs = cpu_count()
    split_run_size = int(simulation_runs / num_procs)
    for num_splits in range(num_procs):           
        proc = Process(target = run_simulations_process, args=(number_months_retired, 
                                                               savings, 
                                                               allowance, 
                                                               stock_change_df, 
                                                               split_run_size,
                                                               multi_proc_list,))
        jobs.append(proc)
        proc.start()
    for proc in jobs:
        proc.join()
    return_list = [results for results in multi_proc_list]
    return return_list

def process_run_manager(simulation_runs, number_months_retired, savings, monthly_allowance, stock_change_df):
    result_savings = defaultdict(list)
    for curr_index, allowance in enumerate(monthly_allowance):
        curr_perc = curr_index / (len(monthly_allowance))
        print('Current Percentage Done Monthly Allowance Sims {}'.format(curr_perc))
        result_savings[allowance] = execute_process_manager(simulation_runs, number_months_retired, savings, allowance, stock_change_df)
    return result_savings

def _main(simulation_runs, number_months_retired, savings, monthly_allowance, confidence_allowance, csv_path, run_type):
    stock_change_df = pd.read_csv(csv_path)
    start_time = time.time()
    if run_type is REGULAR_RUN:
        result_savings = regular_run(simulation_runs, number_months_retired, savings, monthly_allowance, stock_change_df)
    elif run_type is POOL_RUN:
        result_savings = pool_run(simulation_runs, number_months_retired, savings, monthly_allowance, stock_change_df)
    elif run_type is PROCESS_RUN_MANAGER:
        result_savings = process_run_manager(simulation_runs, number_months_retired, savings, monthly_allowance, stock_change_df)
    
    ##We can't predict length to converge for confidence without seed, time before it.  Use some crude timing
    print('{} seconds of run time for run type {}'.format((time.time() - start_time), run_type))
    confidence_savings_amount, confidence_savings_result = determine_number_runs_for_confidence(number_months_retired,
                                                                                                savings,
                                                                                                stock_change_df,
                                                                                                simulation_runs, 
                                                                                                confidence_allowance, run_type)
    result_savings[confidence_allowance] = confidence_savings_result
    report_savings = eval_results_for_savings(result_savings) 
    return result_savings, report_savings, confidence_savings_amount

REGULAR_RUN = 0
POOL_RUN = 1
PROCESS_RUN_MANAGER = 2

if __name__ == "__main__":
    run_type = PROCESS_RUN_MANAGER
    simulation_runs = 1000
    number_months_retired = 12*25
    savings = 10**6
    confidence_allowance = 10000
    monthly_allowance = [3000, 4000, 5000]
    csv_path = 'C:\Users\mitch\Desktop\Masters\DataMiningI\Python-Simulation\sp500.csv'
    result_savings, report_savings, confidence_savings = _main(simulation_runs, 
                                                               number_months_retired,
                                                               savings,
                                                               monthly_allowance, 
                                                               confidence_allowance, 
                                                               csv_path,
                                                               run_type)
    #Pool run time, ~ 60 seconds at 1000 sims for each allowance
    #Regular run time, ~180 seconds 
    #85 seconds for process with manager