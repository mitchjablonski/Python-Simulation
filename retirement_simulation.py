# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 20:50:05 2018

@author: mitch
"""
from __future__ import division, print_function
import pandas as pd
import numpy as np
from collections import defaultdict
from multiprocessing import Pool, Process, Manager, cpu_count, Queue, Pipe
from functools import partial
import time
import run_type_factory
 
def determine_number_runs_for_confidence(number_months_retired, savings, stock_change_df, simulation_runs, confidence_allowance, run_type, process_type):
    ##We want to withdrawl 10k a month and have 95% confidence we won't run out of money, how much do we need?
    savings_for_monthly = []
    savings_df = pd.DataFrame()
    if run_type is POOL_RUN:
        savings_for_monthly = execute_pool_runs(simulation_runs,
                                                number_months_retired,
                                                savings, 
                                                confidence_allowance, 
                                                stock_change_df)
    elif run_type in [PROCESS_RUN_MANAGER, PROCESS_RUN_QUEUE, PROCESS_RUN_PIPE]:
        savings_for_monthly = process_type.run_simulations(number_months_retired, 
                                                           savings, 
                                                           confidence_allowance, 
                                                           stock_change_df, 
                                                           simulation_runs)
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
        return determine_number_runs_for_confidence(number_months_retired, 
                                                    savings, 
                                                    stock_change_df,
                                                    simulation_runs, 
                                                    confidence_allowance, 
                                                    run_type,
                                                    process_type)
    
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

##Function we use to run from main
def regular_run(simulation_runs, number_months_retired, savings, monthly_allowance, stock_change_df):
    result_savings = defaultdict(list)
    for curr_index, allowance in enumerate(monthly_allowance):
        for num_runs in range(simulation_runs):
            if (num_runs == 0):
                curr_perc = curr_index / (len(monthly_allowance))
                print('Current Percentage Done Monthly Allowance Sims {}'.format(curr_perc))
            result_savings[allowance].append(run_simulations(number_months_retired, 
                                                             savings,
                                                             allowance, 
                                                             stock_change_df))
    return result_savings
##Run our simulations normally
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

##The function we use to run our pool from main loop
def pool_run(simulation_runs, number_months_retired, savings, monthly_allowance, stock_change_df):
    result_savings = defaultdict(list)
    for curr_index, allowance in enumerate(monthly_allowance):
        curr_perc = curr_index / (len(monthly_allowance))
        print('Current Percentage Done Monthly Allowance Sims {}'.format(curr_perc))
        result_savings[allowance] = execute_pool_runs(simulation_runs,
                                                      number_months_retired,
                                                      savings, 
                                                      allowance, 
                                                      stock_change_df)
    return result_savings

##Function called from pool_run and the confidence calc
##Make our run list into an iterable, build a partial function, and use pool.map
def execute_pool_runs(simulation_runs, number_months_retired, savings, allowance, stock_change_df):
    run_list = [runs for runs in range(simulation_runs)]
    pool = Pool()
    parital_simulations_func = partial(run_simulations, 
                                       number_months_retired,
                                       savings,
                                       allowance, 
                                       stock_change_df)
    
    return (pool.map(parital_simulations_func, run_list))

##The function we use to run our process from main loop
def process_run_manager(simulation_runs, number_months_retired, savings, monthly_allowance, stock_change_df, process_type):
    result_savings = defaultdict(list)
    for curr_index, allowance in enumerate(monthly_allowance):
        curr_perc = curr_index / (len(monthly_allowance))
        print('Current Percentage Done Monthly Allowance Sims {}'.format(curr_perc))
        ##Function we use to call from process_run_manager and confidence
        ##Split up our run size into chunks based on the number of processors and call 
        ##The process command that many times.  
        result_savings[allowance] = process_type.exectue_runs(simulation_runs, 
                                                             number_months_retired, 
                                                             savings, 
                                                             allowance, 
                                                             stock_change_df)
    return result_savings


def _main(simulation_runs, number_months_retired, savings, monthly_allowance, confidence_allowance, csv_path, run_type):
    
    stock_change_df = pd.read_csv(csv_path)
    start_time = time.time()
    if run_type is REGULAR_RUN:
        result_savings = regular_run(simulation_runs, 
                                     number_months_retired, 
                                     savings, 
                                     monthly_allowance, 
                                     stock_change_df)
    elif run_type is POOL_RUN:
        result_savings = pool_run(simulation_runs,
                                  number_months_retired, 
                                  savings, 
                                  monthly_allowance, 
                                  stock_change_df)

    elif run_type in [PROCESS_RUN_MANAGER, PROCESS_RUN_QUEUE]:
        process_type = run_type_factory.RunFactory(run_type)
        result_savings = process_run_manager(simulation_runs,
                                             number_months_retired, 
                                             savings,
                                             monthly_allowance, 
                                             stock_change_df,
                                             process_type)
        '''
        process_type = run_type_factory.RunFactory(run_type).factory()
        result_savings = process_type.execute_runs(simulation_runs,
                                                   number_months_retired,
                                                   savings,
                                                   monthly_allowance,
                                                   stock_change_df)
    #elif run_type is PROCESS_RUN_QUEUE:
        '''
        result_savings = process_run_queue(simulation_runs,
                                           number_months_retired, 
                                           savings,
                                           monthly_allowance, 
                                           stock_change_df)
        '''
   # elif run_type is PROCESS_RUN_PIPE:
        '''
        result_savings = process_run_pipe(simulation_runs,
                                         number_months_retired,
                                         savings,
                                         monthly_allowance,
                                         stock_change_df)   
        ''' 
    ##We can't predict length to converge for confidence without seed, time before it.  Use some crude timing
    print('{} seconds of run time for run type {}'.format((time.time() - start_time), run_type))
    confidence_savings_amount, confidence_savings_result = determine_number_runs_for_confidence(number_months_retired,
                                                                                                savings,
                                                                                                stock_change_df,
                                                                                                simulation_runs, 
                                                                                                confidence_allowance, 
                                                                                                run_type,
                                                                                                process_type)
    result_savings[confidence_allowance] = confidence_savings_result
    report_savings = eval_results_for_savings(result_savings) 
    return result_savings, report_savings, confidence_savings_amount

REGULAR_RUN = 0
POOL_RUN = 1
PROCESS_RUN_MANAGER = 2
PROCESS_RUN_QUEUE = 3
PROCESS_RUN_PIPE = 4
##Implement pipes
if __name__ == "__main__":
    run_type = PROCESS_RUN_PIPE
    simulation_runs = 100
    number_months_retired = 12*25
    savings = 10**6
    confidence_allowance = 10000
    monthly_allowance = [3000, 4000, 5000]
    csv_path = r'C:\Users\mitch\Desktop\Masters\DataMiningI\Python-Simulation\sp500.csv'
    print(csv_path)
    #for run_type in [REGULAR_RUN, POOL_RUN, PROCESS_RUN_MANAGER, PROCESS_RUN_QUEUE, PROCESS_RUN_PIPE]:
    for run_type in [PROCESS_RUN_MANAGER, PROCESS_RUN_QUEUE, PROCESS_RUN_PIPE]:
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
    ##77 with queue
    ##77 with pipe