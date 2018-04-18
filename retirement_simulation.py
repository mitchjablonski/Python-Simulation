# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 20:50:05 2018

@author: mitch
"""
from __future__ import division, print_function
import pandas as pd
import numpy as np
from collections import defaultdict

def run_simulations(number_months_retired, savings, monthly_allowance, stock_change_df):
     num_rows,_ = stock_change_df.shape
     random_change = np.random.randint(0, num_rows, size = number_months_retired)
     for months in range(number_months_retired):
         monthly_change = stock_change_df.loc[random_change[months],'MonthlyGainOrLoss']
         ##Apply Monthly Change
         savings += savings * monthly_change
         ##Take out our monthly allowance
         savings -= monthly_allowance
     return savings
     

def determine_number_runs_for_confidence(number_months_retired, savings, stock_change_df, simulation_runs, confidence_allowance):
    ##We want to withdrawl 10k a month and have 95% confidence we won't run out of money, how much do we need?
    local_savings = savings
    savings_for_monthly = []
    savings_df = pd.DataFrame()
    for num_runs in range(simulation_runs):
        savings_for_monthly.append(run_simulations(number_months_retired, local_savings, confidence_allowance, stock_change_df))
    savings_df['Savings'] = savings_for_monthly
    rows_eval, _ = savings_df.shape
    rows_pos, _ = savings_df.loc[savings_df['Savings'] >= 0].shape
    perc_confidence = (rows_pos / rows_eval) *100
    print('{} % confidence achieved with {} savings'.format(perc_confidence, local_savings))
    if perc_confidence > 95:
        return local_savings, savings_for_monthly
    else:
        local_savings += 100000
        return determine_number_runs_for_confidence(number_months_retired, local_savings, stock_change_df, simulation_runs, confidence_allowance)
    
def eval_results_for_savings(result_savings):
    results_summary = defaultdict(list)
    for keys in result_savings.keys():
        temp_df = pd.DataFrame()
        temp_df[str(keys)] = result_savings[keys]
        rows_pos, _ = temp_df.loc[temp_df[str(keys)] >= 0].shape
        rows_neg,_ = temp_df.loc[temp_df[str(keys)] < 0].shape
        mean_money = temp_df[str(keys)].mean(axis = 0)
        results_summary['allowance'].append(keys)
        results_summary['positive_sim'].append(rows_pos)
        results_summary['negative_sim'].append(rows_neg)
        results_summary['mean_money'].append(mean_money)
    savings_df = pd.DataFrame(results_summary)
    return savings_df
        
def _main(simulation_runs, number_months_retired, savings, monthly_allowance, confidence_allowance, csv_path):
    stock_change_df = pd.read_csv(csv_path)
    result_savings = defaultdict(list)
    for curr_index, allowance in enumerate(monthly_allowance):
        for num_runs in range(simulation_runs):
            if (num_runs == 0):
                curr_perc = curr_index / (len(monthly_allowance))
                print('Current Percentage Done {}'.format(curr_perc))
            #print(allowance, num_runs)
            result_savings[allowance].append(run_simulations(number_months_retired, savings, allowance, stock_change_df))
           
    confidence_savings_amount, confidence_savings_result = determine_number_runs_for_confidence(number_months_retired,
                                                                                                savings,
                                                                                                stock_change_df,
                                                                                                simulation_runs, 
                                                                                                confidence_allowance)
    report_savings = eval_results_for_savings(result_savings) 
    print(confidence_savings)
    
    return result_savings, confidence_savings_amount, report_savings
if __name__ == "__main__":
    simulation_runs       = 5
    number_months_retired = 12*25
    savings               = 10**6
    confidence_allowance  = 10000
    monthly_allowance     = [3000, 4000, 5000]
    csv_path = 'C:\Users\mitch\Desktop\Masters\DataMiningI\Python-Simulation\sp500.csv'
    result_savings, confidence_savings, report_savings = _main(simulation_runs, number_months_retired, savings, monthly_allowance, confidence_allowance, csv_path)