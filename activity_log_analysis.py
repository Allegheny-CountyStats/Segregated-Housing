# -*- coding: utf-8 -*-
"""
Created on Mon Jan 10 16:26:30 2022

@author: K012352
@Description: Ad-hoc analysis that utilized the activity logs to determine
individuals who were 'medically designated' or 'new/transferred,' which would
mean they were not offered 4 hours of out-of-cell time, and thus everybody
else would have been offered, and thus can be removed from our December report
"""

import pandas as pd
import os
from ast import literal_eval
import numpy as np

sh_file_name = "Segregated Housing list 2021-12-01 to 2021-12-31 (original 2).xlsx"
activity_file_name = "master_activity_log_1_2022.xlsx"
filtered_medical_file_name = "Filtered Medical - master_activity_log_12_2021.xlsx"
#suicide_watch_file_name = "admission management suicide watch DECEMBER 21.xlsx"

def main():
    # Load files
    original_sh = pd.read_excel(os.path.dirname(os.getcwd()) + \
                                "\\Reports\\" + sh_file_name)
    original_sh['SH_Days'] = original_sh['SH_Days'].apply\
        (lambda x: literal_eval(str(x)))
        
    activity_log = pd.read_excel(os.path.dirname(os.getcwd()) + \
                                "\\Reports\\" + activity_file_name)

    # Get unique individuals and dates for which they are on suicide watch
    # from Laura William's original list
    # suicide_watch_list = pd.read_excel(os.path.dirname(os.getcwd()) + \
    #                             "\\Data Logs\\" + suicide_watch_file_name)
    # suicide_watch_list['AdmittedDate'] = suicide_watch_list['AdmittedDate'].\
    #     dt.strftime('%Y-%m-%d')
    # suicide_watch_list['DischargeDate'] = suicide_watch_list['DischargeDate'].\
    #     dt.strftime('%Y-%m-%d')
    # suicide_watch_list['Suicide_Watch_Days'] = suicide_watch_list.\
    #      apply(lambda x: pd.date_range(x['AdmittedDate'], x['DischargeDate'], 
    #                                    freq='D').strftime('%Y-%m-%d').\
    #            tolist(), axis = 1)
    # suicide_watch_list = suicide_watch_list[suicide_watch_list['InmateID'].\
    #                                         apply(lambda x: isinstance(x, int))]
    
    
    # All comments that would indicate medical exclusion
    filtered_medical_list = pd.read_excel(os.path.dirname(os.getcwd()) + \
                                "\\Data Logs\\" + filtered_medical_file_name,
                                sheet_name = 'Sheet6')
    filtered_medical_list = filtered_medical_list[~filtered_medical_list['Unique Comments'].isna()]
    filtered_medical_list = filtered_medical_list[filtered_medical_list['Unique Comments'] != 0]['Unique Comments']
    
    
    # Get unique individual-days who have a 'Shower Comment' in activity log
    # that is found in the filtered_medical_list or were a 'transferred' or 
    # 'new' inmate. Then roll up all dates into a list per unique individual
    # per row
    activity_log = activity_log.loc[
        ((activity_log['Shower REF'].isin(['New', 'Transfer'])) | 
        (activity_log['Shower Comments'].isin(filtered_medical_list))) &
        (activity_log['POD'].isin(['5C', '5MD', '5D']))]
   
    activity_log = activity_log.drop_duplicates(subset=['DOC', 'Date'])
    activity_log['Date'] = activity_log['Date'].dt.strftime('%Y-%m-%d')
    activity_log = activity_log.groupby('DOC', as_index = False)['Date'].agg(list)
    activity_log.rename(columns = {'Date':'Medical Dates'}, inplace = True)
    

    # Join activty_log and suicide_watch_list as master exclusion list
    # master_exclusion_list = activity_log.merge(suicide_watch_list[['InmateID', 'Suicide_Watch_Days']], 
    #                                            how = 'outer', left_on='DOC', 
    #                                            right_on='InmateID', 
    #                                            indicator = True)
    # Add null lists in place of nan
    # master_exclusion_list['Suicide_Watch_Days'] = master_exclusion_list.\
    #     apply(lambda x: [] if x['_merge'] == 'left_only' else x['Suicide_Watch_Days'], axis=1)
        
    # master_exclusion_list['Medical Dates'] = master_exclusion_list.\
    #     apply(lambda x: [] if x['_merge'] == 'right_only' else x['Medical Dates'], axis=1)
        
    # master_exclusion_list['Exclusion Dates'] = \
    #     master_exclusion_list.apply(lambda x: list(set.union(set(x['Medical Dates']), 
    #                                                          set(x['Suicide_Watch_Days']))), 
    #                                 axis = 1)
    # master_exclusion_list['Exclusion Dates'].apply(sorted)
    # master_exclusion_list.loc[master_exclusion_list['DOC'].isna(), 'DOC'] = \
    #     master_exclusion_list['InmateID']
    # master_exclusion_list = master_exclusion_list[['DOC', 'Exclusion Dates']]
    
    # Join original SH report with current activity log to be used for diff
    # on DOC and date
    new_sh = original_sh.merge(master_exclusion_list[['DOC', 'Exclusion Dates']], how = 'outer', 
                                    on='DOC', indicator = True)
    
    new_sh['Diff Days'] = new_sh.apply(lambda x:  x['SH_Days'] if x['_merge'] == 'left_only' \
                                       else (x['Exclusion Dates'] if x['_merge'] == 'right_only' \
                                       else list(set(x['SH_Days']) - set(x['Exclusion Dates']))), 
                                           axis=1)
        
    new_sh['Diff Days'] = new_sh['Diff Days'].apply(sorted)
    new_sh['Num Exclusion Days'] = new_sh['Exclusion Dates'].str.len()
    new_sh['Num Diff Days'] = new_sh['Diff Days'].str.len()
    
    new_sh.to_excel(os.path.dirname(os.getcwd()) + "\\Reports\\New " + \
                    sh_file_name)

if __name__ == "__main__":
    main()

