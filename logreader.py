# -*- coding: utf-8 -*-
"""
Created on Thu Dec  2 16:12:20 2021

@author: K012352
"""

Oracle = __import__("Oracle Connection")
import os
import pandas as pd
import datetime as dt
import warnings
import calendar
import databases as db
import re
from time import strptime

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_columns', 10)

BAD_FILE_LOG = 'bad_file_log' # File to write bad activity logs to
ACTIVITY_DUPLICATES_LOG = 'Activity_log_duplicates' # File to write duplicate
                                                    # activity log entries to.

# Returns master activity log from all activity logs, or if pre_processdd
# is True, retrieve master_activity log from previously created excel sheet.
# Also performs a check 
def load_activity_logs(month:str, year:str, pre_processed, sysid_to_doc):
    bad_file_log = []    
    data_dir = os.path.dirname(os.getcwd()) + '\\Reports\\'
    master_activity_log = pd.DataFrame(
        columns = ['Cell','Last Name','First Name', 'DOC', 'Med Status','Date',
                   'Shower Out','Shower In','Shower Ref','Shower Comments',
                   'Shower Time','Rec1 Out','Rec1 In','Rec1 Ref','Rec1 Comments',
                   'Rec1 Time','Court Out','Court In','Court Ref','Court Comments',
                   'Court Time','Video Out','Video In','Video Ref',
                   'Video Comments','Video Time','Prog/Services Out',
                   'Prog/Services In','Prog/Services Ref','Prog/Services Comments',
                   'Prog Time','Rec2 Out','Rec2 In','Rec2 Ref','Rec2 Comments',
                   'Rec2 Time','Rec3 Out','Rec3 In','Rec3 Ref','Rec3 Comments',
                   'Rec3 Time', 'Rec4 Out','Rec4 In','Rec4 Ref','Rec4 Comments',
                   'Rec 4 Time','Misc Type','Misc Out','Misc In','Misc Ref',
                   'Misc Comments','Misc Time','Offered Out Of Cell Time','POD','file'])
    
    data_log_path = os.path.dirname(os.getcwd()) + "\\Data Logs\\" + month + \
        " "  + year + "\\"
        
    # If pre_processed, load activity log from excel, and exit function
    if pre_processed:
        file_path = data_log_path + 'master_activity_log_' + month + '_' + \
            year + '.xlsx'
        
        master_activity_log = pd.read_excel(file_path, index_col = 0)
                    
        print('Activity logs pre-processed...loading ' + file_path + '\n')
    
        return master_activity_log
    
    # Perform manual aggregation of activity logs
    else:
        activity_log_files  = [x for x in os.listdir(data_log_path) if re.match('POD.*\.xlsx', x)]
        # Check if directory exists
        if os.path.isdir(data_log_path):
            num_files = len(activity_log_files)
        else:
            print('Directory does not exist : ' + data_log_path)
            return
        
        for filename in activity_log_files:
            file_pod = filename.split(' - ')[0].split(' ')[2]
            file_month = filename.split(' - ')[1].split('.')[0].split('-')[0].zfill(2)
            file_day = filename.split(' - ')[1].split('.')[0].split('-')[1].zfill(2)
            file_year = filename.split(' - ')[1].split('.')[0].split('-')[2][0:4]
            # Shifts no longer added to activity log's file name
            #file_shift = filename.split(' - ')[1].split(' ')[1]
            log_date = dt.datetime.strptime(file_month + '-' + file_day + \
                                            '-' + file_year, '%m-%d-%Y')
            
            # Only process log files for the given month/year
            if (log_date.month == strptime(month, '%B').tm_mon) and \
                (log_date.year == int(year)) :
                    
                print('Loading "' + filename + '"')
                data = pd.read_excel(os.path.join(data_log_path, filename), 
                                     sheet_name = 'Log')
                
                # Handle case descrepancies between files. This is kludey
                # solution
                data.columns = data.columns.str.title()
                data.rename(columns={"Doc":"DOC"}, inplace=True)
                
                # Only retrieve relevant data
                data, error_msg = clean_data(data, filename, file_month, file_day,
                                             file_year)
                data['POD'] = file_pod
                data['file'] = filename
                
                # Append data into master activity log
                master_activity_log = master_activity_log.\
                    append(data, ignore_index = True)
                    
                # Update bad_file_log tracker with the filename and the error
                # message if the file triggered an error in the clean_data() check
                if error_msg: bad_file_log.append(filename + ' ' + error_msg)
        
        print('\n' + str(num_files) + ' activity logs loaded\n')
        
        # Check to ensure master_activity_log has entires, before performing
        # QA checks
        if master_activity_log.shape[0] > 0 :
            master_activity_log.sort_values(by = ['Date', 'DOC'], inplace = True)
            master_activity_log.drop(columns=['Unnamed: 36',
                                              'Unnamed: 53'], 
                                     inplace=True, 
                                     errors='ignore')
            
            master_activity_log['Date'] = pd.to_datetime(master_activity_log['Date'],
                                                         errors='coerce')
            master_activity_log['Date'] = master_activity_log['Date'].\
                dt.strftime('%Y-%m-%d')
    
            # Check for duplicate entries in activity logs
            activities_duplicated = check_log_for_duplicates(master_activity_log,
                                                             month, year)
    
            # Perform a check on the master_activity_log to determine if any DOCs
            # have no match to a SYSID record
            missing_doc = master_activity_log.merge(sysid_to_doc, on='DOC', how='outer', 
                                      indicator=True).\
                query('_merge=="left_only"')[['Last Name', 'First Name', 'DOC', 'file']].\
                    drop_duplicates(keep='first')
            missing_doc.to_excel(data_log_path + 'missing_doc_' + month + '_' + \
                                 year + '.xlsx')
                        
            # Write all log records that raised an error to excel sheet
            bad_file_df = pd.DataFrame(bad_file_log)
            bad_file_df.to_excel(data_log_path + BAD_FILE_LOG + "_" + month + \
                                 "_" + year + ".xlsx")
        
        # Export master activity log to excel sheet (this is performed even if
        # activity log is empty.
        master_activity_log.to_excel(data_log_path + 'master_activity_log_' + \
                                     month + '_' + year + '.xlsx')
                
                
        print('\nLOG LOADING COMPLETE\n')
    
    return master_activity_log

def check_log_for_duplicates(master_activity_log, month, year):
    '''Checks master_activity_log for duplicate log entries, and writes all
    duplicate entries into excel file
    
    :param master_activity_log: Dataframe of all activity logs aggregated
    :param month: the current month of investigation
    :param year: the current year of investigation
    
    :return: returns True if duplicates
    '''
    master_duplicate_log = pd.DataFrame()
    data_dir = os.path.dirname(os.getcwd()) + '\\Reports\\'
    data_log_path = os.path.dirname(os.getcwd()) + "\\Data Logs\\" + month + \
        " "  + year + "\\"    
    duplicateShower = master_activity_log[master_activity_log.\
                                          duplicated(subset=['DOC', 'Date', 'Shower Out'], keep=False) & 
                                          master_activity_log['Shower Out'].notna()].copy()
        
    duplicateRec1 = master_activity_log[master_activity_log.\
                                          duplicated(subset=['DOC', 'Date', 'Rec1 Out'], keep=False) & 
                                          master_activity_log['Rec1 Out'].notna()].copy()

    duplicateCourt = master_activity_log[master_activity_log.\
                                          duplicated(subset=['DOC', 'Date', 'Court Out'], keep=False) & 
                                          master_activity_log['Court Out'].notna()].copy()
        
    duplicateVideo = master_activity_log[master_activity_log.\
                                          duplicated(subset=['DOC', 'Date', 'Video Out'], keep=False) & 
                                          master_activity_log['Video Out'].notna()].copy() 
        
    duplicateProg = master_activity_log[master_activity_log.\
                                          duplicated(subset=['DOC', 'Date', 'Prog/Services Out'], keep=False) & 
                                          master_activity_log['Prog/Services Out'].notna()].copy()
        
    duplicateRec2 = master_activity_log[master_activity_log.\
                                          duplicated(subset=['DOC', 'Date', 'Rec2 Out'], keep=False) & 
                                          master_activity_log['Rec2 Out'].notna()].copy()

    master_duplicate_log = master_duplicate_log.\
                append([duplicateShower, duplicateRec1, duplicateCourt,
                        duplicateVideo, duplicateProg, duplicateRec2], ignore_index = True)

    if master_duplicate_log.shape[0] > 0:
        master_duplicate_log.drop(columns=('Unnamed: 53'), 
                                  inplace = True, errors='ignore')
        print('\n' + str(master_duplicate_log.shape[0]) + \
              ' duplicate log entries exist')
        master_duplicate_log.to_excel(data_log_path + ACTIVITY_DUPLICATES_LOG + \
                                      "_" + month + "_" + year + ".xlsx")
        return True
    else:
        return False

# Clean the data retrieved from the excel sheets
# Returns the original dataset with only the rows that match criteria specified
def clean_data(data, filename, file_month, file_day, file_year):
    error_msg = '' # Error message string
    

    log_date = dt.datetime.strptime(file_month + '-' + file_day + '-' + \
                                    file_year, '%m-%d-%Y')
    
    # Clean column names with leading and trailing spaces
    data.columns = data.columns.str.strip()
    
    # Only keep rows with data in the DOC, and Name columns
    data = data.loc[(data.DOC.notna()) & (data['Last Name'].notna()) &
                    (data.DOC != 'EXAMPLE') & ~data.Date.isnull()]
    
    
    # Check that the date in the file matches the Date column
    bad_data = data.loc[(data.Date != log_date) & (data.DOC != 'EXAMPLE') &
                        (data.DOC.notna())]
    data = data.loc[data.Date == log_date]
    
    
    if bad_data.shape[0] > 0 :
        error_msg = str(bad_data.shape[0]) + ' row[s] where date does not' + \
            ' match filename in "' + filename + '"'
    
    return data, error_msg

def main():
    # Catches warning regarding data validation being deprecated in excel sheets
    # this is caused by the fact that the activity logs have data validated cells
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    temp, lookup, num_files, bad_file_log = \
        load_activity_logs('January', '2022')
    
    print('end')
if __name__ == "__main__" :
    main()


