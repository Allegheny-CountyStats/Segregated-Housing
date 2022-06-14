# -*- coding: utf-8 -*-
"""
Created on Fri Jul 30 16:37:35 2021

@author: WMui
"""
import sys
python_path = r'C:\Users\K012352\Allegheny County\Criminal Justice Analytics - Documents\Wilson\Code\dhs_util'

if python_path not in sys.path:
    sys.path.append(python_path)
        
import jail as jail
import house as house
import logreader as log
import databases as db
import pandas as pd
from datetime import timedelta, datetime as dt
from collections import defaultdict
from tqdm import tqdm
import os
import calendar
import numpy as np
from ast import literal_eval
import json

SAVE = True   # Set to True to output report to excel sheet
MOVEMENTS_PRE_PROCESSED = True # Set to True, if processsing of movement logs
                                # already completed and loading all_movements
                                # from previously created excel sheet.
ACTIVITY_LOG_PRE_PROCESSED = True # Set to True, if processing of activity
                                # logs already completed and loading log
                                # from previously created excel sheet.
COHORT_PRE_PROCESSED = False    # Set to True, if original "SH_Aggregated" list
                                # already created and loading cohort list from
                                # previously created excel sheet
BYPASS = False # Set to True, to disregard specific cells as SH, and consider
                # all cells SH for purposes of calculation.
                
PARAM_FILE = "Parameters.json"

# get parameters from json file
with open(PARAM_FILE, 'r+') as file:
    json_obj = json.load(file)
    
HOUR_LIMIT = json_obj['hour limit'] # Constant that defines what the limit of in-cell time
START_DATE = json_obj['start date'] # Start date for jail analysis
END_DATE = json_obj['end date'] # End date for jail analysis
EXCLUSION_TIERS = json_obj['exclusion tiers']
                # Medical tiers that obviate a need for 20 hours of out-of-cell
                # time, thus exclusing any individual who is in that tier for
                # that day.
SEMI_RHU_UNITS = json_obj['semi rhu units'] 
                # This list all the units which requires
                # a participant be in the activity log to be considered in the
                # SH analysis. That is, if a person is not in the activty log
                # we are assuming they do not have to have 20 hours of out-of-cell
                # tell.
                

def main():
    #######################################################################
    # Set starting parameters to use for analysis
    data_dir = os.path.dirname(os.getcwd()) + '\\Reports\\'
    bypass = BYPASS # bypass set to true during COVID lockdown
                    # signifying all cells are considered SH
    
    # start_date = START_DATE
    # end_date = END_DATE
    month = dt.strptime(START_DATE, '%Y-%m-%d').month
    year = dt.strptime(START_DATE, '%Y-%m-%d').year
    #######################################################################
    
    # Retrieve SYSID associated with each DOC number
    sysid_to_doc = get_sysid_doc_lkp()
    
    
    # Create activity log (either through manual aggregation of all logs
    # in the Data Logs folder (in the respective month folder) or by loading
    # the preprocessed excel sheet in there.
    master_activity_log = log.load_activity_logs(calendar.month_name[month], 
                                                 str(year), 
                                                 ACTIVITY_LOG_PRE_PROCESSED,
                                                 sysid_to_doc)
    
    #######################################################################
    
    all_movements = get_all_movements_log(MOVEMENTS_PRE_PROCESSED, 
                                          START_DATE, END_DATE, bypass)
    
    #######################################################################

    # Analyze_all_movements returns a dataframe of all individuals who were
    # considered to be in SH, before the check on medical exclusions.
    SH_Aggregated = analyze_all_movements_log(all_movements, 
                                              master_activity_log,
                                              sysid_to_doc, COHORT_PRE_PROCESSED)
    
    #######################################################################
    
    # Retrieve a list of individuals by SYSID, who had days for which they
    # were excluded from requiring the 4 hours of out-of-cell time
    exclusion_list = get_exclusion_days(master_activity_log, sysid_to_doc,
                                        EXCLUSION_TIERS)
    
    # Merge exclusion_list and find the difference between SH_Days and
    # days when a medical exclusion exists
    SH_Aggregated = SH_Aggregated.merge(exclusion_list, on='SYSID',
                                        how='left')
    # Need to create empty list to do a set.difference
    SH_Aggregated['Med Ex Dates'] = SH_Aggregated['Med Ex Dates'].\
        apply(lambda x: x if isinstance(x, list) else [])
    SH_Aggregated['Non Med Ex Dates'] = SH_Aggregated.\
        apply(lambda x: list(set(x['SH_Days']) - set(x['Med Ex Dates'])), 
              axis = 1)
    
    #######################################################################
    
    # Re-sort Non Med Ex Dates
    SH_Aggregated['Non Med Ex Dates'] = SH_Aggregated.\
        apply(lambda x: sorted(x['Non Med Ex Dates']), axis=1)
    
    # Add Num_SH_days, and Num_episodes (related to SH_Days), and Num_Med_Ex_Days,
    # and Num_Non_Ex_Days
    SH_Aggregated['Num_SH_Days'] = SH_Aggregated['SH_Days'].\
        apply(lambda x : len(x))
    add_num_episodes(SH_Aggregated, 'SH_Days', 'NUM_EPISODES')
    SH_Aggregated['Num_Med_Ex_Days'] = SH_Aggregated['Med Ex Dates'].\
        apply(lambda x: len(x))
    SH_Aggregated['Num_Non_Med_Ex_Days'] = SH_Aggregated['Non Med Ex Dates'].\
        apply(lambda x: len(x))
    add_num_episodes(SH_Aggregated, 'Non Med Ex Dates', 'NUM_NON_EX_EPISODES')
    
    # Add Booking profile + sort by Non Med Ex Dates
    SH_Aggregated = add_booking_profile(SH_Aggregated)
    SH_Aggregated.sort_values(by=['Num_Non_Med_Ex_Days'], ascending = False, 
                              axis=0, inplace = True, ignore_index=True)
    
    # Re-sort columns before saving
    # This was a request by jail staff to place columsn most relevant to
    # the front
    SH_Aggregated = SH_Aggregated[['PODS', 'Non Med Ex Dates', 
                                   'Num_Non_Med_Ex_Days', 'NUM_NON_EX_EPISODES',
                                   'DOC', 'FNAME', 'LNAME', 'SYSID', 'SH_Days',
                                   'Med Ex Dates', 'Non Med Ex Dates', 'Num_SH_Days',
                                   'NUM_EPISODES', 'Num_Med_Ex_Days', 'COMDATE',
                                   'RELDATE', 'GEND_OMS', 'MCI_UNIQ_ID', 'DOB',
                                   'RACE', 'ETHNICITY', 'AGE']]

    if SAVE:
        SH_Aggregated.to_excel(data_dir + 'Segregated Housing list ' + 
                               START_DATE + ' to ' + END_DATE + '.xlsx')
    #######################################################################

def get_exclusion_days(master_activity_log, sysid_to_doc, exclusion_tiers=None):
    '''Retrieve a dataframe listing all individuals from the
    master_activity_log, and the days, for which they had a medical status that
    matched the tiers in the 'exclusion_tiers', or were designated as new or 
    transfer individual. The exclusion_tiers identifies all the tiers for which
    individuals are EXCLUDED from the mandatory out of cell time.
    
    :param master_activity_log: Master activity log, aggregation of all
                    activity logs.
    :param sysid_to_doc: lookup table between sysid and doc 
    :param exclusion_tiers: list of tiers that match the 'Med Status' column of
                    activity logs, that would exclude an individual from the
                    necessary 20 hours of out-of-cell time.
    '''
    # Get all individuals-days for which there was a comment in any column
    # that matched exclusion_tiers, thus eliminating them from consideration
    # for SH inclusion.     
    temp_df = master_activity_log.loc[master_activity_log.isin(exclusion_tiers).any(axis=1)].copy()

    
    # Link SYSID to list of DOC - duplicate rows, where DOCs link to an old
    # SYSID is not an issue, as this list is only used to match to current
    # SYSIDS in the given month's SH_Aggregated list, all 'old' SYSID will be
    # ignored on the join.
    temp_df = temp_df.merge(sysid_to_doc, how='left', on='DOC')
    temp_df['SYSID'] = temp_df['SYSID'].fillna(0).astype(np.int64)
    
    # Get dataframe of distinct SYSID, with days that match the med statuses
    # that are valid exclusions
    temp_df = pd.DataFrame(temp_df.groupby('SYSID')['Date'].\
                           agg(lambda x: list(set(x)))).reset_index()
        
    # Remove row with SYSID = 0, as those are unmatched individuals from
    # sysid_to_doc. QA should investigate why there are individuals unmatched,
    # presumably because the DOC is entered in wrong from the activit log
    temp_df = temp_df.loc[temp_df.SYSID != 0]
    temp_df.rename(columns = {'Date':'Med Ex Dates'}, inplace = True)
    
    if temp_df.shape[0] > 0 :
        temp_df['Med Ex Dates'] = temp_df.apply(lambda x: sorted(x['Med Ex Dates']), 
                                                axis=1)
        
    return temp_df
    
       
def get_all_movements_log(pre_processed, start_date, end_date, bypass=None) :
    ''' Create an all_movements log, either by reading from a pre-processed excel
    sheet, or by running through all housing records and generating an
    an all_movements log manually, which will indicate all the movements as well
    as whether the unit was a SH unit for the duration of the inmate's stay.
    
    This functions by (1) creating a jail object, (2) setting the initial jail 
    state each day, (3) taking a snapshot of that intial state as the first
    entry in the movement_log, (4) iterating through each movement of the day
    and updating the jail for each movement to reflect the state of the jail
    after each movement, (5) checking for all inmates who moved and or had a 
    "state" change between SH/NON-SH, and updating the movement log to reflect
    that change.
    
    :param pre_procesed: True, if all_movements log exists as excel file and
                        can be loaded directly, otherwise run process manually
    
    :return: returns an all_movement log
    '''
    # Setup jail object, and initialize attributes
    if bypass is None:
        bypass = BYPASS
        
    data_dir = os.path.dirname(os.getcwd()) + '\\Reports\\'
    all_movements = pd.DataFrame()
    
    # Run through the iteration of movements in each day if not previously
    # processed, in which case the all_movements log will be loaded from an
    # excel sheet, rather than being generated in here.
    if not pre_processed:
        acj = jail.jail(bypass = bypass)
        jail_days = acj.get_jail_datetimes(start_date, end_date)
        
        # Start a new housing file/log
        acj_movements = house.housing()
        
        # Iterate each day and do the analysis
        for datetime in jail_days:
            
            print("Processing movements on " + datetime)
            # set the jailstate for the given date
            acj.set_jail_state(datetime, bypass)
            
            # grab all housing movements for current date
            # change this to use start date and end date, so we account for different audit times
            curr_day_movements = acj_movements.get_housing_history_by_date_range(datetime)
            
            # Reset the movement_log in the house() object for each day
            acj_movements.reset_movement_log()
            
            # Set initial jail state as first entries in movement log for day
            acj_movements.update_movement_log_from_jail_snapshot(acj)
            
            # Iterate through each movement and update movement_log
            for index, row in tqdm(curr_day_movements.iterrows(), 
                                    total = curr_day_movements.shape[0],
                                    unit = 'Moves', ncols = 100):
                

                acj.move_sysid(row.SYSID, row.SECTION, row.BLOCK, row.CELL, 
                                  row.MDATE.strftime('%Y-%m-%d %H:%M:%S'))
                acj_movements.update_movement_log_from_jail_snapshot(acj)
            
            # Append all house movements of the particular day
            all_movements = all_movements.append(acj_movements.movement_log.\
                                                  assign(JAIL_DAY=datetime.\
                                                         split(' ', 1)[0]))
                
            print("\n")

        # Output all movements as tracking log
        all_movements.to_excel(data_dir + 'All movements ' + start_date + \
                               ' to ' + end_date + '.xlsx')
    
    # If movements were previously processed, load all_movements from
    # previously created movement log excel sheet instead of creating the
    # log manually.
    else:
        file_path = data_dir + 'All movements ' + start_date + ' to ' + \
            end_date + '.xlsx'
            
        print ("All movements pre-processed...loading " + file_path + '\n')
        
        all_movements = pd.read_excel(file_path)
        all_movements.drop(columns='Unnamed: 0', inplace=True, errors='ignore')
                
    return all_movements



def analyze_all_movements_log(all_movements:pd.DataFrame, 
                              master_activity_log:pd.DataFrame,
                              sysid_to_doc:pd.DataFrame,
                              pre_processed = False):
    ''' Takes a multi-day movement log and iterates through each day, and 
        iterates across all individuals in jail each day and returns a dataframe
        (which originated from a dictionary) of individuals who were in SH for 
        more than 20 hours for each JAIL_DAY group, as well as the days for 
        which they were considered to be in SH.
        
        :param all_movements: All movements (including whether
            housing was a SH at the time)
        :param master_activity_log: All activity logs aggregated
        :param sysid_to_doc: lookup table between sysid-to-doc
        :param pre-processed: True, if SH cohort list already generated, and
            results can be read from the pre-processed excel sheet)
        
        :return: DataFrame of all SH individuals
            
    '''
    data_dir = os.path.dirname(os.getcwd()) + '\\Reports\\'
    file_name = 'SH_Cohort list '
    file_path = data_dir + file_name + START_DATE + " to " + END_DATE + ".xlsx"
    
    
    # If pre-processed = True, don't process movements log, and grab results
    # from excel sheet
    if pre_processed :
        df = pd.read_excel(file_path)
        
        # read_excel interprets lists as strings, need to explicitly re-cast
        # string into pandas list
        df.SH_Days = df.SH_Days.apply(literal_eval)
        print ("SH Cohort list pre-processed...loading " + file_path)            
        return df

    print("Identifying cohort in 'Segregated Housing' each day")
    
    # Start a running list of sh individuals as dictionary, as well as the
    # pods housing each individual
    running_sh_dict = defaultdict(list)
    running_pod_dict = defaultdict(list)
      
    # Iterate over each day and retrieve list of sh individuals
    for day in all_movements.JAIL_DAY.unique():
        print('Analyzing movements on ' + str(day))
        day_movements = all_movements.loc[all_movements.JAIL_DAY == day]
    
        # day_movements is all movements for a day. Loop through for each
        # individual in the file and spit out a dictionary of those who
        # have greater than 20 hours (see code from original SH report.py
        for inmate in tqdm(day_movements['SYSID'].unique().tolist(),
                           total = len(day_movements['SYSID'].unique().tolist()),
                           ncols = 100,
                           unit = 'SYSID') :
            # all movements in a particular day, for current inmate
            df = day_movements.loc[day_movements.SYSID == inmate].copy()
            
            # if statement used to handle long words in 'BLOCK' column
            df['unit'] = df.apply(lambda x: x['SECTION'][3:] + \
                                  x['BLOCK'][3:] + '-' + x['CELL'] 
                                  if x['SECTION'] != 'LEVG' else 'LEVG-' + \
                                      x['BLOCK'], axis = 1)
            
            if over_hour_limit(df, master_activity_log, sysid_to_doc) :                                
                #curr_day_sh[inmate] = day
        
                running_sh_dict[inmate].append(day)
                running_pod_dict[inmate].append(','.join(df['unit'].unique().tolist()))
    
        
    # Transfer running list of days in segregated house as well as pods,
    # to dataframe. Write to excel sheet if SAVED = True
    SH_Aggregated = pd.DataFrame(list(running_sh_dict.items()),
                                 columns=['SYSID', 'SH_Days'])
    PODS_Aggregated = pd.DataFrame(list(running_pod_dict.items()),
                                 columns=['SYSID', 'PODS'])
    SH_Aggregated = SH_Aggregated.merge(PODS_Aggregated, how='left',
                                        on='SYSID', indicator=False)
    
    # Write results for future usage
    SH_Aggregated.to_excel(file_path, index=False)
            
    return SH_Aggregated
    
    

def add_num_episodes(df, column_name, new_column_name):
    ''' Returns original df, with added 'episodes' column detailing the number of
    episodes from the array of dates, which signify the number of continguous
    days in the array. Note, this algorithm relies on the dates in 'column_name'
    to be sorted before analysis, as this algorithm looks for # of days 
    differences between the n and n+1 record.
    
    :param column_name: Name of column that contains sequential dates
    :param new_column_name: Name of "Num Episodes" column
    
    :return: returns modified dataframe
    '''
    num_episodes_list = []
    one_day = timedelta(days=1)
    # Find the number of episodes for each row, representing each person who
    # had spent sometime in segregated housing
    for index, row in df.iterrows():        
        # Change string of dates, into iterable list of dates
        row_array = row[column_name]
        row_array = [dt.strptime(date, "%Y-%m-%d").date() for date in row_array]
        
        # Set the default number of episodes
        if len(row_array) >= 1 :
            num_episodes = 1
        else:
            num_episodes = 0
            
        # Go through each list of dates to find the number of "breaks" in
        # sequential days, which will signify the number of episodes
        for i in range(len(row_array)-1) :
            if row_array[i] + one_day != row_array[i+1] :
                num_episodes = num_episodes + 1
            
        # Create running list tracking num_episodes for each person, to append
        # to the original dataframe, df
        num_episodes_list.append(num_episodes)
        
    df[new_column_name] = num_episodes_list
        
    return df

 
def add_booking_profile(df, duplicates=False):
    ''' Updates the original dataframe with the booking profile retrieved from 
        AC_OMS. By default only the most recent demographic profile, from the most
        recent booking episode per DOC.
    '''
    ACPRD1 = db.Oracle('ACPRD1')
   
    # Default to returning the 5 columns below if no demographic columns
    # are specified
    _demos = "SYSID, COMDATE, RELDATE, DOC, GEND_OMS, MCI_UNIQ_ID, " + \
        "F_NAME as FNAME, L_NAME as LNAME, DOB, RACE, ETHN as ETHNICITY, " + \
            "FLOOR(MONTHS_BETWEEN(COMDATE, DOB)/12) as Age"
    
    query = "SELECT DISTINCT " + _demos + " FROM AC_OMS.JAIL_DAILY_STATUS"
                
    dw_df = ACPRD1.query(query)
    dw_df['lkp_rank'] = dw_df.groupby('SYSID')['COMDATE'].rank(method='first',
                                                             ascending=False)
    
    # If duplicates parameter is False, only take the most recent demographic
    # profile
    if ~duplicates:
    #    df['SYSID'] = pd.to_numeric(df['SYSID'])
        dw_df = dw_df.loc[dw_df.lkp_rank == 1]
        
    df = pd.merge(df, dw_df, on='SYSID', how='left')
    df.drop(columns='lkp_rank', inplace=True)
    ACPRD1.disconnect()
    
    return df


def over_hour_limit(movement_log, master_activity_log, sysid_to_doc) :
    ''' Identify if the person from the given movement_log was over the 20 
        hour SH hour limit. This algorithm will factor in out-of-cell time 
        given in the activity logs, as well as using the activity log to 
        determine who should be considered as being in a segregated housing 
        protocol (namely in POD 1C).

        Parameters
        ----------
        movement_log: pandas dataframe, the movement log for a particular day
                        and inmate (sysid)
        master_activity_log: pandas dataframe
        sysid_to_doc: pandas dataframe
        
        Returns
        ----------
        boolean
            Whether the individual represented in the given movement_log was 
            in an isolated cell for more than HOUR_LIMIT time.
    '''
         
    over_hour_limit = False    
    
    movement_log = movement_log.copy().reset_index(drop=True)
    movement_log['DURATION'] = None
    START_DATE = movement_log.loc[0, 'MDATE']
    END_DATE = dt.strptime(START_DATE, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)
    sysid = movement_log['SYSID'][0]
    activity_hours = 0
                                    
    
    # Determine day of log, to extract activity logs for that day
    #rec_day = dt.strptime(movement_log.JAIL_DAY[0], '%Y-%m-%d')
    rec_day = movement_log.JAIL_DAY[0]
    
    # Determine the doc associated with the sysid
    doc = sysid_to_doc.loc[sysid_to_doc.SYSID == sysid, 'DOC'].squeeze()
    
    #print("DOC : " + str(doc) + " Day: " + rec_day)
    
    # Determine total out of cell (activity) time for given inmate with DOC
    # If doc = <int32> indicates a doc was found from sysid_to_doc table,
    # otherwise datatype would be empty Series.
    if isinstance(doc, np.integer):      
        # Modified to calculate total activity time manually instead of using
        # 'Total Out of Cell Time' cell
        activity_hours = round(master_activity_log.\
            loc[(master_activity_log.Date == rec_day) &
                (master_activity_log.DOC == doc), 
                ['Shower Time', 'Rec1 Time', 'Court Time', 'Video Time', 
                 'Prog Time', 'Rec2 Time', 'Rec3 Time', 'Rec 4 Time',
                 'Misc Time']].sum().sum(), 2)
    
    #######################################################################
        
    # Iterates through all movements for particular sysid and sets the DURATION
    # column, identifying the amount of in-cell time a person spent in each
    # pod.
    for index, row in movement_log.iterrows() :
        
        # if at the last row, calculate duration from MDATE to next day's
        # audit time
        if index == (movement_log.shape[0] - 1) :
            movement_log.loc[index, 'DURATION'] = \
                (END_DATE - dt.strptime(movement_log.loc[index, 'MDATE'], 
                                              '%Y-%m-%d %H:%M:%S'))/timedelta(hours=1)
        else :
            movement_log.loc[index, 'DURATION'] = \
                (dt.strptime(movement_log.loc[index + 1, 'MDATE'], '%Y-%m-%d %H:%M:%S') - 
                 dt.strptime(movement_log.loc[index, 'MDATE'], '%Y-%m-%d %H:%M:%S'))/timedelta(hours=1)
    
    
    #######################################################################
    # Kludge section that uses the activity log to identify those who
    # ARE in fact in segregated housing, regardless of the unit they are in.
    # This primarily effects units that will sometimes be
    # used for SH, and sometimes not, and we can only tell, per person, per
    # date, from the existence of a person in the activity log. Run
    # 'activity_log_cohort_check' for each pod applicable.
    #######################################################################
    # Apply above kludge removal for all units listed in 'semi rhu units'
    for unit in SEMI_RHU_UNITS:
        movement_log = activity_log_cohort_check(unit, doc, 
                                                 master_activity_log, 
                                                 movement_log)
    
    # movement_log = activity_log_cohort_check('5MC', doc, 
    #                                          master_activity_log, 
    #                                          movement_log)
    
    
    # Calculates the total time spent in a SH unit, minus activity hours
    total = (movement_log.loc[movement_log.HOUSING == 'SH', 'DURATION']
             .sum()) - activity_hours
    
    # If SH housing is greater than the HOUR_LIMIT then True
    if total > HOUR_LIMIT :
        over_hour_limit = True
        
    return over_hour_limit


def activity_log_cohort_check(pod:str, doc:np.integer, master_activity_log, 
                              movement_log):
    '''Check if individual exists in activity log for specified pod on the day
    specified by the current movement_log. If not, remove all movement_log
    entries in the pod-date in question.

    Parameters
    ----------
    pod : pod that requires an individual be listed in the actiivty log to be
        considered eligible for inclusion in the segregated housing list.
        This should only be pods that have mixed SH and non-SH usage, and thus
        not all individuals in the pod should be considered SH unless they have
        an entry in the activity log. (i.e. '5MC', '1D', etc.)
        
    master_activity_log: pandas dataframe, master activity log
    
    movement_log: pandas dataframe, the movement log for a particular day
        and inmate (sysid)
    '''
    
    # Only consider individuals to be segregated if they exist in the activity 
    # log. Create the list of DOC which are present in the activity log for
    # a particular pod on a particular START_DATE
    if (master_activity_log is not None) and (not master_activity_log.empty) \
        and (not movement_log.empty):
        # Movement log day/activity log day
        try:
            rec_day = movement_log.reset_index(drop=True).JAIL_DAY[0]
        except KeyError:
            print('key error exception, doc: ' + str(doc) + ' pod: ' + pod)
        
        
        sh_eligible_cohort = master_activity_log.loc[
            (master_activity_log.Date == rec_day) &
            (master_activity_log.POD == pod)]
            
        sh_eligible_cohort = sh_eligible_cohort.groupby(['Last Name', 
                                                         'First Name', 'DOC'], 
                                        as_index=False).agg(set)[['Last Name', 
                                                        'First Name', 
                                                        'DOC']]
        sh_eligible_cohort['DOC'] = sh_eligible_cohort['DOC'].astype('int64')
    
        # If individual with doc is not found in the pod_cohort list, remove
        # all movement entries while in that pod.
        if (isinstance(doc, np.integer)) and (doc not in sh_eligible_cohort.values):
            drop_indexes = movement_log[(movement_log.SECTION == ('LEV' + pod[0:-1])) &
                                        (movement_log.BLOCK == ('POD' + pod[-1]))].index
            movement_log.drop(drop_indexes, inplace=True)
            
    return movement_log

def update_sh_dictionary(existing_sh_list:dict, new_sh_list:dict):
    '''Deprecated by defaultdict usage
    Update the running segregated housing list with additional SH individuals
    if they do not exist, or update the list of days they are in segregated 
    housing if the individual currently exists'''
    
    for key, val in new_sh_list.items():
        if key in existing_sh_list:
            existing_sh_list[key] = [existing_sh_list[key], val]
        else:
            existing_sh_list[key] = val



'''Temporary function that identifies all the units an individual was in
for a given day for which they were in SH'''
def add_pods_to_sh_aggregated(sh_aggregated, all_movements):
    all_movements['UNIT'] = all_movements['SECTION'] + ' ' + \
        all_movements['BLOCK'] + ' ' + all_movements['CELL']
        
    all_moves = all_movements.groupby(['SYSID', 'JAIL_DAY'])['UNIT'].\
        apply(lambda x: ', '.join(x)).reset_index()
        
    temp = sh_aggregated.merge(all_moves[['SYSID', 'JAIL_DAY', 'UNIT']], 
                               how='left', on='SYSID')
    
    return temp

'''Join multiple segregation logs into one'''
def join_multiple_reports(report1, report2):
    pass


''' Retrieve df of sysid to doc lookup table'''
def get_sysid_doc_lkp():
    # Retrieve SYSID associated with each DOC number
    AC_OMS = db.Oracle('ACPRD1')
    sysid_to_doc = AC_OMS.query("SELECT DISTINCT SYSID, DOC FROM " + \
                                "AC_OMS.JAIL_DAILY_STATUS jds WHERE ACJ_DATE " + \
                                ">= TO_DATE('2020-01-01', 'YYYY-MM-DD')")
    sysid_to_doc['DOC'] = sysid_to_doc['DOC'].astype(int)
    
    return sysid_to_doc
    
if __name__ == "__main__" :
    main()
    
    # Temporary code that reads Geoff's modified SH report sheet and adds
    # Demographics to the sheet
    # data_dir = os.path.dirname(os.getcwd()) + '\\Reports\\'
    # df = pd.read_excel(data_dir + 'Copy of Segregated Housing list 2022-01-01 to 2022-01-03.xlsx', 'Sheet2')
    
    # sysid_to_doc = get_sysid_doc_lkp()
    # df = df.merge(sysid_to_doc, on='DOC', how='left')
    
    # df = add_booking_profile(df)
    # df = df = df[df['lkp_rank'].notna()]
    # df.drop(columns = ['lkp_rank', 'DOC_y'], inplace=True)
    # df.rename(columns={"DOC_x": "DOC"}, inplace=True)
    
    # writer = pd.ExcelWriter(data_dir + 'Copy of Segregated Housing list 2022-01-01 to 2022-01-03.xlsx')
    # df.to_excel(writer, 'Sheet2')
    # writer.save()
    # writer.close()
    
    