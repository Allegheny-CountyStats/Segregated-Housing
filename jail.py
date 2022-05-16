# -*- coding: utf-8 -*-
"""
Created on Fri Jul 30 16:49:51 2021

@author: WMui
"""

#Oracle = __import__("Oracle Connection")
import databases as db
import pandas as pd
import numpy as np
import sys
import house as house
from datetime import datetime as dt
from datetime import timedelta

C1_MIN_CELL = 220
C1_MAX_CELL = 228
F4_MIN_CELL = 201
F4_MAX_CELL = 213
F5_MIN_CELL = 119
F5_MAX_CELL = 128
PODC1_CELLS = list(map(str, np.arange(C1_MIN_CELL, C1_MAX_CELL + 1).tolist()))
PODF4_CELLS = list(map(str, np.arange(F4_MIN_CELL, F4_MAX_CELL + 1).tolist()))
PODF5_CELLS = list(map(str, np.arange(F5_MIN_CELL, F5_MAX_CELL + 1).tolist()))
BYPASS = bool # True: bypass segregated housing unit checks, and make all
              # solo cells considered segregated housing

class jail :
    '''
    jail contains the 'current' state of the jail housing, as well as 
    parameters that store the entire jail audit history and jail housing
    movements. In addition because audit times may differ each day, the
    audit_date_times are derived from the jail audit history data after object
    initialization.
    '''
    #Complete history of jail audit snapshots since 2021
    jail_history = None
    # Complete history of temporary releases since 2021
    temp_release_history = None
    #All jail audit date times
    audit_date_times = None
    
    '''current_date_time must be in a date format, but can be in a date-time
    format to set the current jail state to mid day'''
    def __init__(self, current_date_time = None, bypass = None):
        """
        Construct a new jail object

        :param current_date_time: The date and time for the current jail state
        :param bypass: If True, the jail object will consider all units,
                        segregated housing units. Otherwise, only units specified
                        in 'update_sh_state' are considered SH.
                        
        :return: returns nothing
        """
        if bypass is None:
            BYPASS = True
        else:
            BYPASS = bypass

        # Load complete jail history into class object
        if jail.jail_history is None :
            
            # Create an Oracle connection object
            conn = db.Oracle('ACPRD1')
            
            with open('jail_state.sql') as f:
                jail_state_query = f.read()
                
            # Retrieve jail states        
            jail.jail_history = conn.query(jail_state_query)
            
            conn.disconnect()
            jail.jail_history[['SECTION', 'CELL', 'BLOCK', 'BED']] = \
                jail.jail_history[['SECTION', 'CELL', 'BLOCK', 'BED']].\
                apply(lambda x: x.str.strip())
                
            # Set all audit_date_times
            jail.audit_date_times = pd.DataFrame(jail.jail_history.ACJ_DATE.unique(),
                                                     columns = ['ACJ_DATE']).\
                sort_values('ACJ_DATE').\
                    reset_index(drop=True)
        
        # Load complete temporary release history into class object
        if jail.temp_release_history is None :
            
            # Create an Oracle connection object
            conn = db.Oracle('ACPRD1')
            
            with open('temp_release.sql') as f:
                temp_release_query = f.read()
                
            # Retrieve jail states        
            jail.temp_release_history = conn.query(temp_release_query)
            
            conn.disconnect()
            jail.temp_release_history['TIME_OUT'] = \
                (jail.temp_release_history['RELRNDT'] - 
                    jail.temp_release_history['RELSTDT'])/ np.timedelta64(1, 'h')
            jail.temp_release_history['ACJ_DATE'] = \
                pd.to_datetime(jail.temp_release_history['RELSTDT']).dt.date
            jail.temp_release_history = jail.temp_release_history[['EXTDESC',
                                                                   'SYSID',
                                                                   'RELSTDT',
                                                                   'RELRNDT',
                                                                   'TIME_OUT',
                                                                   'ACJ_DATE']]
        
        self.bypass = BYPASS
        self.curr_date_time = None
        self.curr_date = None
        self.curr_time = None
        self.jail_state = None
        
        # If current_date_time parameter is given, set the current jail_state
        # otherwise leave all attributes as None
        if current_date_time != None:
            
            # Set the current jail state, jail date/time
            format = '%Y-%m-%d %H:%M:%S'
            try:
                dt.strptime(current_date_time, format)
            except ValueError:
                try:
                    format = '%Y-%m-%d'
                    print('No time set for jail day, try to default to audit ' + \
                          'time on ' + current_date_time)
                    dt.strptime(current_date_time, format)
                except ValueError:
                    print('Date format malformed')
                else:
                    # Format current_date_time to date object
                    current_date_time = dt.strptime(current_date_time, format).date()
                    
                    # Retrieve current_date_time from jail from the date object
                    jail_curr_date_time = jail.audit_date_times.loc[
                        jail.audit_date_times.ACJ_DATE.dt.date == current_date_time, 
                        'ACJ_DATE'].reset_index(drop=True)
                                    
                    # If date in parameter exists in jail_history, use that
                    # datetime to set the initializing datetime of the jail
                    # else break
                    if jail_curr_date_time.empty:
                        print('currently breaks when date is not set to ' + \
                              'audit time of jail. In the future, use this ' + \
                                  'date time in set_jail_state anyway')
                        sys.exit('Date for jail initialization unavailable')
                    else:
                        # This function sets all the object attributes
                        self.set_jail_state(jail_curr_date_time[0])
                        
                        
            # Run, if current_date_time parameter is well-formatted        
            else:
                current_date_time = dt.strptime(current_date_time, "%Y-%m-%d %H:%M:%S")
                
                # Ensure current_date_time exists in jail_history
                # Retrieve current_date_time from jail from the date object
                jail_curr_date_time = jail.audit_date_times.loc[
                        jail.audit_date_times.ACJ_DATE == current_date_time, 
                        'ACJ_DATE'].reset_index(drop=True)
                
                # If date in parameter exists in jail_history, use that
                # datetime to set the initializing datetime of the jail
                # else break
                if jail_curr_date_time.empty:
                    print('currently breaks when date is not set to ' + \
                          'audit time of jail. In the future, use this ' + \
                              'date time in set_jail_state anyway')
                    sys.exit('Date for jail initialization unavailable')
                else:
                    # This function sets all the object attributes
                    self.set_jail_state(jail_curr_date_time[0])
            
    ''' Sets the state of the jail at the given audit_datetime.
    audit_datetime should be the audit_datetime of the day when used by
    jail initiation functions. If set to middle of the day, the process
    will require runnning through the housing_history log to process moves.'''
    def set_jail_state(self, audit_datetime, bypass = None):
        # Sets bypass if given, otherwise default to value set at 
        # initialization
        if bypass is None:
            pass
        else:
            self.bypass = bypass
        
        # Control for audit_datetime to be of string type, this should be
        # deprecated for a overloading function in the future
        if isinstance(audit_datetime, str) :
            audit_datetime = dt.strptime(audit_datetime, "%Y-%m-%d %H:%M:%S")
        
        if audit_datetime in set(jail.audit_date_times.ACJ_DATE):
            # print('audit_datetime is an audit time. Set the jail to initial' +
            #       ' no need to run housing process')
        
            jail_state = self.jail_history.loc[
                self.jail_history.ACJ_DATE == audit_datetime].copy()
            #convert SYSID to string to use a groupby to aggregate into a list
            jail_state.SYSID = jail_state.SYSID.astype(str)
            temp_jail_state = jail_state.groupby(['SECTION', 'BLOCK', 'CELL'], 
                                                 as_index = False).agg({'SYSID': ','.join})
        
            #Split INMATES into separate list items
            temp_jail_state['INMATES'] = temp_jail_state.SYSID.str.split(',')
            #Convert INMATES back to list of integers
            temp_jail_state['INMATES'] = temp_jail_state.INMATES.apply(lambda x: [int(items) for items in x])
            temp_jail_state.drop(['SYSID'], axis = 1, inplace = True)
            temp_jail_state['HOUSING'] = 'NON-SH'        
            temp_jail_state['COUNT'] = temp_jail_state.INMATES.apply(lambda x: len(x))
            temp_jail_state['CHANGED'] = True
            
        # audit_datetime is set to middle of the day and will require some
        # processing of housing_history
        else:
            print('audit_datetime is set to middle of the day and will and ' +
                  'require some processing of housing history')
            
        self.curr_date_time = audit_datetime
        self.curr_date = self.curr_date_time.date()
        self.curr_time = self.curr_date_time.time()
        self.jail_state = temp_jail_state
        self.update_sh_state()


    '''Return the section, block, and cell of the individual _sysid in current
    jail state'''
    def find_sysid(self, sysid:int):
        for index, row in self.jail_state.iterrows() :
            if sysid in row['INMATES'] :
                sect = row['SECTION']
                block = row['BLOCK']
                cell = row['CELL']
                housing = row['HOUSING']
    
                return(index, sect, block, cell, housing)
    
        return(None, None, None, None, None)

    '''Return a dataframe of the unit'''
    def get_unit_state(self, _section:str, _block:str, _cell:str):
        return self.jail_state.loc[(self.jail_state.SECTION == _section) & 
                                   (self.jail_state.CELL == _cell) &
                                   (self.jail_state.BLOCK == _block)]
   
    '''Remove inmate with sysid from current location and move to new location.
    return TRUE if individual existed. Note no check is made on whether new 
    locationi is a valid jail unit'''
    def move_sysid(self, sysid:int, new_section:str, new_block:str,
                   new_cell:str, move_datetime=None):
        
        if move_datetime is None:
           move_datetime = self.curr_date_time.strftime('%Y-%m-%d %H:%M:%S')
        
       #need to check is move is different, if not then don't make the move
       #and don't make the change, just write a print statement that move
       #is no move at all
            
       # Remove sysid from current location, then add to new location
       # if person does not currently exist in jail, remove_sysid returns
       # false and does nothing
        self.remove_sysid(sysid, move_datetime)
        self.add_sysid(sysid, new_section, new_block, new_cell, move_datetime)
        
       # Redundant as remove_sysid and add_sysid both sets new date time
        self.set_new_curr_date_time(move_datetime)
        
    
    '''Remove inmate with sysid from current location'''
    def remove_sysid(self, sysid:int, remove_datetime=None):
        
        # Set remove_datetime to curr_date_time if none is specified
        if remove_datetime is None:
            remove_datetime = self.curr_date_time
        
        # Determine if person is in the jail
        index, sect, block, cell, housing = self.find_sysid(sysid)
    
        if index is not None :
            self.jail_state.at[index, 'INMATES'].remove(sysid)            
            self.jail_state.loc[index, 'COUNT'] = \
                self.jail_state.loc[index, 'COUNT'] - 1
            self.jail_state.loc[index, 'CHANGED'] = True
            
            # after removal set the housing state using the given logic
            #print('update sh state here')

            self.update_sh_state(index)            
            self.set_new_curr_date_time(remove_datetime)
                
            return True
        else :
            #print(str(sysid) + ' not found in the jail')
            return False    
    
    '''Add individual with sysid into new location within the self object'''
    def add_sysid(self, sysid:int, new_section:str, new_block:str, 
                  new_cell:str, add_datetime = None):
        
        # Set datetime to curr_date_time if none is specified
        if add_datetime is None:
            add_datetime = self.curr_date_time
        
        # do check on sysid to make sure it is an int
        if not isinstance(sysid, int):
            sys.exit('sysid was not an integer')

        # if inmate already exist, exit function
        # Note, individual should not exist when this function is called, as
        # the individual should have been removed beforehand
        index, *_ = self.find_sysid(sysid)
        if index is not None:
            print(str(sysid) + ' already exists in the jail')
            return None
        
        
        # If new unit location does not currently exist, create one
        # otherwise update INMATES list with new sysid
        row = self.jail_state.loc[(self.jail_state.SECTION == new_section) &
                                  (self.jail_state.BLOCK == new_block) &
                                  (self.jail_state.CELL == new_cell)]
        
        if row.empty :
            self.jail_state.loc[len(self.jail_state)] = \
                [new_section, new_block, new_cell, [sysid], 'NON-SH', 1, True]
                
            index = len(self.jail_state.index) - 1
        #new unit location exists, update INMATES list
        else :          
            index = row.index[0]
            
            self.jail_state.at[index, 'INMATES'].append(sysid)
            self.jail_state.loc[index, 'COUNT'] = len(self.jail_state.loc[index, 'INMATES'])
            self.jail_state.loc[index, 'CHANGED'] = True
 
        
        #update housing state after a person is added to that unit    
        self.update_sh_state(index)
        self.set_new_curr_date_time(add_datetime)
                
    
    '''Determine whether the unit is SH or NON-SH, and update if necessary'''
    def update_sh_state(self, index = None, bypass = BYPASS):
        
        # index is None indicates an update all units of the entire jail_state
        if index is None:
            self.jail_state.HOUSING = 'NON-SH'
            if self.bypass:
                self.jail_state.loc[(self.jail_state.COUNT == 1) & 
                               (self.jail_state['SECTION'] != 'XXXX'),
                               'HOUSING'] = 'SH'
            else:
                # SH cells for post January 2022
                self.jail_state.loc[(self.jail_state['BLOCK'] == 'PODC') & 
                    (self.jail_state['SECTION'] == 'LEV1') &
                    (self.jail_state['CELL'].isin(PODC1_CELLS)) &
                    (self.jail_state['COUNT'] <= 1), 'HOUSING'] = 'SH'
                                
                self.jail_state.loc[(self.jail_state['BLOCK'] == 'PODC') & 
                    (self.jail_state['SECTION'] == 'LEV5') &
                    (self.jail_state['COUNT'] <= 1), 'HOUSING'] = 'SH'                
                
                self.jail_state.loc[(self.jail_state['BLOCK'] == 'PODD') & 
                    (self.jail_state['SECTION'] == 'LEV5') &
                    (self.jail_state['COUNT'] <= 1), 'HOUSING'] = 'SH'
                
                self.jail_state.loc[(self.jail_state['BLOCK'] == 'PODF') & 
                    (self.jail_state['SECTION'] == 'LEV5') &
                    (self.jail_state['CELL'].isin(PODF5_CELLS)) &
                    (self.jail_state['COUNT'] <= 1), 'HOUSING'] = 'SH'
                
                self.jail_state.loc[(self.jail_state['BLOCK'] == 'PODC') & 
                    (self.jail_state['SECTION'] == 'LEV5M') &
                    (self.jail_state['COUNT'] <= 1), 'HOUSING'] = 'SH'
                
                self.jail_state.loc[(self.jail_state['BLOCK'] == 'PODD') & 
                    (self.jail_state['SECTION'] == 'LEV5M') &
                    (self.jail_state['COUNT'] <= 1), 'HOUSING'] = 'SH'
                
                self.jail_state.loc[(self.jail_state['BLOCK'] == 'PODE') & 
                    (self.jail_state['SECTION'] == 'LEV8') &
                    (self.jail_state['COUNT'] <= 1), 'HOUSING'] = 'SH'                
        # Update a specific jail unit by index
        else:
            section = self.jail_state.loc[index, 'SECTION']
            block = self.jail_state.loc[index, 'BLOCK']
            cell = self.jail_state.loc[index, 'CELL']
            count = self.jail_state.loc[index, 'COUNT']
        
            if self.bypass:
                # Empty or 'jail release' units will be considered NON-SH
                if count != 1 or section == 'XXXX': 
                    self.jail_state.loc[index, 'HOUSING'] = 'NON-SH'
                else:
                    self.jail_state.loc[index, 'HOUSING'] = 'SH'
            else:
                # Empty units will be considered NON-SH
                if section == 'LEV1' and block == 'PODC' \
                    and cell in PODC1_CELLS and count <= 1:
                    self.jail_state.loc[index, 'HOUSING'] = 'SH'                                            
                elif section == 'LEV5' and block == 'PODC' and count <= 1 :
                    self.jail_state.loc[index, 'HOUSING'] = 'SH'
                elif section == 'LEV5' and block == 'PODD' and count <= 1:
                    self.jail_state.loc[index, 'HOUSING'] = 'SH'
                elif section == 'LEV5M' and block == 'PODD' and count <= 1 :
                    self.jail_state.loc[index, 'HOUSING'] = 'SH'
                elif section == 'LEV8' and block == 'PODE' and count <= 1 :
                    self.jail_state.loc[index, 'HOUSING'] = 'SH'
                elif section == 'LEV5' and block == 'PODF' and count <= 1 \
                    and cell in PODF5_CELLS:
                    self.jail_state.loc[index, 'HOUSING'] = 'SH'
                else:
                    self.jail_state.loc[index, 'HOUSING'] = 'NON-SH'
            
    '''Return all units that have changed since the current date'''
    def get_changed_units(self):
        changed_units = self.jail_state.copy()
        
        return changed_units.loc[changed_units.CHANGED == True]
    
    '''Return all units that have not changed since the current date'''
    def get_unchanged_units(self):
        unchanged_units = self.jail_state.copy()
        return unchanged_units.loc[unchanged_units.CHANGED == False]
    
    '''Return all SH units'''
    def get_sh_units(self):
        sh_units = self.jail_state.copy()
        return sh_units.loc[sh_units.HOUSING == 'SH']
        
    '''Return all NON-SH units'''
    def get_non_sh_units(self):
        non_sh_units = self.jail_state.copy()
        return non_sh_units.loc[non_sh_units.HOUSING == 'NON-SH']
    
    '''Return current date corresponding to current jail state.'''
    def get_curr_date(self):
        return self.curr_date.strftime("%Y-%m-%d")
    
    '''Return current time corresponding to current jail state'''    
    def get_curr_time(self):
        return self.curr_time.strftime("%H:%M:%S")
    
    '''Return current datetime corresponding to current jail state'''
    def get_curr_date_time(self):
        return self.curr_date_time.strftime("%Y-%m-%d %H:%M:%S")
    
    def get_total_population(self):
        return self.jail_state.COUNT.sum()
    
    def get_occupied_cells(self):
        return self.jail_state.loc[(self.jail_state.COUNT > 0) &
                                   (self.jail_state.SECTION != 'XXXX')].shape[0]
    
    def set_new_curr_date_time(self, current_date_time:str):
        self.curr_date_time = dt.strptime(current_date_time, 
                                          "%Y-%m-%d %H:%M:%S")
        self.curr_date = dt.strptime(current_date_time, 
                                     "%Y-%m-%d %H:%M:%S").date()
        self.curr_time = dt.strptime(current_date_time, 
                                     "%Y-%m-%d %H:%M:%S").time()
    
    def reset_changed_state(self):
        self.jail_state.CHANGED = False
        
    '''Returns list of inmates released'''
    def get_released_inmates(self):
        return self.jail_state.loc[self.jail_state.SECTION == 'XXXX', 
                                   'INMATES'].iloc[0]
    
    '''Returns jail_state attribute'''
    def get_jail_state(self):
        return self.jail_state
    
    '''Returns bypass attribute'''
    def get_bypass_state(self):
        return self.bypass
    
    '''Returns a list of audit_datetimes from start and end+1day date:string
    inclusive'''
    def get_jail_datetimes(self, start_date:str, end_date:str):
        start_date = dt.strptime(start_date, '%Y-%m-%d').date()
        end_date = dt.strptime(end_date, '%Y-%m-%d').date() # + timedelta(days=1)
        
        date_list = self.audit_date_times.\
            loc[(self.audit_date_times.ACJ_DATE.dt.date >= start_date) & 
                (self.audit_date_times.ACJ_DATE.dt.date <= end_date)].ACJ_DATE.\
                tolist()
                
        date_str_list = list(map(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'), date_list))
        return date_str_list

      
def main():
    start_date = '2021-09-01'
    end_date = '2021-09-30'
    
    # Setup jail day
    wilson = jail('2021-10-01')
    
    # Retrieve all housing movements
    temp = house.housing()
    
    # Update movement log from initial snapshot of jail
    temp.update_movement_log_from_jail_snapshot(wilson)
    
    # Retrieve the movements for a particular day - this should match the jail
    # initiation day
    curr_day_movements = temp.get_housing_history_by_date_range('2021-10-01 00:30:00').reset_index(drop=True)
    
    # Iterate through each movement and update movement_log
    for index, row in curr_day_movements.iterrows() :          
        print("(" + str(index) + ") " + "Processing move at: " + \
              row.MDATE.strftime('%Y-%m-%d %H:%M:%S'))
        wilson.move_sysid(row.SYSID, row.SECTION, row.BLOCK, row.CELL, 
                          row.MDATE.strftime('%Y-%m-%d %H:%M:%S'))
        temp.update_movement_log_from_jail_snapshot(wilson)
    
    
    print("stop")

if __name__ == "__main__" :
    main()