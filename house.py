# -*- coding: utf-8 -*-
"""
Created on Thu Oct  7 08:42:42 2021

@author: K012352
"""
import pandas as pd
import jail as jail
from datetime import timedelta, datetime
Oracle = __import__("Oracle Connection")


class housing:
    raw_housing_history = None
    housing_history = None

    def __init__(self, date=None):
        
        if housing.raw_housing_history is None:
            conn = Oracle.Oracle('ACPRD1')
            
            # Get query that retrieves housing movements
            with open('house_move.sql') as f:
                house_move_query = f.read()
                
            # Retrieve house movements    
            raw_housing_history = conn.query(house_move_query)
            conn.disconnect()
            
            raw_housing_history[['SECTION', 'BLOCK', 'CELL', 'OSECTION', 'OBLOCK', 
                             'OCELL']] = raw_housing_history[['SECTION',
                                                          'BLOCK', 'CELL', 
                                                          'OSECTION', 
                                                          'OBLOCK', 'OCELL']].\
                                                          apply(lambda x: x.str.strip())
                                                          
            housing.raw_housing_history = raw_housing_history
            
            # Create a transformed copy of the raw_housing_history dataset
            # that incorporates the release from jail as a housing movement
            # row, that relocates an individual to SECTION = 'XXXX', BLOCK = 
            # 'XXXX', CELL = 'XXXX'. This is necessary to capture releases
            # from jail.
            temp_housing_history = raw_housing_history.copy()
            temp_releases = temp_housing_history.loc[(temp_housing_history['VACDATE'].notnull()) &
                                                (temp_housing_history['OSECTION'].isnull()), ]
            
            
            # Add a release from jail as a new row, to follow the format
            # expected for the movement_log analysis
            # Make a new row that indicates a move outside of jail (release from jail)
            temp_releases = temp_releases[['SYSID', 'OSECTION', 'OBLOCK', 
                                           'OCELL', 'VACDATE', 'RFORM']]
            
            temp_releases.rename(columns = {'OSECTION': 'SECTION', 
                                        'OBLOCK': 'BLOCK', 'OCELL': 'CELL', 
                                        'VACDATE': 'MDATE'}, inplace=True)
            
            temp_releases[['SECTION', 'BLOCK', 'CELL', 'RFORM']] = \
                            ['XXXX', 'XXXX', 'XXXX', 'Released']

            # This ensures the final housing_history dataframe will not have
            # duplicate indexes with this append
            try:
                temp_housing_history = temp_housing_history.\
                    append(temp_releases, ignore_index=True,
                           verify_integrity=True).sort_values('MDATE')
            except ValueError:
                print("Append of release rows, failed, index integrity")
                
            temp_housing_history = temp_housing_history[['SYSID', 'SECTION',
                                                         'CELL', 'BLOCK',
                                                         'MDATE', 'RFORM']]
                         
            housing.housing_history = temp_housing_history
            
        self.movement_log = pd.DataFrame(columns = ['SYSID', 'SECTION', 
                                                    'BLOCK', 'CELL', 
                                                    'HOUSING', 'CELLMATES'])
            
            
    def get_last_entry(self, sysid:int):
        last_entry = self.movement_log.loc[self.movement_log.SYSID == \
                                              sysid].tail(1)
        if last_entry.empty :
            return None, None, None, None, None
        else:
            return last_entry.index[0], \
                last_entry.iloc[0].SECTION, last_entry.iloc[0].CELL,\
        last_entry.iloc[0].BLOCK, last_entry.iloc[0].HOUSING
        
    
    '''Takes the jail snapshot and updates the movement_log with all
    CHANGED units, for those individuals where a change occurred'''
    def update_movement_log_from_jail_snapshot(self, jail_snapshot):
        # mdate is the date move occurred, jail curr_date_time should reflect
        # the state of the jail after movements occurred
       mdate = jail_snapshot.get_curr_date_time()
       changed_units = jail_snapshot.get_changed_units()
       
       for index, row in changed_units.iterrows() :
            new_sect = row['SECTION']
            new_block = row['BLOCK']
            new_cell = row['CELL']
            new_housing = row['HOUSING']
            
            # inmate_list is list of inmates in each cell. Each inmate per unit
            # needs to be evaluated to see if they've changed from their
            # current position/state
            inmate_list = row['INMATES']
            
            for inmate in inmate_list :
                cellmates = inmate_list.copy()
                cellmates.remove(inmate)
                # if current inmate state (location or housing) is different 
                # than inmate's last entry, update the record
                curr_index, curr_sect, curr_cell, curr_block, curr_housing = \
                    self.get_last_entry(inmate)
                
                if (curr_sect != new_sect or curr_block != new_block or
                    curr_cell != new_cell or curr_housing != new_housing) :
                    
                    new_row = {'SYSID': inmate, 'SECTION': new_sect, 
                               'CELL': new_cell, 'BLOCK': new_block, 
                               'MDATE' : mdate, 'HOUSING' : new_housing,
                               'CELLMATES' : cellmates}
                    
                    self.movement_log = \
                        self.movement_log.append(new_row, ignore_index = True)


       jail_snapshot.reset_changed_state()
       

    def get_housing_history_by_date_range(self, start_datetime:str, 
                                          end_datetime=None):
        ''' Retrieve all housing history (movements) from the given 
        datetime range
        
        @param start_datetime - The starting datetime for movement log retrieval
        @param end_datetime - End_datetime set to start_datetime + 1 (to the 
                            hour) when no date is explicitly set.
        '''
        
        format = '%Y-%m-%d %H:%M:%S'
        start_datetime = datetime.strptime(start_datetime, format)
        
        if end_datetime is None:
            end_datetime = start_datetime + timedelta(days=1)
        else:
            end_datetime = datetime.strptime(end_datetime, format)
            
        return self.housing_history.loc[(self.housing_history.MDATE >= start_datetime) &
                                 (self.housing_history.MDATE < end_datetime)].copy()
                                    
          
    ''' Retrieve dataframe with durations calculated from movement_log, using
    the given end_datetime as the end period, or 1 day from the start_datetime
    if not provided'''
    def get_duration_from_housing_history(self, sysid, end_datetime=None):
        
        start_datetime = min(self.movement_log.MDATE)
        
        # Set end_datetime, used to calculate the duration from last movmement
        # to end of 'day'
        if end_datetime is None:
            end_datetime = start_datetime + timedelta(days=1)
            print("end_datetime not provided, using +1 day from start_date")
        else:
            format = '%Y-%m-%d %H:%M:%S'
            end_datetime = datetime.strptime(end_datetime, format)
        
        temp_log = self.movement_log.loc[self.movement_log.SYSID == sysid].copy()
        temp_log.reset_index(drop=True, inplace=True)
        temp_log['DURATION'] = None
        
    
        for index, row in temp_log.iterrows() :
            # if the last row is not a full release to 'XXXX', calculate 
            # duration from MDATE end_datetime
            if index == (temp_log.tail(1).index[0]) and \
                temp_log.loc[index, 'SECTION'] != 'XXXX':
                temp_log.loc[index, 'DURATION'] = \
                    (end_datetime - temp_log.loc[index, 'MDATE'])/timedelta(hours=1)
            elif index == (temp_log.tail(1).index[0]) and \
                temp_log.loc[index, 'SECTION'] == 'XXXX':
                temp_log.loc[index, 'DURATION'] = 0
            else :
                temp_log.loc[index, 'DURATION'] = \
                    (temp_log.loc[index + 1, 'MDATE'] - 
                     temp_log.loc[index, 'MDATE'])/timedelta(hours=1)
                   
        return temp_log
    
    
    def get_inmate_movements(self, sysid:int):
        movements = self.movement_log.loc[self.movement_log.SYSID == sysid].copy()
        
        if movements is None:
            print("No day set for current day movements")
        
        return movements
    
    def get_all_movements_to_unit(self, section:str, block:str, cell:str):
        return self.housing_history.loc[(self.housing_history.SECTION == section) &
                                        (self.housing_history.BLOCK == block) &
                                        (self.housing_history.CELL == cell)].copy()
            
    def get_curr_movements_to_unit(self, section:str, block:str, cell:str):
        movements = self.movement_log.loc[(self.movement_log.SECTION == section) &
                                          (self.movement_log.BLOCK == block) &
                                          (self.movement_log.CELL == cell)].copy()
        
        if movements is None:
            print("No day set for current day movements")
            
        return movements
    
    def get_raw_housing_history(self):
        return self.raw_housing_history.copy()
    
    def get_housing_history(self):
        return self.housing_history.copy()
    
    def get_movement_log(self):
        return self.movement_log.copy()
    
    def get_all_inmate_movements(self, sysid:int):
        return self.housing_history.loc[self.housing_history.SYSID == sysid].\
                                        copy()
    
def main():
        
    # Setup jail day
    wilson = jail.jail('2021-10-01')
    
    # Retrieve all housing movements
    temp = housing()
    
    # Update movement log from initial snapshot of jail
    temp.update_movement_log_from_jail_snapshot(wilson)
    
    # Retrieve the movements for a particular day - this should match the jail
    # initiation day
    curr_day_movements = temp.get_housing_history_by_date_range('2021-10-01 00:30:00')
    
    # Iterate through each movement and update movement_log
    for index, row in curr_day_movements.iterrows() :          
        print("Processing move at: " + row.MDATE.strftime('%Y-%m-%d %H:%M:%S'))
        wilson.move_sysid(row.SYSID, row.SECTION, row.BLOCK, row.CELL, row.MDATE.strftime('%Y-%m-%d %H:%M:%S'))
        temp.update_movement_log_from_jail_snapshot(wilson)
    
    
    print("stop")
if __name__ == "__main__":
    main()