# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 15:43:44 2021

@author: WMui
"""

Oracle = __import__("Oracle Connection")
import pandas as pd
import numpy as np

# Functions in here by default connects to DWPRD at 10.92.17.84/DWPRD
# with the provided password

# Returns original df, with added demos columns from datawarehouse
def get_demographics(df, mci_uniq_id_col = None, demos = None):
    conn = Oracle.connection(dsn='10.92.17.84/DWPRD', password='PVTKUFFb')
    
   
    # Default to returning the 5 columns below if no demographic columns
    # are specified
    _demos = ", ".join(demos) + ", MCI_UNIQ_ID" if demos!=None else \
        "MCI_UNIQ_ID, SSN, DOB, FNAME, LNAME"
 
    # Create a string of mci_uniq_ids from df parameter, unless a column
    # index is specified
    mci_col = df['MCI_UNIQ_ID'] if mci_uniq_id_col==None else \
        df.iloc[:, mci_uniq_id_col-1]  
    _mci_uniq_id_list_str = ",".join(map(str,tuple(mci_col)))
    
    query = "SELECT " + _demos + " FROM DW.DIM_CLIENT_SOURCE SOURCE " + \
    "LEFT JOIN DW.DIM_RACE RACE ON RACE.RACE_KEY = SOURCE.RACE_KEY " + \
    "LEFT JOIN DW.DIM_ETHNICITY ETHNIC ON ETHNIC.ETHNIC_KEY = SOURCE.ETHNIC_KEY " + \
    "WHERE SOURCE.SRC_SYS_KEY = 0 AND SOURCE.LAST_FLAG = 'Y' " + \
    "AND RACE.LAST_FLAG = 'Y' AND ETHNIC.LAST_FLAG = 'Y'"
            
    dw_df = conn.query(query)
    dw_df = pd.merge(df, dw_df, on='MCI_UNIQ_ID', how='left')
    
    conn.disconnect()
            
    return dw_df
