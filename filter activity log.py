# -*- coding: utf-8 -*-
"""
Created on Wed Jan 26 10:18:39 2022
@Description: Filter out rows with particular terms from the master_activity_log
@author: K012352
"""

import pandas as pd
import os

sh_file_name = "Segregated Housing list 2021-12-01 to 2021-12-31 (original 2).xlsx"
activity_file_name = "master_activity_log_1_2022.xlsx"
filtered_medical_file_name = "Filtered Medical - master_activity_log_12_2021.xlsx"

# Load files    
activity_log = pd.read_excel(os.path.dirname(os.getcwd()) + \
                            "\\Reports\\" + activity_file_name)
    
refused_early_log = activity_log[(activity_log.eq('Refused').any(axis=1)) |
                            (activity_log.eq('End Early').any(axis=1))]

refused_early_log.to_excel(os.path.dirname(os.getcwd()) + \
                            "\\Reports\\refused_end early " + activity_file_name)
    
new_transfer_log = activity_log[(activity_log.eq('New').any(axis=1)) |
                            (activity_log.eq('Transfer').any(axis=1))]

new_transfer_log.to_excel(os.path.dirname(os.getcwd()) + \
                            "\\Reports\\new_transfer " + activity_file_name)

print('in here')