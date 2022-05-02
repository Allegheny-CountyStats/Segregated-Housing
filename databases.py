# -*- coding: utf-8 -*-
"""
Created on Wed Sep  2 12:49:08 2020

@author: WMui
"""

import cx_Oracle
import pyodbc
import pandas as pd
import re
import json
from dotenv import load_dotenv, find_dotenv
import os
import sys

# Add temporary path to the location of dhs_util. Eventually the package will
# be moved to a more permanent package location.
python_path = r'C:\Users\K012352\Allegheny County\Criminal Justice Analytics - Documents\Wilson\Code\dhs_util'
if python_path not in sys.path:
    sys.path.append(python_path)


class SQL_Server() :

    def __init__(self, database=None, user=None, password=None):
        database_known = False
        
        # Set user credentials, or use windows authentication
        if user == None or password == None:
            _user_login = 'Trusted_Connection=yes'
        else:
            _user_login = 'UID=' + user + ';PWD=' + password + ';'
        
        # Determine database
        if database == 'APCMS' :
            _server = 'SQL-CLDSRV-00.5JDCP.NET'
            _database = 'APCMS'
            database_known = True
        elif database == 'Adult Probation' :
            _server = 'SQL-CLDSRV-00.5JDCP.NET'
            _database = 'AdultProbation'
            database_known = True            
        elif database == 'Pretrial' :
            _server = 'SQL-CLDSRV-00.5JDCP.NET'
            _database = 'Pretrial'
            database_known = True            
        elif database == 'AOPC_DEX' :
            _server = 'SQL-SRV-00.5JDCP.NET'
            _database = 'AOPC_DEX'
            database_known = True            

        if database_known:            
            try:
                cnxn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + \
                    _server + ';DATABASE=' + _database + ';' + _user_login
    
                connection = pyodbc.connect(cnxn_str)
                
                self.conn = connection
                self.server = self.query("SELECT @@SERVERNAME").iloc[0,0] + \
                    "." + self.query("SELECT DEFAULT_DOMAIN()").iloc[0,0]
                self.user = self.query("SELECT CURRENT_USER").iloc[0,0]
                self.database = self.query("SELECT DB_NAME()").iloc[0,0]
                self.host = self.query("SELECT HOST_NAME()").iloc[0,0]
                
            except pyodbc.Error as ex:
                sqlstate = ex.args[1]
                print(sqlstate)
                
        else:
            self.conn = None
            self.user = None
            self.database = None
            self.server = None
            self.host = None
                
    # %% Print info about connection
    def print_info(self) :
        for key in vars(self) :
            print(key + ": " + str(vars(self)[key]))


    # %% Run a query and return a dataframe
    def query(self, query) :
    
        #cursor = self.conn.cursor()
        if query is None :
            print("Query is None, or invalid")
            return None
        else:
            try:
                sql = query             
                temp_df = pd.read_sql(sql, self.conn)

                return temp_df
    
            except pyodbc.Error as ex:
                    sqlstate = ex.args[1]
                    print('There is an error in the SQL Server database:', sqlstate)
            
            except Exception as er:
                print('Error:'+str(er))
        
    # %% Run a query from a file
    def query_from_file(self, file_path) :
        # Get query that retrieves housing movements
    
        with open(file_path) as f:
            file_query = f.read()
            
        return self.query(query=file_query)


class Oracle() :

    def __init__(self, database=None, schema=None, user=None, password=None):
        """Initializes an Oracle [database] object"""
        database_known = False
        
        # If credential file does not exist, exit from class creation
        if find_dotenv(python_path + '\\credentials.env') == '':
            print('file not found ' + '\'' + python_path + '\\credentials.env\'')
            sys.exit()
        else:
            load_dotenv(python_path + '\\credentials.env')

        
        ACPRD_USER = os.getenv('ACPRD_USER')
        ACPRD_PWD = os.getenv('ACPRD_PWD')
        ACPRD1_USER = os.getenv('ACPRD1_USER')
        ACPRD1_PWD = os.getenv('ACPRD1_PWD')
        DWPRD_USER = os.getenv('DWPRD_USER')
        DWPRD_PWD = os.getenv('DWPRD_PWD')
        KIDS_USER = os.getenv('KIDS_USER')
        KIDS_PWD = os.getenv('KIDS_PWD')
        
        if database == 'ACPRD':
            _dsn = '10.92.17.59/ACPRD'
            _user = ACPRD_USER if user==None else user
            _password = ACPRD_PWD if password==None else password
            database_known = True
        elif database == 'ACPRD1':
            _dsn = '10.92.17.59/ACPRD1'
            _user = ACPRD1_USER if user==None else user
            _password = ACPRD1_PWD if password==None else password
            database_known = True
        elif database == 'DWPRD':
            _dsn = '10.92.17.84/DWPRD'
            _user = DWPRD_USER if user==None else user
            _password = DWPRD_PWD if password==None else password
            database_known = True
        elif database == 'KIDSPRD3':
            _dsn = 'DHSDARE90/KIDSPRD3'
            _user = KIDS_USER if user==None else user
            _password = KIDS_PWD if password==None else password
            database_known = True
        
        if database_known:            
            try:
                connection = cx_Oracle.connect(user=_user,
                                               password=_password,
                                               dsn=_dsn)
            
                self.conn = connection
                self.user = self.query("SELECT USER FROM DUAL").loc[0, 'USER']
                self.database = self.query("SELECT ora_database_name FROM dual").\
                    loc[0, 'ORA_DATABASE_NAME']
                self.server = self.query("select SYS_CONTEXT('USERENV', " + \
                                         "'IP_ADDRESS', 15) ipaddr from dual").\
                    loc[0, 'IPADDR']
                self.host = self.query("select SYS_CONTEXT('USERENV'," + \
                                       "'HOST', 15) host_name from dual").\
                    loc[0, 'HOST_NAME']
            
                # Change schema, at initialization if provided
                if schema is not None: self.conn.current_schema = schema
            
            except cx_Oracle.DatabaseError as er:
                print('There is an error in the Oracle database:', er)
        else:
            self.conn = None
            self.user = None
            self.database = None
            self.server = None
            self.host = None

    # %% Set current_schema
    def set_current_schema(self, schema) :
        self.conn.current_schema = schema

    # %% Print info about connection
    def print_info(self) :
        """Return all defined attributes of the object"""
        
        for key in vars(self) :
            print(key + ": " + str(vars(self)[key]))
        
    # %% Connect to another database
    def connect(self, dsn, user=None) :
        self.conn.close()
        self.__init__(dsn, user)

    # %% Close the Oracle connection
    def disconnect(self) :
    
        if self.conn:
            self.conn.close()
        else:
            print('No Oracle connection to close')

    # %% Run a query and return a dataframe
    def query(self, query=None) :
        
        if query is None :
            print("Query is None, or invalid")
            return None
        
        else: 
            try:
                #cursor = self.conn.cursor()
                sql = query             
                temp_df = pd.read_sql(sql, self.conn)
    
                return temp_df 
             
            except cx_Oracle.DatabaseError as er:
                print('There is an error in the Oracle database:', er)
             
            except Exception as er:
                print('Error:'+str(er))
            
    # %% Run a query from a file
    def query_from_file(self, file_path) :
        # Get query that retrieves housing movements
    
        with open(file_path) as f:
            file_query = f.read()
            
        return self.query(query=file_query)

#%% Module functions

# Loads a sql file into an existing json file with given name of sql query, 
# with *kwargs as all parameters for entry
def load_sql_to_json(sql_file:str, json_file:str, name:str, **kwargs):

    # Load sql file to be read, and clean string
    with open(sql_file) as f:
        contents = f.read()
            
        # Changes new line escape characters with \n literal
        new_contents = contents.replace('\\n', '\n')
        # Replace multiple spaces with one space
        new_contents = re.sub(' {2,}', ' ', new_contents)
        # Changes double quotes with escaped double quotes
        new_contents = new_contents.replace('"', '\"')        
    
    # Write to json file with new script. Over-write existing entry if one
    # matching the name exists
    with open(json_file, 'r+') as file:
        json_obj = json.load(file)
        json_len = len(json_obj['queries'])

        for index, query in enumerate(json_obj['queries']):
            # If name matches remove the original item with all the given kwargs
            # key-value pairs, or keep existing values
            if query['Name'] == name:
                print ('Over-writing existing query for "' + name + '"')
                # Rebuild dictionary with updated keys from kwargs if exists
                new_dict = {key: kwargs.get(key, query[key]) for key in query}
                new_dict['SQL'] = new_contents
                
                json_obj['queries'].remove(query)
                        
                # Add new query item to json
                json_obj['queries'].insert(index, new_dict)
                
                file.seek(0)
                json.dump(json_obj, file, indent = 4)
                file.truncate()
                
                return
                
            # If no query exists with the given name, and at the end of the
            # the query list, create a new dictionary object
            elif index == json_len - 1 :
                new_dict = {}
                new_dict['Name'] = name
                new_dict.update({key: kwargs.get(key, query[key]) for key in kwargs})
                new_dict.update({'SQL': new_contents})
                # Add new query item to json
                json_obj['queries'].insert(index, new_dict)
        
                file.seek(0)
                json.dump(json_obj, file, indent = 4)
                file.truncate()
                
    return


# Update the key-value pairs, by query name, in json_file
def update_key_value(json_file:str, query_name:str, **kwargs):
    # Write to json file with new script. Over-write existing entry if one
    # matching the name exists
    with open(json_file, 'r+') as file:
        json_obj = json.load(file)

        for index, query in enumerate(json_obj['queries']):
            # If name matches remove the original item with all the given kwargs
            # key-value pairs
            if query['Name'] == query_name:
                print ('Over-writing existing key-value for "' + query_name + '"')
                # Rebuild dictionary with updated keys from kwargs if exists
                new_dict = {key: kwargs.get(key, query[key]) for key in query}
    
                # Remove old entry in json_obj
                json_obj['queries'].remove(query)
                        
                # Add new query item to json
                json_obj['queries'].insert(index, new_dict)
        
        file.seek(0)
        json.dump(json_obj, file, indent = 4)
        file.truncate()
        
    return
    
# Get query from json file, given name
def get_sql_from_json(json_file:str, query_name:str):
    # Write to json file with new script. Over-write existing entry if one
    # matching the name exists
    with open(json_file, 'r+') as file:
        json_obj = json.load(file)

        for index, query in enumerate(json_obj['queries']):
            # If name matches remove the original item with all the given kwargs
            # key-value pairs
            if query['Name'] == query_name:
                return query['SQL']
            
    return None

# Print the sql statement of given query name from json file, processing all
# escape characters into human-readable format
def print_sql_from_json(json_file:str, query_name:str):
    ''' Write to json file with new script. Over-write existing entry if one
         matching the name exists
    '''
    with open(json_file, 'r+') as file:
        json_obj = json.load(file)

        for index, query in enumerate(json_obj['queries']):
            # If name matches remove the original item with all the given kwargs
            # key-value pairs
            if query['Name'] == query_name:
                print (query['SQL'].encode('ascii', 'ignore').\
                       decode('unicode_escape'))
                return
        
        print("Query:'" + query_name + "' not found")

def output_json_to_sql(json_file:str, query_name=None):
    ''' Reads json file and outputs the query_name code to separate files. If 
        no query_name is specified, all queries in json file will be outputted
    '''
    with open(json_file, 'r+') as file:
        json_obj = json.load(file)
        
        for index, query in enumerate(json_obj['queries']):
            if query_name is None or query['Name'] == query_name:
                with open(query['Result_File'] + '.sql', 'w') as f:
                    f.write('--Name:' + query['Name'] + "\n")
                    f.write('--Description:' + query['Description'] + "\n")
                    f.write('--Author:' + query['Author'] + "\n")
                    f.write('--Database:' + query['Database'] + "\n\n")                   
                    f.write(query['SQL'].encode('ascii', 'ignore').\
                                decode('unicode_escape'))
                    f.close()
                        


# Get all queries in json file, including escape characters in their literal
# format
def get_all_queries(json_file:str):
    with open(json_file, 'r') as file:
        json_obj = json.load(file)
        
        for index, query in enumerate(json_obj['queries']):
            print("Name : " + query['Name'])
            print("Description : " + query['Description'] + '\n')


def run_sql_from_json(json_file:str, query_name:str):
    ''' Open json file, create a database connection from the given "database"
        key-value pair and run query and return results.
        
        Currently only usable on Oracle databases
    '''
    with open(json_file, 'r') as file:
        json_obj = json.load(file)
    
    for index, query in enumerate(json_obj['queries']) :
        if query['Name'] == query_name :
            conn = Oracle(query['Database'])
            df = conn.query(query['SQL'])

    return df


def main():
    pd.set_option('display.max_columns', 10)
    ACPRD1 = Oracle('ACPRD1')

if __name__ == "__main__" :
    main()