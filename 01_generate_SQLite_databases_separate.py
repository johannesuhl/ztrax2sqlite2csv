# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 06:43:18 2021

@author: Johannes H. Uhl, University of Colorado Boulder
"""

#Script creates an Asmt and Trans SQLite DB for each state.

import pandas as pd
import os
import subprocess
##############################################################################
dbpath = r'F:\ZTRAX_2021\ZTRAX_SQLITE_20210802'
csvfolder = r'H:\ZTRAX_2021\ZTRAX_CSV_20210802'

db_schema_file_zasmt = 'Layout_ZAsmt.csv'
create_table_script_zasmt = r'H:\ZTRAX_2021\IMPORT_SCRIPTS\create_tables_ZAsmt_cont_SQLite.sql'

db_schema_file_ztrans = 'Layout_ZTrans.csv'
create_table_script_ztrans = r'H:\ZTRAX_2021\IMPORT_SCRIPTS\create_tables_ZTrans_cont_SQLite.sql'

##############################################################################
df = pd.read_csv(db_schema_file_zasmt)
createtable_statement = ''    
for i, cols_per_table in df.groupby('TableName'):        
       
    curr_tableName =  'cont_zasmt_' + cols_per_table['TableName'].unique()[0].lower()
    #print curr_tableName
    createtable_statement += '\n CREATE TABLE ' + curr_tableName + '(\n'  
        
    for index, col in cols_per_table.iterrows():
        
        FieldName = col['FieldName']#.lower()
        DataType = col['DateType']
        MaxLength = col['MaxLength'] 
        
        if DataType == 'uniqueidentifier':
            DataType = 'text'            
        if DataType == 'varchar':
            DataType = 'text'             
        if DataType == 'date':
            DataType = 'text'            
        if DataType == 'money':
            DataType = 'real'
        if MaxLength=='-1':
            DataType = 'text'   
        if DataType == 'int':
            DataType = 'integer'
        if DataType == 'char':
            DataType = 'text'
        if DataType == 'tinyint':
            DataType = 'integer'
        if DataType == 'decimal':
            DataType = 'real'
        if DataType == 'datetime':
            DataType = 'text'
        if DataType == 'smallint':
            DataType = 'integer'
        if DataType == 'bigint':
            DataType = 'integer'
                   
        createtable_statement+='    %s %s,\n' %(FieldName,DataType) 
            
    if curr_tableName == 'cont_zasmt_utmain':#.lower(): #add a geometry field
        createtable_statement+='    %s %s,\n' %('geom','blob') 
    
    createtable_statement+='    %s %s PRIMARY KEY,\n' %('fme_id','text') 

    createtable_statement = createtable_statement[:-2] + '\n);'
    createtable_statement = createtable_statement.replace('text()','text')
    print(createtable_statement)

file = open(create_table_script_zasmt,'w') 
file.write(createtable_statement) 
file.close() 

##############################################################################

df = pd.read_csv(db_schema_file_ztrans)
createtable_statement = ''    
for i, cols_per_table in df.groupby('TableName'):        
           
    curr_tableName =  'cont_ztrans_' + cols_per_table['TableName'].unique()[0].lower()
    #print curr_tableName
    createtable_statement += '\n CREATE TABLE ' + curr_tableName + '(\n'  
        
    for index, col in cols_per_table.iterrows():
        FieldName = col['FieldName']#.lower()
        DataType = col['DateType']
        MaxLength = col['MaxLength'] 
        
        if DataType == 'uniqueidentifier':
            DataType = 'text'            
        if DataType == 'varchar':
            DataType = 'text'             
        if DataType == 'date':
            DataType = 'text'            
        if DataType == 'money':
            DataType = 'real'
        if MaxLength=='-1':
            DataType = 'text'   
        if DataType == 'int':
            DataType = 'integer'
        if DataType == 'char':
            DataType = 'text'
        if DataType == 'tinyint':
            DataType = 'integer'
        if DataType == 'decimal':
            DataType = 'real'
        if DataType == 'datetime':
            DataType = 'text'
        if DataType == 'smallint':
            DataType = 'integer'
        if DataType == 'bigint':
            DataType = 'integer'
                   
        createtable_statement+='    %s %s,\n' %(FieldName,DataType)       
         
    if curr_tableName == 'cont_ztrans_utpropertyinfo':#.lower(): #add a geometry field
        createtable_statement+='    %s %s,\n' %('geom','blob') 
 
    createtable_statement+='    %s %s PRIMARY KEY,\n' %('fme_id','text') 
 
    createtable_statement = createtable_statement[:-2] + '\n);'
    createtable_statement = createtable_statement.replace('text()','text')
    print(createtable_statement)

file = open(create_table_script_ztrans,'w') 
file.write(createtable_statement) 
file.close() 

###########################################
#now we create the database for each state and each of the domains:

for root, dirs, files in os.walk(csvfolder):
    for dir in dirs:
        state = dir
		
        dbfile = dbpath+os.sep+state+'_ZAsmt_cont_SQLite.db'
        create_cmd = 'sqlite3 %s < %s' % (dbfile,create_table_script_zasmt)
        print(create_cmd)
        os.system(create_cmd)

        dbfile = dbpath+os.sep+state+'_ZTrans_cont_SQLite.db'
        create_cmd = 'sqlite3 %s < %s' % (dbfile,create_table_script_ztrans)
        print(create_cmd)
        os.system(create_cmd)
   
  
