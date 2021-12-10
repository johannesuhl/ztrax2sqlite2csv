# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 06:43:18 2021

@author: Johannes H. Uhl, University of Colorado Boulder
"""

#script reads the column names of each table from the layout files and prepends 
#them to the data files.

import pandas as pd
import shutil
import sys
import os

import time
start = time.time()

outpath = r'H:\ZTRAX_2021\IMPORT_SCRIPTS\header_files'
datafile_path = r'H:\ZTRAX_2021\ZTRAX_CSV_20210802'
db_schema_file_zasmt = 'Layout_ZAsmt.csv'
db_schema_file_ztrans = 'Layout_ZTrans.csv'

def appendHeaderToData(db_schema_file,key1):
    
    df = pd.read_csv(db_schema_file)
      
    for i, cols_per_table in df.groupby('TableName'):  
        curr_tableName =  cols_per_table['TableName'].unique()[0]
        header_line = ''  
        
        for index, col in cols_per_table.iterrows():
            FieldName = col['FieldName']
            header_line += FieldName + "|"
    
        print(curr_tableName) 
        print(header_line)    
        
        headerfile = 'header_' + curr_tableName + ".csv"
        
        file = open(outpath+os.sep+headerfile,'w') 
        file.write(header_line[:-1]+'\n') 
        file.close()        
            

    for root, dirs, files in os.walk(outpath):
        
        for filename in files:
            current_tablename = filename.split("_")[1].replace('.csv','')[2:]
            
            for root2, dirs2, files2 in os.walk(datafile_path):
                for datafile in files2:
                    if key1 in root2 and (current_tablename == datafile.replace('.txt','')) and not 'header' in datafile:
                                            
                        infile = os.path.join(root2,datafile)
                        outfile = infile.replace('.txt','_header.txt')
                        
                        ################################
                        #if '37' in infile: continue
                        #if '42' in infile: continue
                        ################################
                        
                        print(infile, current_tablename, datafile)
    
                        #get header file:
                        curr_headerfile = outpath+os.sep+'header_ut' + current_tablename + ".csv"                    
    
                        with open(outfile,'wb') as wfd:                        
                            for f in [curr_headerfile,infile]:
                                with open(f,'rb') as fd:
                                    shutil.copyfileobj(fd, wfd, 1024*1024*10)
                                    #10MB per writing chunk to avoid reading big file into memory.  
                                    fd.close
                            wfd.close
                            
                        os.remove(infile)
                        os.rename(outfile,infile)
                        #sys.exit(0)



##########################################
key1 = 'ZAsmt'
appendHeaderToData(db_schema_file_zasmt,key1)
##########################################
key1 = 'ZTrans'
appendHeaderToData(db_schema_file_ztrans,key1)
##########################################

print('It took', time.time()-start, 'seconds.')