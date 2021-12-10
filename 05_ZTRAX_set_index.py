# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 06:43:18 2021

@author: Johannes H. Uhl, University of Colorado Boulder
"""

### sets indices on selected columns in the ZTRAX SQLite databases, for faster processing.

import os,sys,subprocess
import sqlite3
from sqlite3 import Error

zipfolder = r'H:\ZTRAX_2021\entire20210802'
db_folder = r'F:\ZTRAX_2021\ZTRAX_SQLITE_20210802'

idx_tables_asmt=[]
idx_tables_asmt.append('cont_zasmt_utmain')
idx_tables_asmt.append('cont_zasmt_utvalue')
idx_tables_asmt.append('cont_zasmt_utexteriorwall')
idx_tables_asmt.append('cont_zasmt_utbuilding')
idx_tables_asmt.append('cont_zasmt_uttypeconstruction')
idx_tables_asmt.append('cont_zasmt_utmailaddress')
idx_tables_asmt.append('cont_zasmt_utbuildingAreas')

idx_cols_asmt=['RowID','FIPS']

idx_tables_trans=[]
idx_tables_trans.append('cont_ztrans_utproperty')
idx_cols_trans=['TransID','FIPS']

statelist=[]
for root, dirs, files in os.walk(zipfolder):
    for filename in files:
        if '.zip' in filename:
            state = filename.replace('.zip','')
            
            #if state in ('20','28','35','38','50','56'): 
                #continue
                
            statelist.append(str(state))
            
statelist=sorted(list(set(statelist)))

statelist=['01']

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e) 
    return None

for state in statelist:
    
    zasmt=db_folder+os.sep+'%s_ZAsmt_cont_SQLite.db' %state
    ztrans=db_folder+os.sep+'%s_ZTrans_cont_SQLite.db' %state

    ### ZASMT
    
    conn = create_connection(zasmt)    
    with conn: 
        c = conn.cursor()
        for idx_table in idx_tables_asmt:
            for idx_col in idx_cols_asmt:
                sqlcmd = """
                     DROP INDEX IF EXISTS idx_%s_%s;
                """  %(idx_table,idx_col)
                c.execute(sqlcmd)                        
                
                sqlcmd = """
                     CREATE INDEX idx_%s_%s ON %s(%s);
                """  %(idx_table,idx_col,idx_table,idx_col)
                c.execute(sqlcmd)  
        
    conn.commit()
    conn.close()
    del conn,c
    print('set index %s' %state)
    
    continue

    ### ZTRANS ########################
    
    conn = create_connection(ztrans)    
    with conn: 
        c = conn.cursor()
        for idx_table in idx_tables_trans:
            for idx_col in idx_cols_trans:
                sqlcmd = """
                     DROP INDEX IF EXISTS idx_%s_%s;
                """  %(idx_table,idx_col)
                c.execute(sqlcmd)                        
                
                sqlcmd = """
                     CREATE INDEX idx_%s_%s ON %s(%s);
                """  %(idx_table,idx_col,idx_table,idx_col)
                c.execute(sqlcmd)  
        
    conn.commit()
    conn.close()
    del conn,c
    print('set index %s' %state)    
    
            