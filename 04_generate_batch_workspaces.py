# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 06:43:18 2021

@author: Johannes H. Uhl, University of Colorado Boulder
"""

import os,sys
import shutil
import fileinput

### This script uses the template FME workbench files (FMW) and creates an FMW file for each DB and for each state.
### Also it creates the BAT file "_all.bat".
### Running this BAt file will start the import into sqlite DBs.

batchoutputs_asmt = []
batchoutputs_trans = []

transact_size = 25000

inpath = r'H:\ZTRAX_2021\IMPORT_SCRIPTS\TEMPLATES'
outpath = r'H:\ZTRAX_2021\IMPORT_SCRIPTS\BATCH'
zipfolder = r'H:\ZTRAX_2021\entire20210802'
fmepath='C:\\Program Files\\FME2019\\fme.exe'

replaceDBpath = True
if replaceDBpath:
    olddbpath = r'H:\ZTRAX_ALL\SQLITE'
    newdbpath = r'F:\ZTRAX_2021\ZTRAX_SQLITE_20210802' 

replaceCSVpath = True
if replaceCSVpath:
    oldcsvpath = r'H:\ZTRAX_ALL\CSV\cont'
    newcsvpath = r'H:\ZTRAX_2021\ZTRAX_CSV_20210802'     
    
    
    

statelist=[]
for root, dirs, files in os.walk(zipfolder):
    for filename in files:
        if '.zip' in filename:
            state = filename.replace('.zip','')
            
            #if state in ('20','28','35','38','50','56'): 
                #continue
                
            statelist.append(str(state))
            
statelist=sorted(list(set(statelist)))

for root, dirs, files in os.walk(inpath):
    for fmw in files:
        if '.fmw' in fmw and 'XX' in fmw:
            print(fmw)
            for state in  statelist:
                infmw = root+os.sep+fmw
                outfmw = infmw.replace('XX',state).replace(inpath,outpath)
                shutil.copyfile(infmw,outfmw)
                      
                #replace paths in the fmw file:
                textToSearch = '\\XX_\\'
                textToReplace = '\\'+state+'_'
                for line in fileinput.input(outfmw, inplace = 1): 
                      print(line.replace(textToSearch, textToReplace).rstrip())
                #fileinput.close()
 
                 #replace paths in the fmw file:
                textToSearch = 'XX_'
                textToReplace = ''
                for line in fileinput.input(outfmw, inplace = 1): 
                      print(line.replace(textToSearch, textToReplace).rstrip())
                      
                #replace paths in the fmw file:
                textToSearch = 'XX'
                textToReplace = state
                for line in fileinput.input(outfmw, inplace = 1): 
                      print(line.replace(textToSearch, textToReplace).rstrip())
                      
                if replaceDBpath:
                    #replace paths in the fmw file:
                    textToSearch = olddbpath
                    textToReplace = newdbpath+os.sep+state+'_'
                    for line in fileinput.input(outfmw, inplace = 1): 
                          print(line.replace(textToSearch, textToReplace).rstrip())
                    #fileinput.close()

                if replaceCSVpath:
                    #replace paths in the fmw file:
                    textToSearch = oldcsvpath
                    textToReplace = newcsvpath
                    for line in fileinput.input(outfmw, inplace = 1): 
                          print(line.replace(textToSearch, textToReplace).rstrip())
                    #fileinput.close()
                    
                #replace paths in the fmw file:
                textToSearch = 'XX_ZTrans_cont_SQLite.db'
                textToReplace = state+'_ZTrans_cont_SQLite.db'
                for line in fileinput.input(outfmw, inplace = 1): 
                      print(line.replace(textToSearch, textToReplace).rstrip())
                #fileinput.close()

                textToSearch = 'XX_ZAsmt_cont_SQLite.db'
                textToReplace = state+'_ZAsmt_cont_SQLite.db'
                for line in fileinput.input(outfmw, inplace = 1): 
                      print(line.replace(textToSearch, textToReplace).rstrip())
                #fileinput.close()

                #replace paths in the fmw file:
                textToSearch = '.log'
                textToReplace = '_'+state+'.log'
                for line in fileinput.input(outfmw, inplace = 1): 
                      print(line.replace(textToSearch, textToReplace).rstrip())
                #fileinput.close()

                #replace paths in the fmw file:
                textToSearch = state+'_\\'
                textToReplace = state+'_'
                for line in fileinput.input(outfmw, inplace = 1): 
                      print(line.replace(textToSearch, textToReplace).rstrip())
                #fileinput.close()
                
                #increase transaction size to speedup process:
                textToSearch = 'DEFAULT_MACRO SPATIALITE_OUT_TRANSACTION_SIZE_SPATIALITE_1 1000'
                textToReplace = 'DEFAULT_MACRO SPATIALITE_OUT_TRANSACTION_SIZE_SPATIALITE_1 '+str(transact_size)
                for line in fileinput.input(outfmw, inplace = 1): 
                      print(line.replace(textToSearch, textToReplace).rstrip())
                #fileinput.close()

                #increase transaction size to speedup process:
                textToSearch = 'DEFAULT_MACRO SQLITE3_OUT_TRANSACTION_INTERVAL_SQLITE3_1 500'
                textToReplace = 'DEFAULT_MACRO SQLITE3_OUT_TRANSACTION_INTERVAL_SQLITE3_1 '+str(transact_size)
                for line in fileinput.input(outfmw, inplace = 1): 
                      print(line.replace(textToSearch, textToReplace).rstrip())
                fileinput.close()

                if 'ZAsmt' in fmw:                    
                    batchoutputs_asmt.append('"%s" ' %fmepath+outfmw+'\n')            
                if 'ZTrans' in fmw:                    
                    batchoutputs_trans.append('"%s" ' %fmepath+outfmw+'\n')            
                
                print(state)
                #sys.exit(0)
                
for state in statelist:
    state_batchfile = outpath+os.sep+state+'.bat'
    batchfileobj = open(state_batchfile,'w')
    
    for line in batchoutputs_asmt+batchoutputs_trans:
        if state+'.' in line:
            print(line,file=batchfileobj)
    print('pause',file=batchfileobj)

    batchfileobj.close()   

batchoutputs=sorted(batchoutputs_asmt+batchoutputs_trans)
all_batchfile = outpath+os.sep+'_all.bat'
batchfileobj = open(all_batchfile,'w') 
for line in batchoutputs:
    print(line,file=batchfileobj)
print('pause',file=batchfileobj)

batchfileobj.close()                