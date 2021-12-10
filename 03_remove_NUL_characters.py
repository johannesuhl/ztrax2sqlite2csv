# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 06:43:18 2021

@author: Johannes H. Uhl, University of Colorado Boulder
"""

import fileinput
import os


### In the first version imported in 2017, some “NULL” characters in specific csv files 
### caused FME to crash. The script 03_remove_NUL_characters.py will search for these characters 
### and replace them by “0”. If these errors occur in 10), this script can be used 
### to clean the csv files.

datafile_path = r'H:\ZTRAX_2021\ZTRAX_CSV_20210802\06\ZTrans'
counter = 0
for root, dirs, files in os.walk(datafile_path):
    for datafile in files:
        
        if '.bak' in datafile: continue
    
        if not 'PropertyInfo' in datafile: continue
        
        counter+=1
        print(counter, root+os.sep+datafile)
        
        #if counter < 226: continue
                
        textToSearch = '\0'
        textToReplace = ''
        try:
            for line in fileinput.input(root+os.sep+datafile, inplace = 1): 
                print(line.replace(textToSearch, textToReplace)) 
            print('done')
            fileinput.close()
        except:
            print('ERROR', root+os.sep+datafile)

        