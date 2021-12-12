# -*- coding: utf-8 -*-
"""
Created on Wed Nov 04 06:36:47 2020

@author: Johannes H. Uhl, University of Colorado Boulder
"""

#### for a given list of US counties, script loops ZAsmt databases and extracts attributes as specified below.
#### records are joined to the utmain table.
#### output: 
#### a csv per county with all area type records (from utbuildingarea) per building and property.

import sqlite3
from sqlite3 import Error
import os,sys
import pandas as pd
import numpy as np

csv_folder = r'F:\ZTRAX_2021\ZTRAX_CSV_COUNTY_RELEVANT_ATTRIBUTES' #target folder for csv files
county_csv = 'us_counties.csv'# list with county FIPS to be extracted.
db_folder = r'F:\ZTRAX_2021\ZTRAX_SQLITE_20210802' # folder where SQLite dbs are stored.
num_feat_fetchfromQuery = 50000

addtltables=[]
#utmain needs to be first, for joining
addtltables.append(['cont_zasmt_utmain','FIPS,ImportParcelID,TaxAmount,PropertyZip,PropertyZoningDescription,PropertyAddressCensusTractAndBlock,PropertyStreetPreDirectional,PropertyStreetName,PropertyStreetSuffix,PropertyStreetPostDirectional,PropertyFullStreetAddress,PropertyCity,PropertyAddressLatitude,PropertyAddressLongitude,PropertyAddressConfidenceScore,PropertyAddressCBSACode,PropertyAddressCBSADivisionCode,LotSizeSquareFeet,NoOfBuildings']) 
#these attributes will be joined. the first element is the table, other elements are attributes of interest, in a comma-separated string (NOT list of strings!)
#join on RowID only
addtltables.append(['cont_zasmt_utvalue','TotalAssessedValue,LandAssessedValue,ImprovementAssessedValue'])                 
addtltables.append(['cont_zasmt_utmailaddress','MailZip,MailState']) 
#join on RowID+BuildingOrImprovementNumber
addtltables.append(['cont_zasmt_utexteriorwall','BuildingOrImprovementNumber,ExteriorWallStndCode'])    
addtltables.append(['cont_zasmt_utbuilding','BuildingOrImprovementNumber,YearBuilt,EffectiveYearBuilt,YearRemodeled,BuildingQualityStndCode,BuildingConditionStndCode,ArchitecturalStyleStndCode,BuildingClassStndCode,NoOfStories,RoofCoverStndCode,RoofStructureTypeStndCode,PropertyLandUseStndCode,TotalRooms,TotalKitchens,HeatingTypeorSystemStndCode,AirConditioningTypeorSystemStndCode,FoundationTypeStndCode,NoOfUnits'])   
addtltables.append(['cont_zasmt_uttypeconstruction','BuildingOrImprovementNumber,TypeConstructionStndCode']) 
addtltables.append(['cont_zasmt_utbuildingAreas','BuildingOrImprovementNumber,BuildingAreaSqFt,BuildingAreaStndCode']) 

## the attributes used to join to the previous table (needs to be a 1:1 or 1:n join, but not m:n)
joinatt_lookup=[]
joinatt_lookup.append(['cont_zasmt_utvalue','RowID'])
joinatt_lookup.append(['cont_zasmt_utmailaddress','RowID'])
joinatt_lookup.append(['cont_zasmt_utexteriorwall','RowID'])
joinatt_lookup.append(['cont_zasmt_utbuilding',['RowID','BuildingOrImprovementNumber']])
joinatt_lookup.append(['cont_zasmt_uttypeconstruction',['RowID','BuildingOrImprovementNumber']])
joinatt_lookup.append(['cont_zasmt_utbuildingAreas',['RowID','BuildingOrImprovementNumber']])

## the join types used - depending whether there is a 1:1 or 1:n relationship
jointype_lookup=[]
jointype_lookup.append(['cont_zasmt_utvalue','left'])
jointype_lookup.append(['cont_zasmt_utmailaddress','left'])
jointype_lookup.append(['cont_zasmt_utexteriorwall','right'])
jointype_lookup.append(['cont_zasmt_utbuilding','right'])
jointype_lookup.append(['cont_zasmt_uttypeconstruction','right'])
jointype_lookup.append(['cont_zasmt_utbuildingAreas','right'])

aggregate_indoorareas_to_buildings=True
aggregate_buildings_to_properties=True


def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e) 
    return None
    
get_COUNTYLEVEL_data_from_sqlite=True

if get_COUNTYLEVEL_data_from_sqlite:
    
    countydf=pd.read_csv(county_csv)
    countydf=countydf.sort_values(by='FIPS')
    countyfipss = countydf.FIPS.dropna().map(int).map(str).str.zfill(5).values

    countycount=0
    for county in countyfipss:
        countycount+=1

        state=county[:2]

        ###########################################
        #if int(state)<9:
        #    continue        
        #if countycount<544:
        #    continue          
        #if state in ['01','08']:
        #    continue
        ###########################################
        
        dbs_current_state=[]
        for root, dirs, files in os.walk(db_folder):
            for dbfile in files:                               
                if not state in dbfile:
                    continue         
                if not 'ZAsmt_cont' in dbfile:
                    continue
                dbpath = root+os.sep+dbfile
                dbs_current_state.append(dbpath)
                            
        for db in dbs_current_state:
            if 'ZAsmt_cont' in db and not 'journal' in db:
                db_zasmt_cont = db
            conn = create_connection(db_zasmt_cont)
            #datadf=pd.DataFrame()    
            
            try:
                with conn: 
                    #c = conn.cursor()
                    print('connected to databases for state', state)
                    print(db_zasmt_cont)

                    #joineddf=pd.DataFrame()
                    tablecounter=0
                    for addtltable in addtltables:
                        tablecounter+=1
                        addtl_data=[]
                        c = conn.cursor()
                        sqlitetablename=addtltable[0]
                        sqlitecolnames=addtltable[1]
                        sqlcmd = """
                            SELECT RowID,%s FROM %s WHERE FIPS = '%s';
                        """  %(sqlitecolnames,sqlitetablename,county)
                        c.execute(sqlcmd)                                                
                        query_not_empty = True
                        while query_not_empty:
                            result = list(np.array(c.fetchmany(num_feat_fetchfromQuery)).copy())
                            print(county,sqlitetablename,'returned rows:', str(len(result)))
                            if result == []:
                                query_not_empty = False
                                break
                            #print(result)
                            addtl_data+=result
                        c.close()
                        del c
                        addtl_data_df = pd.DataFrame(addtl_data)
                        try:
                            addtl_data_df.columns=['RowID']+sqlitecolnames.split(',')#
                        except:
                            print('error assigning columns')
                            #### error if query is empty
                            #### then we try next table
                            tablecounter-=1
                            continue
                                                        
                        if tablecounter==1:
                            datadf = addtl_data_df.copy()
                        else:
                            for joinatt in joinatt_lookup:
                                if joinatt[0]==sqlitetablename:
                                    joincols=joinatt[1]
                                    break
                                
                            print ('joining...', county)
                            try:
                                addtl_data_df.BuildingOrImprovementNumber=addtl_data_df.BuildingOrImprovementNumber.map(str)
                            except:
                                True
                                
                            for jointype in jointype_lookup:
                                if jointype[0]==sqlitetablename:
                                    jointype=jointype[1]
                                    break                            
                            
                            datadf = datadf.copy().merge(right=addtl_data_df,on=joincols,how=jointype)
                            datadf=datadf.reset_index(drop=True) 
                            print('datadf',len(datadf))
                    print('county done', county)
                #del c        
                conn.close() 
                del conn
            except:
                conn.close() 
                del conn
                print('no data county?? error while extracting %s' %county)
                try:
                    del c
                except:
                    pass        
        try:
            print('writing %s area rows to csv...' %len(datadf))
        except:
            print('no data county?? error while extracting %s' %county)
            continue
        
        datadf.to_csv(csv_folder+os.sep+'ztrax_2021_extraction_county_areatypes_%s.csv' %county,index=False)        
        print(countycount,len(countyfipss),'ztrax attributes written to file. COUNTY:', county) 
        #sys.exit(0)
        del datadf
        try:
            del datadf2
        except:
            pass
            
