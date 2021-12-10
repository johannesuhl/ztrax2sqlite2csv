# ztrax2sqlite2csv
A FME- and Python based workflow to import ZTRAX into a set of SQLite databases, and from there into county-level CSV files

1)	Extract ZTRAX ZIP into subfolder.
2)	Extract each state zip into a state subfolder (eg all csvs in 01.zip must be in \01\*.csv).7-ZIP can do that using the Extract to * command.
3)	Copy these subfolders in a separate folder “CSV”.
4)	Prepare script 01_generate_SQLite_databases_separate.py: Set zipfolder path and dbpath. Also, place the metadata files in the same directory as the script.
5)	Open the Layout.xlsx and copy the ZAsmt tab into a csv called Layout_ZAsmt.csv in the scripts folder – likewise for zTrans.
6)	Run 01_generate_SQLite_databases_separate.py. This will generate empty SQLITE databases in the db_path folder.
7)	Run 02_append_headers_to_csv_files.py. Set paths in script prior to that. This will append the column headers to each csv file. 
8)	In the first version imported in 2017, some “NULL” characters in specific csv files caused FME to crash. The script 03_remove_NUL_characters.py will search for these characters and replace them by “0”. If these errors occur in 10), this script can be used to clean the csv files.
9)	Then adjust paths in the script 04_generate_batch_workspaces.py. this script will take the FMW files in the TEMPLATE folder, and adjust the paths for each state-level database. It also creates a batchfile in . \BATCH\_all.bat.
10)	Run the _all.bat. This will import the data into SQLite databases (XX_ZTrans_cont_SQLite.db and XX_ZAsmt_cont_SQLite.db, for each state ID XX).
11)	Run 05_ZTRAX_set_index.py. This will set indices on database columns to speed up subsequent extractions.
12)	Run 06_ZTRAX_ZAsmt_relevant_attributes_SQLite2CSV.py to generate county-level extractions of ZTRAX attributes of interest. Implemented only for ZAsmt and for a set of around 50 specific attributes from different ZAsmt tables. There will be 3 csv files per county: (a) property-level, (b) building level, and (c) building area level.
