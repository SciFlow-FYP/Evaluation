#Select GDELT data files for processing 
#Files in the format 20180720.CSV 

#future work : Train review of file name to direct to suitable file selection functions

import os
import glob
from datetime import datetime, timedelta
import csv
import pandas as pd

import sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import userScript

path = userScript.datafilesLocation
os.chdir(path)

allFiles = os.listdir(path)

##File selection

selectedFiles = []

#select data files for a given month
def getMonthlyFiles(m):
	if (m == "JANUARY"):
		selectMonth = "01"
	elif (m == "FEBRUARY"):
		selectMonth = "02"
	elif (m == "MARCH"):
		selectMonth = "03"
	elif (m == "APRIL"):
		selectMonth = "04"
	elif (m == "MAY"):
		selectMonth = "05"
	elif (m == "JUNE"):
		selectMonth = "06"
	elif (m == "JULY"):
		selectMonth = "07"
	elif (m == "AUGUST"):
		selectMonth = "08"
	elif (m == "SEPTEMBER"):
		selectMonth = "09"
	elif (m == "OCTOBER"):
		selectMonth = "10"
	elif (m == "NOVEMBER"):
		selectMonth = "11"
	elif (m == "DECEMBER"):
		selectMonth = "12"
    
	#userdefined month- input from swift script
	userMonth = "JULY"

	#for all files in directory select files with matching int of user defined month
	for filename in allFiles:
		#print(filename)
		desiredMonth = getMonthlyFiles(userMonth)
		#print(desiredMonth)
		filenameMonth=filename[4:6]
		#print(filenameMonth)
		#print(filenameMonth == desiredMonth)
		if filenameMonth == desiredMonth:
			selectedFiles.append(filename)

	return selectedFiles    

#def getAnnualFiles(m):

#select files for a given range
def getDateRangeFiles():
	#get these two days in the userscript
	startingDate = userScript.startingDate
	endingDate = userScript.endingDate
	#startingDate = '2019.11.30'
	#endingDate = '2019.12.01'
	startingDate_obj = datetime.strptime(startingDate, '%Y.%m.%d').date()
	endingDate_obj = datetime.strptime(endingDate, '%Y.%m.%d').date()

	#print(startingDate_obj)
	#print(endingDate_obj)

	delta = endingDate_obj - startingDate_obj       # as timedelta

	selectedDays=[]
	for i in range(delta.days + 1):
		day = startingDate_obj + timedelta(days=i)
		str_day = day.strftime('%Y%m%d')
		selectedDays.append(str_day)
	
	#print(selectedDays)

	for filename in allFiles:
	
		filenameNew = filename[0:8]
	
		#print(filenameNew)
		for i in selectedDays:
			if i == filenameNew:
				selectedFiles.append(filename)
	
	#selectedFiles = selectedFiles.sort()			
	return selectedFiles



#print(getDateRangeFiles())
            
#def getDayOfTheWeekFiles(m):


selectedFilesList = getDateRangeFiles()

#print(selectedFilesList)
print("File Selection Completed")

modifiedSelectedFiles = []
#Append header to every CSV file
with open(path + "CSV.header.dailyupdates.txt") as csvfile:
	reader = csv.reader(csvfile, delimiter = "\t") # change contents to floats
	header = list(reader)[0]

for filename in selectedFilesList:
	#print(filename)
	filenameNew = filename[0:8] + '.csv'
	#print(filenameNew)
	modifiedSelectedFiles.append(filenameNew)
	#create modified files inside a different folder which is in the gdelt folder
	with open(path+  "modifiedSelectedFiles/" +filenameNew , "w", newline='', encoding='utf-8') as outcsv:
		writer = csv.writer(outcsv, delimiter=',')
		writer.writerow(header) # write the headers
		# write the actual content line by line
		f = open(filename, 'r', newline='', encoding='utf-8')
		z = csv.reader(f, delimiter='\t')
		writer.writerows(row + [0.0] for row in z)
		f.close()
		
	
	
print("Append headers to each file completed")
	
for filenameNew in modifiedSelectedFiles:
	df = pd.read_csv(path+  "modifiedSelectedFiles/" + filenameNew, index_col=False, low_memory=False)
	#print(df)

	#select specific country records
	df2 = df.loc[(df['Actor1Geo_CountryCode'] == userScript.Actor1CountryCode) | (df['Actor2Geo_CountryCode'] == userScript.Actor2CountryCode)]
	#print(df2)

	#overwrite the csv file
	df2.to_csv(path+  "modifiedSelectedFiles/" + filenameNew, index = False, header=True)

#print("Select country completed")
