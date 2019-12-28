#Select GDELT data files for processing 
#Files in the format 20180720.CSV 

#future work : Train review of file name to direct to suitable file selection functions

import os
import glob
from datetime import datetime, timedelta

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
						
	return selectedFiles


#print(getDateRangeFiles())
            
#def getDayOfTheWeekFiles(m):


selectedFilesList = getDateRangeFiles()

print("Selected files module completed")
