
import os
import glob
import pandas as pd
import csv
import numpy as np

import appendRecords
years, uniqueCountries = appendRecords.main("2")


import os.path
import sys
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import userScript

currentModule = "integrate"
workflowNumber = sys.argv[1]

if workflowNumber == "1":
	orderOfModules = userScript.orderOfModules1
	inputDataset = userScript.inputDataset1
	outputLocation = userScript.outputLocation1
elif workflowNumber == "2":
	orderOfModules = userScript.orderOfModules2
	inputDataset = userScript.inputDataset2
	outputLocation = userScript.outputLocation2


df = pd.DataFrame()
for i in range(len(orderOfModules)):
	#print(orderOfModules[i])
	if currentModule == orderOfModules[i]:
		if i == 0:
			df = pd.read_csv(inputDataset)
			break
		else:
			previousModule = orderOfModules[i-1]
			dfOriginal = pd.read_csv(outputLocation + previousModule + ".csv")
			break

outputDataset = outputLocation + currentModule + ".csv"

# CONSOLIDATE GDELT OUTPUT WITH ACTUAL EVENTS IN ORDER TO GENERATE A LABEL

# read original csv generated from gdelt
#dfOriginal = pd.read_csv(, header=0)

# read manual csv with riot information
dfManual = pd.read_csv(userScript.outputLocation2+"appendRecords.csv", header=0)

# number of rows in gdelt generated dataset
numberOfRowsOriginal = dfOriginal.shape[0]

# number of rows in manually generated dataset
numberOfRowsManual = dfManual.shape[0]

# insert new label column with default value of zero
dfOriginal['year']=0
dfOriginal['month']=0
dfOriginal['date']=0
dfOriginal['label']=0


# iterating over rows of df original and generating values for Y,M,D from SQLDATE
for i, j in dfOriginal.iterrows():
	sqldate = dfOriginal.loc[i][0]
	strSqlDate = str(sqldate)

	year = strSqlDate[0:4]
	month = strSqlDate[4:6]
	date = strSqlDate[6:8]

	dfOriginal.set_value([i], ['year'], year)
	dfOriginal.set_value([i], ['month'], month)
	dfOriginal.set_value([i], ['date'], date)

# function to generate a monthly dataframe from the Manually created dataset
def generateMonthlyDf(year,month,country):
	dfMonthly = pd.DataFrame(columns = ["Year", "Month", "Date", "ActorGeo_CountryCode", "Indicator"])
	for p,q in dfManual.iterrows():
		if (int(dfManual.loc[p]["Year"])==int(year)):
			if (int(dfManual.loc[p]["Month"])==int(month)):
				if dfManual.loc[p]["ActorGeo_CountryCode"]==country:
					dfMonthly=dfMonthly.append(dfManual.loc[p][:], ignore_index=True)
	return dfMonthly

# call generateMonthlyDF function to all months in given years
for i in years:
	for j in range (1,13):
		for c in uniqueCountries:
			y=str(i)
			m=str(j)
			# add "0" in front of one digit months
			if m in ["1", "2", "3", "4", "5", "6", "7", "8", "9"]:
				m=m.zfill(2)

			dfName=y+m+c
			dfName1=dfName
			dfName=generateMonthlyDf(i,j,c)
			#LETS NOT WRITE THIS TO A CSV. TAKE DFS
			dfName.to_csv(userScript.outputLocation2+"monthlyDF/"+dfName1+".csv", sep=',', encoding='utf-8', index=False, header=True)


loc1 = userScript.outputLocation2+"monthlyDF/"

# function to find dataframe for given month
def findMonthlyDf(loc, name):
	name=name+".csv"
	for f in os.listdir(loc):
		if f.endswith(".csv"):
			if (f == name):
				df = pd.read_csv(loc+f,  sep = ',', header=0)
				df.fillna('O', inplace=True)
				return df

# labelled as riot only if average tone is negative

# iterate over all records from gdelt dataset
for i in range (numberOfRowsOriginal):
	y=dfOriginal.loc[i]["year"]
	m=dfOriginal.loc[i]["month"]
	d=dfOriginal.loc[i]["date"]
	c=dfOriginal.loc[i]["ActorGeo_CountryCode"]
	dfName2=y+m+c
	#print(dfName2)
	comparativeDF = findMonthlyDf(loc1,dfName2)
	# find record with matching date and country from monthlyDF
	for m, n in comparativeDF.iterrows():
		cmpDateInt=int(comparativeDF.loc[m]["Date"])
		cmpDate=str(cmpDateInt)
		if cmpDate==d:
			# set label to zero if corresponding record exists in monthlyDF
			if comparativeDF.loc[m]["Indicator"]==1:
				if dfOriginal.loc[i]["AvgTone"]<0:
					dfOriginal.set_value([i], ["label"], 1)


dfOriginal.to_csv(outputDataset, sep=',', encoding='utf-8', index=False, header=True)

print("Module Completed: Merge dataset complete")
