from parsl import load, python_app

import pandas as pd
import numpy as np
import statistics
from statistics import mode, StatisticsError

import sys
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import userScript
import dataType
import parslConfig
#ignore warnings printed on terminal
pd.options.mode.chained_assignment = None  # default='warn'

currentModule = "missingValuesMode"
workflowNumber = sys.argv[1]

if workflowNumber == "1":
	orderOfModules = userScript.orderOfModules1
	inputDataset = userScript.inputDataset1
	outputLocation = userScript.outputLocation1
	colsToMode = userScript.modeColumns1
elif workflowNumber == "2":
	orderOfModules = userScript.orderOfModules2
	inputDataset = userScript.inputDataset2
	outputLocation = userScript.outputLocation2
	colsToMode = userScript.modeColumns2


df = pd.DataFrame()
for i in range(len(orderOfModules)):
	#print(orderOfModules[i])
	if currentModule == orderOfModules[i]:
		if i == 0:
			df = pd.read_csv(inputDataset)
			break
		else:
			previousModule = orderOfModules[i-1]
			df = pd.read_csv(outputLocation + previousModule + ".csv")
			break

outputDataset = outputLocation + currentModule + ".csv"

@python_app
def missingValuesMode(startColIndex, endColIndex, dFrame, colsMode):
	df = pd.DataFrame()
	df = dFrame.iloc[: , np.r_[startColIndex : endColIndex]]
	numOfRows = df.shape[0]
	'''
	#drop unique columns
	for col in df.columns:
		if len(df[col].unique()) == numOfRows:
			df.drop(col,inplace=True,axis=1)
	'''
	if(colsMode == "all"):
		#Mode of all columns
		colNames = list(df)
	else:
		#Mode of user defined columns
		colNames = colsMode

	df2 = df
	df1 = pd.DataFrame()


	for col in colNames:
		#print (col)
		try:
			df1 = df[col].dropna()
			modeOfCol = statistics.mode(df1)
			df2[col].fillna(modeOfCol, inplace = True)
		except StatisticsError:
			print(col)
			print ("No unique mode found")



	ret  = df2
	return ret

maxThreads = userScript.maxThreads
numOfCols = df.shape[1]
#print(numOfCols)
dfNew = pd.DataFrame()
results = []

#one col per thread
if numOfCols <= maxThreads:
	for i in range (numOfCols):
		#print("test1")
		df1 = missingValuesMode(i, i+1, df, colsToMode)
		results.append(df1)

elif numOfCols > maxThreads:
	#print("test2")
	eachThreadCols = numOfCols // maxThreads
	for i in range (0,(maxThreads*eachThreadCols), eachThreadCols):
		df1 = missingValuesMode(i,(i+eachThreadCols),df,colsToMode)
		results.append(df1)

	if (numOfCols % maxThreads != 0):
		#print("test3")
		df2 = missingValuesMode((eachThreadCols * maxThreads),numOfCols,df,colsToMode)
		results.append(df2)

# wait for all apps to complete
[r.result() for r in results]

newlist = []
for i in results:
	newlist.append(i.result())

for i in newlist:
	dfNew = pd.concat([dfNew, i], axis=1)

#print(dfNew)
dfNew.to_csv (outputDataset, index = False, header=True)
print("Module Completed: Fill Missing Values with Mode")
#print(missingValuesMode(0,7,df,colsToMode).result())
