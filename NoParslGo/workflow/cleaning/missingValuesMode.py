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

numOfCols = df.shape[1]
#print(numOfCols)
dfNew =  missingValuesMode(0, numOfCols, df, colsToMode)
#print(dfNew)
dfNew.to_csv (outputDataset, index = False, header=True)
print("Module Completed: Fill Missing Values with Mode")
