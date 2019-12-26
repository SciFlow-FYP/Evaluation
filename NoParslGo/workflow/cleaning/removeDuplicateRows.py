import pandas as pd
import numpy as np
import sys
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import userScript


currentModule = "removeDuplicateRows"
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
			df = pd.read_csv(outputLocation + previousModule + ".csv")
			break

outputDataset = outputLocation + currentModule + ".csv"

def removeDuplicateRows(startRowIndex, endRowIndex, dFrame):

	df = pd.DataFrame()
	df = dFrame.iloc[np.r_[startRowIndex : endRowIndex] , : ]
	dfDroppedDuplicates = df.drop_duplicates()
	dfDroppedDuplicates.reset_index(inplace=True)

	return dfDroppedDuplicates

numOfRows = df.shape[0]
#print(numOfRows)
dfNew = removeDuplicateRows(0, numOfRows, df)

dfNew.drop("index",inplace=True,axis=1)
#print(dfNew)

dfNew.to_csv (outputDataset, index = False, header=True)
print("Module Completed: Remove Duplicate Rows")
