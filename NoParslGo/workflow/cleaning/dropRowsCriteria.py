import pandas as pd
import numpy as np
import sys
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import userScript

currentModule = "dropRowsCriteria"

workflowNumber = sys.argv[1]

if workflowNumber == "1":
	orderOfModules = userScript.orderOfModules1
	inputDataset = userScript.inputDataset1
	outputLocation = userScript.outputLocation1
	maxPercentageOfMissingValues = userScript.userDefinedRowPercentage1
elif workflowNumber == "2":
	orderOfModules = userScript.orderOfModules2
	inputDataset = userScript.inputDataset2
	outputLocation = userScript.outputLocation2
	maxPercentageOfMissingValues = userScript.userDefinedRowPercentage2

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



def dropRowsCriteria(startRowIndex, endRowIndex, dFrame, maxPercentageOfMissingValues):

	df = pd.DataFrame()
	df = dFrame.iloc[np.r_[startRowIndex : endRowIndex] , : ]
	numOfColumns = len(df.columns)
	#Keep only the rows with at least threshold non-NA values.
	threshold = (numOfColumns * maxPercentageOfMissingValues)/100
	dfDroppedRowsCriteria = df.dropna(thresh=threshold)
	return dfDroppedRowsCriteria


numOfRows = df.shape[0]
#print(numOfRows)
df1 = dropRowsCriteria(0, numOfRows, df, maxPercentageOfMissingValues)


df1.to_csv (outputDataset, index = False, header=True)
print("Module Completed: Drop Rows based on User Defined Criteria")

#dropRowsCriteria(0,9999, df, maxPercentageOfMissingValues).result()).to_csv(outputDataset, index = False, header=True)
