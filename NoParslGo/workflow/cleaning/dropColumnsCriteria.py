import pandas as pd
import numpy as np
import sys
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import userScript

currentModule = "dropColumnsCriteria"
workflowNumber = sys.argv[1]

if workflowNumber == "1":
	orderOfModules = userScript.orderOfModules1
	inputDataset = userScript.inputDataset1
	outputLocation = userScript.outputLocation1
	maxPercentageOfMissingValues = userScript.userDefinedColPercentage1
elif workflowNumber == "2":
	orderOfModules = userScript.orderOfModules2
	inputDataset = userScript.inputDataset2
	outputLocation = userScript.outputLocation2
	maxPercentageOfMissingValues = userScript.userDefinedColPercentage2

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
dropList = []


def dropColumnsCriteria(startColIndex, endColIndex, dFrame, maxPercentage, dropList):

	import pandas as pd
	import numpy as np


	#function to count thr number of missing values in a given column
	def countMissingValues(colName, df):
	  dfCol = df[colName]
	  return dfCol.isnull().sum()

	df = pd.DataFrame()
	df = dFrame.iloc[: , np.r_[startColIndex : endColIndex]]

	colNames = list(df)
	noOfRows = df.shape[0]

#	dfMissingValueCriteriaDropped=df

	for col in df.columns:
	  noMissingValues = countMissingValues(col,df)

	  if ((noMissingValues/noOfRows)>(maxPercentage/100)):
	    #dfMissingValueCriteriaDropped = dfMissingValueCriteriaDropped.drop(i, axis=1)
	    dropList.append(col)

	#ret = dfMissingValueCriteriaDropped

	return dropList

numOfCols = df.shape[1]
dfNew = pd.DataFrame()
dropColumnsCriteria(0, numOfCols, df, maxPercentageOfMissingValues, dropList)


df.drop(dropList,inplace=True,axis=1)
df.to_csv (outputDataset, index = False, header=True)
print("Module Completed: Drop Columns based on User Defined Criteria")
