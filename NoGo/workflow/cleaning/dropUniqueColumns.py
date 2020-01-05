from parsl import load, python_app

import pandas as pd
import numpy as np
import sys
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import userScript


currentModule = "dropUniqueColumns"
uniqueColList = []

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

#use this function to drop columns that contain primary key like data
@python_app
def dropUniqueColumns(startColIndex, endColIndex, dFrame, uniqueColList):

	import pandas as pd
	import numpy as np

	df = pd.DataFrame()
	df = dFrame.iloc[: , np.r_[startColIndex : endColIndex]]
	numOfRows = df.shape[0]

	for col in df.columns:
		#print(col)
		#dfNew = df[[col]]

		#dfNew = dfNew.dropna()

		#numOfRows = dfNew.shape[0]
		if len(df[col].unique()) == numOfRows:
			#print(col)
			#df.drop(col,inplace=True,axis=1)
			uniqueColList.append(col)

	return uniqueColList

maxThreads = userScript.maxThreads
numOfCols = df.shape[1]
results = []

#one col per thread
if numOfCols <= maxThreads:
	for i in range (numOfCols):
		uList1 = dropUniqueColumns(i, i+1, df, uniqueColList)
		results.append(uList1)
		#dfNew = pd.concat([dfNew, df1] , axis=1)


elif numOfCols > maxThreads:
	#print("test1")
	eachThreadCols = numOfCols // maxThreads
	for i in range (0,(maxThreads)*eachThreadCols, eachThreadCols):
		uList1 = dropUniqueColumns(i,(i+eachThreadCols),df,uniqueColList)
		results.append(uList1)

	if (numOfCols % maxThreads != 0):
		#non parallel
		uList2 = dropUniqueColumns((eachThreadCols * maxThreads), numOfCols, df, uniqueColList)
		results.append(uList2)


# wait for all apps to complete
[r.result() for r in results]

#dropUniqueColumns(0,58,df,uniqueColList).result()
#print(uniqueColList)
df.drop(uniqueColList,inplace=True,axis=1)

df.to_csv(outputDataset, index = False, header=True)
print("Module Completed: Drop Unique Columns")
