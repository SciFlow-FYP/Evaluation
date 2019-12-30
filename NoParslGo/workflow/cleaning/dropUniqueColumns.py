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


numOfCols = df.shape[1]
dropUniqueColumns(0, numOfCols, df, uniqueColList)

# wait for all apps to complete
#[r.result() for r in results]

#dropUniqueColumns(0,58,df,uniqueColList).result()
#print(uniqueColList)
df.drop(uniqueColList,inplace=True,axis=1)

df.to_csv(outputDataset, index = False, header=True)
print("Module Completed: Drop Unique Columns")
