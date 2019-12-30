import pandas as pd
import numpy as np
from sklearn import preprocessing

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import userScript
import dataType
#ignore warnings printed on terminal
pd.options.mode.chained_assignment = None  # default='warn'

currentModule = "normalize"
workflowNumber = sys.argv[1]

if workflowNumber == "1":
	orderOfModules = userScript.orderOfModules1
	inputDataset = userScript.inputDataset1
	outputLocation = userScript.outputLocation1
	colsToNormalize = userScript.userDefinedNormalizeColumns1
elif workflowNumber == "2":
	orderOfModules = userScript.orderOfModules2
	inputDataset = userScript.inputDataset2
	outputLocation = userScript.outputLocation2
	colsToNormalize = userScript.userDefinedNormalizeColumns2

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

for i in colsToNormalize:
	if dataType.dataType(i, df) == "str":
		#print("Cannot normalize string column: ", i)
		colsToNormalize.remove(i)

#print(colsToNormalize)

def normalize(startColIndex, endColIndex, dFrame, normalizeCols):
	import pandas as pd
	import numpy as np
	from sklearn import preprocessing

	df = pd.DataFrame()
	df = dFrame.iloc[: , np.r_[startColIndex : endColIndex]]

	dfColNames = list(df)
	normalizeCols = list(set(dfColNames).intersection(normalizeCols))
	#print(normalizeCols)

	if len(normalizeCols)!=0:

		# Normalize The Column
		# Create x, where x the 'scores' column's values as floats
		x = df[normalizeCols].values.astype(float)
		#print(x)

		# Create a minimum and maximum processor object
		min_max_scaler = preprocessing.MinMaxScaler()
		# Create an object to transform the data to fit minmax processor
		x_scaled = min_max_scaler.fit_transform(x)

		# Run the normalizer on the dataframe
		df_normalized = pd.DataFrame(x_scaled)
		j = 0
		#replace the normalized column in the original df
		for i in normalizeCols:
			df[i] = df_normalized[j]
			j=j+1
	return df

#print(normalize(0,3,df,['AvgTone', 'QuadClass']).result())
#this returns the column(s) that was normalized.
#drop the previous column and concat at the end.
#parallelize the number of cols in the user script instruction - change for mode, normalize, encode


numOfCols = df.shape[1]
#print(numOfCols)
dfNew = normalize(0, numOfCols, df, colsToNormalize)

#print(dfNew)
dfNew.to_csv (outputDataset, index = False, header=True)
print("Module Completed: Normalize")
