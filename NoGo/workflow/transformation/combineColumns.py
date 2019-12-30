from parsl import load, python_app
from parsl.configs.local_threads import config
load(config)

import pandas as pd
import numpy as np
from datetime import datetime

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import userScript
#ignore warnings printed on terminal
pd.options.mode.chained_assignment = None  # default='warn'


currentModule = "combineColumns"
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
columnsToAggregate = [["Actor1Geo_CountryCode", "Actor2Geo_CountryCode", "ActorGeo_CountryCode" ]]


@python_app
def combineColumns(startRowIndex, endRowIndex, dFrame, columnsToAggregate):
	import pandas as pd
	import numpy as np
	df = pd.DataFrame()
	df = dFrame.iloc[np.r_[startRowIndex : endRowIndex] , : ]

	columnList = list(df.columns.values)

	for i in columnsToAggregate:
		column1 = i[0]
		#print(i[0])
		#column1index = df.columns.get_loc(column1)
		column2 = i[1]
		#column2index = df.columns.get_loc(column2)
		newColumn = i[2]
		newColumnList = []

		newColumnList = columnList
		newColumnList.remove(column1)
		newColumnList.remove(column2)
		newColumnList.append(newColumn)

		#print(newColumnList)


		dfNew = pd.DataFrame(columns = newColumnList)
		#print(dfNew)

		for index,row in df.iterrows():

			if row[column1] == row[column2]:
				x = row[column1]
				row = row.drop(labels=[column1,column2])
				#print(row)
				rowAdd = pd.Series([x], index=[newColumn])
				row = row.append(rowAdd)
				dfNew = dfNew.append(row, ignore_index=True)

			elif not row.notnull()[column1] and not row.notnull()[column2]:
				#print("2")
				#print("both null")
                		s = 0

			elif row.notnull()[column1] and not row.notnull()[column2]:
				#print("3")
				x = row[column1]
				row = row.drop(labels=[column1,column2])
				#print(row)
				rowAdd = pd.Series([x], index=[newColumn])
				row = row.append(rowAdd)
				dfNew = dfNew.append(row, ignore_index=True)

			elif row.notnull()[column2] and not row.notnull()[column1]:
				#print("4")
				x = row[column2]
				row = row.drop(labels=[column1,column2])
				#print(row)
				rowAdd = pd.Series([x], index=[newColumn])
				row = row.append(rowAdd)
				dfNew = dfNew.append(row, ignore_index=True)
			else:
				x = row[column1]
				y = row[column2]

				row = row.drop(labels=[column1,column2])
				#print(row)
				rowAdd1 = pd.Series([x], index=[newColumn])
				rowAdd2 = pd.Series([y], index=[newColumn])
				row1 = row.append(rowAdd1)
				dfNew = dfNew.append(row1, ignore_index=True)
				row2 = row.append(rowAdd2)
				dfNew = dfNew.append(row2, ignore_index=True)
				#print(row)



		#print(dfNew)
		return dfNew
		#dfNew.to_csv ("/home/amanda/FYP/testcsv/RFout1.csv", index = False, header=True)

maxThreads = userScript.maxThreads
results = []
numOfRows = df.shape[0]
results = []
dfNew = pd.DataFrame()
#df1 = combineColumns(0,100,df,columnsToAggregate)
#print(df1.result())

#not parallel --> relatively small number of rows here
if numOfRows <= maxThreads:
	df1 = combineColumns(0, numOfRows, df, columnsToAggregate)
	results.append(df1)

#parallel
elif numOfRows > maxThreads:
	#print("test2")
	eachThreadRows = numOfRows // maxThreads
	for i in range (0,(maxThreads*eachThreadRows), eachThreadRows):
		df1 = combineColumns(i,(i+eachThreadRows),df, columnsToAggregate)
		results.append(df1)
	if (numOfRows % maxThreads != 0):
		df2 = combineColumns((eachThreadRows * maxThreads), numOfRows, df, columnsToAggregate)
		results.append(df2)

# wait for all apps to complete
[r.result() for r in results]

newlist = []
for i in results:
	newlist.append(i.result())

#concat all the dfs into one row wise
for i in newlist:
	dfNew = pd.concat([dfNew, i], axis=0)


#dfNew = newlist[0]
#print(dfNew)
dfNew = dfNew.loc[(dfNew['ActorGeo_CountryCode'] == "CE")]
dfNew.to_csv (outputDataset, index = False, header=True)
print("Module Completed: Combine multiple columns.")


'''
for i in range(0,200000,200):
	df1 = combineColumns(i,i+200,df,columnsToAggregate)
	print(i)
	results.append(df1)

# Wait for all apps to finish and collect the results
outputs = [i.result() for i in results]

endTime = datetime.now().replace(microsecond=0)

print('\nEnd Time: ' + str(endTime) + ' Caluculation Done!\n')
print('Duration ' + str(getDuration(startTime,endTime)))

# Print results
print(outputs)
'''
