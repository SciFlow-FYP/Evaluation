import pandas as pd
import os.path
import sys
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import userScript

currentModule = "selectUserDefinedColumns"
workflowNumber = sys.argv[1]

if workflowNumber == "1":
	selectedColumns = userScript.selectColumns1
	orderOfModules = userScript.orderOfModules1
	inputDataset = userScript.inputDataset1
	outputLocation = userScript.outputLocation1
elif workflowNumber == "2":
	selectedColumns = userScript.selectColumns2
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



def selectUserDefinedColumns(df1):
	df = df1
	if (selectedColumns != "all"):
		dfConcat = pd.DataFrame()

		for i in selectedColumns:
		    #print(i)
		    #print(df[i])
		    df_i=df[i]
		    dfAfterUserSelectedColumns=pd.concat([dfConcat, df_i], axis=1)
		    dfConcat=dfAfterUserSelectedColumns
		    dfConcat.to_csv (outputDataset, index = False, header=True)

	else:
		dfConcat = df.to_csv (outputDataset, index = False, header=True)
	return "Selection of user defined columns done."


selectUserDefinedColumns(df)
print("Module Completed: Select User Defined Columns")
