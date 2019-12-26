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

currentModule = "dropUserDefinedColumns"
workflowNumber = sys.argv[1]

if workflowNumber == "1":
	orderOfModules = userScript.orderOfModules1
	inputDataset = userScript.inputDataset1
	outputLocation = userScript.outputLocation1
	dropCols = userScript.dropCols1
elif workflowNumber == "2":
	orderOfModules = userScript.orderOfModules2
	inputDataset = userScript.inputDataset2
	outputLocation = userScript.outputLocation2
	dropCols = userScript.dropCols2
elif workflowNumber == "3":
	orderOfModules = userScript.orderOfModules3
	inputDataset = userScript.inputDataset3
	outputLocation = userScript.outputLocation3
	dropCols = userScript.dropCols3


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

def dropUserDefinedColumns(df, dropCols):
    dfUserDroppedCols = df.drop(dropCols, axis=1)
    dfUserDroppedCols.to_csv (outputDataset, index = False, header=True)

dropUserDefinedColumns(df, dropCols)
print("Module Completed: Drop User Defined Columns")
