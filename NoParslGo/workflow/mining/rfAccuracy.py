import pandas as pd
import numpy as np


import os.path
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import userScript
#ignore warnings printed on terminal
pd.options.mode.chained_assignment = None  # default='warn'


workflowNumber = sys.argv[1]


if workflowNumber == "1":
	outputLocation = userScript.outputLocation1
	
elif workflowNumber == "2":
	outputLocation = userScript.outputLocation2

inputLocation = outputLocation + "rf/"
outputLocation = outputLocation + "rf.json"

from os import listdir
from os.path import isfile, join
filesInputLocation = [f for f in listdir(inputLocation) if isfile(join(inputLocation, f))]

dfNew = pd.DataFrame()

for i in filesInputLocation:
	#print(i)
	df = pd.read_csv(inputLocation + i)
	maxRow = df['Accuracy'].idxmax()
	rowData = df.iloc[[maxRow]]
	#print(rowData)
	#dfNew.append(rowData, ignore_index =True)
	#print(dfNew) 	
	dfNew = pd.concat([dfNew, rowData], axis=0, ignore_index=True)


print(dfNew)


maxAccuracy = dfNew['Accuracy'].idxmax()
print(maxAccuracy)
dfMaximum = dfNew.iloc[[maxAccuracy]]

out = dfMaximum.to_json(orient='records')[1:-1].replace('},{', '} {')
with open(outputLocation, 'w') as f:
    f.write(out)
