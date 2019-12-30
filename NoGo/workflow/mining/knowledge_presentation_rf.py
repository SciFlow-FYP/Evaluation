import pandas as pd
import csv
from pandas import DataFrame
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
import sys
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import userScript
import json


currentModule = "knowledge_presentation_rf"
workflowNumber = sys.argv[1]

if workflowNumber == "1":
	orderOfModules = userScript.orderOfModules1
	#inputDataset = "/home/mpiuser/Documents/FYP/gdelt/rf.json"
	#inputDataset = "/home/amanda/FYP/gdelt/rf.json"
	outputLocation = userScript.outputLocation1
	rfAccuracyJson = outputLocation + userScript.rfAccuracyJson1
elif workflowNumber == "2":
	orderOfModules = userScript.orderOfModules2
	#inputDataset = userScript.inputDataset2
	outputLocation = userScript.outputLocation2
	rfAccuracyJson = outputLocation + userScript.rfAccuracyJson2
elif workflowNumber == "3":
	orderOfModules = userScript.orderOfModules3
	#inputDataset = "/home/mpiuser/Documents/FYP/gdelt/test.txt"
	#inputDataset = "/home/amanda/FYP/gdelt/test.txt"
	outputLocation = userScript.outputLocation3
	rfAccuracyJson = outputLocation + userScript.rfAccuracyJson3
df = pd.DataFrame()
'''
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
'''
previousModule = "normalize"
df = pd.read_csv(outputLocation + previousModule + ".csv")

#read json file
with open(rfAccuracyJson, 'r') as myfile:
    data=myfile.read()

# parse file
obj = json.loads(data)
'''
# show values
print("usd: " + str(obj['usd']))
print("eur: " + str(obj['eur']))
print("gbp: " + str(obj['gbp']))
'''

X = df.iloc[:, 1:5].values
y = df.iloc[:, 6].values

#from sklearn.model_selection import train_test_split
#X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

# Feature Scaling
sc = StandardScaler()
X = sc.fit_transform(X)
#X_test = sc.transform(X_test)


classifier = RandomForestClassifier(n_estimators=obj['estimators'], max_depth = obj['depth'], min_samples_split=obj['split'], max_features=obj['maxfeatures'], random_state=0)
classifier.fit(X, y)
y_pred = classifier.predict([[-0.25011820853917, 5.4, 2,2]])
print(y_pred)
print("Module Completed: Rf knowledge presentation completed")
