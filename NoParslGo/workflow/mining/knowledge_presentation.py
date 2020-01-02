import pandas as pd
import csv
from pandas import DataFrame
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
import sys

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import userScript


currentModule = "knowledge_presentation"
workflowNumber = sys.argv[1]

if workflowNumber == "1":
	orderOfModules = userScript.orderOfModules1
	inputDataset = userScript.inputDataset1
	outputLocation = userScript.outputLocation1
	kmeansAccuracy = outputLocation + userScript.kmeansAccuracy1
elif workflowNumber == "2":
	orderOfModules = userScript.orderOfModules2
	inputDataset = userScript.inputDataset2
	outputLocation = userScript.outputLocation2
	kmeansAccuracy = outputLocation + userScript.kmeansAccuracy2
elif workflowNumber == "3":
	orderOfModules = userScript.orderOfModules3
	#inputDataset = "/home/mpiuser/Documents/FYP/gdelt/kmeans.txt"
	#inputDataset = "/home/amanda/FYP/gdelt/kmeans.txt"
	outputLocation = userScript.outputLocation3
	kmeansAccuracy = outputLocation + userScript.kmeansAccuracy3

df = pd.DataFrame()
previousModule = "dropUserDefinedColumns"
df = pd.read_csv(outputLocation + previousModule + ".csv")


outputDataset = outputLocation + currentModule + ".csv"

#data = pd.read_csv('/home/mpiuser/Documents/FYP/gdelt/missingValuesMode.csv')
dfin = DataFrame(df, columns = ['AvgTone', 'GoldsteinScale', 'NumMentions'])
X = dfin.values

f= open(kmeansAccuracy, "r")
n = int(f.read())#int(sys.argv[1])


kmeans = KMeans(n_clusters=n, random_state= 0).fit(X)
dfin['clusterNo'] = kmeans.labels_[:]
#print(np.unique(kmeans.labels_[:]))
u = kmeans.labels_[:]
zeros = np.sum(u == 0)
ones = np.sum(u == 1)
#print(zeros)
#print(ones)
centroids = kmeans.cluster_centers_
#print(centroids)
#print(dfin)

dfin.to_csv (outputDataset, index = None, header=True)
print("Module Completed: append label module after kmeans completed")
