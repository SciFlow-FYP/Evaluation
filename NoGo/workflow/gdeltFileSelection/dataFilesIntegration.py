import os
import glob
import pandas as pd
from dataFileSelection import *
import csv

import sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import userScript

currentModule = "dataFilesIntegration"
path = userScript.datafilesLocation
workflowNumber = sys.argv[1]

if workflowNumber == "1":
	orderOfModules = userScript.orderOfModules1
	outputLocation = userScript.outputLocation1
elif workflowNumber == "2":
	orderOfModules = userScript.orderOfModules2
	outputLocation = userScript.outputLocation2

#dataFilesIntegration can only be called as the first module of a workflow
for i in range(len(orderOfModules)):
	if currentModule == orderOfModules[i]:
		if i != 0:
			break

outputDataset = outputLocation + currentModule + ".csv"

#create csv header
with open(path + "CSV.header.dailyupdates.txt") as csvfile:
    reader = csv.reader(csvfile, delimiter = "\t") # change contents to floats
    header = list(reader)[0]
    #print(header)


with open(outputDataset, "w", newline='', encoding='utf-8') as outcsv:
    writer = csv.writer(outcsv, delimiter=',')
    writer.writerow(header) # write the header


    # write the actual content line by line
    for filename in selectedFilesList:
        with open(filename, 'r', newline='', encoding='utf-8') as incsv:
            reader = csv.reader(incsv, delimiter="\t")
            writer.writerows(row + [0.0] for row in reader)


print("Integrated Selected files module completed")
