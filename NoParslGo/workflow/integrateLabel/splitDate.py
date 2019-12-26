import pandas as pd
import os.path
import sys
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import userScript

currentModule = "splitDate"

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

def assignCountryCode(df1):
	df = df1

	count=0
	df["date"]=0
	df["month"]=0

	for i in df["event_date"]:
		
		#set date column
		d=i[0:2]
		df.set_value([count], ["date"], d)

		#set month column
		m=i[3:6]
		if m=="Jan":
			df.set_value([count], ["month"], 1)
		elif m=="Feb":
			df.set_value([count], ["month"], 2)
		elif m=="Mar":
			df.set_value([count], ["month"], 3)
		elif m=="Apr":
			df.set_value([count], ["month"], 4)
		elif m=="May":
			df.set_value([count], ["month"], 5)
		elif m=="Jun":
			df.set_value([count], ["month"], 6)
		elif m=="Jul":
			df.set_value([count], ["month"], 7)
		elif m=="Aug":
			df.set_value([count], ["month"], 8)
		elif m=="Sep":
			df.set_value([count], ["month"], 9)
		elif m=="Oct":
			df.set_value([count], ["month"], 10)
		elif m=="Nov":
			df.set_value([count], ["month"], 11)
		elif m=="Dec":
			df.set_value([count], ["month"], 12)
		
		count=count+1

	df = df.drop('event_date', 1)

	dfConcat = df.to_csv (outputDataset, index = False, header=True)
	return "Split SQL date done."


assignCountryCode(df)
print("Module Completed: Split SQL date")
