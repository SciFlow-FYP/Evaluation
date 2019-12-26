import os
import glob
import pandas as pd
import csv
import numpy as np

import os.path
import sys
import os,sys,inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import userScript

currentModule = "appendRecords"


def main(x):
	workflowNumber =x
	print(x)

	if workflowNumber == "1":
		orderOfModules = userScript.orderOfModules1
		inputDataset = userScript.inputDataset1
		outputLocation = userScript.outputLocation1
		years = userScript.generateRecordsYears1
	elif workflowNumber == "2":
		orderOfModules = userScript.orderOfModules2
		inputDataset = userScript.inputDataset2
		outputLocation = userScript.outputLocation2
		years = userScript.generateRecordsYears2


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
	# GENERATE COMPARATIVE DATAFRAME CONTAINING RIOT INFORMATION

	# locations in dataframe
	dfYear=df["year"]
	dfMonth=df["month"]
	dfDate=df["date"]
	dfCountry=df["country"]

	# SEPERATE DATAFRAMES BY COUNTRY
	uniqueCountries=df["country"].unique()

	# determine unique countries; used to make seperate dataframe per country
	for i in df["country"].unique():
		dfCountry=pd.DataFrame()
		k=0
		for j in df["country"]:
			# query to check whether the country matches between the dataframe and row
			if j == i:
				l=df.loc[k][:]
				# matches appended to country dataframe
				dfCountry=dfCountry.append(l)
			k=k+1
		# write each country dataframe to csv file. (in seperate folder)
		dfCountry.to_csv(outputLocation+"countryDF/dfCountry"+i+".csv", sep=',', encoding='utf-8', index=False, header=True)


	# INSERT NEW RECORD FOR EVERY DAY IN THE YEAR

	loc = outputLocation+"countryDF/"

	list_of_df = []

	# run loop for all files in previously created dataframe folder
	for f in os.listdir(loc):
		if f.endswith(".csv"):
			df = pd.read_csv(loc+f,  sep = ',')
			# append dataframe to a list
			list_of_df.append(df)

	ThirtyDays = [4,6,9,11]
	ThirtyOneDays = [1,3,5,7,8,10,12]
	TwentyEightDays = [2]

	# create records with zero values for unrest for all days, months and years
	for df in list_of_df:
		# get country name for dataframe from the first row of df
		countryName = df.loc[1]["country"]

		for y in years:
			for m in range (1,13):
				#print(m)
				if m in ThirtyDays:
					for d in range (1,31):
						df = df.append({'country':countryName, 'date':d, 'label':0, 'month':m, 'year':y}, ignore_index=True)

				if m in TwentyEightDays:
					for d in range (1,29):
						df = df.append({'country':countryName, 'date':d, 'label':0, 'month':m, 'year':y}, ignore_index=True)

				if m in ThirtyOneDays:
					for d in range (1,32):
						df = df.append({'country':countryName, 'date':d, 'label':0, 'month':m, 'year':y}, ignore_index=True)

		df.to_csv(outputLocation+"filledDF/dfFilledCountry"+countryName+".csv", sep=',', encoding='utf-8', index=False, header=True)


	# REMOVE DUPLICATE ROWS

	loc2 = outputLocation+"filledDF/"

	list_of_filled_df = []

	# run loop for all files in previously created dataframe folder
	for f in os.listdir(loc2):
		if f.endswith(".csv"):
			df = pd.read_csv(loc2+f,  sep = ',')
			# append dataframe to a list
			list_of_filled_df.append(df)


	# handle duplicate zero records
	for df in list_of_filled_df:
		# get country name for dataframe from the first row of df
		countryName = df.loc[1]["country"]

		# Select all duplicate rows based on multiple columns
		# leave the first record (showing riot) and delete the next
	# For the subset argument, specify the first n-1 columns
	df = df[['year', 'month', 'date', 'country','label']]
	df = df.drop_duplicates(subset=df.columns[:-1], keep='first')
	df.to_csv(outputLocation+"removeDuplicateDF/dfRemoveDuplicate"+countryName+".csv", sep=',', encoding='utf-8', index=False, header=False)


	# COMBINE ALL DATAFRAMES

	loc4 = outputLocation+"removeDuplicateDF/"
	os.chdir(loc4)

	allFiles = os.listdir(loc4)

	# CSV file selection

	selectedFiles = []

	for filename in allFiles:
	    	selectedFiles.append(filename)

	# Create new CSV file to write all CSV files generated from previous step
	with open(outputLocation+currentModule+".csv", "w", newline='', encoding="utf8") as outcsv:
		writer = csv.writer(outcsv, delimiter=',')

		# write the header
		writer.writerow(["Year", "Month", "Date", "ActorGeo_CountryCode", "Indicator"])

		# write the actual content line by line
		for filename in selectedFiles:
			with open(filename, 'r', newline='', encoding="utf8") as incsv:
				reader = csv.reader(incsv, delimiter=',')
				writer.writerows(row for row in reader)

	print("Module Completed: Create labelled data record for all days in range")
	return years, uniqueCountries


if __name__ == '__main__':
	years = main(sys.argv[1])
	#print(years)
