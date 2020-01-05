from parsl import load, python_app

import pandas as pd
import numpy as np
import time

import os.path
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import userScript
import parslConfig
#ignore warnings printed on terminal
pd.options.mode.chained_assignment = None  # default='warn'

currentModule = "randomForestClassification"
workflowNumber = sys.argv[1]
Iteration_no = sys.argv[2]

if workflowNumber == "1":
	orderOfModules = userScript.orderOfModules1
	inputDataset = userScript.inputDataset1
	outputLocation = userScript.outputLocation1
	randomForestEstimatorRange = userScript.randomForestEstimatorRange1
	randomForestDepthRange = userScript.randomForestDepthRange1
	randomForestSplitRange = userScript.randomForestSplitRange1
	randomForestFeaturesRange = userScript.randomForestFeaturesRange1
elif workflowNumber == "2":
	orderOfModules = userScript.orderOfModules2
	inputDataset = userScript.inputDataset2
	outputLocation = userScript.outputLocation2
	randomForestEstimatorRange = userScript.randomForestEstimatorRange2
	randomForestDepthRange = userScript.randomForestDepthRange2
	randomForestSplitRange = userScript.randomForestSplitRange2
	randomForestFeaturesRange = userScript.randomForestFeaturesRange2


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

outputLocation = outputLocation + "rf/"

@python_app
def rfClassifier(estimators, depth, split, features, dFrame):
	dataset = dFrame
	dataset.head()

	X = dataset.iloc[:, 1:5].values
	y = dataset.iloc[:, 9].values

	from sklearn.model_selection import train_test_split
	X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

	# Feature Scaling
	from sklearn.preprocessing import StandardScaler

	sc = StandardScaler()
	X_train = sc.fit_transform(X_train)
	X_test = sc.transform(X_test)


	from sklearn.ensemble import RandomForestClassifier
	classifier = RandomForestClassifier(n_estimators=estimators, max_depth = depth, min_samples_split=split, max_features=features, random_state=0)
	classifier.fit(X_train, y_train)
	y_pred = classifier.predict(X_test)

	from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
	#print(confusion_matrix(y_test,y_pred))
	#print(classification_report(y_test,y_pred))
	#print(accuracy_score(y_test, y_pred))
##	return str(confusion_matrix(y_test,y_pred)) + '\n' + str(classification_report(y_test,y_pred)) + '\n' + str(accuracy_score(y_test, y_pred))
	accuracyScore = accuracy_score(y_test, y_pred)
	retArray = [estimators, depth, split,features, accuracyScore]
	return retArray

results = []
#print(rfClassifier(100, 3, 2, 'auto', df).result())

#print(randomForestEstimatorRange)
#print(randomForestDepthRange)
#print(randomForestSplitRange)
#print(randomForestFeaturesRange)

for i in range(randomForestEstimatorRange[0], randomForestEstimatorRange[1]):
	for j in range (randomForestDepthRange[0], randomForestDepthRange[1]):
		for k in range(randomForestSplitRange[0],randomForestSplitRange[1]):
			for l in range(randomForestFeaturesRange[0],randomForestFeaturesRange[1]):
				x = rfClassifier(i,j,k,l,df)
				results.append(x)


# wait for all apps to complete
return_array = [r.result() for r in results]

dfa=pd.DataFrame(return_array)
dfa.columns = ["Estimators","Depth","Split","MaxFeatures", "Accuracy"]
#print(dfa)

dfa.to_csv (outputLocation + Iteration_no + '_rf.csv', index = None, header=True)
print("Random forest classification ran for " + Iteration_no + " time(s).\n")

# wait for all apps to complete
#print("Job Status: {}".format([r.result() for r in results]))
