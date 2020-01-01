#from parsl import load, python_app

import pandas as pd
import numpy as np
#from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split
#from sklearn.cluster import KMeans
#from sklearn.metrics import accuracy_score
#import matplotlib.pyplot as plt
#from matplotlib.backends.backend_pdf import PdfPages

import os.path
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import userScript
#import parslConfig
#ignore warnings printed on terminal
pd.options.mode.chained_assignment = None  # default='warn'

currentModule = "kmeansModelTraining"
workflowNumber = sys.argv[1]
Iteration_no = sys.argv[2]

if workflowNumber == "1":
	orderOfModules = userScript.orderOfModules1
	inputDataset = userScript.inputDataset1
	outputLocation = userScript.outputLocation1
	clusterLabel = userScript.clusterLabel1
	otherInputs = userScript.otherInputs1
	numberOfClusters=userScript.numberOfClusters1
elif workflowNumber == "2":
	orderOfModules = userScript.orderOfModules2
	inputDataset = userScript.inputDataset2
	outputLocation = userScript.outputLocation2
	clusterLabel = userScript.clusterLabel3
	otherInputs = userScript.otherInputs3
	numberOfClusters = userScript.numberOfClusters2
elif workflowNumber == "3":
	orderOfModules = userScript.orderOfModules3
	inputDataset = userScript.inputDataset3
	outputLocation = userScript.outputLocation3
	clusterLabel = userScript.clusterLabel3
	otherInputs = userScript.otherInputs3
	numberOfClusters = userScript.numberOfClusters3

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
outputLocation = outputLocation + "kmeans/"


#print(df)
#make this an input
#df = pd.read_csv('/home/mpiuser/Documents/FYP/gdelt/missingValuesMode.csv')
#input


#@python_app
def kmeans(n,clusterLabel, otherInputs, df):
	import pandas as pd
	from sklearn.cluster import KMeans
	import numpy as np

	#from sklearn.cluster import KMeans
	from sklearn.metrics import accuracy_score
	from sklearn.model_selection import train_test_split
	
	y = df[clusterLabel].values
	dfin = df[otherInputs]
	X = dfin.values.astype(np.float)

	X_train, X_test,y_train,y_test =  train_test_split(X,y,test_size=0.20,random_state=70)

	k_means = KMeans(n_clusters=n)
	kmeans = k_means.fit(X_train)

	#print(k_means.labels_[:])
	#print(y_train[:])

	k_means.predict(X_test)

	#print(k_means.labels_[:])
	#print(y_test[:])

	score = accuracy_score(y_test,k_means.predict(X_test))
	#print('Accuracy:{0:f}'.format(score) + ' For ' + str(n) + ' clusters.\n' )


	#.reshape(-1, 1)
	#pass all the input parameters and the score
	ClusterNo_accuracy = [n,score]
	return ClusterNo_accuracy


results = []
for i in numberOfClusters:
	app_future = kmeans(i, clusterLabel, otherInputs, df)
	results.append(app_future)

# print each job status, initially all are running
#print ("Job Status: {}".format([r.done() for r in results]))

# wait for all apps to complete
#return_array = [r.result() for r in results]

dfa=pd.DataFrame(results)
dfa.columns = ["No_of_clusters", "Accuracy"]
#print(dfa)

dfa.to_csv (outputLocation + Iteration_no + '_kmeans.csv', index = None, header=True)
print("Kmeans with clusters 2,3,4,5,6,7 ran for " + Iteration_no + " time(s).\n")

# print each job status, they will now be finished
#print ("Job Status: {}".format(return_array))

#print('Kmeans model traning completed')
