#!/bin/bash

 
python3 gdeltFileSelection/dataFilesIntegration.py 1 
python3 gdeltFileSelection/countrySelection.py 1 
python3 selection/selectUserDefinedColumns.py 1

python3 cleaning/dropUniqueColumns.py 1
python3 cleaning/dropColumnsCriteria.py 1
python3 cleaning/dropRowsCriteria.py 1
python3 cleaning/removeDuplicateRows.py 1
python3 cleaning/missingValuesMode.py 1

python3 transformation/combineColumns.py 1

python3 selection/selectUserDefinedColumns.py 2 

python3 cleaning/dropUniqueColumns.py 2
python3 cleaning/removeDuplicateRows.py 2
python3 cleaning/missingValuesMode.py 2

python3 integrateLabel/addLabelColumn.py 2
python3 integrateLabel/assignCountryCode.py 2
python3 integrateLabel/splitDate.py 2
#python3 integrateLabel/integrate.py 1

python3 transformation/normalize.py 1

for VARIABLE in 1 2 3 4 5 6 7 8 9 10
do
	
	python3 mining/randomForestClassification.py 1 $VARIABLE
done

python3 mining/rfAccuracy.py 1
python3 mining/knowledge_presentation_rf.py 1

python3 cleaning/dropUserDefinedColumns.py 3


for VARIABLE in 1 2 3 4 5 6 7 8 9 10
do
	python3 mining/kmeansModelTraining.py 3 $VARIABLE
done

python3 mining/kmeansAccuracy.py 3

python3 mining/knowledge_presentation.py 3
python3 mining/svm.py 3





