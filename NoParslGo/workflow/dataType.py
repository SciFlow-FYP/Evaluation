import pandas as pd
import numpy as np 

def dataType(colName, dataFrame):
    intcount = 0
    floatcount = 0
    stringcount = 0

    df_no_missing = dataFrame[colName].dropna()
  
    for row in df_no_missing:
      
        try:
            int(row)
            intcount+=1
        except ValueError:
            try:
                float(row) 
                floatcount+=1
            except ValueError: 
                stringcount+=1
                pass
            pass
    
    #print("Integer count in ",colName, intcount)
    #print("Float count in ", colName, floatcount)
    #print("String count in ", colName , stringcount)
    
    if intcount>floatcount and intcount>stringcount:
    	return "int"
    elif floatcount>intcount and floatcount>stringcount:
        return "float"
    elif stringcount>intcount and stringcount>floatcount:
        return "str"    
    

