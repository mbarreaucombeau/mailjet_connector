# -*- coding: utf-8 -*-
# Code for custom code recipe MAILJET_compute_list_name (imported from a Python recipe)

# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
#listName = get_recipe_config()['listName']

# For optional parameters, you should provide a default value in case the parameter is not present:
saName = get_recipe_config().get('saName', None)
#chunkSize = int(get_recipe_config().get('chunkSize', 10000))



#############################
# Your original recipe
#############################


import dataiku
from dataiku import pandasutils as pdu
from mailjet_rest import Client
import pandas as pd
import datetime
import mailjet_helper as mjt



            
def convertMJJsonToDf(json):
    # this function take a mailjet json as input
    # this json is the result of a call to mailjet REST API
    # then it cleanup the data and return count, dataframe and column list
    #
    #create a list with all data
    data=[]
    if ('Data' in json):
        #create list with data, data=[] if no data
        for rec in json["Data"]:
            data.append(rec)
        #check if data of errors
        if ('Count' in json):
            count=json["Count"]
        else:
            count=-1
         
        #if result gave us data, then create a list with column names
        if count>0:
            field_list=[]        
            for key in data[0].keys():
                field_list.append(key)
            
            #create the dataframe
            df=pd.DataFrame(data, columns=field_list)
            #print the first 5 lines of the Dataframe
            #return count as int, data as dataframe, field_list as list
            return count,df, field_list
        else:
            df=pd.DataFrame(data)
            #return count as int, empty dataframe, empty list
            return count,df, []
    else:
        df=pd.DataFrame(data)
        return count,df, []




#Lecture du dataset en input : liste des jobs à vérifier
contactsName = get_input_names_for_role('ContactList_JobStatus')[0]
contactsDs=dataiku.Dataset(contactsName)
#contactsDf = contactsDs.get_dataframe()

#Ouverture du client mailjet
#auth=[]
#auth.append((saName,'29822c92447d9f35f1c6cc4b7544aa09'))
#cree le client
#mj = Client(auth=auth[0])
mj=mjt.getSaMj(saName)


job_lst=[]

i=0
for df in contactsDs.iter_dataframes(chunksize=1000):
    JobIds=df["ID"]
    for JobId in JobIds:
        result =    mj.csvimport.get(id=JobId)
        count,df,fl0=convertMJJsonToDf(result.json())
        if count > 0:
            job_lst.append(df)

jobAllDf=pd.concat(job_lst)
# Recipe outputs for job status
main_output_name = get_output_names_for_role('ContactList_JobStatus')[0]
output_job_ds =  dataiku.Dataset(main_output_name)
output_job_ds.write_with_schema(jobAllDf)