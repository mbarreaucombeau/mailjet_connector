# -*- coding: utf-8 -*-
# Code for custom code recipe MAILJET_compute_list_name (imported from a Python recipe)

# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
listName = get_recipe_config()['listName']

# For optional parameters, you should provide a default value in case the parameter is not present:
saName = get_recipe_config().get('saName', None)
chunkSize = int(get_recipe_config().get('chunkSize', 10000))



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


#Creates the list
def createContactList (listName,saName):
    debug=False
    #auth=[]
    #auth.append((saName,'636c9fb347163fcb5e6fffc9daaba895'))
    #mot de passe
    #auth.append((saName,'29822c92447d9f35f1c6cc4b7544aa09'))
    mj=mjt.getSaMj(saName)
    #cree le client
     #mj = Client(auth=auth[0])
    data = {
      'Name': listName
    }
    result = mj.contactslist.get(filters={'Name':listName})
    if ((result.status_code == 200)):
        result=result.json()
        if (('Data' in result and 'Count' in result)):
        #   print (result)
            count,df,fl0=convertMJJsonToDf(result) 
            if count >0:
                print ("The list already exists !")
                list_id=df["ID"][0]
                print ("list id : " + str(list_id))
                print ("list name : " + df["Name"][0])
                print ("List Address #: " + df["Address"][0])
                print ("CreatedAt : " + str(df["CreatedAt"][0]))
                return list_id

            else:
                #creating the new list
                result = mj.contactslist.create(data=data)
                if ((result.status_code == 201)): #code 201 = object created
                    result=result.json()
                    count,df,fl0=convertMJJsonToDf(result)    
                    print ("The new list has been created !")
                    list_id=df["ID"][0]
                    print ("list id : " + str(list_id))
                    print ("list name : " + df["Name"][0])
                    print ("List Address #: " + df["Address"][0])
                    print ("CreatedAt : " + str(df["CreatedAt"][0]))
                    return (list_id)
                    
def readContactList(listId, saName):
    debug=False
    auth=[]
    #auth.append((saName,'636c9fb347163fcb5e6fffc9daaba895'))
    #auth.append((saName,'29822c92447d9f35f1c6cc4b7544aa09'))
    #mj = Client(auth=auth[0])
    mj=mjt.getSaMj(saName)
#read the created list
    result = mj.contactslist.get(id=str(listId))
    if ((result.status_code == 200)):
        result=result.json()
        if (('Data' in result and 'Count' in result)):
        #   print (result)
            count,df,fl0=convertMJJsonToDf(result)    
            if count==10:
                return None
            else:
                print ("count :" + str(count))
                print (df.head())
                return (df)



def loadContactList(listId, saName, data, col_other):
    debug=False
    auth=[]
    mj=mjt.getSaMj(saName)
    #auth.append((saName,'29822c92447d9f35f1c6cc4b7544aa09'))
    #mj = Client(auth=auth[0])
    for col in col_other:
        datacol=dict()
        datacol['Datatype']='str'
        datacol['Name']=col
        datacol['NameSpace']= 'static'
        result = mj.contactmetadata.create(data=datacol)
        print( result.status_code)
        print( result.json())
    result = mj.contactslist_managemanycontacts.create(id=list_id, data=data)
    print( result.status_code)
    print( result.json())
  
    count,df,fl0=convertMJJsonToDf(result.json())
    JobID=df['JobID'][0]
    result =    mj.csvimport.get(id=JobID)
    count,df,fl0=convertMJJsonToDf(result.json())
    return (df)


#listName = 'TESTAPI_MBC_3'
#listName = 'TESTAPI_FF'

contactsName = get_input_names_for_role('contacts')[0]
contactsDs=dataiku.Dataset(contactsName)
#contactsDf = contactsDs.get_dataframe()


#saName='2915ff6285fc52f229b8d93e3a066b30'
#saName='416ac9ae36127b4e8650d626837bbdf3'

#Creation de la list
list_id=createContactList(listName,saName)

#
df=readContactList(list_id,saName)

# Recipe outputs for contact list
main_output_name = get_output_names_for_role('contactList')[0]
output_dataset =  dataiku.Dataset(main_output_name)
output_dataset.write_with_schema(df)
            

#contactsDf.info()
main_col = ["Email", "Name"]

job_lst=[]
loadDataLst= []


i=0
for df in contactsDs.iter_dataframes(chunksize=chunkSize):
    # df_main =  les colonnes principales
    df_main = df[main_col]
    #df : toutes les colonnes sauf les colonnes principales
    df_other=df.drop (main_col,axis=1)

    #conversion du dataframe en liste de dictionnaire
    contact_ext_lst=df_other.to_dict(orient='records')
    col_other=df_other.columns
    #ruse de sioux pour passer les dictionnaires de u'champ' à 'champ'
    contact_ext_lst2=[]
    for item in contact_ext_lst:
        dict_encode=dict()
        for key in item:
            dict_encode[key.encode('latin-1')]=item[key]
        contact_ext_lst2.append(dict_encode)

    #ajout du dictionnaire dans la colonne Properties
    df_main["Properties"]=contact_ext_lst2

    # prépareration de la liste de contacts
    contact_lst=df_main.to_dict(orient='records')



    #ruse de sioux pour passer les dictionnaires de u'champ' à 'champ'
    contact_lst2=[]
    for item in contact_lst:
        dict_encode=dict()
        for key in item:
            dict_encode[key.encode('latin-1')]=item[key]
        contact_lst2.append(dict_encode)
    data=dict()
    data["Action"] = 'addforce'
    data["Contacts"]=contact_lst2
    #print(data)

    #premier dataframe : on met à jour les metadata dans mailjet
    #ajout bourrin car pas d'api pour vérifier l'existance de la metadata
    if i ==0:
        # chargement du contact simple (Email et Name) dans la liste de contact
        #result = mj.contactslist_managemanycontacts.create(id=list_id, data=data)
        i+=1
    #chargement en bulk des contacts
    jobDf=loadContactList(list_id,saName,data, col_other)
    
    #print jobDf.head(0)
                          #JobID =jobDf['JobID'][0]                   
    #print("******************** JOB ID", str(JobID),"*********************")
    job_lst.append(jobDf)
    df["JobID"]=jobDf["ID"][0]
    loadDataLst.append(df)
jobAllDf=pd.concat(job_lst)
loadDataLst=pd.concat(loadDataLst)

# Recipe outputs for job status
main_output_name = get_output_names_for_role('ContactList_JobStatus')[0]
output_job_ds =  dataiku.Dataset(main_output_name)
output_job_ds.write_with_schema(jobAllDf)

# Recipe outputs for data job verification
ld_output_name = get_output_names_for_role('ContactList_LoadData')[0]
output_ld_ds =  dataiku.Dataset(ld_output_name)
output_ld_ds.write_with_schema(loadDataLst)