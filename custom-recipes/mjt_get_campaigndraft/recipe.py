# -*- coding: utf-8 -*-
# This file is the actual code for the custom Python dataset MAILJET_campaign


# Import the helpers for custom recipes
import dataiku
from dataiku.customrecipe import *

from dataiku import pandasutils as pdu
from mailjet_rest import Client
import pandas as pd
import datetime
import mailjet_helper as mjt
# random sleep to avoid dataiku bug
# should be fixed soon. ask Pierre
from random import randint
from time import sleep
sleep(randint(1,12))


#read and define start date from partition date selection

# Get your environment Mailjet keys
auth, saname, said = mjt.readSa()
#create empty lists
d_co_dflst=[]#will contain the list of campaign overview dataframe
d_cdd_dflst=[]#will contain the list of campaign overview dataframe

for sa in range(len(auth)):
    print("-----------starting subaccount " + saname[sa] + '-------------')
    #create the connection to mailjet with the sub account    
    mj = Client(auth=auth[sa])

    #read the campaigns overview of the subaccount
    co_d_count, df_co=mjt.getCampaignsDraft(mj)
    
    #Add the resulting dataframe with campaign dimension to a list of dataframe
    if co_d_count > 0:
        df_co['SubAccount']=saname[sa]
        
        df_co["SubAccount_PK"]=auth[sa][0]
        d_co_dflst.append(df_co)

#union of all dataframes    
df_co=pd.concat(d_co_dflst)
df_co["thedate"]=df_co["CreatedAt"]  
#and we write the resulting dataset to dataiku



# Recipe outputs
main_output_name = get_output_names_for_role('campaign_draft')[0]
output_dataset =  dataiku.Dataset(main_output_name)
output_dataset.write_with_schema(df_co)