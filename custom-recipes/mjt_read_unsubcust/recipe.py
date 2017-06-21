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
import campcube_helper as cah




#read and define start date
# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
#campStartdt = get_input_names_for_role('start_date')[0]
campStartdt=get_recipe_config().get('start_date', '2016:01:01')
mjt.campStartdt=campStartdt

# Get your environment Mailjet keys
auth, saname, said = mjt.readSa()
#create empty lists
d_co_dflst=[]#will contain the list of campaign overview dataframe
co_d_count=0
print '##############get campaign data############################'
sa_dict=dict()
for sa in range(len(auth)):
    mj = Client(auth=auth[sa])
    sa_dict[saname[sa]]=auth[sa][0]
    co_d_count, df_co=mjt.getContactData(mj)
    if co_d_count > 0:
        df_co['SubAccount']=saname[sa]
        d_co_dflst.append(df_co)
#union of all dataframes    
if co_d_count > 0:
    df=pd.concat(d_co_dflst)

    df["SubAccount_PK"]=df["SubAccount"].map(lambda x : sa_dict[x])
    #and we write the resulting dataset to dataiku

    # Recipe outputs
    # Recipe outputs for contact list
    main_output_name = get_output_names_for_role('customers')[0]
    output_dataset =  dataiku.Dataset(main_output_name)
    output_dataset.write_with_schema(df)
