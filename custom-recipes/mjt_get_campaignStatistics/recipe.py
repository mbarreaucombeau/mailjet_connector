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




#read and define start date
# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
#campStartdt = get_input_names_for_role('start_date')[0]
campStartdt=get_recipe_config().get('start_date', '2016:01:01')
mjt.campStartdt=campStartdt

# Get your environment Mailjet keys
auth, saname, said = mjt.readSa()
#create empty lists
d_c_dflst=[]#will contain the list of campaign  dataframe
print '##############get campaign data############################'
sa_dict=dict()
d_cs_dflst=[]
for sa in range(len(auth)):
    print("-----------starting subaccount " + saname[sa] + '-------------')
    #create the connection to mailjet with the sub account    
    mj = Client(auth=auth[sa])
    sa_dict[saname[sa]]=auth[sa][0]
  

    #get the campaign statistics of the sub-account
    cs_d_count, df_cs=mjt.getCampaignStatistics(mj)
      #Add the resulting dataframe with campaign dimension to a list of dataframe
    if cs_d_count > 0:

        df_cs['SubAccount']=saname[sa]
        #df_cs["SubAccount_PK"]=auth[sa][0]
        d_cs_dflst.append(df_cs)
#union of all dataframes    
df_cs=pd.concat(d_cs_dflst)

#and we write the resulting dataset to dataiku

# Recipe outputs
# Recipe outputs for contact list
main_output_name = get_output_names_for_role('campaign')[0]
output_dataset =  dataiku.Dataset(main_output_name)
output_dataset.write_with_schema(df_cs)
