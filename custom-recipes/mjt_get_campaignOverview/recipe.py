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
d_co_dflst=[]#will contain the list of campaign overview dataframe
d_c_dflst=[]#will contain the list of campaign  dataframe
d_cs_dflst=[]#will contain the list of campaign statistics dataframe
print '##############get campaign data############################'
sa_dict=dict()
for sa in range(len(auth)):
	print("-----------starting subaccount " + saname[sa] + '-------------')
    #create the connection to mailjet with the sub account    
	mj = Client(auth=auth[sa])
	sa_dict[saname[sa]]=auth[sa][0]
    #read the campaigns overview of the subaccount
	co_d_count, df_co=mjt.getCampaignsOverview(mj)

    #Add the resulting dataframe with campaign dimension to a list of dataframe
	if co_d_count > 0:
        	print saname[sa], auth[sa][0]
	        df_co['SubAccount']=saname[sa]
        #df_co['SubAccount_PK']=sa_key
        #d_co_dflst.append(df_co[df_co['CampaignType']=='Campaign'])
        	d_co_dflst.append(df_co)
#union of all dataframes    
df_co=pd.concat(d_co_dflst)


#and we write the resulting dataset to dataiku

# Recipe outputs
# Recipe outputs for contact list
main_output_name = get_output_names_for_role('campaign')[0]
output_dataset =  dataiku.Dataset(main_output_name)
output_dataset.write_with_schema(df_co)
