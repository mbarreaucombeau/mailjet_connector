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
for sa in range(len(auth)):
    print("-----------starting subaccount " + saname[sa] + '-------------')
    #create the connection to mailjet with the sub account    
    mj = Client(auth=auth[sa])
    sa_dict[saname[sa]]=auth[sa][0]

 
    #read the campaign                                           
    c_d_count, df_c=mjt.getCampaigns(mj )

    #Add the resulting dataframe with campaign dimension to a list of dataframe
    if c_d_count > 0:
        df_c['SubAccount']=saname[sa]
        
        #df_c["SubAccount_PK"]=sa_key
        #d_c_dflst.append(df_c[df_c['CampaignType']==u'Campaign'])
        d_c_dflst.append(df_c)



#union of all dataframes    
df_c=pd.concat(d_c_dflst)

#and we write the resulting dataset to dataiku

# Recipe outputs
# Recipe outputs for contact list
main_output_name = get_output_names_for_role('campaign')[0]
output_dataset =  dataiku.Dataset(main_output_name)
output_dataset.write_with_schema(df_c)
