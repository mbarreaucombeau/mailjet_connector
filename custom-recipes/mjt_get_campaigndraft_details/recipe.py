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

datepart=get_recipe_config().get('start_date', '2016:01:01')
mjtDate=str(datepart).replace('-', ':')
print "I am working for day %s" % (mjtDate)
mjt.campStartdt=mjtDate


# Recipe input
main_input_name = get_input_names_for_role('campaign_draft')[0]
input_dataset =  dataiku.Dataset(main_input_name)
camp_draft_df=input_dataset.get_dataframe()

# Get your environment Mailjet keys
auth, saname, said = mjt.readSa()
#create empty lists
d_co_dflst=[]#will contain the list of campaign overview dataframe
d_cdd_dflst=[]#will contain the list of campaign overview dataframe
schema=[]
schema.append({ 'name' : 'ID', 'type' : 'bigint'})
schema.append({ 'name' : 'SubAccount', 'type' : 'string'})
schema.append({ 'name' : 'SubAccount_PK', 'type' : 'string'})
schema.append({ 'name' : 'Html-part', 'type' : 'string'} )
schema.append({ 'name' : 'MJMLContent', 'type' : 'string'})
schema.append({ 'name' : 'Text-part', 'type' : 'string'})
#list_cols = [x['name'] for x in schema]

for sa in range(len(auth)):
    print("-----------starting subaccount " + saname[sa] + '-------------')
    #create the connection to mailjet with the sub account    
    mj = Client(auth=auth[sa])
    CampaignIds=camp_draft_df[ (camp_draft_df.SubAccount ==saname[sa])  ]['ID']
    for id in CampaignIds:
        #read the campaigns overview of the subaccount
        co_d_count, df_co=mjt.getCampaignDraftDetail(mj,id)
    
        #Add the resulting dataframe with campaign dimension to a list of dataframe
        if co_d_count > 0:
            df_co['SubAccount']=saname[sa]
            df_co["SubAccount_PK"]=auth[sa][0]
            d_co_dflst.append(df_co)

#union of all dataframes    
if d_co_dflst!=None:
    df_co=pd.concat(d_co_dflst)
#and we write the resulting dataset to dataiku




# Recipe outputs
main_output_name = get_output_names_for_role('campaign_draft_details')[0]

output_dataset =  dataiku.Dataset(main_output_name)

output_dataset.write_schema(schema)

output_dataset.write_from_dataframe(df_co)
