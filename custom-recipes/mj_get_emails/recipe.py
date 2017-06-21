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
#campStartdt=get_recipe_config().get('start_date', '2017:01:01')

#Getting current partition date and converting it to mailjet format yyyy:MM:dd
print "I am working for year %s" % (dataiku.dku_flow_variables["DKU_DST_DATE"])
datepart=dataiku.dku_flow_variables["DKU_DST_DATE"]
mjtDate=str(datepart).replace('-', ':')

mjt.campStartdt=mjtDate

# random sleep to avoid dataiku bug
# should be fixed soon. ask Pierre
from random import randint
from time import sleep
sleep(randint(1,20))

# Get your environment Mailjet keys
auth, saname, said = mjt.readSa()

# Recipe outputs
main_output_name = get_output_names_for_role('email_stats')[0]
#create empty lists
d_c_dflst=[]#will contain the list of campaign overview dataframe
d_e_dflst=[]#will contain the list of campaign overview dataframe
#create empty list to store the list of dataframe
email_dflst=[]
count_emails=0

mailjet_EMAILS = dataiku.Dataset(main_output_name)

schema=[]
schema.append({ 'name' : 'CntRecipients', 'type' : 'bigint'})
schema.append({ 'name' : 'Status', 'type' : 'string'})
schema.append({ 'name' : 'ToEmail', 'type' : 'string'} )
schema.append({ 'name' : 'CampaignID', 'type' : 'bigint'}) 
schema.append({ 'name' : 'Details', 'type' : 'string'})
schema.append({ 'name' : 'StateID', 'type' : 'bigint'})
schema.append({ 'name' : 'StatePermanent', 'type' : 'boolean'}) 
schema.append({ 'name' : 'FBLSource', 'type' : 'string'}) 
schema.append({ 'name' : 'ContactID', 'type' : 'bigint'} )
schema.append({ 'name' : 'BounceDate', 'type' : 'string'})
schema.append({ 'name' : 'Queued', 'type' : 'boolean'} )
schema.append({ 'name' : 'BounceReason', 'type' : 'string'} )
schema.append({ 'name' : 'ComplaintDate', 'type' : 'string'}) 
schema.append({ 'name' : 'Spam', 'type' : 'boolean'} )
schema.append({ 'name' : 'Bounce', 'type' : 'boolean'} )
schema.append({ 'name' : 'MessageID', 'type' : 'bigint'} )
schema.append({ 'name' : 'Unsub', 'type' : 'boolean'} )
schema.append({ 'name' : 'ArrivalTs', 'type' : 'string'} )
schema.append({ 'name' : 'Open', 'type' : 'boolean'} )
schema.append({ 'name' : 'Click', 'type' : 'boolean'} )
schema.append({ 'name' : 'Sent', 'type' : 'boolean'} )
schema.append({ 'name' : 'Blocked', 'type' : 'boolean'} )
schema.append({ 'name' : 'SubAccount', 'type' : 'string'})
schema.append({ 'name' : 'SubAccount_PK', 'type' : 'string'})
schema.append({ 'name' : 'SendStartAt_date', 'type' : 'string'})

list_cols = [x['name'] for x in schema]

#schema.append({ 'name' : 'date', 'type' : 'string'  })
    # write schema
print 'writing dataset schema of size ',len(schema)
mailjet_EMAILS.write_schema(schema)
#print ("schema ecrit avec mailjet_EMAILS.write_schema(schema)", schema)
sa_dict=dict()
with mailjet_EMAILS.get_writer() as writer:  
    for sa in range(len(auth)):
        sa_dict[saname[sa]]=auth[sa][0]
        print("-----------starting subaccount " + saname[sa] + '-------------')
        #create the connection to mailjet with the sub account    
        mj = Client(auth=auth[sa])
        mjt.campStartdt=mjtDate
        df_c=None
        #read the campaigns overview of the subaccount
        co_d_count, df_c=mjt.getCampaigns_foremails(mj)
        count_total=0
        #Add the resulting dataframe with campaign dimension to a list of dataframe
        if co_d_count > 0:
            df_c['SubAccount']=saname[sa]
            print "adding subaccount data : ", saname[sa], auth[sa][0]
            #d_c_dflst.append(df_c)
            df_c=df_c[pd.notnull(df_c['SendStartAt_date'])]
            #print(df_c.head(10))   
            sa_for_emails=saname[sa]
            #print("subaccount :"   +  saname[sa])
            #create the connection to mailjet with the sub account    
            mj = Client(auth=auth[sa])
            #get list of campaign ids for this sub_account
            #CampaignIds=mailjet_CAMPAIGN_Dim_df[ (mailjet_CAMPAIGN_Dim_df.SubAccount ==saname[sa]) & (mailjet_CAMPAIGN_Dim_df["SendStartAt_MonthNumber"]==1) ]['CampaignID']
            CampaignIds=[]
            #Get list of campaigns of this subaccount 
            nCamp=0
            for i,row in  df_c.iterrows():
                           
                if row[11]== pd.to_datetime(datepart):
                    nCamp=nCamp+1
                    CampaignIds.append(row[0])
                    #print "nCamp=", nCamp, "and CampaignIds", CampaignIds
                #else: # for debug only
                    #print row[0] , row[11], pd.to_datetime(datepart), "n est pas equivalent"
            #CampaignIds=df_c[ (df_c.SubAccount ==saname[sa] & df_c.SendStartAt_date ==pd.to_datetime(datepart))  ]['CampaignID']

            #For debug :
            campaign_dict=dict()
            df_e=None
            #read the emails of the campaigns of the sub account    
            #count_e_sa, df_e =getCampaignEmails(mj, CampaignIds, campaign_dict)                    
            count_total=0
            if len(CampaignIds) > 0: #if there are campaigns for this day and this sub account
                #we create a dictionnary with campaign id : campaign start date

                campsa_df=pd.DataFrame()
                campsa_df['id']=df_c[ (df_c.SubAccount ==saname[sa])  ]['CampaignID']
                campsa_df['thedate']=df_c[ (df_c.SubAccount ==saname[sa])  ]['SendStartAt_date']
                
                campsa_df['SubAccount_PK']=df_c[ (df_c.SubAccount ==saname[sa])  ]['SubAccount'].map(lambda x : sa_dict[x])
                count_e_sa =mjt.getCampaignEmails(mj, CampaignIds, campsa_df, campaign_dict, sa_for_emails, writer,list_cols)                    
                count_total=count_total+ count_e_sa
            else:
                count_e_sa=0
            #Add status of data extract to the dimension dataframe
            if count_e_sa > 0:
                #print (saname[sa])
                #adding subaccount associated to the campaign as campaign id is no unique
                #df_e.SubAccount=sa_for_emails
                print("read " + str(count_e_sa) + " emails for Sub-Account" + saname[sa])
            else :
                print "No campaign on ", mjtDate, "for ", saname[sa]
    #union of all dataframes    
    #df_c=pd.concat(d_c_dflst)
    print "####### Successfully get", count_total, " emails."