# -*- coding: utf-8 -*-
"""
Created on Mon Oct  3 10:12:31 2016

@author: mbarreau
"""

from mailjet_rest import Client
import pandas as pd
import json
import datetime

IDType={'Campaign' : 'Email Promo',
         'NL' : 'Newsletter',
         'AX': 'AB Test'}
CampaignType={2:'Campaign',
                  1:'transactional'}

CampDraftStatusDict={-3:'AXCanceled',
                 -2:'Deleted',
                 -1:'Archived',
                 0:'Draft',
                 1:'Programmed',
                 2:'Sent',
                 3:'AXTested',
                 4:'AXSelected'}

MailjetErrorCode={
    200 : "OK. All went well. Congrats!",
    201 : "Created",
    204 : "No content found or expected to return. Returned when DELETE was successful.",
    304 : "Not Modified. The PUT request didn’t affect any record.",
    400 : "Bad Request. One or more parameters are missing or maybe mispelled (unknown resource or action)",
    401 : "Unauthorized. You have specified an incorrect Api Key / API Secret Key. You may be unauthorized to access the API or your API key may be expired. Visit API keys Management section to check your keys.",
    403 : "Forbidden. You are not authorised to access this resource.",
    404 : "Not Found. The resource with the specified ID you are trying to reach does not exist.",
    405 : "Method Not Allowed. The method requested on the resource does not exist.",
    429 : "Too Many Requests. Oops! You have reach the maximum number of calls allowed per minute by our API. Please review your integration to reduce the number of call issued by your system.",
    500 : "Internal Server Error. Ouch! Something went wrong on our side and we apologize! When such error occurs, it will contain an error identifier in it’s description (e.g. “ErrorIdentifier” : “D4DF574C-0C5F-45C7-BA52-7AA8E533C3DE”), which is crucial for us to track the problem and identify the root cause. Please contact our support team , providing the error identifier and we will do our best to help."
    }

campStartdt='2016:01:01'

def getCountFromJson(json):
    # this function take a mailjet json as input
    # this json is the result of a call to mailjet REST API
    # then it return only the number of record (count)
    #
        #create a list with column names
    if ('Count' in json):
         count=json["Count"]
    else:
        count=-1
    return count
            
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
#
def readSa():
    #read the input file
    xls_SA_df  = pd.read_csv('/home/dataiku/data/plugins/dev/MAILJET/python-lib/mailjet_accounts.csv', sep=';')
    #get only the sub accounts we want to retrieve

    auth_auto_lst=xls_SA_df[xls_SA_df.Get_Statistics==True][["Name", "API_key", "Secret_Key", "ID"]].values.tolist()
    auth=[]
    for x in auth_auto_lst:
        auth.append((x[1], x[2]))
    saname=[]
    said=[]
    for x in auth_auto_lst:
        saname.append(x[0])
        said.append(x[3])
    return auth, saname, said

def getSaMj(saName):
    auth, saname, said=readSa()
    print auth
    for sa in range(len(auth)):
        if saname[sa]==saName:
            mj = Client(auth=auth[sa])
    return mj
                    
def getCampaignsOverview(mymj):
    #
    # this function get all campaign data from Mailjet
    # then it cleanup data 
    # and merge all in a dimension dataframe
    #
    while_b=True
    i=0
    limit=1000
    b_continue=True
    offset=0
    count_campaign = 0
    r=[]
    # loop campaignsentstatistics  until we got 0 messages
    while b_continue:
        #we loop 10 times to get campaigns until we got a 200 return code
        while((while_b) and (i<10)):
            result_e = mymj.campaignoverview.get(filters={'FromTS': campStartdt,'limit':limit, 'offset' : offset})
            
            #if the result is ok (return_code=200)
            if ((result_e.status_code == 200)):
 
                result=result_e.json()
                if (('Data' in result and 'Count' in result)):
                #   print (result)
                    ct0,df0,fl0=convertMJJsonToDf(result)         
                #    print(df0.head())
                    
                    df0['IDType_Label']=df0['IDType'].map(IDType)
                    count_campaign=count_campaign+ct0
   
                    #getting the list of campaigns
                    #
                    r.append(df0)     
                    while_b=False
        if (ct0 ==1000): #1000 message = offset size = there are other messages
            offset=offset + 1000 #compute the next offset
            print("read " + str(ct0) + " campaignoverview with offset " + str (offset) )
            b_continue=True
            ct0=0 # reset the count for  # messages retrieved
        else:
            offset=0 #we go to the next campaign : we reset the offset
            b_continue=False  
            
        co_full=pd.concat(r)
        count_campaign=co_full['ID'].count()
        print("Total campaignoverview  :" + str(count_campaign))
        return count_campaign, co_full     

       
def getCampaigns(mymj):
    while_b=True
    i=0
    limit=1000
    #we loop 10 times to get campaigns until we got a 200 return code
    b_continue=True
    offset=0
    count_campaign = 0
    isData=False
    r=[]
    # loop campaignsentstatistics  until we got 0 messages
    while b_continue:
        while_b=True
        i=0
        while((while_b) and (i<10)):
            
            result_e = mymj.campaign.get(filters={'FromTS': campStartdt,'limit':str(limit), 'offset' : str(offset)})
            i=i+1 

            #if the result is ok (return_code=200)
            if ((result_e.status_code == 200)):
                result=result_e.json()
                
                if (('Data' in result and 'Count' in result)):
                #   print (result)
                    ct0,df0,fl0=convertMJJsonToDf(result)
                    if ct0 > 0:
                        count_campaign=count_campaign+ct0
  
    
                    #Converting dateTime fields to dateTime type fields
                                       
                        df0['CampaignType_label']=df0['CampaignType'].map(CampaignType)
    
            
                        r.append(df0)
                        isData=True
                        while_b=False
                    else:
                        print "no campaign found"
                        return 0, None
            else:
                #The result is not ok
                print("Failed to retrieved campaigns") 
                return  0, None
        if (ct0 ==limit): #1000 message = offset size = there are other messages
            print("read " + str(ct0) + " campaigns with offset " + str (offset) )
            offset=offset + limit #compute the next offset
            b_continue=True
            ct0=0 # reset the count for  # messages retrieved
        else:
            offset=0 #we go to the next campaign : we reset the offset
            b_continue=False                
            print("Total Campaign  read :" + str(count_campaign))
    if isData:
        df=pd.concat(r)
        return count_campaign, df
    else:
        return 0, None

          
def getCampaigns_foremails(mymj):
    while_b=True
    i=0
    limit=1000
    #we loop 10 times to get campaigns until we got a 200 return code
    b_continue=True
    offset=0
    count_campaign = 0
    isData=False
    r=[]
    # loop campaignsentstatistics  until we got 0 messages
    while b_continue:
        while_b=True
        i=0
        while((while_b) and (i<10)):
            
            result_e = mymj.campaign.get(filters={'FromTS': campStartdt,'limit':str(limit), 'offset' : str(offset)})
            i=i+1 

            #if the result is ok (return_code=200)
            if ((result_e.status_code == 200)):
                result=result_e.json()
                
                if (('Data' in result and 'Count' in result)):
                #   print (result)
                    ct0,df0,fl0=convertMJJsonToDf(result)
                    if ct0 > 0:
                        count_campaign=count_campaign+ct0
  
    
                     
                   
                    #Converting dateTime fields to dateTime type fields
                        df0.SendStartAt_dt=pd.to_datetime(df0.SendStartAt,  errors='coerce')
                        df0.SendEndAt=pd.to_datetime(df0.SendEndAt_dt,  errors='coerce')
                        try :
                            df0['SendStartAt_date']=df0['SendStartAt'].map(lambda x: pd.datetime(x.year, x.month, x.day))
                        except:
                            print "Erreur de  conversion de date SendStartAt_date"
                            df0['SendStartAt_date']=""
                        try:
                            df0['SendEndAt_date']=df0['SendEndAt'].map(lambda x: pd.datetime(x.year, x.month, x.day))
                            df0['SendEndAt_date']=""
                        except:
                            print "Erreur de  conversion de date SendEndAt_date"
               
                        df0.CampaignType=df0['CampaignType'].map(CampaignType)
    
            
                        r.append(df0)
                        isData=True
                        while_b=False
                    else:
                        print "no campaign found"
                        return 0, None
            else:
                #The result is not ok
                print("Failed to retrieved campaigns") 
                return  0, None
        if (ct0 ==limit): #1000 message = offset size = there are other messages
            print("read " + str(ct0) + " campaigns with offset " + str (offset) )
            offset=offset + limit #compute the next offset
            b_continue=True
            ct0=0 # reset the count for  # messages retrieved
        else:
            offset=0 #we go to the next campaign : we reset the offset
            b_continue=False                
            print("Total Campaign  read :" + str(count_campaign))
    if isData:
        df=pd.concat(r)
        return count_campaign, df
    else:
        return 0, None

            
def getCampaignStatistics(mymj):
    while_b=True
    i=0
    limit=1000
    #we loop 10 times to get campaigns until we got a 200 return code
    b_continue=True
    offset=0
    count_campaign = 0
    isData=False
    r=[]
    # loop campaignsentstatistics  until we got 0 messages
    while b_continue:
        while_b=True
        i=0
        while((while_b) and (i<10)):
            
            result_e = mymj.campaignstatistics.get(filters={'FromTS': campStartdt, 'limit':'1000', 'isDeleted': 'false', 'ShowExtraData': 'true', 'Offset': offset})
            i=i+1 

            #if the result is ok (return_code=200)
            if ((result_e.status_code == 200)):
                try:
                    result=result_e.json()
                except:
                    print "!!!!!!!!!! erreur de conversion du json - Campaign draft not retrieved"
                    result=''
                    
                
                if (('Data' in result and 'Count' in result)):
                #   print (result)
                    ct0,df0,fl0=convertMJJsonToDf(result)
                    count_campaign=count_campaign+ct0
                #    print(df0.head())
        
                    r.append(df0)
                    isData=True
                    while_b=False
            else:
                #The result is not ok
                print("Try #" + str(i) + "failed to retrieved campaignstatistics with id=" + str(id))    
                return
        if (ct0 ==limit): #1000 message = offset size = there are other messages
            print("read " + str(ct0) + " campaignstatistics with offset " + str (offset) )
            offset=offset + limit #compute the next offset
            b_continue=True
            ct0=0 # reset the count for  # messages retrieved
        else:
            offset=0 #we go to the next campaign : we reset the offset
            b_continue=False                
            print("Total Campaign statistics read :" + str(count_campaign))
    if isData:
        df=pd.concat(r)
        return count_campaign, df
    else:
        return 0, None


def getCampaignEmails(mymj, campaign_IDs, campsa_df, campaign_status_dict, saname, writer,list_cols):
    #    
    #this function get all emails from Mailjet for a list of campaigns
    #
    r=[]
    # for all campaigns in campaign id list
    count_total=0
    for id in campaign_IDs:
        #id=rec[0]
        date=campsa_df[campsa_df['id']==id]['thedate']
        print "  -> CAMPAIGN ID=" , str(id)
        b_continue=True
        offset=0
        count_campaign = 0
        # loop messagesentstatistics  until we got 0 messages
        while b_continue:
            while_b=True
            i=0            
            #we loop 10 times to get messages until we got a 200 return code
            while((while_b) and (i<10)):
                isData=False
                result_e=mymj.messagesentstatistics.get(filters={'CampaignID': id, 'allmessages': 'true', 'limit':1000, 'Offset': offset})
                i=i+1
                #if the result is ok (return_code=200)
                if ((result_e.status_code == 200)):
                    result=result_e.json()           
                    #we convert the result to python variables
                    
                    if (('Data' in result and 'Count' in result)): #not an error
                        count, df3, fl=convertMJJsonToDf(result) #more than 0 records
                        if (count > 0):
                            count_campaign = count_campaign + count # total count by campaign
                            count_total=count_total+count #total count for all campaigns
                            #Preparing messages features on small dataset to avoid memory errors
                            
                            df3['SendStartAt_date']=date
                            df3['thedate']=date
                            #df3['date']=date
                            df3['SubAccount']=saname

                            r.append(df3)#adding the dataframe to a list of dataframe
                            
                            # force column list to handle rare used columns
                            for col in list_cols : 
                                if col not in df3.columns : 
                                    df3[col] = ''
                            #order columns :
                            df3 = df3[list_cols]
                            
                            print 'writing dataframe with ',len(df3.columns), 'columns'
                            writer.write_dataframe(df3)
                            #for row in df3.iterrows():
                            #     writer.write_row_dict(row[1])
                            isData=True
                        else:
                            print ("-> result ok mais no email retrieved")
                    while_b=False #use to retry 10 times if there is a fail
                else:#The result is not ok (result code not equal to 200)
                    print("Try #" + str(i) + "failed to retrieved messages for campaign with id=" + str(id))    
                    
            #End of while    ((while_b) and (i<10)): 
            #if we were not able to retrieve message
            if ((i==10) and (while_b==False)):
                #printing an error message
                print("failed to retrieve emails for  campaign with id=" + str(id))    
                #we update the campaign status
                campaign_status_dict.update({id,False})
                #return None, campaign_status_dict
            else: #we were able to retrieve messages
                #we update the campaign status
                campaign_status_dict.update({id:True})
            # we check if we need to continue with the next offset
            if (count==1000): #1000 message = offset size = there are other messages
                offset=offset + 1000 #compute the next offset
                print("read " + str(count) + " messages with offset " + str (offset) )
                b_continue=True
                count=0 # reset the count for  # messages retrieved
            else:
                offset=0 #we go to the next campaign : we reset the offset
                b_continue=False                
                print("Total message read :" + str(count_campaign))
            
            
            #end of while b_continue:             
                
    #concatening message datasets
    if isData:
        dft=pd.concat(r)
        countdone=dft.count()[0]
           #Checking messages count
        if(count_total!=countdone):    
            #if the dataframe does not contain the same # record than the total count
            print("We have read " + str(count_total) + " messages")    
            print ("and there is " + str(countdone) + " messages in the dataframe")
        else:
            #if we got the correct count in the dataframe
            print ("All " + str(countdone) + " messages collected in dataframe.")
            return count_total
    else:
        countdone=0
        return 0




def getCampaignDraftDetails(mymj, campdraftIDs):
    #    
    #this function get all emails from Mailjet for a list of campaigns
    #
    r=[]

    # for all campaigns in campaign id list
    count_total=0
    print "list of campaign draft Id's :", campdraftIDs
    isData=False
    for id in campdraftIDs:   
        print("-----PROCESSING CAMPAIGN Draft ID=" + str(id))
        b_continue=True
        offset=0
        count_campaign = 0
 
        result_e=mymj.campaigndraft_detailcontent.get(id=id)
        #if the result is ok (return_code=200)
        if ((result_e.status_code == 200)):
            try:
                result=result_e.json()
            except:
                print result_e
            #we convert the result to python variables
            if (('Data' in result and 'Count' in result)): #not an error
                count, df3, fl=convertMJJsonToDf(result) #more than 0 records
                if (count > 0):
                    count_campaign = count_campaign + count # total count by campaign
                    count_total=count_total+count #total count for all campaigns
                    df3.fillna(0)
                    df3["ID"]=id
                    r.append(df3)#adding the dataframe to a list of dataframe
                    isData=True
            else:
                count=0
                print("no campaign draft  content with id" + id)
        else:#The result is not ok (result code not equal to 200)
            print("Failed to retrieved campaign draft with id=" + str(id))    


    if isData:
        dft=pd.concat(r)  
        if(count_total!=dft.count()[0]):    
            #if the dataframe does not contain the same # record than the total count
            print("We have read " + str(count_total) + " campaign drafts")    
            print ("and there is " + str(dft.count()[0]) + " campaign drafts in the dataframe")

            return 0, None
        else:
            #if we got the correct count in the dataframe
            print ("All " + str(dft.count()[0]) + " campaign drafts collected in dataframe.")


            #df_cube_detail = pd.merge(df_campaign_dim, dft, on='CampaignID')
            return count_total, dft

    else:
        return 0, None        
    #Checking campaign draft count count
    
def getCampaignDraftDetail(mymj, id):
    #    
    #this function get all emails from Mailjet for a list of campaigns
    #
    r=[]

    # for all campaigns in campaign id list
    count_total=0
    isData=False
    print("-----PROCESSING CAMPAIGN Draft ID=" + str(id))
    b_continue=True
    offset=0
    count_campaign = 0

    result_e=mymj.campaigndraft_detailcontent.get(id=id)
    #if the result is ok (return_code=200)
    if ((result_e.status_code == 200)):
        try:
            result=result_e.json()    #we convert the result to python variables
            if (('Data' in result and 'Count' in result)): #not an error
                count, df3, fl=convertMJJsonToDf(result) #more than 0 records
                if (count > 0):
                    count_campaign = count_campaign + count # total count by campaign
                    count_total=count_total+count #total count for all campaigns
                    df3.fillna(0)
                    df3["ID"]=id
                    isData=True
            else:
                count=0
                print("no campaign draft  details with id" + id)
        except:
            print result_e

    else:#The result is not ok (result code not equal to 200)
        print("Failed to retrieved campaign draft details with id=" + str(id))    


    if isData:
        
        if(count_campaign==0): 
            return 0, None
        else:
            return count_campaign, df3

    else:
        return 0, None        
    #Checking campaign draft count count
    


def getCampaignsDraft(mymj):
    while_b=True
    i=0
    limit=1000
 
    offset=0
    count_campaign = 0
    isData=False
    r=[]
    cdd_lst=[]
    b_continue=True
    while b_continue:

        result_e = mymj.campaigndraft.get(filters={'DeliveredAt': campStartdt,'limit':str(limit), 'offset' : str(offset)})


        if ((result_e.status_code == 200)):
            result=result_e.json()

            if (('Data' in result and 'Count' in result)):
            #   print (result)
                ct0,df0,fl0=convertMJJsonToDf(result)
                if ct0 > 0:
                    count_campaign=count_campaign+ct0


                    r.append(df0)              
                    isData=True
            else:
                #The result is not ok
                print "No campaign draft found" ,  result_e.status_code , MailjetErrorCode[result_e.status_code]
        if (ct0 ==limit): #1000 campaigns = offset size = there are other messages
            print("read " + str(ct0) + " campaign draft read with offset " + str (offset) )
            offset=offset + limit #compute the next offset
            b_continue=True
            ct0=0 # reset the count for  # messages retrieved
        else:
            offset=0 #we go to the next campaign : we reset the offset
            b_continue=False                
            print("Total Campaign draft read :" + str(count_campaign))
    if isData:
        df=pd.concat(r)
        return count_campaign, df
    else:
        return 0, None
    
    
def getContactData(mymj):
    #
    # this function get all campaign data from Mailjet
    # then it cleanup data 
    # and merge all in a dimension dataframe
    #
    while_b=True
    i=0
    limit=1000
    b_continue=True
    offset=0
    count_contact = 0
    r=[]
    isData=False
    # loop campaignsentstatistics  until we got 0 messages
    while b_continue:
        #we loop 10 times to get campaigns until we got a 200 return code
        while((while_b) and (i<10)):
            #LastActivityAt
            #'ContactEmail': 'lenaerts_j@hotmail.com'
            result_e = mymj.contactdata.get(filters={ 'IsUnsubscribed':'true','limit':str(limit), 'offset' : str(offset)})
            ct0=0
            #if the result is ok (return_code=200)
            if ((result_e.status_code == 200)):
 
                result=result_e.json()
                print result
          
                if (('Data' in result and 'Count' in result)):
                #   print (result)
                    ct0,df0,fl0=convertMJJsonToDf(result)         
                    print df0.columns
                    print(df0.head())
                    print "count", str(ct0)

                    #getting the list of campaigns
                    #
                    if ct0 >0:
                        count_contact=count_contact+ct0
                        r.append(df0)  
                        isData=True
                    while_b=False
            else:
                #The result is not ok
                print "No contact found" ,  result_e.status_code , MailjetErrorCode[result_e.status_code]                    
            if (ct0 ==1000): #1000 message = offset size = there are other messages
                offset=offset + 1000 #compute the next offset
                print("read " + str(ct0) + " contacts with offset " + str (offset) )
                b_continue=True
                ct0=0 # reset the count for  # messages retrieved
            else:
                offset=0 #we go to the next campaign : we reset the offset
                b_continue=False  
            
    if isData:
        df=pd.concat(r)
        return count_contact, df
    else:
        return 0, None
    

           
