# mailjet_connector
mailjet plugin for dataiku platform, written in python

# Installation
Install it using Dataiku plugin setup view
Modify the file MAILJET_CONNECTOR\python-lib : add one line per mailjet sub-account with the correct key/secret information

Functions available :
## Get Campaign
For all mailjet-subaccounts : list mailjet campaign using mailjet campaign rest api. For details refer to https://dev.mailjet.com/email-api/v3/campaign/
## Get Campaign Overview
For all mailjet-subaccounts : list mailjet campaign using mailjet campaignpoverview rest api. For details refer to https://dev.mailjet.com/email-api/v3/campaignoverview/
## Get Campaign Statistics
For all mailjet-subaccounts : list mailjet campaign using mailjet campaignpstatistics rest api. For details refer to https://dev.mailjet.com/email-api/v3/campaignstatistics/
## Get Campaign Draft
For all mailjet-subaccounts : Get mailjet Campaign drafts from Mailjet API. Refer to https://dev.mailjet.com/email-api/v3/campaigndraft/
## Get Campaign Draft details
For all mailjet-subaccounts : Get mailjet Campaign drafts details from Mailjet API. refer to https://dev.mailjet.com/email-api/v3/campaigndraft-detailcontent/
## Get Emails
For all mailjet-subaccounts : list of mailjet email statistics details. For details refer to https://dev.mailjet.com/email-api/v3/messagestatistics/
## Create ContactList : 
bulk import contact  a contact list in Mailjet using Mailjet contact-managemanycontactsRest API. Refer to https://dev.mailjet.com/email-api/v3/contact-managemanycontacts/
- Input : list of contacts to be imported
- Parameters  : 
  - sa Name : Sub-Account name to be used for the import
  - Contact List : contact list name to be used. The list is created if if does not already exists.
-Output : 
- Contact List : dataset will contact the details about the contact list itself.
- Job Status  : dataset will contact the details about job status. Can be used as input of "Update Job Status" in order get updated information about the job
- Load Data : Data that have been loaded into the contact list
## Get unsub customers 
Uses Mailjet contactdata rest api to read customers with 'IsUnsubscribed':'true'
## Update JobStatus
uses Mailjet Update JobStatus rest api to show an updated status of job given in input. Should be used be Create ContactList.

## Additionnal information
The library MAILJET_CONNECTOR\python-lib\mailjet_helper.py has been written in python 2.7 and can be used outside dataiku.
