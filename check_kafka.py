

import csv
#import viedata
#import vie.py
#import vie.py.jobloads.json
#from ..vie.py import jobloads
import requests
import json
#import pytest
import colored
from colored import stylize
import sys
from termcolor import colored,cprint



job_id = input("Please enter the jobid    ")


headers = {"Connection": "keep-alive", "Content-Length": '102', "Content-Type": "application/json"}
authurl = "http://metalab.tubeqa.tel-dev.io/oauth/v2/token"
parameters = {" grant_type" :"password","client_id" : "25_669oml5s8igw8o8kokcg04cc0ook0occso0oco4s00wkccos80","client_secret" : "gji9gbn0pnkk0os4w8og4c04sgkk44w4wsc0sgscco0sso408","username" : "admin@proteus.com" ,"password" : "password"}
#req1 = requests.get(authurl,params=parameters)

#authdata = "BEARER" + " " + req1.json()["access_token"]

headers2 = {'Connection': 'keep-alive','Content-Length': '0','Content-Type': 'text/plain; charset=ISO-8859-1','Host': 'proteus.tubeqa.tel-dev.io:9401'}


odin_url = 'http://vie-m02.tel-dev.io:9090/api/v1/messages/external_platform_entity?correlationId=107485&limit=10',
tubeqa_url = "http://vie-m02.tel-dev.io:9091/api/v1/messages/' + 'external_platform_entity' + '?correlationId="+job_id  + "&limit=1',headers=headers2).json()"
req = requests.get("http://vie-m02.tel-dev.io:9090/api/v1/messages/external_platform_entity?correlationId="+job_id+"&limit=50",headers=headers2).json()
print("http://vie-m02.tel-dev.io:9090/api/v1/messages/external_platform_entity?correlationId="+job_id+"&limit=50")
#req = requests.get(odin_url,headers=headers2)
#print(req)

message = req['messages']
try:
    message2 = message[0]
    message3 = message2['payload']
    message4 = message3.split()
    message5 = json.loads(message3)
    message6 = message5['data']
    messages = [message6, message5, message4, message3, message2, message]
except IndexError:
    print('No data Found for this job')
    exit()



APPNEXUS_Advertiser_platform_Items = ['platformAdvertiserName','active','currency','rawEntity','currency','externalAdvertiserId']
APPNEXUS_PI_platform_Items =  ['overallBudget','dailyBudget','endDate','startDate','active','name','external_platform_item_id','platform_campaign']
APPNEXUS_Placement_PI = []
APPNEXUS_conversion_platform_items = ['advertiserId','conversionPixelId','conversionPixelName']
APPNEXUS_Creative_platform_Items = ['externalAdvertiserId','externalCreativeId','platformCreativeName','state','width','height','template','content','content_secure','audit_status','audit_feedback','type']
APPNEXUS_Campaign_platform_Items = ['startDate','endDate','lifetimeBudget','dailyBudget','goalType','pacingInterval','rawEntity','lifetime_pacing?','enable_pacing','budget_intervals.start_date','budget_intervals.lifetime_pacing']
DBM_Advertiser_PI = ['platform_id','external_advertiser_id','platformAdvertiserName','active','currency','timezone_code']
DBM_Campaign_PI =['platform_advertiser_id','external_campaign_id','name','active','start_date','end_date','overall_budget','overall_budget_impressions','daily_budget','daily_budget_impressions','pacing_type','frequency_cap','frequency_interval','frequency_interval_amount','pacing_interval']
DBM_PI_PI = ['externalCampaignId', 'externalPlatformItemId', 'platformItemName', 'active', 'startDate', 'endDate', 'dailyBudget', 'dailyBudgetImpressions', 'overallBudget', 'overallBudgetImpressions', 'baseBid', 'pacingType', 'pacingInterval', 'pacingInterval', 'frequencyCap', 'frequencyInterval', 'frequencyIntervalAmount', 'goalValue', 'goalType']
DBM_Creative_PI = ['externalCreativeId','externalAdvertiserId','platformCreativeName','width','height','adServerPlacementId']
TTD_Advertiser_PI = ['platformSeat','externalAdvertiserId','name','currency','rawEntity','partnerId']
TTD_Campaign_PI = ['platformAdvertiser','externalCampaignId','name','startDate','endDate','dailyBudget','overallBudget']
TTD_Creative_PI = ['externalAdvertiserId','externalCreativeId','platformCreativeName','width','height','adTag','audits','auditType','IsSecure']
TTD_PI_PI = ['platformCampaign','externalPlatformItemId','name','active','startDate','endDate','dailyBudget','overallBudget','dailyBudgetImpressions','baseBid','maxBid','pacingType','pacingInterval','frequencyCap','frequencyIntervalAmount','frequencyInterval','goalType','currency','rawEntity','goalValue']
MEDIAMATH_Advertiser_PI = ['platform_id','externalAdvertiserId','platformAdvertiserName','active']
MEDIAMATH_Campaign_PI = ['frequencyLevel','externalAdvertiserId', 'externalCampaignId', 'platformCampaignName', 'active', 'startDate', 'endDate', 'overallBudget', 'dailyBudget', 'dailyBudgetImpressions', 'goalValue', 'pacingType', 'frequencyCap', 'frequencyInterval', 'external_version', 'goalType', 'currency', 'overallBudgetImpressions']
MEDIAMATH_Placement_PI = []
MEDIAMATH_Creative_PI = ['externalAdvertiserId','externalCreativeId','platformCreativeName','active','expandable','adTag','status','adServerPlacementId']
MEDIAMATH_PI_PI = ['externalCampaignId','externalPlatformItemId','platformItemName','active','startDate','endDate','dailyBudget','dailyBudgetImpressions','overallBudget','maxBid','minBid','pacingInterval','pacingType','frequencyCap','frequencyInterval','frequencyIntervalAmount','goalValue','external_version','goal_type','currency']
DCM_Advertiser_PI = ['platformId,','externalAdvertiserId','platformAdvertiserName','platformSeatId']
DCM_Campaign_PI = ['platform_campaign_id','externalAdvertiserId','externalCampaignId','platformCampaignName','startDate','endDate']
DCM_Placement_PI = ['messageType','externalAdvertiserId','platformPlacementName','externalPlacementId','externalCampaignId','width','height']
DCM_PI_PI = []
TUBEMOGUL_Advertiser_Pi = ['externalAdvertiserId','platformAdvertiserName','active']

#print(message3)

#****************APPNEXUS***************************************
if 'APPNEXUS' and 'UPDATE_PROTEUS_PLATFORM_ADVERTISER_CMD' in message3:
    item_list = APPNEXUS_Advertiser_platform_Items
elif 'APPNEXUS' and 'UPDATE_PROTEUS_PLATFORM_CAMPAIGN_CMD' in message3:
    item_list = APPNEXUS_Campaign_platform_Items
elif 'APPNEXUS' and 'UPDATE_PROTEUS_PLATFORM_PLACEMENT_CMD' in message3:
    item_list = APPNEXUS_Placement_PI
elif 'APPNEXUS' and 'UPDATE_PROTEUS_PLATFORM_ITEM_CMD' in message3:
    item_list = APPNEXUS_PI_platform_Items
elif 'APPNEXUS' and 'UPDATE_PROTEUS_PLATFORM_CREATIVE_CMD' in message3:
    item_list = APPNEXUS_Creative_platform_Items
elif 'APPNEXUS' and 'UPDATE_PROTEUS_PLATFORM_PIXEL_CMD' in message3:
    item_list = APPNEXUS_conversion_platform_items
else:

    print("No Definition found  for " )
#********************DBM********************************************
if 'DBM' and 'UPDATE_PROTEUS_PLATFORM_CREATIVE_CMD' in message3:
    item_list = DBM_Creative_PI
elif 'DBM' and 'UPDATE_PROTEUS_PLATFORM_ITEM_CMD' in message3:
    item_list = DBM_PI_PI
elif 'DBM' and 'UPDATE_PROTEUS_PLATFORM_ADVERTISER_CMD' in message3:
    item_list = DBM_Advertiser_PI
elif 'DBM' and 'UPDATE_PROTEUS_PLATFORM_CAMPAIGN_CMD' in message3:
    item_list = DBM_Campaign_PI
else:

    print("No Definition found  for " )
#*****************TTD**********************************************
if 'TTD' and  'UPDATE_PROTEUS_PLATFORM_ADVERTISER_CMD' in message3:
    item_list = TTD_Advertiser_PI
elif 'TTD' and 'UPDATE_PROTEUS_PLATFORM_CAMPAIGN_CMD' in message3:
    item_list = TTD_Campaign_PI
elif 'TTD' and 'UPDATE_PROTEUS_PLATFORM_ITEM_CMD' in message3:
    item_list = TTD_PI_PI
elif 'TTD' and 'UPDATE_PROTEUS_PLATFORM_CREATIVE_CMD' in message3:
    item_list = TTD_Creative_PI
else:

    print("No Definition found  for " )
#****************TUBEMOGUL***************************************
if 'TUBEMOGUL' and 'UPDATE_PROTEUS_PLATFORM_ADVERTISER_CMD' in message3:
    item_list = TUBEMOGUL_Advertiser_Pi
else:

    print("No Definition found  for " )
#***************MEDIAMATH*******************************************
if 'MEDIAMATH' and 'UPDATE_PROTEUS_PLATFORM_ADVERTISER_CMD' in message3:
    item_list = MEDIAMATH_Advertiser_PI
elif 'MEDIAMATH' and 'UPDATE_PROTEUS_PLATFORM_CAMPAIGN_CMD' in message3:
    item_list = MEDIAMATH_Campaign_PI
elif 'MEDIAMATH' and 'UPDATE_PROTEUS_PLATFORM_PLACEMENT_CMD' in message3:
    item_list = MEDIAMATH_Placement_PI
elif 'MEDIAMATH' and 'UPDATE_PROTEUS_PLATFORM_ITEM_CMD' in message3:
    item_list = MEDIAMATH_PI_PI
elif 'MEDIAMATH' and 'UPDATE_PROTEUS_PLATFORM_CREATIVE_CMD' in message3:
    item_list = MEDIAMATH_Creative_PI
else:

    print("No Definition found  for " )
#**********DCM*************************************************
if 'DCM' and 'UPDATE_PROTEUS_PLATFORM_ADVERTISER_CMD' in message3:
    item_list = DCM_Advertiser_PI
elif 'DCM' and 'UPDATE_PROTEUS_PLATFORM_CAMPAIGN_CMD' in message3:
    item_list = DCM_Campaign_PI
elif 'DCM' and 'UPDATE_PROTEUS_PLATFORM_PLACEMENT_CMD' in message3:
    item_list = DCM_Placement_PI
elif 'DCM' and 'UPDATE_PROTEUS_PLATFORM_ITEM_CMD' in message3:
    item_list = DBM_PI_PI
else:

    print("No Definition found  for " )

#***********test for items happens here*********************************
for item in item_list:
    print(colored('testing for  ', 'green') + colored(str(item),'yellow'))
    if item in message3:
        print(colored("This test passed for ******" + item +"---------" + str(message6.get(item)), 'green'))
    elif item in message6:
        print((colored("This test passed for ******" + item +" " + str(message6.get(item)), 'green')))
    elif item in message5:
        print((colored("This test passed for ******" + item +" " + str(message6.get(item)), 'green')))
    elif item in message:
        print((colored("This test passed for ******" + item+" " + str(message6.get(item)), 'green')))
    elif item in message2:
        print((colored("This test passed for ******" + item+" " + str(message6.get(item)), 'green')))
    elif item in message4:
        print((colored("This test passed for ******" + item+" " + str(message6.get(item)), 'green')))
    else:
        print((colored("This test failed" + "******" + item + " " + "not found", 'red')))
print(message6)
print('testing edge please wait**********************')

















