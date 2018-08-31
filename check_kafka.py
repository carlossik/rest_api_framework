import csv
import requests
import json
import pandas as pd
import pytest
import colored
from colored import stylize
import sys
from termcolor import colored,cprint
import pymysql
import MySQLdb

class Kafka():
    APPNEXUS_Advertiser_platform_Items = ['platformAdvertiserName', 'active', 'currency', 'rawEntity',
                                          'currency', 'externalAdvertiserId']
    APPNEXUS_PI_platform_Items = []
    APPNEXUS_Placement_PI = []
    APPNEXUS_Creative_platform_Items = ['externalAdvertiserId', 'externalCreativeId', 'platformCreativeName',
                                        'state', 'width', 'height', 'template', 'content', 'content_secure',
                                        'audit_status', 'audit_feedback', 'type']
    APPNEXUS_Campaign_platform_Items = ['startDate', 'endDate', 'lifetimeBudget', 'dailyBudget', 'goalType',
                                        'pacingInterval', 'rawEntity', 'lifetime_pacing?', 'enable_pacing',
                                        'budget_intervals.start_date', 'budget_intervals.lifetime_pacing']
    APPNEXUS_conversion_platform_items = ['advertiserId', 'conversionPixelId', 'conversionPixelName']
    DBM_Advertiser_PI = ['platform_id', 'external_advertiser_id', 'platformAdvertiserName', 'active',
                         'currency', 'timezone_code']
    DBM_Campaign_PI = ['platform_advertiser_id', 'external_campaign_id', 'name', 'active', 'start_date',
                       'end_date', 'overall_budget', 'overall_budget_impressions', 'daily_budget',
                       'daily_budget_impressions', 'pacing_type', 'frequency_cap', 'frequency_interval',
                       'frequency_interval_amount', 'pacing_interval']
    DBM_PI_PI = ['platform_item_id', 'platform_campaign_id', 'amount', 'goal_value', 'campaignId', 'goal_type',
                 'external_platform_item_id', 'budget_type', 'id', 'name', 'active', 'start_date', 'end_date',
                 'daily_budget', 'daily_budget_impressions', 'overall_budget', 'max_bid', 'base_bid', 'min_bid',
                 'pacing_type', 'pacing_interval', 'frequency_cap', 'frequency_interval',
                 'frequency_interval_amount', 'time_range', 'frequency_level', 'currency']
    DBM_Creative_PI = ['externalCreativeId', 'externalAdvertiserId', 'platformCreativeName', 'width', 'height',
                       'adServerPlacementId', 'adServer']
    TTD_Advertiser_PI = ['platformSeat', 'externalAdvertiserId', 'name', 'currency', 'rawEntity', 'partnerId']
    TTD_Campaign_PI = ['platformAdvertiser', 'externalCampaignId', 'name', 'startDate', 'endDate',
                       'dailyBudget', 'overallBudget']
    TTD_Creative_PI = ['externalAdvertiserId', 'externalCreativeId', 'platformCreativeName', 'width', 'height',
                       'adTag', 'audits', 'auditType', 'IsSecure']
    TTD_PI_PI = ['platformCampaign', 'externalPlatformItemId', 'name', 'active', 'startDate', 'endDate',
                 'dailyBudget', 'overallBudget', 'dailyBudgetImpressions', 'baseBid', 'maxBid', 'pacingType',
                 'pacingInterval', 'frequencyCap', 'frequencyIntervalAmount', 'frequencyInterval', 'goalType',
                 'currency', 'rawEntity', 'goalValue']
    MEDIAMATH_Advertiser_PI = ['platform_id', 'externalAdvertiserId', 'platformAdvertiserName', 'active']
    MEDIAMATH_Campaign_PI = ['externalAdvertiserId', 'externalCampaignId', 'platformCampaignName', 'active',
                             'startDate', 'endDate', 'overallBudget', 'dailyBudget', 'dailyBudgetImpressions',
                             'goalValue', 'pacingType', 'frequencyCap', 'frequencyInterval', 'external_version',
                             'goalType', 'currency', 'overallBudgetImpressions']
    MEDIAMATH_Placement_PI = []
    MEDIAMATH_Creative_PI = ['externalCreativeId', 'externalAdvertiserId', 'externalCreativeId',
                             'platformCreativeName', 'active', 'expandable', 'adTag', 'status',
                             'adServerPlacementId']
    MEDIAMATH_PI_PI = ['externalCampaignId', 'externalPlatformItemId', 'platformItemName', 'active',
                       'startDate', 'endDate', 'dailyBudget', 'dailyBudgetImpressions', 'overallBudget',
                       'maxBid', 'minBid', 'pacingInterval', 'pacingType', 'frequencyCap', 'frequencyInterval',
                       'frequencyIntervalAmount', 'goalValue', 'external_version', 'goal_type', 'currency']
    MEDIAMATH_PROTEUS_PIXEL = ['']
    DCM_Advertiser_PI = ['platformId,', 'externalAdvertiserId', 'platformAdvertiserName']
    DCM_Campaign_PI = ['platform_campaign_id', 'externalAdvertiserId', 'externalCampaignId',
                       'platformCampaignName']
    DCM_Placement_PI = ['messageType', 'externalAdvertiserId', 'platformPlacementName', 'externalPlacementId',
                        'externalCampaignId', 'width', 'height']
    DCM_PI_PI = []
    DCM_Proteus_platform_acpi = ['advertiserId', 'advertiserName', 'campaignId', 'campaignName', 'placementId',
                                 'placementName', 'date''keyword', 'floodlightname', 'floodlighId']
    TUBEMOGUL_Advertiser_Pi = ['externalAdvertiserId', 'externalAdvertiserId', 'platformAdvertiserName',
                               'active']



    def kafka_test():
        job_id = input("Please enter the jobid ")
        headers2 = {'Connection': 'keep-alive', 'Content-Length': '0', 'Content-Type': 'text/plain; charset=ISO-8859-1',
                    'Host': 'proteus.odin.tel-dev.io:9401'}
        external_acpi = "http://vie-m02.tel-dev.io:9090/api/v1/messages/external_acpi?correlationId=" + job_id + "&limit=1"
        internal_proteus = " "
        internal_data_lake_command = "http://vie-m02.tel-dev.io:9090/api/v1/messages/internal_datalake_command?correlationId=" + job_id + "&limit=1000"
        external_platform_entity = "http://vie-m02.tel-dev.io:9090/api/v1/messages/external_platform_entity?correlationId=" + job_id + "&limit=1000"
        external_ad_server = "http://vie-m02.tel-dev.io:9090/api/v1/messages/external_ad_server?correlationId=" + job_id + "&limit=1"
        external_dbm = "http://vie-m02.tel-dev.io:9090/api/v1/messages/external_dbm?correlationId=" + job_id + "&limit=10000"
        external_conversion = "http://vie-m02.tel-dev.io:9090/api/v1/messages/external_conversion?correlationId=" + job_id + "&limit=1"
        external_ad_server_dcm = "http://vie-m02.tel-dev.io:9090/api/v1/messages/external_ad_server_dcm?correlationId=" + job_id + "&limit=1"
        topics = [external_acpi,external_platform_entity]

        for topic in topics:
            try:
                req = requests.get(topic, headers=headers2).json()

            except requests.RequestException as e:
                print("No connection established " + str(e))
            else:
                try:
                    message = req['messages']

                    tot_message = req['total']

                except (IndexError, KeyError):
                    print("")
                try:
                    message2 = message[0]
                    message3 = message2['payload']
                    message4 = message3.split()
                    message5 = json.loads(message3)
                    message6 = message5['data']
                    messages = [message6, message5, message4, message3, message2, message]
                    print(str(tot_message) + " Messages found on " + topic)
                    print(messages)
                   # print(messages.__class__)
                    # print("**********************************************These are the values we need in our test **************************************************************************")
                    # external_advertiser_id = message
                    # for x in external_advertiser_id:
                    #    print((external_advertiser_id[0]['payload']))

                    #print(messages[1])
                    return messages
                except (IndexError,KeyError,SyntaxError):
                    print("No data found on " + topic)

    def bulk_verification(self):
        job_ids = input(" please enter the jobs you need to verify separated by a comma")




    def test_dbm(self):
        try:
            if 'DBM' and 'UPDATE_PROTEUS_PLATFORM_CREATIVE_CMD' in str(self.kafka_test()):
                item_list =self.DCM_Advertiser_PI
            elif 'DBM' and 'UPDATE_PROTEUS_PLATFORM_ITEM_CMD' in str(self.kafka_test()):
                item_list = self.DCM_Advertiser_PI
            elif 'DBM' and 'UPDATE_PROTEUS_PLATFORM_ADVERTISER_CMD' in str(self.kafka_test()):
                item_list = self.DCM_Advertiser_PI
            elif 'DBM' and 'UPDATE_PROTEUS_PLATFORM_CAMPAIGN_CMD' in str(self.kafka_test()):
                item_list = self.DCM_Advertiser_PI
            else:
                print("No Definition found  for JobId" + " " + self.job_id)
        except (KeyError, NameError, IndexError) as e:
            print("There was an issue " + e)



    def test_appnexus(self):
        data = self.kafka_test()
        data_orig = {k: [v] for k, v in data[0].items()}
        for jso in data_orig[1:]:
            for k, v in jso.items():
                data[k].append(v)
        df = pd.DataFrame(data)
        print(df)

    def test_fields(self):
        mydict = self.kafka_test()[0]
        mynewdict = (mydict)
        print(mydict)
        #
        # filename ="canonical_message"
        # mydata = pd.DataFrame((mydict),index=[0])
        # mydata.to_csv(filename)
        #
        #
        data_to_test = ['Platform Item',
                        'externalAdvertiserId',
                        'externalCampaignId',
                        'externalPlatformItemId',
                        'platformItemName',
                        'active',
                        'code',
                        'timezone',
                        'vendorTimezone',
                        'dailyBudget',
                        'dailyBudget',
                        'overallBudget',
                        'overallBudget',
                        'dailyBudgetImpressions',
                        'overallBudgetImpressions',
                        'baseBid',
                        'minBid',
                        'maxBid',
                        'pacingType',
                        'pacingInterval',
                        'frequencyCap',
                        'frequencyInterval']

        dict = json.dumps(mydict)  # k:v for k,v in (x.split(':') for x in mydict) }

        for data in data_to_test:
            if data in dict:
                print("This key is present in data" + " " + data)
            else:
                print(colored(data + " " + "is not present in this report", 'red'))

        pass









mykafka = Kafka
mykafka.kafka_test()


#field_test = Kafka
#field_test.test_fields()









































