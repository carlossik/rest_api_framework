import pymysql
import colored
import pandas as pd
import json
import MySQLdb
import csv
import colored
import requests
import re
from sqlalchemy import create_engine
from termcolor import colored,cprint



headers2 = {'Connection': 'keep-alive', 'Content-Length': '0', 'Content-Type': 'text/plain; charset=ISO-8859-1'}


def create_jobid_list():
    job_ids = []
    with open('C:\\Users\\carlos.attafuah\\Desktop\\Jmeter\\testdata_folder\\extract_job_details.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            id = str(row[4])
            job_ids.append(id)
            return job_ids




def create_delivery_sql():
    with open('C:\\Users\\carlos.attafuah\\Desktop\\Jmeter\\testdata_folder\\extract_job_details.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if "DELIVERY" in row:
                sql = "select count(*) from delivery_" + str(row[0]).lower() + " " + "where" + " " + "job_id = " + str(row[4])
                print(sql)
            else:
                pass

def create_conversion_sql():
    with open('C:\\Users\\carlos.attafuah\\Desktop\\Jmeter\\testdata_folder\\extract_job_details.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if 'CONVERSION'  in row:
                sql = "select count(*) from pixel_" + str(row[0]).lower() + " " "where" + " " + "job_id = " + str( row[4])
                print(sql)
            else:
                pass

def create_all_sql():
    with open('C:\\Users\\carlos.attafuah\\Desktop\\Jmeter\\testdata_folder\\extract_job_details.csv', 'r') as f:
        reader = csv.reader(f)
        sqls = []
        for row in reader:
            if 'LOG_CLICK' in row:
                sql = "select count(*) from dspe.file_audit_info where job_task_id =" + str(row[4])
                sqls.append(sql)
            elif 'LOG_CONVERSION' in row:
                sql = "select count(*) from dspe.file_audit_info where job_task_id =" + str(row[4])
                sqls.append(sql)
            elif 'LOG_RICH_MEDIA' in row:
                sql = "select count(*) from dspe.file_audit_info where job_task_id =" + str(row[4])
                sqls.append(sql)
            elif 'LOG_ACTIVITY' in row:
                sql =  "select count(*) from dspe.file_audit_info where job_task_id =" + str(row[4])
                sqls.append(sql)
            elif 'LOG_STANDARD' in row:
                sql =  "select count(*) from dspe.file_audit_info where job_task_id =" + str(row[4])
                sqls.append(sql)
            elif 'LOG_DISPLAY_ANALYTICS' in row:
                sql =  "select count(*) from dspe.file_audit_info where job_task_id =" + str(row[4])
                sqls.append(sql)
            elif 'LOG_SEGMENT' in row:
                sql =  "select count(*) from dspe.file_audit_info where job_task_id =" + str(row[4])
                sqls.append(sql)
            elif 'LOG_AD_PIXEL' in row:
                 sql =  "select count(*) from dspe.file_audit_info where job_task_id =" + str(row[4])
                 sqls.append(sql)
            elif 'CONVERSION' in row:
                sql = "select count(*) from pixel_" + str(row[0]).lower() + " " "where" + " " + "job_id = " + str(
                    row[4])
                sqls.append(sql)
            elif "DELIVERY" in row:
                sql = "select count(*) from delivery_" + str(row[0]).lower() + " " + "where" + " " + "job_id = " + str(
                    row[4])
                sqls = [sql.replace('[delivery_trueview]','delivery_true_view') for sql in sqls]
                sqls.append(sql)
            elif 'LOG_PLATFORM_ITEM_PIXEL' in row:
                sql = "select count(*) from dspe.file_audit_info where job_task_id =" + str(row[4])
                sqls.append(sql)
            elif 'LOG_AUCTION_SEGMENT' in row:
                sql = "select count(*)  from dspe.file_audit_info where job_task_id =" + str(row[4])
                sqls.append(sql)
            elif 'LOG_CAMPAIGN' in row:
                sql = "select count(*)  from dspe.file_audit_info where job_task_id =" + str(row[4])
                sqls.append(sql)

    

    for sql in sqls:
        if "select count(*) from delivery_trueview" in sql:
            trueview_sql = sql
            new_sql = trueview_sql.replace('delivery_trueview','delivery_true_view')
            sqls.remove(sql)
            sqls.append(new_sql)
        elif "delivery_avocet" in sql:
            avocet_sql = sql
            avonew = avocet_sql.replace('delivery_avocet','delivery_standard_aggregate_v6')
            sqls.remove(sql)
            sqls.append(avonew)
        elif "pixel_avocet" in sql:
            pixavo = sql
            pixavonew = pixavo.replace('pixel_avocet','pixel_standard_aggregate_v6')
            sqls.remove(sql)
            sqls.append(pixavonew)
            

    return sqls



def before_test():

    try:
        conn = MySQLdb.connect(passwd="tli00eNND2ROLm:d,cq-", db="dspe", host="proteus.odin.tel-dev.io", port=3306,
                               user="root")
        engine = create_engine('mysql+pymysql://username:password,dburl')
        sql = 'DELETE from dspe.file_audit_info'
        sql2 = 'select count(*) from dspe.file_audit_info'
        cur = conn.cursor()
        res1 = cur.execute(sql)
        conn.commit()
        results = (str(cur.fetchall()).strip("[],(),)' datetime.datetime("))
        print(results)
        res3 = cur.execute(sql2)
        actual_res = str(cur.fetchall()).strip("[],(),)' datetime.datetime(")
        if actual_res == 0 :
            print(res3)
            print("The database has been cleaned")
        else: print("database has not been cleared")
    except MySQLdb.OperationalError as opErr:
        print('there is an issue with the connection pls try again' + str(opErr))


def execute_sql():
    print('there are ' + ' ' + str(len(create_all_sql())) + ' ' + 'queries' + ' ' + 'to execute')

    import pandas as pd
    try:
        conn = MySQLdb.connect(passwd="tli00eNND2ROLm:d,cq-", db="dspe", host="proteus.odin.tel-dev.io", port=3306,
                               user="root")
        engine = create_engine('mysql+pymysql://root:tli00eNND2ROLm:d,cq-@proteus.odin.tel-dev.io:3306/dspe')
        sqls = create_all_sql()
        for sql in sqls:
            try:
                cur = conn.cursor()
                cur.execute(sql)
                res = (str(cur.fetchall()).strip("[],(),)' datetime.datetime("))
                if int(res) > 0:
                    print(colored("This test Passed::" + " "+ sql + res,'green'))
                else:
                    print(colored("This test failed to collect any data::" + " " + sql + " " + res,'yellow'))



            except MySQLdb.DatabaseError as dataerror:
                print("table does not exist" + str(dataerror))
    except MySQLdb.ProgrammingError as error:
        print("There was an error executing the query, Please check your queries" + error)




def create_kafka_checks():
    with open('C:\\Users\\carlos.attafuah\\Desktop\\Jmeter\\testdata_folder\\extract_job_details.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if 'true' in row:
                job_id = (str(row[4]))
                kafka_string = "http://vie-m02.tel-dev.io:9090/api/v1/messages/external_platform_entity?correlationId="+job_id+"&limit=1"
                kaf_tests = requests.get(kafka_string,headers=headers2).json()
                print(kaf_tests)

                print([job_id])


create_all_sql()
execute_sql()

