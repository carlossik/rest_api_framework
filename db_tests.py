import MySQLdb
import pymysql
import json
import time
import colored
from colored import stylize
import sys
from termcolor import colored,cprint

jobid = input("Please enter your Job ID")

try:
    conn = pymysql.connect(passwd="tli00eNND2ROLm:d,cq-", db="dspe", host="proteus.odin.tel-dev.io", port=3306,user="root")
except pymysql.err.OperationalError as error:
    code,message = error.args
    #print(colored("Sorry cannot connect to the Database please see error message :",code,message),'yellow')
    print((colored("Sorry cannot connect to the Database please see error message :", 'yellow')))
    print(colored(message,'cyan'))
    exit()


cur = conn.cursor()
cur.execute("select status from dspe.vendor_data_collection_job where id = " + jobid)
for r in cur:
    t = ''.join(r)
while t == 'IN_PROGRESS':
    print("Job is still in progress")

    time.sleep(5)
    continue
    conn
    cur.execute("select status from dspe.vendor_data_collection_job where id  = " +jobid)
else:
    try:
     if t == 'ERROR':
        print('The job has errored')
        sql = ("select status_message from dspe.vendor_data_collection_job where id =" + jobid)
        cur.execute(sql)
        conn.commit()
        results = cur.fetchall()
        #results1 = ''.join(results)
        num_data = ''.join(results)
        print(num_data)
    except TypeError as error:
        print("No data found ")
if t  == "COMPLETED":
    #print("The job has Completed ")
    sql = ("select count(*) from  dspe.delivery_standard_aggregate_v6 where job_id = " +jobid)
    cur.execute(sql)
    results = cur.fetchall()
    for result in results:

       print("The job has Completed with  " + str(results).strip('(),),') +" " + "rows persisted")
    #print('the number of row persisted  ')
    #print(str( (results)))
if t == "REQUESTED":
    print(" This job has not been run yet, please trigger a run")


cur.close()
conn.close()

def verify_job_runs():
    try:
        conn = pymysql.connect(passwd="tli00eNND2ROLm:d,cq-", db="dspe", host="proteus.odin.tel-dev.io", port=3306,
                               user="root")
    except pymysql.err.OperationalError as error:
        code, message = error.args
        # print(colored("Sorry cannot connect to the Database please see error message :",code,message),'yellow')
        print((colored("Sorry cannot connect to the Database please see error message :", 'yellow')))

        sql = ("select * from dspe.vendor_data_collection_job where id in(110929,110928,110930) ")
        cur.execute(sql)
        results = cur.fetchall()
        for result in results:
            print(result)
            return result

verify_job_runs()