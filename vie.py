import threading
import os
import requests
import json
import time
import csv
import io
import MySQLdb


data = {'grant_type': 'password', 'client_secret': '413elqq52m0wkgc8wcwc0k88g04go4k8o8oswk84wsg8o80s0k',
        'client_id': '9_4q0da7s42l8g040g4oscs8gw4k4cogcowoccgkc84kswksgcko',
        'username': 'carlos.attafuah@theexchangelab.com', 'password': 'Proteus1'}
r = requests.get('http://metalab.odin.tel-dev.io/oauth/v2/token?', params=data)
authdata = "BEARER" + " " + r.json()["access_token"]
if r.status_code == 200:
    print(authdata)
else:
    print (" The authorization process failed")
headers = {"Connection": "keep-alive", "Authorization":  authdata, "Content-Length": '102',
           "Content-Type": "application/json"}
headers2 = {"Accept": "*/*","Authorization": authdata}
#payload = json.dumps({"endDate": "2017-02-17 23:59:00", "seatConfigId": 5, "startDate": "2017-02-16 00:00:00"})


class dspe(object):


     def create_jobs(self,payload):
        url = 'http://vie-m01.tel-dev.io:9401/api/v1/create-data-collection-job'
        r = requests.post(url, data=w, headers=headers)
        self.results = r.json()[0]["jobGroupId"]
        self.job_id = r.json()[0]["jobId"]
        self.vendor = r.json()[0]["vendor"]
        self.vendor_data_type = r.json()[0]["vendorDataType"]
        job_id = r.json()[0]["id"]
        f = open('jobstore.csv', 'w')
        for id in str(job_id):
            f.write(id )

        print ( "Created job for " + str(self.vendor) + " " + self.vendor_data_type + " with Job Id of " + str(self.job_id))
        return self.results
        #return self.job_id






     def runjob(self):
      jobgroupid = self.results
      url = 'http://vie-m01.tel-dev.io:9401/api/v1/run-data-collection-job-group/'+jobgroupid
      self.runjob = requests.get(url,headers=headers2)
      if self.runjob.status_code == 408:
          print("The job running process" + " " + "for" + " " + self.vendor_data_type + " " + self.vendor + " " + "has timed out , please investigate further")
          exit()
      elif self.runjob.status_code == 200:

          print (
          "Started job for " + str(self.vendor) + " " + self.vendor_data_type + " with Job Id of " + str(self.job_id))
      else:
          print("There has been an error")
          exit()
     #print(self.runjob)

     def get_status(self):
         jobid = self.job_id
         status_url = 'http://vie-m01.tel-dev.io:9401/api/v1/data-collection-job-status/'+str(jobid)
         verify_status_results = requests.get(status_url,headers=headers2)
         self.status = verify_status_results.json()["status"]
         self.vendor = verify_status_results.json()["vendor"]
         self.vendor_data_type = verify_status_results.json()["vendorDataType"]
         print("The" + " " + self.vendor + " " + self.vendor_data_type + " " + "job is " + "" + self.status )
         return self.status
      #def write_to_file(self):

     def get_seat_configs_by_id(self):

         with open('seatids.csv','r') as seatids:
             for id in seatids:
                 configurl = 'http://vie-m01.tel-dev.io:9401/api/v1/seat-data-collection-configurations/by-platform-id/'+str(id)
                 requests.get(configurl)


     def GetDataStore(self, sql):
         conn = MySQLdb.connect(host=self.host, user=self.userid, password=self.pwd, database=self.db)
         c = conn.cursor()
         c.execute(sql)
         rows = c.fetchall()
         for row in rows:
             print(rows)






with open('vietestdata.json') as payload:
    data = json.load(payload)
    no_of_jobs_to_run = 0
    for r in data['payload']:
        w=json.dumps(r)
        mydspe = dspe()
        mydspe.create_jobs(payload=w)

        mydspe.runjob()
        no_of_jobs_to_run += 1
        print(str(no_of_jobs_to_run) + ("Jobs Created"))

        if mydspe.get_status() == "IN_PROGRESS":
         pass
        else:
            print ("The job reported status of " + " " + mydspe.get_status())









