# this file can be used to process any data file from production KCP
# this file will only keep the important fields to reduce the file size
import json
ff = open('tier3ajobs.json', 'r+')
oldjson = json.load(ff)
olddata = oldjson['data']

newdata = []
for data_entry in olddata:
    stored_job = {}
    debug_job = {}
    job1 = data_entry['job']
    job2 = job1['job']
    stored_job['jobId'] = job2['jobId']
    debug_job['job'] = stored_job
    debug_job['state'] = job1['state']
    debug_job['workerId'] = job1['workerId']
    debug_job['scale'] = job1['scale']
    new_data_entry = {}
    new_data_entry['job'] = debug_job
    new_data_entry['jobGroupId'] = data_entry['jobGroupId']
    newdata.append(new_data_entry)

newjson = {}
newjson['data'] = newdata

newfile = open('tier3ajobsnew.json', 'w+')
newfile.write(json.dumps(newjson))
