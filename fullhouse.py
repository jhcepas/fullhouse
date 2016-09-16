import uuid
import time
from pprint import pprint
from pymongo import MongoClient

def printjob(job):
    print job["jobname"]
    for k,v in sorted(job.items()):
        print " % 20s: %s " %(k, v)

class FullHouseConnection(object):
    def __init__(self, db_host, db_port, db_name):
        self.client = MongoClient(db_host, db_port)
        self.db = self.client[db_name]
        self.jobs = self.db.jobs

    def get_job(self, jobid):
        if isinstance(jobid, dict):
            jobid = jobid["jid"]
        print {"jid":jobid}
        return self.jobs.find_one({"jid":jobid})

    def update_job(self, jobid, **kw):
        if isinstance(jobid, dict):
            jobid = jobid["jid"]

        return self.jobs.update_one({"jid":jobid}, {"$set": kw})

    def register_job(self, jobdict):
        jobdict["date"] = time.time()
        jobdict['jid'] = uuid.uuid4().hex
        res = self.jobs.insert_one(dict(jobdict))
        return jobdict['jid']

    def delete_job(self, jobid):
        if isinstance(jobid, dict):
            jobid = job["jid"]

        return self.jobs.delete_one({"jid":jobid})

    def template_job(self, **kw):
        job = {'jobname':None, 'cpu':1, 'date':None, 'first_launch':0, 'last_launch':0,
               'end_time':0, 'user':None, 'stdout':None, 'stderr':None,
               'exitcode':None, 'status':'W', 'tasks':1, "last_task":0, 'cmd':None, 'jid':None,
               'deps':None}

        job.update(kw)
        return job

    def active_jobs(self):
        query = {"$or":
                 [{"status": "W"},
                  {"status": "R"}]}
        return list(self.jobs.find(query))

    def all_jobs(self):
        return list(self.jobs.find())

def clear():
    db = FullHouseConnection('localhost', 27017, 'fullhouse')
    db.jobs.delete_many({})

def test():
    import os
    test_script = os.path.abspath('test.sh')
    cmd = 'bash %s'%test_script

    db = FullHouseConnection('localhost', 27017, 'fullhouse')
    for x in range(4):
        job = db.template_job(jobname='test4', status='W', cpu=5, user='lore', cmd=cmd)
        db.register_job(job)
        job = db.template_job(jobname='test2', status='W', cpu=2, user='jaime', cmd=cmd)
        db.register_job(job)
        job = db.template_job(jobname='test3', status='W', cpu=2, user='lore', cmd=cmd)
        db.register_job(job)
        job = db.template_job(jobname='test4', status='W', cpu=5, user='laura', cmd=cmd)


if __name__ == '__main__':
    db = FullHouseConnection('localhost', 27017, 'fullhouse')
    for job in  db.all_jobs():
        printjob(job)
    

# > use fullhouse
# switched to db fullhouse
# > db.createCollection("jobs", {autoIndexID : true})
# { "ok" : 1 }
# > db.jobs.createIndex({jobname:1, status:1})
# {
#             "createdCollectionAutomatically" : false,
#             "numIndexesBefore" : 1,
#             "numIndexesAfter" : 2,
#             "ok" : 1

# }
