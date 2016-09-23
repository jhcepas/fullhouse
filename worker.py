import sys
import os
from subprocess import Popen, PIPE
import time
import signal

from argparse import ArgumentParser
from fullhouse.job_handler import FullHouseConnection

import daemon

ACTIVE_JOBID = None

def clean_exit(signum, frame):
    if ACTIVE_JOBID:
        db = FullHouseConnection(args.db_host, args.db_port, args.db_name)
        job = db.update_job(ACTIVE_JOBID, status='E', exitcode=666)
    sys.exit(666)

def main(args):
    global ACTIVE_JOBID
    db = FullHouseConnection(args.db_host, args.db_port, args.db_name)
    job = db.get_job(args.jobid)

    if not job:
        raise ValueError('Job not found: %s' %args.jobid)

    # ensure db is updated if process dies
    ACTIVE_JOBID = job["jid"]
    signal.signal(signal.SIGTERM, clean_exit)
    signal.signal(signal.SIGINT, clean_exit)
    #signal.signal(signal.SIGKILL, clean_exit)
    #signal.signal(signal.SIGSTOP, clean_exit)

    current_task = args.tasknumber
    os.environ["FULLHOUSE_TASK"] = str(current_task)

    try:
        if job.get('wd', None):
            os.chdir(job['wd'])
        else:
            home = os.path.expanduser("~")
            os.chdir(home)

        cmd = job["cmd"]
        if job['log']:
            OUT = open("%s.%s.out.txt" %(job["log"], current_task), "w")
            ERR = open("%s.%s.err.txt" %(job["log"], current_task), "w")
        else:
            OUT = open("%s.%s.out.txt" %(job["jid"], current_task), "w")
            ERR = open("%s.%s.err.txt" %(job["jid"], current_task), "w")

        p = Popen(cmd, shell=True, stdout=OUT, stderr=ERR)
        db.update_job(job, pid=p.pid)
        p.wait()

        print >>ERR, "exitcode:", p.returncode
        exitcode = p.returncode
        OUT.flush()
        ERR.flush()
    except:
        db = FullHouseConnection(args.db_host, args.db_port, args.db_name)
        db.jobs.update_one({'jid':job['jid']}, {"$set": {"status":"E"},
                                                "$pull": {'running': current_task}, "$inc":{"complete":1}})
        send_email(job, "Aborted")
        raise
    else:
        db = FullHouseConnection(args.db_host, args.db_port, args.db_name)
        if exitcode == 0:
            if current_task == job['tasks']:
                db.jobs.update_one({'jid':job['jid']}, {"$set": {"end_time":time.time(), "exitcode":exitcode},
                                                        "$pull": {'running': current_task}, "$inc":{"complete":1}})
            else:
                db.jobs.update_one({'jid':job['jid']}, {"$set": {"end_time":time.time(), "exitcode":exitcode},
                                                        "$pull": {'running': current_task}, "$inc":{"complete":1}})
        else:
            db.jobs.update_one({'jid':job['jid']}, {"$set": {"status":"E", "end_time":time.time(), "exitcode":exitcode},
                                                    "$pull": {'running': current_task}, "$inc":{"complete":1}})
            send_email(job, 'Failed')

def send_email(job, msg):
    if "email" in job:
        cmd = 'echo "" | /home/huerta/eggnog/eggnog-4.5/src/mappermail -s "Job (%s) %s" %s' %(job["jobname"], msg, str(job['email']))
        s = os.system(cmd)

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-db_host', type=str, default='localhost')
    parser.add_argument('-db_port', type=int, default=27017)
    parser.add_argument('-db_name', type=str, default='fullhouse')

    # If submitting from here
    parser.add_argument('--cpu', type=int)
    parser.add_argument('--wd', type=str)
    parser.add_argument('--jobname', type=str)
    parser.add_argument('--script', type=str)
    parser.add_argument('--run', action='store_true')

    # if already in DB
    parser.add_argument('-j', dest="jobid", type=str, required=True)
    parser.add_argument('-t', dest="tasknumber", type=int, required=True)

    args = parser.parse_args()



    with daemon.DaemonContext():
        main(args)


