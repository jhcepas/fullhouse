import sys
import os
from subprocess import Popen, PIPE
import time
import signal

from argparse import ArgumentParser
from fullhouse.job_handler import FullHouseConnection

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

    current_task = job['last_task']+1
    db.update_job(job, status='R', stime=time.time(), last_task=current_task)
    # ensure db is updated if process dies
    ACTIVE_JOBID = job["jid"]
    signal.signal(signal.SIGTERM, clean_exit)
    signal.signal(signal.SIGINT, clean_exit)
    #signal.signal(signal.SIGKILL, clean_exit)
    #signal.signal(signal.SIGSTOP, clean_exit)

    os.environ["FULLHOUSE_TASK"] = str(current_task)
    #cmd = cmd.replace('FULLHOUSE_TASK', str(current_task))
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

        exitcode = p.returncode
        print >>ERR, "exit:", exitcode
        print "---------------------------------", exitcode
        #OUT.flush()
        #ERR.flush()
    except:
        db = FullHouseConnection(args.db_host, args.db_port, args.db_name)
        send_email("Exception")
        db.update_job(job, status='X', end_time=time.time())
        raise
    else:
        db = FullHouseConnection(args.db_host, args.db_port, args.db_name)
        if exitcode == 0:
            if current_task == job['ntasks']:
                db.update_job(job, status='D', end_time=time.time(), exitcode=exitcode)
            else:
                db.update_job(job, status='W', end_time=time.time(), exitcode=exitcode)
            send_email('Done')
        else:
            db.update_job(job, status='E', end_time=time.time(), exitcode=exitcode)
            send_email('Error')

def send_email(msg):
    print 'sending email....', msg
    pass

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
    parser.add_argument('-j', dest="jobid", type=str)

    args = parser.parse_args()
    main(args)
