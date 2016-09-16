import sys
from subprocess import Popen, PIPE
import time
import signal

from argparse import ArgumentParser
from fullhouse import FullHouseConnection

ACTIVE_JOBID = None

def clean_exit(signum, frame):
    if ACTIVE_JOBID:
        db = FullHouseConnection(args.db_host, args.db_port, args.db_name)
        job = db.update_job(ACTIVE_JOBID, status='E')
    sys.exit(666)

def main(args):
    global ACTIVE_JOBID
    db = FullHouseConnection(args.db_host, args.db_port, args.db_name)
    job = db.get_job(args.jobid)

    if not job:
        raise ValueError('Job not found: %s' %args.jobid)

    db.update_job(job, status='R', stime=time.time(), last_task=job['last_task']+1)
    # ensure db is updated if process dies
    ACTIVE_JOBID = job["jid"]
    signal.signal(signal.SIGTERM, clean_exit)
    signal.signal(signal.SIGINT, clean_exit)
    #signal.signal(signal.SIGKILL, clean_exit)
    #signal.signal(signal.SIGSTOP, clean_exit)

    cmd = job["cmd"]
    try:
        p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
        db.update_job(job, pid=p.pid)
        output, err = p.communicate()
        exitcode = p.returncode
        p.wait()
        #exitcode = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        send_email()
    except:
        db = FullHouseConnection(args.db_host, args.db_port, args.db_name)
        send_email()
        db.update_job(job, status='E', end_time=time.time())
        raise
    else:
        db = FullHouseConnection(args.db_host, args.db_port, args.db_name)
        if exitcode == 0:
            if job['last_task'] == job['ntasks']:
                db.update_job(job, status='D', end_time=time.time(), exitcode=exitcode)
            else:
                db.update_job(job, status='W', end_time=time.time(), exitcode=exitcode)
            send_email()
        else:
            db.update_job(job, status='E', end_time=time.time(), exitcode=exitcode)
            send_email()

def send_email():
    print 'sending email....'
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
