import os
from collections import defaultdict
import time
from pprint import pprint
from functools import cmp_to_key
from argparse import ArgumentParser
from fullhouse.job_handler import FullHouseConnection
from fullhouse.utils import print_table
import subprocess

USER_PRIO = 0
USER_SLOT_PRIO = 0
CPU_PRIO = 1
WAIT_PRIO = 1

def send_email(job, msg):
    if "email" in job:
        cmd = 'echo "" | /home/huerta/eggnog/eggnog-4.5/src/mappermail -s "Job (%s) %s" %s' %(job["jobname"], msg, str(job['email']))
        s = os.system(cmd)

def schedule(args):
    def sort_jobs(a, b):
        a["cputime"] = CPU_TIME[a['jid']] * CPU_PRIO
        b["cputime"] = CPU_TIME[b['jid']] * CPU_PRIO

        a["user_prio"] = sum(user_slots[a['user']]) * USER_SLOT_PRIO
        b["user_prio"] = sum(user_slots[b['user']]) * USER_SLOT_PRIO

        a_prio = a["priority"]
        b_prio = b["priority"]
        r = -1 * cmp(a_prio, b_prio)
        if r:
            return r

        r = cmp(a["user_prio"], b["user_prio"])
        if r:
            return r


        r = cmp(a["cputime"], b["cputime"])
        if r:
            return r

        now = time.time()
        a_prio, b_prio = WAIT_PRIO, WAIT_PRIO
        a_prio *= (now - a["last_launch"]) if a["last_launch"] else (now - a["date"])
        b_prio *= (now - b["last_launch"]) if b["last_launch"] else (now - b["date"])
        r = -1 * cmp(a_prio, b_prio)
        if r:
            return r

        return 0

    CPU_SLOTS = args.cpu_slots
    db = FullHouseConnection(args.db_host, args.db_port, args.db_name)
    CPU_TIME = defaultdict(float)
    while True:
        print time.ctime(), 'Scheduling'
        jobs = db.active_jobs()
        used_cpus = 0
        user_slots = defaultdict(list)
        running_jobs = 0
        active_jobs = []
        for j in jobs:
            if j["status"] == 'Q':
                for task in j["running"]:
                    user_slots[j['user']].append(j['cpu'])
                    used_cpus += j['cpu']
                    running_jobs += 1
                    CPU_TIME[j["jid"]] += (args.schedule_time * j['cpu'])

                if j["complete"] == j["tasks"]:
                    # Finalize job
                    send_email(j, "Completed")

                    db.update_job(j, status="D")
                else:
                    active_jobs.append(j)

            elif j["status"] == 'D':
                pass

        avail_cpus = CPU_SLOTS - used_cpus

        print '  Total CPUs in use: %d/%d' %(used_cpus, CPU_SLOTS)
        print '  Total running jobs', running_jobs
        print '  Total active jobs', len(active_jobs)

        if active_jobs:
            # This makes that any new job starts with the same CPU as the longest
            # job in queue


            entering_time = max(CPU_TIME.values()) - 1 if CPU_TIME else 0
            CPU_TIME.default_factory = lambda: entering_time
            #for user, slots in user_slots.items():
            #    print "   % 25s: %d" %(user, sum(slots))

            active_jobs.sort(cmp=sort_jobs)

            columns = 'user jobname status cpu tasks last_task running last_launch cputime user_prio nseqs database mode'.split()
            matrix = [ [j.get(k, '') for k in columns] for j in active_jobs]
            print_table(matrix, header=columns)

            for j in active_jobs:
                if j['last_task'] >= j['tasks']:
                    continue

                if j['cpu'] > avail_cpus:
                    continue

                if j['deps']:
                    depjob_status = set([e["status"] for e in db.search_jobs({"jid": {"$in":j['deps']}}, {"status":1})])
                    print depjob_status, j['deps']
                    if depjob_status != set('D'):
                        continue

                # Submit job
                current_task = j['last_task'] + 1
                db.jobs.update_one({'jid':j['jid']}, {"$set": {"last_launch":time.time(), 'last_task':current_task},
                                                      "$push": {'running': current_task}})
                print "submitting ", j['user'], j['jobname']
                cmd = 'python worker.py -t %d -j %s' %(current_task, j['jid'])
                p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if current_task == 1:
                    send_email(j, "Started")
                break

        print 'Waiting %s secs' %(args.schedule_time)
        time.sleep(args.schedule_time)

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-db_host', type=str, default='localhost')
    parser.add_argument('-db_port', type=int, default=27017)
    parser.add_argument('-db_name', type=str, default='fullhouse')
    parser.add_argument('-s', '--schedule_time', type=int, default=5)
    parser.add_argument('--cpu_slots', type=int, default=1)
    parser.add_argument('--mem', type=int)
    parser.add_argument('--dev', action='store_true')

    args = parser.parse_args()

    schedule(args)

