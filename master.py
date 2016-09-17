import os
from collections import defaultdict
import time
from pprint import pprint
from argparse import ArgumentParser
from fullhouse.job_handler import FullHouseConnection
USER_WEIGHT = 1
WAIT_WEIGHT = 1

def schedule(args):
    CPU_SLOTS = args.cpu_slots
    db = FullHouseConnection(args.db_host, args.db_port, args.db_name)

    while True:
        print time.ctime(), 'Scheduling'
        jobs = db.active_jobs()
        used_cpus = 0
        user_slots = defaultdict(int)
        running_jobs = 0
        waiting_times = defaultdict(int)
        for j in jobs:
            if j["status"] == 'R':
                user_slots[j['user']] += j['cpu']
                used_cpus = j['cpu']
                running_jobs += 1

        print '  Total CPUs in use', used_cpus
        print '  Total running jobs', running_jobs
        for user, slots in user_slots.items():
            print "   % 25s: %d" %(user, slots)

        # Calculate launching priorities
        def get_next_job(jobs):
            now = time.time()
            j2pri = {}
            queued_jobs = []
            for j in jobs:
                if j["status"] == "W":
                    priority = 0.0
                    priority -= user_slots[j["user"]] * USER_WEIGHT
                    last_launch = j["last_launch"]
                    priority += (now - last_launch) * WAIT_WEIGHT
                    j2pri[j["jid"]] = priority
                    queued_jobs.append(j)

            if queued_jobs:
                sorted_jobs = sorted(queued_jobs, key=lambda x:j2pri[x["jid"]], reverse=True)
                for j in sorted_jobs:
                    print j["user"], j2pri[j['jid']]
                next_job = sorted_jobs[0]
            else:
                return None, None

            return next_job, j2pri[next_job['jid']]

        next_job, pri = get_next_job(jobs)
        if next_job:
            print next_job["user"], pri
            print next_job['cmd']
            os.system('python worker.py -j %s &'%next_job['jid'])
            
        print 'Waiting %s secs' %(args.schedule_time)
        time.sleep(args.schedule_time)

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-db_host', type=str, default='localhost')
    parser.add_argument('-db_port', type=int, default=27017)
    parser.add_argument('-db_name', type=str, default='fullhouse')
    parser.add_argument('-s', '--schedule_time', type=int, default=5)
    parser.add_argument('--cpu_slots', type=int)
    parser.add_argument('--mem', type=int)
    parser.add_argument('--dev', action='store_true')

    args = parser.parse_args()

    schedule(args)

