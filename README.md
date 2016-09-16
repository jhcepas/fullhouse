Basic job balancer in python

REQUIRES: mongo and pymongo

- initialize mongo db
```bash
# $ mongo
# > use fullhouse
# switched to db fullhouse
# > db.createCollection("jobs", {autoIndexID : true})
# { "ok" : 1 }
# > db.jobs.createIndex({jid:1, status:1})
# {
#             "createdCollectionAutomatically" : false,
#             "numIndexesBefore" : 1,
#             "numIndexesAfter" : 2,
#             "ok" : 1
# }

```

- Start master in a terminal:
```
 python master.py
```

- submit jobs using the fullhose API or `fsub`

```
fsub --cpu 5 -u jaime 'ls -ltr; sleep 10'
```