# MongoDB Payments Write & Read Concern Tester Project

Processes simulated Payments records using MongoDB. Provides ability to ingest randomly generated records into a collection with one of various Write Concerns and the ability to read the ingested records back out using one of various Read Concerns. Processing times and response times for a sample of the generated requests are appended to a log file.

_Note:_ If you specify the number of client injection processes as exceeding the 6 less than the total number of hardware threads (vCPUs) of the host machine/VM the result data logged to file may not fully appear due to a suspected Python multiprocessing issue.

For usage first ensure the '.py' script is executable and then run:
```
./payments-records-loader.py -h
```

Example to connect to an Atlas cluster to collect statistics from:
```
./payments-records-loader.py -u "mongodb+srv://myuser:mypswd@testcluster-ap.mongodb.net" -p 8
```

