#!/usr/bin/env python3
##
# Processes simulated Payments records using MongoDB. Provides ability to ingest randomly generated
# records into a collection with one of various Write Concerns and the ability to read the ingested
# records back out using one of various Read Concerns. Processing times and response times for a
# sample of the generated requests are appended to a log file.
#
# Note: If you specify the number of client injection processes as exceeding the 6 less than the
# total number of hardware threads (vCPUs) of the host machine/VM the result data logged to file
# may not fully appear due to a suspected Python multiprocessing issue.
#
# For usage run first ensure the '.py' script is executable and then run:
#   $ ./payments-records-loader.py -h
#
# Example (connect to Atlas cluster and use 8 client OS processes for injection):
#   $ ./payments-records-loader.py -u "mongodb+srv://myuser:mypswd@testcluster-ap.mongodb.net" -p 8
##
import argparse
from random import randint, choice
import traceback
from datetime import datetime
from pymongo import MongoClient
import datamultiproc as dmp


# Seeds for random generated field values
PAYMENT_REASONS = ['pay bill', 'replayment', 'owed money', 'temp loan', 'buy goods']
FIRST_NAMES = ['Suzi', 'Bobby', 'Gertrude', 'Gordon', 'Mandy', 'Sandy', 'Randy', 'Candy', 'Bambi']
LAST_NAMES = ['Brown', 'Jones', 'Roberts', 'McDonald', 'Barrett', 'Saunders', 'Reid',
              'Whittington-Smythe', 'Parker-Tweed']


##
# Main function spawning multiple processes to each generate and ingest a set of randomly generated
# Payment records.
##
def main():
    # Parse all arguments passed to this script
    argparser = argparse.ArgumentParser(description='Ingest or query Payment records in a MongoDB '
                                        f'database)')
    argparser.add_argument('-u', '--url', default=DEFAULT_MONGODB_URL,
                           help=f'MongoDB Cluster URL (default: {DEFAULT_MONGODB_URL})')
    argparser.add_argument('-p', '--procs', type=int, default=DEFAULT_TOTAL_PROCESSES,
                           help=f'Number of processes to run (default: {DEFAULT_TOTAL_PROCESSES})')
    argparser.add_argument('-l', '--logfile', default=DEFAULT_LOG_FILENAME,
                           help=f'Name of file to log to (default: {DEFAULT_LOG_FILENAME})')
    argparser.add_argument('-m', '--mode', choices=['local', 'majority', 'linearizable'],
                           default=DEFAULT_MODE, help=f'Defines which Write & Read Concern mode to '
                           f'use for DB interaction: local (WC:1, RC:local), majority (WC:majority,'
                           f'RC:majority), linearizable (WC:majority, RC:linearizable) (default: '
                           f'{DEFAULT_MODE})')
    argparser.add_argument('-t', '--totalrec', type=int, default=DEF_INSERTS_TOTAL,
                           help=f'Number of records to insert (default: {DEF_INSERTS_TOTAL})')
    argparser.add_argument('-q', '--doqueries', default=DEFAULT_DO_QUERIES,
                           action='store_true', help=f'Perform querying of existing Payment data'
                           f' set rather than inserting new Payment data set (default:'
                           f' {DEFAULT_DO_QUERIES})')
    args = argparser.parse_args()

    # Run main data processing work split into multiple processes, tracking start & end times
    print()
    print(f"Using URL '{args.url}' (mode: {args.mode}), check file '{args.logfile}' for progress "
          f"information")
    start = datetime.now()

    if args.doqueries:
        dmp.run_data_processors(args.procs, args.totalrec, args.logfile, query_payment_records,
                                args.url, args.mode)
    else:
        dmp.run_data_processors(args.procs, args.totalrec, args.logfile, insert_payment_records,
                                args.url, args.mode)

    end = datetime.now()
    print(f'{args.totalrec} records processed by {args.procs} processes in '
          f'{int((end-start).total_seconds())} seconds')
    print()


##
# The function for a single OS process to run, looping and querying random records from the total
# set of Payment records.
##
def query_payment_records(processor_count, process_id, records_per_process, log_queue, uri, mode):
    (connection, database, collection) = getClientConnDBCollectionWithConcerns(uri, mode)

    # Loop creating each random payment record and ingesting into a collection
    for count in range(records_per_process):
        try:
            doc_id = f'{randint(0, processor_count-1)}_{randint(0, records_per_process-1)}'

            if count % (records_per_process/100) == 0:
                start = datetime.utcnow().timestamp()

            # FIND
            payment_records = collection.find_one({'_id': doc_id})

            if payment_records is None:
                print(f'Queried record not found for _id: {doc_id}')

            logSampleStatusOnEachPercent(log_queue, records_per_process, process_id, count, start)
        except Exception as e:
            print(f'Terminating due to error whilst performing queries:\n')
            traceback.print_exc()
            break


##
# The function for a single OS process to run, looping and inserting a subset of Payment records.
##
def insert_payment_records(processor_count, process_id, records_per_process, log_queue, uri, mode):
    (connection, database, collection) = getClientConnDBCollectionWithConcerns(uri, mode)
    # Example index creation: collection.create_index([('fld1', ASCENDING), ('fld2', DESCENDING)])

    # Loop creating each random payment record and ingesting into a collection
    for count in range(records_per_process):
        try:
            ingest_doc = {
                        '_id': f'{process_id}_{count}',
                        'timestamp': datetime.now(),
                        'payment_ref': choice(PAYMENT_REASONS),
                        'payer_sort_code': str(randint(111111, 999999)),
                        'payer_acc_num': str(randint(111111111, 999999999)),
                        'payer_name': f'{choice(FIRST_NAMES)} {choice(LAST_NAMES)}',
                        'payee_sort_code': str(randint(111111, 999999)),
                        'payee_acc_num': str(randint(111111111, 999999999)),
                        'payee_name': f'{choice(FIRST_NAMES)} {choice(LAST_NAMES)}',
                        'amount': str(randint(1, 99999)),
                    }

            if count % (records_per_process/100) == 0:
                start = datetime.utcnow().timestamp()

            # INSERT
            collection.insert_one(ingest_doc)

            logSampleStatusOnEachPercent(log_queue, records_per_process, process_id, count, start)
        except Exception as e:
            print(f'Terminating due to error whilst performing inserts:\n')
            traceback.print_exc()
            break


##
# Create a MongoDB client connection using specific Read & Write Concerns and with timeouts set
##
def getClientConnDBCollectionWithConcerns(uri, mode):
    if mode == 'local':
        wc = 1
        rc = 'local'
    elif mode == 'majority':
        wc = 'majority'
        rc = 'majority'
    else:
        wc = 'majority'
        rc = 'linearizable'

    connection = MongoClient(host=uri, w=wc, readConcernLevel=rc, readPreference='primary',
                             socketTimeoutMS=DEFAULT_TIMEOUT_MILLIS,
                             wtimeout=DEFAULT_TIMEOUT_MILLIS,
                             connectTimeoutMS=DEFAULT_TIMEOUT_MILLIS,
                             serverSelectionTimeoutMS=DEFAULT_TIMEOUT_MILLIS)
    database = connection[DB_NAME]
    collection = database[COLLECTION_NAME]
    return (connection, database, collection)


##
# Log a sample status and response time for every 1% of records processed
##
def logSampleStatusOnEachPercent(log_queue, records_per_process, process_id, count, start_utc):
    if count % (records_per_process/100) == 0:
        log_queue.put(f'{int(count / records_per_process * 100)}% - {count} '
                      f'documents inserted for data set id {process_id} - '
                      f'{datetime.now()} - sample response time for one request: '
                      f'{1000 * (datetime.utcnow().timestamp() - start_utc)} ms\n')


# Constants
DEFAULT_TIMEOUT_MILLIS = 2000
DEFAULT_MONGODB_URL = 'mongodb://localhost:27017'
DEFAULT_TOTAL_PROCESSES = 2
DEFAULT_MODE = 'local'
DEF_INSERTS_TOTAL = 1000000
DEFAULT_DO_QUERIES = False
DEFAULT_LOG_FILENAME = 'processing-output.log'
DB_NAME = 'fs'
COLLECTION_NAME = 'payments'


##
# Main
##
if __name__ == '__main__':
    main()
