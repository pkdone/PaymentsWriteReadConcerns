#!/usr/bin/env python3
##
# Module to enable a data processing function to be run multiple times in parallel each against a
# subset of a total data set.
##
import sys
import os
import contextlib
from multiprocessing import Process, Queue


##
# Spawns multiple process, each running the same logic in parallel against a data set and including
# a shared inter-process file logger.
#
# The 'func_to_parallelise' argument should have the following signature:
#     myfunc(processor_count, dataset_id, number_to_insert, log_queue, *args)
##
def run_data_processors(processor_count, records_count, logfile_name, func_to_parallelise, *args):
    records_per_process = int(records_count / processor_count)

    with contextlib.suppress(FileNotFoundError):
        os.remove(logfile_name)

    # Create separate safe shared process queue for logging by other processes
    log_queue = Queue()
    log_process = Process(target=logger_process_run, args=(log_queue, STOP_TOKEN, logfile_name))
    log_process.start()
    processesList = []

    # Create a set of OS processes to perform the main work to leverage CPU cores
    for i in range(processor_count):
        process = Process(target=wrapper_process_with_keyboard_exception,
                          args=(func_to_parallelise, processor_count, i, records_per_process,
                                log_queue, *args))
        processesList.append(process)

    try:
        # Start all processes
        for process in processesList:
            process.start()

        # Wait for all processes to finish
        for process in processesList:
            process.join()

        # Send queue message to end logging process
        log_queue.put(STOP_TOKEN)
        log_process.join()
    except KeyboardInterrupt:
        print(f'\nInterrupted - view file "{logfile_name}"\n')
        shutdown()


##
# Dequeue messages and write to single log file
# (a message written from one process doesn't interleave with messages written from another
# process)
##
def logger_process_run(log_queue, stop_token, logfile):
    try:
        with open(logfile, 'w') as f:
            while True:
                line = log_queue.get()

                if line == stop_token:
                    f.close()
                    break

                f.write(line)
                f.flush()
    except KeyboardInterrupt:
        shutdown()


##
# For a newly spawned process wraps a business function with the catch of a keyboard interrupt to
# then immediately ends the process when the exception occurs without spitting out verbiage
##
def wrapper_process_with_keyboard_exception(*args):
    try:
        args[0](*(args[1:]))
    except KeyboardInterrupt:
        os._exit(0)


##
# Swallow the verbiage that is spat out when using 'Ctrl-C' to kill the script
# and instead just print a simple line message
##
def shutdown():
    try:
        sys.exit(0)
    except SystemExit as e:
        os._exit(0)


# Constants
STOP_TOKEN = 'EOF!!'
