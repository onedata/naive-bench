#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Benchmarking program
TODO: better module description
"""

#
# The MIT License (MIT)
# Copyright (c) 2016 Bartosz Kryza <bkryza at gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.
#

import functools
import hashlib
import math
import optparse
import os
import random
import re
import shutil
import socket
import string
import sys
import time
from itertools import repeat
from multiprocessing import Manager, Process, Queue, Value

import humanize
from retrying import retry

# Global constants
#
KIBYBYTES = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB']
KILOBYTES = ['KB', 'MB', 'GB', 'TB', 'PB', 'EB']

PROCESS_MANAGER = Manager()

#
# All proceses put graphite messages here
# Another proces empties it and sends the data
#
MESSAGE_QUEUE = Queue()

#
# Global reference counter, that monitors
# if the benchmark is running stedealy
#
REFERENCE_COUNTER = Value('l', 0)

#
# Shared boolean value that is turned to false
# at the end of the benchmark
#
BENCHMARK_ACTIVE = Value('b', True)

#
# Initialize CSV column labels
#
STORAGE_NAME_LABEL = "STORAGE NAME"
TOTAL_TIME_LABEL = "TOTAL TIME [s]"
GLOBAL_START_TIME_LABEL = "START TIME [s]"
GLOBAL_END_TIME_LABEL = "END TIME [s]"
NUMBER_FILES_LABEL = "FILE COUNT"
AVERAGE_FILE_SIZE_LABEL = "AVERAGE FILE SIZE [b]"
CREATE_FILES_LABEL = "CREATE TIME [s]"
CREATE_FILES_SIZE_LABEL = "CREATE SIZE [b]"
CREATE_FILES_THROUGHPUT_LABEL = "CREATE THROUGHPUT [b/s]"
OVERWRITE_FILES_LABEL = "WRITE TIME [s]"
OVERWRITE_FILES_SIZE_LABEL = "WRITE SIZE [b]"
OVERWRITE_FILES_THROUGHPUT_LABEL = "WRITE THROUGHPUT [b/s]"
RANDOMWRITE_FILES_LABEL = "RANDOM WRITE TIME [s]"
RANDOMWRITE_FILES_SIZE_LABEL = "RANDOM WRITE SIZE [b]"
RANDOMWRITE_FILES_THROUGHPUT_LABEL = "RANDOM WRITE THROUGHPUT [b/s]"
LINEAR_READ_LABEL = "LINEAR READ TIME [s]"
LINEAR_READ_SIZE_LABEL = "LINEAR READ SIZE [b]"
LINEAR_READ_THROUGHPUT_LABEL = "LINEAR READ THROUGHPUT [b/s]"
RANDOM_READ_LABEL = "RANDOM READ TIME [s]"
RANDOM_READ_SIZE_LABEL = "RANDOM READ SIZE [b]"
RANDOM_READ_THROUGHPUT_LABEL = "RANDOM READ THROUGHPUT [b/s]"
DELETE_LABEL = "DELETE"

__TEST_DATA_DIR = "naive-bench-data"

##########
#
# Communicate with graphite
#
#

def reference_counter_sender():
    """
    Increment global counter every second to monitor of bechmarki is running fine
    """

    while BENCHMARK_ACTIVE.value:
        with REFERENCE_COUNTER.get_lock():
            REFERENCE_COUNTER.value += 1
        time.sleep(1)

def increase_counters(cbytes_processed, bytes_processed, \
                      cblock_size, block_size, \
                      cthroughput, throughput):
    """
    Modify counters with locks
    """

    with cbytes_processed.get_lock():
        cbytes_processed.value += bytes_processed
    with cblock_size.get_lock():
        cblock_size.value = block_size
    with cthroughput.get_lock():
        cthroughput.value = int(throughput)


def create_graphite_counter_prefix(*args, prefix=""):
    """
    Create a counter prefix in format compatibile with graphite
    """

    p = ""
    for i in args:
        if str(i) is not "": p += "." + str(i).replace(".", "_")
    if prefix is not "":
        return prefix + p
    return p[1:]


def send_data_to_graphite(*names, prefix="", **keywords):
    """
    Enqueue data to be send to graphite
    """

    for key in keywords:
        MESSAGE_QUEUE.put("{} {} {}\n".format(
                          create_graphite_counter_prefix(*names,key, "value", prefix=prefix), \
                          keywords[key], time.time()))


def graphite_sender(graphite_server, graphite_port):
    """
    Send data from a queue to a graphite server
    """

    while BENCHMARK_ACTIVE.value:
        while not MESSAGE_QUEUE.empty():
            d = MESSAGE_QUEUE.get()
            sock = socket.socket()
            sock.connect((graphite_server, graphite_port))
            sock.send(d.encode())
            sock.close()
        time.sleep(0.25)


##########
#
# Benchmark I/O utils
#
#


def get_random_file_size(filesize, dev):
    """
    Get randomized file size based on average 'filesize' and deviation range.
    """

    min_range = (1.0 - dev) * filesize
    max_range = (1.0 + dev) * filesize

    return int((max_range - min_range) * random.random() + min_range)


def get_random_data(size):
    """
    Create a an array of specified size filled with a random bytes
    """
    # return bytearray(os.urandom(size))
    return bytearray(size)


def parse_file_size(file_size_string):
    """
    This function parses the file sizes supporting both conventions
    (i.e. KiB and KB)
    """

    file_size = float('nan')
    #
    # First check if the number is in bytes without suffix
    # if it's not try to match a known suffix
    #
    try:
        file_size = int(file_size_string)
        return file_size
    except ValueError:
        parse_result = re.split(r'([\.\d]+)', file_size_string)
        try:
            file_size = float(parse_result[1])
            if parse_result[2] in KIBYBYTES:
                file_size *= math.pow(1024,
                                      (KIBYBYTES.index(parse_result[2]) + 1))
                return file_size
            elif parse_result[2] in KILOBYTES:
                file_size *= math.pow(1000,
                                      (KILOBYTES.index(parse_result[2]) + 1))
                return file_size
            return float('nan')
        except ValueError:
            return float('nan')
        return False


##########
#
# Benchmarking  I/O utils
#
#
# @retry
def try_open(file_path, flags):
    """
    Open a file with specified flags
    """
    return os.open(file_path, flags)


def try_read(file, blocksize):
    """
    Read blocks from a file
    """
    return os.read(file, blocksize)


def try_write(file, wbytes):
    """
    Write blocks to a file
    """
    return os.write(file, wbytes)


def drop_caches():
    """
    Drops file cache
    """

    if shutil.which("sudo") is not None:
        shell = "sudo sh"
    else:
        shell = "sh"

    cmd = ""
    if sys.platform == "linux" or sys.platform == "linux2":
        cmd = shell + " -c 'sync ; echo 3 > /proc/sys/vm/drop_caches'"
    elif sys.platform == "darwin":
        cmd = shell + " -c 'sync; purge'"
    else:
        print(sys.platform, " platform is not supported - exiting.")
        sys.exit(1)

    os.system(cmd)


##########
#
# Benchmark functions
#
#

def format_progress_message(name, progress, total, suffix, width=40, \
                            numtype='numeric'):
    """
    Formats the progress
    """
    if numtype == 'normal':
        p = int((progress * width) / total)
        percentage = int((progress * 100) / total)
        return name + (" [%-40s] %d%%" % ('='*p, percentage)) + " " \
            + progress  + "/"\
            + total  \
            + " | " \
            + suffix + "        "
    elif numtype == 'filesize':
        p = int((progress * width) / total)
        percentage = int((progress * 100) / total)
        return name + (" [%-40s] %d%%" % ('='*p, percentage)) + ", " \
            + humanize.naturalsize(progress)  + " of "\
            + humanize.naturalsize(total)  \
            + " | " \
            + suffix + "        "

def run_benchmark(benchmark, benchmark_name, \
                  filecount, threadcount, deviation, \
                  blocksize, threads_results, \
                  threads_progress_messages, repeat):
    """
    This is a generic function for running naive benchmarks
    """

    cbytes_processed = Value('l', 0)
    cblock_size = Value('l', 0)
    cthroughput = Value('l', 0)
    #
    # Initialize barrier lock to wait until the threads initialize before
    # starting time measurement
    #
    start_barrier = Manager().Barrier(threadcount + 1)

    #
    # Prepapre a list of arguments for each benchmark task
    #
    benchmark_args = []
    for tidx in range(threadcount):

        r = None
        if filecount == threadcount:
            r = [tidx]
        else:
            low_range = int(tidx * (filecount / threadcount))
            high_range = int((tidx + 1) * (filecount / threadcount))
            r = list(range(low_range, high_range))

        benchmark_args.append(\
            (tidx, benchmark_name, r, filesize, deviation, blocksize, \
               __TEST_DATA_DIR, threads_results, threads_progress_messages, \
               start_barrier, cbytes_processed, cblock_size, \
               cthroughput, repeat))
        threads_results[tidx] = 0
        threads_progress_messages[tidx] = "Starting task " + str(tidx)

    #
    # Create the process pool and run the benchmark
    #
    for i in range(threadcount):
        child = Process(target=benchmark, \
                        args=benchmark_args[i],)
        child.start()
        threads.append(child)

    #
    # Wait for all benchmark tasks to initialize
    #
    start_barrier.wait()

    start_time = time.time()
    #
    # Wait for the threads to complete and the progress every
    # 0.5 second
    #
    executed = False
    time.sleep(0.5)
    while any(thread.is_alive() for thread in threads) or not executed:
        executed = True
        time.sleep(0.5)
        for i in range(threadcount):
            print(threads_progress_messages[i], file=sys.stderr)

        with cbytes_processed.get_lock():
            bytes_processed_tmp = cbytes_processed.value
        with cblock_size.get_lock():
            block_size_tmp = cblock_size.value
        with cthroughput.get_lock():
            throughput_tmp = cthroughput.value
        with REFERENCE_COUNTER.get_lock():
            reference_counter_tmp = REFERENCE_COUNTER.value

        send_data_to_graphite("measurment", \
                                benchmark_name, prefix=graphite_prefix, bytes_processed=bytes_processed_tmp, \
                                block_size=block_size_tmp, \
                                throughput=throughput_tmp, \
                                REFERENCE_COUNTER=reference_counter_tmp)
        for i in range(threadcount):
            sys.stderr.write("\x1b[A")

    real_execution_time = time.time() - start_time

    return real_execution_time


#pylint: disable=unused-argument
def file_create_benchmark(task_id, task_name, file_ids, filesize, deviation, \
                          blocksize, test_data_dir, thread_results, \
                          thread_progress_messages, start_barrier, \
                          cbytes_processed, cblock_size, cthroughput, \
                          repeat):
    """
    Task which creates a set of test files and measures total time
    """

    _measurment = "create"

    total_written_bytes = 0

    #
    # Generate random file sizes and calculate total size for this task
    #
    random_file_sizes = \
                  [get_random_file_size(filesize, deviation) for i in file_ids]
    total_size_to_write = sum(random_file_sizes)

    thread_progress_messages[task_id] = \
        format_progress_message("Task #" + str(task_id),
                                total_written_bytes,
                                total_size_to_write,
                                "???",
                                width=40, numtype='filesize')

    randdata = get_random_data(blocksize)

    start_barrier.wait()
    start_time = time.time()

    files = []
    for i in range(len(file_ids)):
        files.append(
            try_open(test_data_dir + "/" + str(file_ids[i]),
                     os.O_CREAT | os.O_RDWR))  #| os.O_BINARY | os.O_NOATIME ))

    for i in range(len(file_ids)):

        #
        # Create random size file
        #
        rand_size = random_file_sizes[i]
        os.lseek(files[i], 0, os.SEEK_SET)

        #
        # Rewrite random device to the output file in 'blocksize' blocks
        #
        file_written_bytes = 0
        while file_written_bytes + blocksize < rand_size:
            block_written_bytes = try_write(files[i], randdata)
            file_written_bytes += block_written_bytes
            total_written_bytes += block_written_bytes
            throughput = total_written_bytes / (time.time() - start_time)

            #
            # Format progress message
            #
            create_current_throughput = humanize.naturalsize(throughput) \
                                         + "/s"

            thread_progress_messages[task_id] = \
                format_progress_message("Task #" + str(task_id),
                                        total_written_bytes,
                                        total_size_to_write,
                                        create_current_throughput,
                                        width=40, numtype='filesize')

            increase_counters(cbytes_processed, block_written_bytes, \
                              cblock_size, block_written_bytes, \
                              cthroughput, throughput)

        #
        # Write remainder of the file
        #
        block_written_bytes = \
                    try_write(files[i],randdata[0:rand_size - file_written_bytes])
        total_written_bytes += block_written_bytes

        increase_counters(cbytes_processed, block_written_bytes, \
                            cblock_size, block_written_bytes, \
                            cthroughput, throughput)

        #
        # Truncate if configured for consecutive write benchmarks
        #
        if options.truncate:
            os.truncate(files[i], 0)

    end_time = time.time() - start_time

    throughput = total_written_bytes / (time.time() - start_time)
    current_throughput = humanize.naturalsize(throughput) \
                         + "/s"

    thread_progress_messages[task_id] = \
        format_progress_message("Task #" + str(task_id),
                                total_written_bytes,
                                total_size_to_write,
                                current_throughput,
                                width=40, numtype='filesize')

    increase_counters(cbytes_processed, block_written_bytes, \
                      cblock_size, block_written_bytes, \
                      cthroughput, throughput)

    thread_results[task_id] = (total_written_bytes, end_time)

def file_linear_write_benchmark(task_id, task_name, file_ids, filesize, deviation, \
                         blocksize, test_data_dir, thread_results, \
                         thread_progress_messages, start_barrier, \
                         cbytes_processed, cblock_size, cthroughput, \
                         repeat):
    """
    Benchmark testing writing to existing files
    """

    total_written_bytes = 0

    #
    # Generate random file sizes and calculate total size for this task
    # \todo fix when files have random sizes
    random_file_sizes = \
                  [get_random_file_size(filesize, deviation) for i in file_ids]
    total_size_to_write = sum(random_file_sizes)

    thread_progress_messages[task_id] = \
        format_progress_message("Task #" + str(task_id),
                                total_written_bytes,
                                total_size_to_write,
                                "???",
                                width=40, numtype='filesize')

    randdata = get_random_data(blocksize)

    start_barrier.wait()
    start_time = time.time()

    files = []
    for i in range(len(file_ids)):
        files.append(
            try_open(test_data_dir + "/" + str(file_ids[i]), os.O_RDWR))

    with cbytes_processed.get_lock():
        cbytes_processed.value = 0
    with cthroughput.get_lock():
        cthroughput.value = 0

    for average_loop in range(repeat):
        total_written_bytes = 0
        print("Starting {} loop, iteration {}" \
              .format(task_name,average_loop), file=sys.stderr)
        for i in range(len(file_ids)):
            #
            # Create random size file
            #
            rand_size = random_file_sizes[i]
            os.lseek(files[i], 0, os.SEEK_SET)

            #
            # Rewrite random device to the output file in 'blocksize' blocks
            #
            file_written_bytes = 0
            while file_written_bytes + blocksize < rand_size:
                block_written_bytes = try_write(files[i], randdata)
                file_written_bytes += block_written_bytes
                total_written_bytes += block_written_bytes
                throughput = total_written_bytes / (time.time() - start_time)

                #
                # Format progress message
                #
                create_current_throughput = humanize.naturalsize(throughput) \
                                            + "/s"

                thread_progress_messages[task_id] = \
                    format_progress_message("Task #" + str(task_id),
                                            total_written_bytes,
                                            total_size_to_write,
                                            create_current_throughput,
                                            width=40, numtype='filesize')

                increase_counters(cbytes_processed, block_written_bytes, \
                                  cblock_size, block_written_bytes, \
                                  cthroughput, throughput)

            #
            # Write remainder of the file
            #
            block_written_bytes = \
                        try_write(files[i],randdata[0:rand_size - file_written_bytes])
            total_written_bytes += block_written_bytes

            increase_counters(cbytes_processed, block_written_bytes, \
                              cblock_size, block_written_bytes, \
                              cthroughput, throughput)

    end_time = time.time() - start_time

    throughput = total_written_bytes / (time.time() - start_time)
    current_throughput = humanize.naturalsize(throughput) \
                         + "/s"

    thread_progress_messages[task_id] = \
        format_progress_message("Task #" + str(task_id),
                                total_written_bytes,
                                total_size_to_write,
                                current_throughput,
                                width=40, numtype='filesize')

    thread_results[task_id] = (total_written_bytes, end_time)

def file_random_write_benchmark(task_id, task_name, file_ids, filesize, deviation, \
                                blocksize, test_data_dir, thread_results, \
                                thread_progress_messages, start_barrier, \
                                cbytes_processed, cblock_size, cthroughput, \
                                repeat):
    """
    Benchmark testing writing to existing files
    """

    _measurment = "random_write"

    total_written_bytes = 0

    #
    # Generate random file sizes and calculate total size for this task
    # \todo fix when files have random sizes
    random_file_sizes = \
                  [get_random_file_size(filesize, deviation) for i in file_ids]
    total_size_to_write = sum(random_file_sizes)

    thread_progress_messages[task_id] = \
        format_progress_message("Task #" + str(task_id),
                                total_written_bytes,
                                total_size_to_write,
                                "???",
                                width=40, numtype='filesize')

    randdata = get_random_data(blocksize)

    start_barrier.wait()
    start_time = time.time()

    files = []
    for i in range(len(file_ids)):
        files.append(
            try_open(test_data_dir + "/" + str(file_ids[i]), os.O_RDWR))

    for _ in range(repeat):
        for i in range(len(file_ids)):
            #
            # Create random size file
            #
            rand_size = random_file_sizes[i]

            #
            # Prepare a shuffled list of block indexes to write in random order
            #
            random_block_indexes = [
                i for i in range(0, int(rand_size / blocksize))
            ]
            random.shuffle(random_block_indexes)

            file_written_bytes = 0
            for block_index in random_block_indexes:
                os.lseek(files[i], block_index * blocksize, os.SEEK_SET)
                block_written_bytes = try_write(files[i], randdata)
                file_written_bytes += block_written_bytes
                total_written_bytes += block_written_bytes
                throughput = total_written_bytes / (time.time() - start_time)

                #
                # Format progress message
                #
                create_current_throughput = humanize.naturalsize(throughput) \
                                            + "/s"

                thread_progress_messages[task_id] = \
                    format_progress_message("Task #" + str(task_id),
                                            total_written_bytes,
                                            total_size_to_write,
                                            create_current_throughput,
                                            width=40, numtype='filesize')

                increase_counters(cbytes_processed, block_written_bytes, \
                                  cblock_size, block_written_bytes, \
                                  cthroughput, throughput)

        #
        # Write remainder of the file
        #
        block_written_bytes = \
                    try_write(files[i], randdata[0:rand_size - file_written_bytes])
        total_written_bytes += block_written_bytes

        increase_counters(cbytes_processed, block_written_bytes, \
                          cblock_size, block_written_bytes, \
                          cthroughput, throughput)

    end_time = time.time() - start_time

    throughput = total_written_bytes / (time.time() - start_time)
    current_throughput = humanize.naturalsize(throughput) \
                         + "/s"

    thread_progress_messages[task_id] = \
        format_progress_message("Task #" + str(task_id),
                                total_written_bytes,
                                total_size_to_write,
                                current_throughput,
                                width=40, numtype='filesize')

    thread_results[task_id] = (total_written_bytes, end_time)

#pylint: disable=unused-argument
def file_linear_read_benchmark(task_id, task_name, file_ids, filesize, deviation, \
                               blocksize, test_data_dir, thread_results, \
                               thread_progress_messages, start_barrier, \
                               cbytes_processed, cblock_size, cthroughput, \
                               repeat):
    """
    Benchmark testing the time of linear reading from files
    """

    total_read_bytes = 0

    #
    # Calculate the size of files to read
    #
    file_sizes = {}
    for f in file_ids:
        file_sizes[f] = os.path.getsize(test_data_dir + "/" + str(f))

    total_size_to_read = sum(file_sizes.values())

    thread_progress_messages[task_id] = \
        format_progress_message("Task #" + str(task_id),
                                total_read_bytes,
                                total_size_to_read,
                                "???",
                                width=40, numtype='filesize')

    outfile = os.open("/dev/null", os.O_RDWR)
    start_barrier.wait()
    start_time = time.time()

    files = []
    for i in range(len(file_ids)):
        files.append(
            try_open(test_data_dir + "/" + str(file_ids[i]), os.O_RDONLY))

    for _ in range(repeat):
        for i in range(len(file_ids)):
            #
            # Read the file in blocks
            #
            file_read_bytes = 0
            os.lseek(files[i], 0, os.SEEK_SET)

            while file_read_bytes + blocksize < file_sizes[file_ids[i]]:
                block_read_bytes = try_write(outfile,
                                             try_read(files[i], blocksize))
                file_read_bytes += block_read_bytes
                total_read_bytes += block_read_bytes
                throughput = total_read_bytes / (time.time() - start_time)

                #
                # Format progress message
                #
                current_throughput = humanize.naturalsize(throughput) \
                                     + "/s"

                thread_progress_messages[task_id] = \
                    format_progress_message("Task #" + str(task_id),
                                            total_read_bytes,
                                            total_size_to_read,
                                            current_throughput,
                                            width=40, numtype='filesize')

                increase_counters(cbytes_processed, block_read_bytes, \
                                  cblock_size, block_read_bytes, \
                                  cthroughput, throughput)

        #
        # Write remainder of the file
        #
        block_read_bytes = \
            try_write(outfile, try_read(files[i],file_sizes[file_ids[i]]-file_read_bytes))
        total_read_bytes += block_read_bytes

        increase_counters(cbytes_processed, block_read_bytes, \
                          cblock_size, block_read_bytes, \
                          cthroughput, throughput)

    os.close(outfile)
    end_time = time.time() - start_time

    throughput = total_read_bytes / (time.time() - start_time)
    current_throughput = humanize.naturalsize(throughput) \
                         + "/s"

    thread_progress_messages[task_id] = \
        format_progress_message("Task #" + str(task_id),
                                total_read_bytes,
                                total_size_to_read,
                                current_throughput,
                                width=40, numtype='filesize')

    thread_results[task_id] = (total_read_bytes, end_time)

#pylint: disable=unused-argument
def file_random_read_benchmark(task_id, task_name, file_ids, filesize, deviation, \
                               blocksize, test_data_dir, thread_results, \
                               thread_progress_messages, start_barrier, \
                               cbytes_processed, cblock_size, cthroughput, \
                               repeat):
    """
    Benchmark measures the time of random read from files using seek
    """
    _measurment = "random_read"

    total_read_bytes = 0

    #
    # Calculate the size of files to read
    #
    file_sizes = {}
    for f in file_ids:
        file_sizes[f] = os.path.getsize(test_data_dir + "/" + str(f))

    total_size_to_read = sum(file_sizes.values())

    thread_progress_messages[task_id] = \
        format_progress_message("Task #" + str(task_id),
                                total_read_bytes,
                                total_size_to_read,
                                "???",
                                width=40, numtype='filesize')

    outfile = os.open("/dev/null", os.O_RDWR)
    start_barrier.wait()
    start_time = time.time()

    files = []
    for i in range(len(file_ids)):
        files.append(
            try_open(test_data_dir + "/" + str(file_ids[i]), os.O_RDONLY))

    for _ in range(repeat):
        for i in range(len(file_ids)):
            #
            # Open file
            #
            infile_size = file_sizes[file_ids[i]]

            #
            # Read the file in blocks
            #
            file_read_bytes = 0

            #
            # Prepare a shuffled list of block indexes to read in random order
            #
            random_block_indexes = [
                i for i in range(0, int(infile_size / blocksize))
            ]
            random.shuffle(random_block_indexes)

            for block_index in random_block_indexes:
                os.lseek(files[i], block_index * blocksize, os.SEEK_SET)
                block_read_bytes = try_write(outfile,
                                             try_read(files[i], blocksize))
                file_read_bytes += block_read_bytes
                total_read_bytes += block_read_bytes
                throughput = total_read_bytes / (time.time() - start_time)

                #
                # Format progress message
                #
                current_throughput = humanize.naturalsize(throughput) \
                                     + "/s"

                thread_progress_messages[task_id] = \
                    format_progress_message("Task #" + str(task_id),
                                            total_read_bytes,
                                            total_size_to_read,
                                            current_throughput,
                                            width=40, numtype='filesize')

                increase_counters(cbytes_processed, block_read_bytes, \
                                  cblock_size, block_read_bytes, \
                                  cthroughput, throughput)

        #
        # Write remainder of the file
        #
        block_read_bytes = \
            try_write(outfile, try_read(files[i],file_sizes[file_ids[i]]-file_read_bytes))
        total_read_bytes += block_read_bytes

        increase_counters(cbytes_processed, block_read_bytes, \
                          cblock_size, block_read_bytes, \
                          cthroughput, throughput)

    os.close(outfile)
    end_time = time.time() - start_time

    throughput = total_read_bytes / (time.time() - start_time)
    current_throughput = humanize.naturalsize(throughput) \
                         + "/s"

    thread_progress_messages[task_id] = \
        format_progress_message("Task #" + str(task_id),
                                total_read_bytes,
                                total_size_to_read,
                                current_throughput,
                                width=40, numtype='filesize')

    thread_results[task_id] = (total_read_bytes, end_time)

if __name__ == '__main__':
    #
    # Parse command line options
    #
    parser = optparse.OptionParser()

    parser.add_option(
        '-f',
        '--filecount',
        action="store",
        dest="filecount",
        type='int',
        help="Number of files to create",
        default=100)

    parser.add_option(
        '-s',
        '--filesize',
        type='string',
        action="store",
        dest="filesize",
        help="""Average created file size. The file sizes will be random
in the range (0.5*filesize, 1.5*filesize].""",
        default=1024 * 1024)

    parser.add_option(
        '-b',
        '--blocksize',
        type='string',
        action="store",
        dest="blocksize",
        help="""Size of data block for random read test.""",
        default=1024)

    parser.add_option(
        '-n',
        '--name',
        action="store",
        dest="name",
        help="""Name of storage which identifies the performed test.
Defaults to hostname.""",
        default=socket.gethostname())

    parser.add_option(
        '-c',
        '--csv',
        action="store_true",
        dest="csv",
        help="Generate CSV output.",
        default=False)

    parser.add_option(
        '-A',
        '--add-columns',
        dest="add_columns",
        help=
        """Add custom columns with values between first (storage name) and the rest of the columns
Eg. -A "TIME=$(date +"%Y%m%d_%H%M%S"),TEST=mytest""",
        default="")

    parser.add_option(
        '-H',
        '--no-header',
        action="store_true",
        dest="skipheader",
        help="Skip CSV header.",
        default=False)

    parser.add_option(
        '-r',
        '--read-only',
        action="store_true",
        dest="readonly",
        help="""This test will only perform read tests.
It assumes that the current folder contains 'naive-bench-data' folder
with test files uniformly numbered in the specified range.""",
        default=False)

    parser.add_option(
        '-w',
        '--write-only',
        action="store_true",
        dest="writeonly",
        help="""This test will only perform write tests.
This option can be used to create data on storage for peforming
remote read tests.""",
        default=False)

    parser.add_option(
        '-k',
        '--keep',
        action="store_true",
        dest="keep",
        help="""Keep the files after running the test.""",
        default=False)

    parser.add_option(
        '-u',
        '--truncate-after-create',
        action="store_true",
        dest="truncate",
        help="""Truncates the created files to 0 size so that
consecutive write benchmarks work with empty files.""",
        default=False)

    parser.add_option(
        '-F',
        '--force',
        action="store_true",
        dest="force",
        help="""Run the test even when the available storage
size is too small.""",
        default=False)

    parser.add_option(
        '-d',
        '--deviation',
        type='float',
        action="store",
        dest="deviation",
        help="""Generate the files with random size in range
((1.0-deviation)*filesize, (1.0+deviation)*filesize].""",
        default=0.0)

    parser.add_option(
        '-t',
        '--thread-count',
        type='int',
        action="store",
        dest="threadcount",
        help="""Number of threads to execute for each test.""",
        default=4)

    parser.add_option(
        '-P',
        '--no-purge',
        action="store_true",
        dest="nopurge",
        help="""If specified, disables cache clearing between steps.""",
        default=False)

    parser.add_option(
        '-R',
        '--repeat',
        type='int',
        action="store",
        dest="repeat",
        help="""Number of runs of all benchamrks except create""",
        default=1)

    parser.add_option(
        '--graphite-server',
        action="store",
        dest="graphite_server",
        help="""Graphite server address""",
        default=None)

    parser.add_option(
        '--graphite-port',
        action="store",
        dest="graphite_port",
        help="""Graphite carbon port""",
        default=2003)

    parser.add_option(
        '--graphite-prefix',
        action="store",
        dest="graphite_prefix",
        help="""Graphite host""",
        default="")

    parser.add_option(
        '--graphite-reporting-interval',
        action="store",
        dest="graphite_reporting_interval",
        help="""graphite_reporting_interval""",
        default=5)

    #
    # Parse the command line
    #
    options, arguments = parser.parse_args()

    filesize = parse_file_size(options.filesize)
    filecount = int(options.filecount)
    blocksize = parse_file_size(options.blocksize)
    deviation = options.deviation
    threadcount = options.threadcount
    dropcaches = not options.nopurge
    repeat = options.repeat
    add_columns = list(
        map(lambda x: x.split('='), options.add_columns.split(',')))

    graphite_server = options.graphite_server
    graphite_port = options.graphite_port
    graphite_prefix = options.graphite_prefix
    graphite_reporting_interval = int(options.graphite_reporting_interval)

    if math.isnan(filesize):
        print("Invalid filesize - exiting.", file=sys.stderr)
        sys.exit(2)
    else:
        filesize = int(filesize)

    if math.isnan(blocksize):
        print("Invalid blocksize - exiting.", file=sys.stderr)
        sys.exit(2)
    else:
        blocksize = int(blocksize)

    if blocksize > filesize:
        print("Blocksize must not be larger than filesize - exiting.", \
              file=sys.stderr)
        sys.exit(2)

    if deviation < 0.0 or deviation > 0.9:
        print("Deviation must be in range [0.0, 0.9] - exiting.", \
              file=sys.stderr)
        sys.exit(2)

    if threadcount > filecount or filecount % threadcount != 0:
        print("Total file count must be a multiple of thread count - exiting.", \
              file=sys.stderr)
        sys.exit(2)

    #
    # Calculate available disk space on the current volume
    #
    st = os.statvfs(os.getcwd())
    available_disk_space = st.f_bavail * st.f_frsize

    #
    # Printout basic benchmark parameters
    #
    print("------------------------------", file=sys.stderr)
    print('Starting test', file=sys.stderr)
    print("  Number of files: ", filecount, file=sys.stderr)
    print('  Average file size: ', humanize.naturalsize(filesize), \
          file=sys.stderr)
    print('  Maximum disk space needed: ', \
          humanize.naturalsize(filesize * filecount * (1.0+deviation)),
          file=sys.stderr)
    print('  Available disk space: ', \
          humanize.naturalsize(available_disk_space), file=sys.stderr)
    print('  Number of parallel threads:', \
          threadcount, file=sys.stderr)
    print('------------------------------', file=sys.stderr)

    #
    # Check available disk space for test
    #
    if (filesize * filecount * (1.0+deviation)) > available_disk_space \
                                                 and not options.force:
        print("Not enough disk space to perform test - exiting.", \
              file=sys.stderr)
        sys.exit(1)

    #
    # Check conflicting options
    #
    if options.readonly and options.writeonly:
        print(
            "Cannot perform readonly and writeonly test - exiting.",
            file=sys.stderr)
        sys.exit(2)

    if options.filecount < 1:
        print("Cannot perform test with no files - exiting.", file=sys.stderr)
        sys.exit(2)

    #
    # Initialize time variables
    #
    create_files_time = float('NaN')
    create_files_bytes_size = 0
    overwrite_files_time = float('NaN')
    overwrite_files_bytes_size = 0
    linear_read_time = float('NaN')
    linear_read_bytes_size = 0
    random_read_time = float('NaN')
    random_read_bytes_size = 0
    delete_time = float('NaN')

    print("\n\nCreating test folder 'naive-bench-data'...", end="", \
           file=sys.stderr)
    #
    # Cleanup old test data
    #
    os.system("rm -rf naive-bench-data")
    starttime = time.time()
    os.system("mkdir naive-bench-data")
    endtime = time.time() - starttime
    print("DONE [%d s]\n" % (endtime), file=sys.stderr)

    if dropcaches:
        print("\n--- DROPPING FILE CACHE...", end="", file=sys.stderr)
        drop_caches()
        print(" DONE", file=sys.stderr)

    if graphite_server is not None:
        graphiteSender = Process(target=graphite_sender, \
                                args=(graphite_server, \
                                    graphite_port))
        graphiteSender.start()

        refcounterSender = Process(target=reference_counter_sender)
        refcounterSender.start()

    ##########
    #
    # Start file creation benchmark
    #
    #

    global_start_time = time.time()

    if graphite_server is not None:
        send_data_to_graphite("measurment", "results",\
                                prefix=graphite_prefix, \
                                global_start_time=global_start_time, \
                                blocksize=blocksize, \
                                filecount=filecount, \
                                filesize=filesize, \
                                threadcount=threadcount, \
                                iterations=repeat)

    threads = []
    threads_results = PROCESS_MANAGER.dict()
    threads_progress_messages = PROCESS_MANAGER.dict()
    print("\n--- INITIALIZING FILE CREATION BENCHMARK...\n", file=sys.stderr)

    file_create_benchmark_name = "create"
    create_files_time = run_benchmark(file_create_benchmark, \
                                        file_create_benchmark_name, \
                                        filecount, threadcount, deviation, \
                                        blocksize, threads_results, \
                                        threads_progress_messages, repeat)

    #
    # Calculate total benchmark size and time
    #
    create_files_bytes_size = sum(s[0] for s in threads_results.values())

    print("", file=sys.stderr)
    print("--- CREATED " + str(filecount) + " FILES OF TOTAL SIZE " \
        + str(humanize.naturalsize(create_files_bytes_size)) + " IN " \
        + str(create_files_time) + "s", file=sys.stderr)
    create_files_throughput = create_files_bytes_size / create_files_time
    print("--- THROUGHPUT: " \
        + str(humanize.naturalsize(create_files_throughput))\
        + "/s", file=sys.stderr)
    print("", file=sys.stderr)

    if dropcaches:
        print("\n--- DROPPING FILE CACHE...", end="", file=sys.stderr)
        drop_caches()
        print(" DONE", file=sys.stderr)

    ##########
    #
    # Start linear read benchmark
    #
    #

    if not options.readonly:
        ##########
        #
        # Start file linear write benchmark
        #
        #

        threads = []
        threads_results = PROCESS_MANAGER.dict()
        threads_progress_messages = PROCESS_MANAGER.dict()
        print(
            "\n--- INITIALIZING FILE LINEAR WRITE BENCHMARK...\n",
            file=sys.stderr)

        file_linear_write_benchmark_name = "linear_write"
        overwrite_files_time = run_benchmark(file_linear_write_benchmark, \
                                          file_linear_write_benchmark_name, \
                                          filecount, threadcount, deviation, \
                                          blocksize, threads_results, \
                                          threads_progress_messages, repeat)

        #
        # Calculate total benchmark size and time
        #
        overwrite_files_bytes_size = sum(
            s[0] for s in threads_results.values())

        print("", file=sys.stderr)
        print("--- OVERWRITTEN " + str(filecount) + " FILES WITH TOTAL SIZE" \
            + str(humanize.naturalsize(overwrite_files_bytes_size)) + " IN " \
            + str(overwrite_files_time) + "s", file=sys.stderr)
        overwrite_files_throughput = overwrite_files_bytes_size / overwrite_files_time
        print("--- THROUGHPUT: " \
            + str(humanize.naturalsize(overwrite_files_throughput)) \
            + "/s", file=sys.stderr)
        print("", file=sys.stderr)

        if dropcaches:
            print("\n--- DROPPING FILE CACHE...", end="", file=sys.stderr)
            drop_caches()
            print(" DONE", file=sys.stderr)

        #########
        #
        # Start file random write benchmark
        #
        #

        threads = []
        threads_results = PROCESS_MANAGER.dict()
        threads_progress_messages = PROCESS_MANAGER.dict()
        print(
            "\n--- INITIALIZING FILE RANDOM WRITE BENCHMARK...\n",
            file=sys.stderr)

        file_linear_write_benchmark_name = "random_write"
        randomwrite_files_time = run_benchmark(file_random_write_benchmark, \
                                                file_linear_write_benchmark_name, \
                                               filecount, threadcount, deviation, \
                                               blocksize, threads_results, \
                                               threads_progress_messages, repeat)

        #
        # Calculate total benchmark size and time
        #
        randomwrite_files_bytes_size = sum(
            s[0] for s in threads_results.values())

        print("", file=sys.stderr)
        print("--- WRITTE " + str(filecount) + " FILES WITH TOTAL SIZE" \
            + str(humanize.naturalsize(randomwrite_files_bytes_size)) + " IN " \
            + str(randomwrite_files_time) + "s", file=sys.stderr)
        randomwrite_files_throughput = randomwrite_files_bytes_size / randomwrite_files_time
        print("--- THROUGHPUT: " \
            + str(humanize.naturalsize(randomwrite_files_throughput)) \
            + "/s", file=sys.stderr)
        print("", file=sys.stderr)

        if dropcaches:
            print("\n--- DROPPING FILE CACHE...", end="", file=sys.stderr)
            drop_caches()
            print(" DONE", file=sys.stderr)

    ##########
    #
    # Start random read benchmark
    #
    #

    if not options.writeonly:
        threads = []
        threads_results = PROCESS_MANAGER.dict()
        threads_progress_messages = PROCESS_MANAGER.dict()
        print(
            "\n--- INITIALIZING FILE LINEAR READ BENCHMARK...\n",
            file=sys.stderr)

        file_linear_read_benchmark_name = "linear_read"
        linear_read_time = run_benchmark(file_linear_read_benchmark, \
                                         file_linear_read_benchmark_name, \
                                         filecount, threadcount, deviation, \
                                         blocksize, threads_results, \
                                         threads_progress_messages, repeat)

        #
        # Calculate total benchmark size and time
        #
        linear_read_bytes_size = sum(s[0] for s in threads_results.values())

        print("", file=sys.stderr)
        print("--- READ " + str(filecount) + " FILES WITH TOTAL SIZE " \
            + str(humanize.naturalsize(linear_read_bytes_size)) + " IN " \
            + str(linear_read_time) + "s", file=sys.stderr)
        linear_read_throughput = linear_read_bytes_size / linear_read_time
        print("--- THROUGHPUT: " \
            + str(humanize.naturalsize(linear_read_throughput)) \
            + "/s", file=sys.stderr)
        print("", file=sys.stderr)

        if dropcaches:
            print("\n--- DROPPING FILE CACHE...", end="", file=sys.stderr)
            drop_caches()
            print(" DONE", file=sys.stderr)

        threads = []
        threads_results = PROCESS_MANAGER.dict()
        threads_progress_messages = PROCESS_MANAGER.dict()
        print(
            "\n--- INITIALIZING FILE RANDOM READ BENCHMARK...\n",
            file=sys.stderr)

        file_random_read_benchmark_name = "random_read"
        random_read_time = run_benchmark(file_random_read_benchmark, \
                                         file_random_read_benchmark_name, \
                                         filecount, threadcount, deviation, \
                                         blocksize, threads_results, \
                                         threads_progress_messages, repeat)

        #
        # Calculate total benchmark size and time
        #
        random_read_bytes_size = sum(s[0] for s in threads_results.values())

        print("", file=sys.stderr)
        print("--- READ " + str(filecount) + " FILES WITH TOTAL SIZE " \
            + str(humanize.naturalsize(random_read_bytes_size)) + " IN " \
            + str(random_read_time) + "s", file=sys.stderr)
        random_read_throughput = random_read_bytes_size / random_read_time
        print("--- THROUGHPUT: " \
            + str(humanize.naturalsize(random_read_bytes_size/random_read_time)) \
            + "/s", file=sys.stderr)
        print("", file=sys.stderr)

        if dropcaches:
            print("\n--- DROPPING FILE CACHE...", end="", file=sys.stderr)
            drop_caches()
            print(" DONE", file=sys.stderr)

    #
    # Delete the entire test folder
    #
    if not options.keep:
        print("\n--- CLEANING UP...", end="", file=sys.stderr)
        starttime = time.time()
        os.system("rm -rf naive-bench-data")
        delete_time = time.time() - starttime
        print("DONE [%d s]" % (delete_time), file=sys.stderr)

    global_end_time = time.time()

    print(file=sys.stderr)
    print(file=sys.stderr)

    total_time = global_end_time - global_start_time

    BENCHMARK_ACTIVE.value = False

    #
    # Print CSV on stdout
    #
    if options.csv:
        if not options.skipheader:
            if options.add_columns != "":
                print(';'.join(map(lambda x: x[0], add_columns)) + ';', end='')

            print(STORAGE_NAME_LABEL + ";" \
                  + TOTAL_TIME_LABEL + ";" \
                  + GLOBAL_START_TIME_LABEL + ";" \
                  + GLOBAL_END_TIME_LABEL + ";" \
                  + NUMBER_FILES_LABEL + ";" \
                  + AVERAGE_FILE_SIZE_LABEL + ";" \
                  + CREATE_FILES_LABEL + ";" \
                  + CREATE_FILES_SIZE_LABEL + ";" \
                  + CREATE_FILES_THROUGHPUT_LABEL + ";" \
                  + OVERWRITE_FILES_LABEL + ";" \
                  + OVERWRITE_FILES_SIZE_LABEL + ";"\
                  + OVERWRITE_FILES_THROUGHPUT_LABEL + ";"\
                  + RANDOMWRITE_FILES_LABEL + ";" \
                  + RANDOMWRITE_FILES_SIZE_LABEL + ";"\
                  + RANDOMWRITE_FILES_THROUGHPUT_LABEL + ";"\
                  + LINEAR_READ_LABEL + ";" \
                  + LINEAR_READ_SIZE_LABEL + ";"\
                  + LINEAR_READ_THROUGHPUT_LABEL + ";"\
                  + RANDOM_READ_LABEL + ";" \
                  + RANDOM_READ_SIZE_LABEL + ";"\
                  + RANDOM_READ_THROUGHPUT_LABEL + ";"\
                  + DELETE_LABEL)

        if options.add_columns != "":
            print(';'.join(map(lambda x: x[1], add_columns)) + ';', end='')

        print(options.name + ";" \
              + str(total_time) + ';' \
              + str(global_start_time) + ';' \
              + str(global_end_time) + ';' \
              + str(filecount) + ';' \
              + str(filesize) + ';' \
              + str(create_files_time) + ';' \
              + str(create_files_bytes_size) + ';' \
              + str(create_files_throughput) + ';' \
              + str(overwrite_files_time) + ';' \
              + str(overwrite_files_bytes_size) + ';' \
              + str(overwrite_files_throughput) + ';' \
              + str(randomwrite_files_time) + ';' \
              + str(randomwrite_files_bytes_size) + ';' \
              + str(randomwrite_files_throughput) + ';' \
              + str(linear_read_time) + ';' \
              + str(linear_read_bytes_size) + ';' \
              + str(linear_read_throughput) + ';' \
              + str(random_read_time) + ';' \
              + str(random_read_bytes_size) + ';' \
              + str(random_read_throughput) + ';' \
              + str(delete_time))

    if graphite_server is not None:
        send_data_to_graphite("measurment", "results",\
                                prefix=graphite_prefix,
                                total_time=total_time, \
                                global_start_time=global_start_time, \
                                filecount=filecount, \
                                filesize=filesize, \
                                create_files_time=create_files_time, \
                                create_files_bytes_size=create_files_bytes_size, \
                                overwrite_files_time=overwrite_files_time, \
                                overwrite_files_bytes_size=overwrite_files_bytes_size, \
                                randomwrite_files_time=randomwrite_files_time, \
                                randomwrite_files_bytes_size=randomwrite_files_bytes_size, \
                                linear_read_time=linear_read_time, \
                                linear_read_bytes_size=linear_read_bytes_size, \
                                random_read_time=random_read_time, \
                                random_read_bytes_size=random_read_bytes_size, \
                                delete_time=delete_time, \
                                create_files_throughput=create_files_throughput, \
                                overwrite_files_throughput=overwrite_files_throughput, \
                                randomwrite_files_throughput=randomwrite_files_throughput, \
                                linear_read_throughput=linear_read_throughput, \
                                random_read_throughput=random_read_throughput)

        print("Graphite prefix for repored data:", file=sys.stderr)
        print(graphite_prefix, file=sys.stderr)
