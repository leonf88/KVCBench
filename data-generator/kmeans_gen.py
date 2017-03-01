#!/usr/bin/env python

# Kmeans data generator
# Each record is a point which has D dimensions, each dimension has a float value range from [vmin, vmax]
# The data is formated as:
#
#   d11, d12, d13, ...
#   d21, d22, d23, ...
#   ...


import optparse

import os
import math
import random
import multiprocessing

# fileSize = 234 * 1024 * 1024
# fileNum = 40
#
# dimension = 100
# max_value = 80
# min_value = -40

output_file_prefix = "kmeans.data"


def gen_worker(p_id, file_number, file_size, store_path, dim, vmax, vmin):
    print "Work-%d, process %d" % (p_id, file_number)
    if file_number <= 0:
        return
    for k in range(file_number):
        output_file_name = "%s-%d-%d.out" % (output_file_prefix, p_id, k)
        output_file_path = os.path.join(store_path, output_file_name)
        out = open(output_file_path, "w")
        while os.stat(output_file_path).st_size < file_size:
            lineStr = ""
            for j in xrange(dim):
                lineStr = lineStr + " %.4f" % (random.random() * vmin + vmax)
            lineStr = lineStr[1:] + "\n"
            out.write(lineStr)
        print out.name + " complete."


if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-w', '--worker',
                      action="store", dest="worker_size", help="number of worker process",
                      type='int', default=1)
    parser.add_option('-s', '--filesize',
                      action="store", dest="file_size", help="each slice file size in mega-bytes",
                      type='int', default=234)
    parser.add_option('-n', '--filenum',
                      action='store', dest="file_number", help="number of slice files",
                      type='float', default=40.0)
    parser.add_option('-d', '--dimension',
                      action='store', dest='dim', help="dimension for each record",
                      type='int', default=100)
    parser.add_option('-p', '--path',
                      action='store', dest='dest_path', help='destination path',
                      type='string', default='.')
    parser.add_option('--min',
                      action='store', dest='min_value', help="maximum for each dimension",
                      type='float', default=-80.0)
    parser.add_option('--max',
                      action='store', dest='max_value', help="minimum for each dimension",
                      type='float', default=40)

    options, args = parser.parse_args()

    worker_size = options.worker_size
    file_size = options.file_size * 1024 * 1024
    total_file_number = options.file_number
    dim = options.dim
    min_value = options.min_value
    max_value = options.max_value
    dest_path = options.dest_path

    if not os.path.exists(dest_path):
        os.makedirs(dest_path)


    def formatSize(fsize):
        ONE_KB_MAX = 1024.0
        ONE_MB_MAX = 1024.0 * 1024
        ONE_GB_MAX = 1024.0 * 1024 * 1024
        if fsize < ONE_KB_MAX:
            return "%d B" % fsize
        elif fsize < ONE_MB_MAX:
            return "%.2f KB" % (fsize / ONE_KB_MAX)
        elif fsize < ONE_GB_MAX:
            return "%.2f MB" % (fsize / ONE_MB_MAX)
        else:
            return "%.2f GB" % (fsize / ONE_GB_MAX)


    print """
Generator Worker Number: %d
Total data size: %s, total file number: %d, each file size: %s
Destination path: %s
Dimension: %d, min value: %.2f, max value: %.2f
""" % (worker_size,
       formatSize(file_size * total_file_number), total_file_number, formatSize(file_size),
       dest_path,
       dim, min_value, max_value)

    jobs = []
    file_number_per_worker = math.ceil(1.0 * total_file_number / worker_size)
    for pid in xrange(worker_size):
        if total_file_number >= file_number_per_worker:
            fnum = file_number_per_worker
        else:
            fnum = total_file_number
        p = multiprocessing.Process(target=gen_worker, args=(
            pid, int(fnum), file_size, dest_path, dim, max_value, min_value))
        jobs.append(p)
        p.start()
        total_file_number -= file_number_per_worker
