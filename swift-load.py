#!/usr/bin/env python
# Copyright (c) 2012-2013 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import Queue
import threading
import subprocess
import swift_func as gf

from time import time, gmtime, strftime
from datetime import datetime


class ProgressThread(threading.Thread):
  '''
  Emit a progress report with the number of files processed, and the sizes of
  those files processed, for every 10,000 received (can be sent in batches if
  so desired).
  '''

  def __init__(self, out_q, rfd):
    '''
    initialize progress thread providing it with the output queue used by
    worker threads.
    '''
    threading.Thread.__init__(self)
    self.out_q = out_q
    self.rfd = rfd

  def run(self):
    '''
    Read from the output queue, totalling the number of files and their sizes
    processed, emitting reports approximately every 10,000 files.

    We terminate when a packet is posted with a count of zero files.
    '''
    q = self.out_q
    total_files = 0
    prev_total_files = 0
    total_errors = 0
    prev_total_errors = 0
    total_size = 0
    prev_total_size = 0
    start_time = time()
    batch_start_time = 2 * start_time
    batch_end_time = 0
    while True:
      cnt, errs, size, st, et = q.get()
      total_files += cnt
      total_errors += errs
      total_size += size
      if st < batch_start_time:
        batch_start_time = st
      if et > batch_end_time:
        batch_end_time = et
      if cnt == 0 or (total_files - prev_total_files) >= 10000:
        now = time()
        stime = now - start_time
        curr_files = total_files - prev_total_files
        curr_errors = total_errors - prev_total_errors
        curr_size = total_size - prev_total_size
        curr_time = batch_end_time - batch_start_time
        result = "%s - Total(%15d bytes  %8d files  %8d errors  %7.1fs" \
            "  %6.2f MB/s  %7.2f Files/s) - Interval (%15d bytes" \
            "  %6d files  %6d errors  %5.1fs  %6.2f MB/s  %7.2f Files/s)" % (
            strftime('%Y/%m/%d/%H/%M/%S', gmtime()),
            total_size, total_files, total_errors, stime,
            ((total_size/(1024*1024))/stime), total_files/stime, curr_size,
            curr_files, curr_errors, curr_time,
            ((curr_size/(1024*1024))/curr_time), curr_files/curr_time)
        self.rfd.write(result + "\n")
        self.rfd.flush()
        print result
        sys.stdout.flush()
        prev_total_files = total_files
        prev_total_size = total_size
        batch_start_time = 2 * now
        batch_end_time = 0
      if cnt == 0:
        break


def write_error(key, elfd, uri, resp):
  elfd.write('%s: %s %s %s %s\n' % (key, str(time()), str(resp.status), resp.reason, uri))
  elfd.flush()


class SimpleUploader(threading.Thread):
  '''
  Upload thread which simply reads from a queue where a file size and URI have
  been previously loaded. The thread will exit when the queue becomes empty.
  '''

  def __init__(self, use_auth, q, out_q, host, port, version, volume, bucket,
               chunk_size, size_multiplier, idx, elfd, pfd):
    '''
    initialize uploader thread
    '''
    threading.Thread.__init__(self)
    if use_auth:
      self.tickettime, self.headers = gf.get_auth_token('user', 'pass')
      scheme = 'https'
    else:
      self.tickettime, self.headers = None, {}
      scheme = 'http'
    self.q = q
    self.out_q = out_q
    self.chunk_size = chunk_size
    self.size_multiplier = size_multiplier
    self.idx = idx
    self.elfd = elfd
    self.pfd = pfd
    self.putter = gf.Putter(scheme, host, port, version, volume, bucket, idx, elfd)
    self.curr_upload = (0, '')

  def run(self):
    '''
    run a single uploader thread
    each thread will continue to send new put requests
    as long as there is work in q
    '''
    try:
      self.pfd.write("%s (th%2d): started\n" % (strftime('%Y/%m/%d/%H/%M/%S', gmtime()), self.idx))
      self.pfd.flush()

      q = self.q
      total_size = 0
      total_files = 0
      error_files = 0
      total_time = 0
      start_time = time()
      while True:
        try:
          self.curr_upload = q.get_nowait()
          size, uri = self.curr_upload
          size *= self.size_multiplier
          try:
            status, tt = self.upload(uri, size)
            total_files += 1
            if status:
              total_size += size
              total_time += tt
            else:
              error_files += 1
          finally:
            # Always be sure task_done is called to prevent main loop from stalling
            assert self.curr_upload != (0, ''), "unexpected value for curr_upload: curr_upload=%r size=%r uri=%r" % (self.curr_upload, size, uri)
            self.curr_upload = (0, '')
            q.task_done()
        except Queue.Empty as e:
          break
      end_time = time()
      self.out_q.put((total_files, error_files, total_size, start_time, end_time))
      self.pfd.write("%s (th%2d): %15d bytes  %6d files  %6d errors  %5.1fs" \
                     "  %6.2f MB/s  %7.2f Files/s\n" % (
                     strftime('%Y/%m/%d/%H/%M/%S', gmtime()), self.idx,
                     total_size, total_files, error_files, total_time,
                     ((total_size/(1024*1024))/total_time), total_files/total_time))
      self.pfd.flush()
    except Exception as e:
      self.elfd.write("%s (th%2d): %r\n" % (strftime('%Y/%m/%d/%H/%M/%S', gmtime()), self.idx, e))
      self.elfd.flush()
    finally:
      self.putter.close()

  def upload(self, uri, size):
    '''
    use swift_func.put_file function to send a put request
    to the swift-api. Returns True on success, False on failure.
    '''
    if self.tickettime is not None and (datetime.now() - self.tickettime).seconds > 600:
      self.tickettime, self.headers = gf.get_auth_token('user', 'pass')

    resp = self.putter.put_file(self.headers, uri, None, size, self.chunk_size)
    if resp.status == 201:
      status = True
    else:
      write_error('PUT', self.elfd, uri, resp)
      status = False
    return status, resp.total_time


class SimpleDownloader(threading.Thread):
  '''
  Download thread which simply reads from a queue where a file size and URI have
  been previously loaded. The thread will exit when the queue becomes empty.
  '''

  def __init__(self, use_auth, q, out_q, host, port, version, volume, bucket,
               chunk_size, size_multiplier, idx, elfd, pfd):
    '''
    initialize uploader thread
    '''
    threading.Thread.__init__(self)
    if use_auth:
      self.tickettime, self.headers = gf.get_auth_token('user', 'pass')
      scheme = 'https'
    else:
      self.tickettime, self.headers = None, {}
      scheme = 'http'
    self.q = q
    self.out_q = out_q
    self.chunk_size = chunk_size
    self.size_multiplier = size_multiplier
    self.idx = idx
    self.elfd = elfd
    self.pfd = pfd
    self.getter = gf.Getter(scheme, host, port, version, volume, bucket, idx, elfd)
    self.curr_download = (0, '')

  def run(self):
    '''
    run a single downloader thread
    each thread will continue to send new get requests
    as long as there is work in q
    '''
    try:
      self.pfd.write("%s (th%2d): started\n" % (strftime('%Y/%m/%d/%H/%M/%S', gmtime()), self.idx))
      self.pfd.flush()

      q = self.q
      total_size = 0
      total_files = 0
      error_files = 0
      total_time = 0
      start_time = time()
      while True:
        try:
          self.curr_download = q.get_nowait()
          size, uri = self.curr_download
          size *= self.size_multiplier
          try:
            status, tt = self.download(uri, size)
            total_files += 1
            if status:
              total_size += size
              total_time += tt
            else:
              error_files += 1
          finally:
            # Always be sure task_done is called to prevent main loop from stalling
            assert self.curr_download != (0, ''), "unexpected value for curr_download: curr_download=%r size=%r uri=%r" % (self.curr_download, size, uri)
            self.curr_download = (0, '')
            q.task_done()
        except Queue.Empty:
          break
      end_time = time()
      self.out_q.put((total_files, error_files, total_size, start_time, end_time))
      self.pfd.write("%s (th%2d): %15d bytes  %6d files  %6d errors  %5.1fs" \
                     "  %6.2f MB/s  %7.2f Files/s\n" % (
                     strftime('%Y/%m/%d/%H/%M/%S', gmtime()), self.idx,
                     total_size, total_files, error_files, total_time,
                     ((total_size/(1024*1024))/total_time), total_files/total_time))
      self.pfd.flush()
    except Exception as e:
      self.elfd.write("%s (th%2d): %r\n" % (strftime('%Y/%m/%d/%H/%M/%S', gmtime()), self.idx, e))
      self.elfd.flush()
    finally:
      self.getter.close()

  def download(self, uri, size):
    '''
    use swift_func.put_file function to send a put request
    to the swift-api. Returns True on success, False on failure.
    '''
    if self.tickettime is not None and (datetime.now() - self.tickettime).seconds > 600:
      self.tickettime, self.headers = gf.get_auth_token('user', 'pass')

    resp = self.getter.get_file(self.headers, uri, None, size, self.chunk_size)
    if resp.status == 200:
      status = True
    else:
      write_error('GET', self.elfd, uri, resp)
      status = False
    return status, resp.total_time


class PerDirUploader(SimpleUploader):
  '''
  Upload thread which uploads the specified file names in the given directory
  hierarchy.
  '''

  def __init__(self, use_auth, q, out_q, host, port, version, volume, bucket,
               size_multiplier, chunk_size, idx, elfd, pfd):
    '''
    initialize uploader thread
    '''
    super(PerDirUploader, self).__init__(use_auth, q, out_q, host, port,
                                         version, volume, bucket, chunk_size,
                                         size_multiplier, idx, elfd, pfd)
    self.batch_size = 0
    self.batch_cnt = 0
    self.batch_err = 0
    self.batch_start = 0

  def run(self):
    '''
    run a single uploader thread
    each thread will continue to send new put requests
    as long as there is work in q
    '''
    try:
      q = self.q
      while True:
        try:
          dname, files, diridx, dirtotal = q.get_nowait()
          try:
            self.batch_start = time()
            total_files = 0
            error_files = 0
            total_size = 0
            stime = 0
            for size, fname in files:
              uri = os.path.join(dname, fname)
              size *= self.size_multiplier
              status, tt = self.upload(uri, size)
              total_files += 1
              if status:
                total_size += size
                stime += tt
              else:
                error_files += 1
            end_time = time()
            if self.batch_cnt > 0:
              self.out_q.put((self.batch_cnt, self.batch_err, self.batch_size, self.batch_start, end_time))
              self.batch_start = 0
              self.batch_cnt = 0
              self.batch_err = 0
              self.batch_size = 0
            self.pfd.write("%s (th%2d, %5d of %5d): %15d bytes  %6d files" \
                   "  %6d errors  %5.1fs  %6.2f MB/s  %7.2f Files/s" \
                   "  %s\n" % (strftime('%Y/%m/%d/%H/%M/%S', gmtime()),
                   self.idx, diridx, dirtotal, total_size, total_files,
                   error_files, stime, ((total_size/(1024*1024))/stime),
                   total_files/stime, dname))
            self.pfd.flush()
          finally:
            # Always be sure task_done is called to prevent main loop from stalling
            q.task_done()
        except Queue.Empty:
          break
          assert self.batch_cnt == 0
    finally:
      self.putter.close()

  def upload(self, uri, size):
    '''
    use swift_func.put_file function to send a put request
    to the swift-api
    '''
    status, tt = super(PerDirUploader, self).upload(uri, size)

    self.batch_cnt += 1
    if status:
      self.batch_size += size
    else:
      self.batch_err += 1

    if self.batch_cnt >= 333:
      batch_end = time()
      self.out_q.put((self.batch_cnt, self.batch_err, self.batch_size, self.batch_start, batch_end))
      self.batch_start = batch_end
      self.batch_cnt = 0
      self.batch_err = 0
      self.batch_size = 0

    return status, tt


class PerDirDownloader(SimpleDownloader):
  '''
  Download thread which downloads the specified file names in the given directory
  hierarchy.
  '''

  def __init__(self, use_auth, q, out_q, host, port, version, volume, bucket,
               chunk_size, size_multiplier, idx, elfd, pfd):
    '''
    initialize downloader thread
    '''
    super(PerDirDownloader, self).__init__(use_auth, q, out_q, host, port,
                                           version, volume, bucket, chunk_size,
                                           size_multiplier, idx, elfd, pfd)
    self.batch_size = 0
    self.batch_cnt = 0
    self.batch_err = 0
    self.batch_start = 0

  def run(self):
    '''
    run a single downloader thread
    each thread will continue to send new get requests
    as long as there is work in q
    '''
    try:
      q = self.q
      while True:
        try:
          dname, files, diridx, dirtotal = q.get_nowait()
          try:
            self.batch_start = time()
            total_files = 0
            error_files = 0
            total_size = 0
            stime = 0
            for size, fname in files:
              uri = os.path.join(dname, fname)
              size *= self.size_multiplier
              status, tt = self.download(uri, size)
              total_files += 1
              if status:
                total_size += size
                stime += tt
              else:
                error_files += 1
            end_time = time()
            if self.batch_cnt > 0:
              self.out_q.put((self.batch_cnt, self.batch_err, self.batch_size, self.batch_start, end_time))
              self.batch_start = 0
              self.batch_cnt = 0
              self.batch_err = 0
              self.batch_size = 0
            self.pfd.write("%s (th%2d, %5d of %5d): %15d bytes  %6d files" \
                   "  %6d errors  %5.1fs  %6.2f MB/s  %7.2f Files/s" \
                   "  %s\n" % (strftime('%Y/%m/%d/%H/%M/%S', gmtime()),
                   self.idx, diridx, dirtotal, total_size, total_files,
                   error_files, stime, ((total_size/(1024*1024))/stime),
                   total_files/stime, dname))
            self.pfd.flush()
          finally:
            # Always be sure task_done is called to prevent main loop from stalling
            q.task_done()
        except Queue.Empty:
          break
          assert self.batch_cnt == 0
    finally:
      self.putter.close()

  def download(self, uri, size):
    '''
    use swift_func.get_file function to send a get request
    to the swift-api
    '''
    status, tt = super(PerDirDownloader, self).download(uri, size)

    self.batch_cnt += 1
    if status:
      self.batch_size += size
    else:
      self.batch_err += 1

    if self.batch_cnt >= 333:
      batch_end = time()
      self.out_q.put((self.batch_cnt, self.batch_err, self.batch_size, self.batch_start, batch_end))
      self.batch_start = batch_end
      self.batch_cnt = 0
      self.batch_err = 0
      self.batch_size = 0

    return status, tt


def threadPerDir(method, use_auth, hosts, port, version, volume, bucket,
                 uri_prefix, max_threads, chunk_size, size_multiplier,
                 elfd, pfd, rfd, worklist_dir, q, out_q):
  '''
  loop over file lists and create worker threads to upload files, where each
  thread is given its own directory on which to work. The list of directories
  is created first by pouring over all the input files, and then sorting them
  by the directories with the list number of files files first.
  '''
  dirs = {}

  thelist = [ f for f in os.listdir(worklist_dir) if os.path.isfile(os.path.join(worklist_dir, f)) ]
  thelist.sort()
  for afile in thelist:
    pfd.write("Processing %s\n" % afile)
    pfd.flush()

    f = open(os.path.join(worklist_dir,afile), "r")
    lines = f.readlines()
    f.close()

    for line in lines:
      line = line.split('\t', 2)
      size = int(line[0])
      name = os.path.join(uri_prefix, line[1].replace('//', '/').strip())
      fname = os.path.basename(name)
      dname = os.path.dirname(name)
      if dname not in dirs:
        dirs[dname] = [(size, fname),]
      else:
        dirs[dname].append((size, fname))

  # Give the biggest directories first so that as threads finish with a
  # large directory, they can move to the smaller ones, overlapping them
  # with threads are still finishing larger ones.
  #
  # FIXME: When a thread finally has no more work to do, it would be
  # nice to have it buddy up with another thread working on a directory.
  sorted_dirs = sorted(dirs.items(), key=lambda (k,v): len(v), reverse=True)
  total_dirs = len(sorted_dirs)
  idx = 0
  for k, v in sorted_dirs:
    pfd.write("Queueing directory %s (%d files, %d of %d)\n" % (k, len(v), idx, total_dirs))
    pfd.flush()
    q.put((k, v, idx, total_dirs))
    idx += 1

  pfd.write("Starting %d threads\n" % max_threads)
  pfd.flush()

  out_q_th = ProgressThread(out_q, rfd)
  out_q_th.start()

  start_time = time()

  ths = []
  for idx in range(max_threads):
    if method == "PUT":
      th = PerDirUploader(use_auth, q, out_q, hosts[idx % len(hosts)], port, version, volume, bucket, chunk_size, size_multiplier, idx, elfd, pfd)
    else:
      assert method == "GET"
      th = PerDirDownloader(use_auth, q, out_q, hosts[idx % len(hosts)], port, version, volume, bucket, chunk_size, size_multiplier, idx, elfd, pfd)
    ths.append(th)
    th.start()

  pfd.write("Waiting for work queue ... \n")
  pfd.flush()

  q.join()

  pfd.write("Waiting for %d threads ...\n" % max_threads)
  pfd.flush()

  for th in ths:
    th.join()

  pfd.write("Waiting for progress thread ...\n")
  pfd.flush()

  out_q.put((0, 0, 0))
  out_q_th.join()

  # open progress log and write time for previous file
  end_time = time()
  stime = end_time - start_time
  pfd.write("Total Time: %.1fs" % stime)
  pfd.flush()

def stock_queue(lst_paths, worklist_dir, uri_prefix, q):
  '''
  pop the next file off of lst_paths
  and read lines into a queue for uploading
  '''
  path_file = lst_paths.pop()

  #read paths into Queue
  with open(os.path.join(worklist_dir, path_file), 'r') as f:
    total_size = 0
    total_files = 0
    for line in f:
      line = line.split('\t', 1)
      path_els = line[-1].strip().split('/')
      # Change "./docs/" into "insightdemo12/docs/"
      if path_els[0] == '.':
        path_els[0] = 'insightdemo12'
      uri = '%s/%s' % (uri_prefix, '/'.join(path_els))
      size = int(line[0])
      q.put((size, uri))
      total_files += 1
      total_size += size
  return path_file, total_size, total_files

def threadPerFile(method, use_auth, hosts, port, version, volume, bucket,
                  uri_prefix, max_threads, chunk_size, size_multiplier,
                  elfd, pfd, rfd, worklist_dir, q, out_q):
  '''
  Loop over file lists and create worker threads to upload files, where each
  thread just picks the next available file regardless of the directory in
  which it will reside.

  '''
  lst_paths = [ f for f in os.listdir(worklist_dir) if os.path.isfile(os.path.join(worklist_dir, f)) ]
  lst_paths.sort()
  lst_paths.reverse()

  while len(lst_paths) > 0:
    path_file, total_size, total_files = stock_queue(lst_paths, worklist_dir, uri_prefix, q)
    start_time = time()
    pfd.write("Stocked queue with %s\n" % path_file)
    pfd.flush()

    ths = []
    for idx in range(max_threads):
      if method == "PUT":
        th = SimpleUploader(use_auth, q, out_q, hosts[idx % len(hosts)], port,
                            version, volume, bucket, chunk_size, size_multiplier,
                            idx, elfd, pfd)
      else:
        assert method == "GET"
        th = SimpleDownloader(use_auth, q, out_q, hosts[idx % len(hosts)], port,
                              version, volume, bucket, chunk_size, size_multiplier,
                              idx, elfd, pfd)
      ths.append(th)
      th.start()

    pfd.write("Joining %s work queue ...\n" % path_file)
    pfd.flush()

    q.join()

    pfd.write("Joining threads ...\n")
    pfd.flush()

    for th in ths:
      while th.isAlive():
        th.join(30)
        if th.isAlive():
          elfd.write("%s, th %d: %r\n" % (path_file, th.idx, th.curr_upload if method == "PUT" else th.curr_download))
          elfd.flush()
      th.join()

    pfd.write("Processing out_q ...\n")
    pfd.flush()

    start_time = time()
    end_time = 0
    r_total_files = 0
    r_error_files = 0
    r_total_size = 0
    while not out_q.empty():
      tf, ef, ts, st, et = out_q.get()
      r_total_files += tf
      r_error_files += ef
      r_total_size += ts
      if st < start_time:
          start_time = st
      if et > end_time:
          end_time = et

    # open progress log and write time for previous file
    stime = end_time - start_time
    pfd.write('%s\t%s\t%.2f\t%s\t%s\t%s\n' % (path_file, method, stime, r_total_size, r_total_files, r_error_files))
    pfd.flush()
    result = "%s %s: %s %15d bytes  %6d files  %6d errors  %5.1fs  %6.2f MB/s  %7.2f Files/s" % (
        strftime('%Y/%m/%d/%H/%M/%S', gmtime()), path_file, method, r_total_size, r_total_files,
        r_error_files, stime, ((r_total_size/(1024*1024))/stime), r_total_files/stime)
    rfd.write(result + "\n")
    rfd.flush()
    print result
    sys.stdout.flush()


def main(argv):
  '''
  Example command:
    $ ./repono-gluster-uploader.py \
      0 \                                         1/0 (True/False) - use thread-per-dir / thread-per-file upload method
      PUT                                         PUT | GET - http method to use
      0 \                                         1/0 (True/False) - https
      gprfs015-10ge,gprfs016-10ge \               1 or more server nodes
      8080 \                                      Port Swift is listening on
      AUTH_del0 \                                 Swift Account
      58586052B2C246E9E4928080BE25F0391379ADE5 \  Swift Container Name
      load0 \                                     Name of first subdir below cnt
      30 \                                        Number of concurrent clients (threads)
      1 \                                         Size multiplier
      65536                                       Default chunk size (ignored, optional)
  '''
  argc = len(argv)
  thread_per_dir = int(argv[0])
  if thread_per_dir:
    themethod = threadPerDir
  else:
    themethod = threadPerFile
  method = argv[1]
  assert method in ( "PUT", "GET" )
  use_auth = int(argv[2])
  hosts = argv[3].split(',')
  port = argv[4]
  version = 'v1'
  volume = argv[5]
  bucket = argv[6]
  uri_prefix = argv[7]
  max_threads = int(argv[8])
  if argc >= 10:
    size_multiplier = int(argv[9])
  else:
    size_multiplier = 1
  if argc >= 11:
    chunk_size = int(argv[10])
  else:
    chunk_size = 0

  ftemplate = "%s/log/gl-%s-%s-%%s.log" % (os.path.dirname(__file__) or '.', uri_prefix, method)
  errorlog =     os.path.abspath(ftemplate % 'errors')
  elfd = open(errorlog, 'a+')
  progresslog =  os.path.abspath(ftemplate % 'progress')
  pfd = open(progresslog, 'a+')
  resultlog =    os.path.abspath(ftemplate % 'results')
  rfd = open(resultlog, 'a+')
  worklist_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "test-lists"))
  q = Queue.Queue()
  out_q = Queue.Queue()

  pfd.write("++++++++\nBegin Load (themethod=%r, method=%r, use_auth=%r," \
            " hosts=%r, port=%r, volume=%r, bucket=%r, uri_prefix=%r," \
            " max_threads=%r, size_multiplier=%r, chunk_size=%r," \
            " worklist_dir=%r\n" % (themethod, method, use_auth, hosts, port,
            volume, bucket, uri_prefix, max_threads, size_multiplier,
            chunk_size, worklist_dir))
  pfd.flush()

  res = themethod(method, use_auth, hosts, port, version, volume, bucket, uri_prefix, max_threads, chunk_size, size_multiplier, elfd, pfd, rfd, prfd, worklist_dir, q, out_q)

  pfd.write("End Load\n--------\n")
  pfd.flush()
  return res


if __name__ == "__main__":
  main(sys.argv[1:])
