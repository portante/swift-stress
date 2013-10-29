#!/usr/bin/env python
#
# functions to interact w/ swiftdev

import os
import base64
import httplib
import urllib2
import httplib2
import pycurl
import cStringIO
import re
from time import sleep

CHUNK_SIZE = 2048

from datetime import datetime
from collections import defaultdict

def get_auth_token(u, p):
  """
  GETs an auth ticket from insight_vendor/vending_service.php

  returns a dictionary of x-auth headers
  """
  vendor_host = 'insight-vendor.caseshare.com'
  vendor_script = '/vending_service.php'

  # set request credentails
  creds = base64.encodestring('%s:%s' % (u, p)).rstrip()
  headers = {'Authorization': 'Basic %s' % creds}

  conn = httplib.HTTPSConnection(vendor_host)
  conn.request('GET', vendor_script, headers=headers)
  resp = conn.getresponse()
  resp_headers = dict(resp.getheaders())

  auth_headers = {}
  for k, v in resp_headers.items():
    if k.startswith('x-auth'):
      auth_headers[k] = v
  conn.close()

  return datetime.now(),auth_headers

def put_bucket(headers, scheme, host, version, volume, bucket):
  bucket_url = '%s://%s/%s/%s/%s' % (scheme, host, version, volume, bucket)

  h = httplib2.Http()
  resp, content = h.request(bucket_url, 'PUT', headers=headers)

  return resp, content

# Class which holds a file reference and the read callback
class FileReader(object):
  def __init__(self, fp):
    self.fp = fp
  def read_callback(self, size):
    if not self.fp:
      return ''
    return self.fp.read(size)
  def reset(self):
    self.fp.seek(0)
  def close(self):
    self.fp.close()
    self.fp = None

# Fake file reader which just generates data on the fly.
class FakeReader(object):
  def __init__(self, size):
    self.orig_size = int(size)
    self.size = self.orig_size
  def read_callback(self, size):
    if self.size <= 0:
      buff = ''
    elif self.size <= size:
      buff = 'x' * self.size
      self.size = 0
    else:
      buff = 'x' * size
      self.size -= size
    return buff
  def reset(self):
    self.size = self.orig_size
  def close(self):
    self.size = 0

# Fake file writer which just adds up the total bytes that would have been written.
class FakeWriter(object):
  def __init__(self):
    self.size = 0
  def write_callback(self, buf):
    self.size += len(buf)

class PycurlResp(object):
  def __init__(self, status, reason, total_time):
    self.status = int(status)
    self.reason = reason
    self.total_time = total_time

class Putter(object):
  def __init__(self, scheme, host, port, version, volume, bucket, idx, elfd):
    self.scheme = scheme
    self.host = host
    self.port = port
    self.version = version
    self.volume = volume
    self.bucket = bucket
    self.idx = idx
    self.elfd = elfd
    self.c = pycurl.Curl()
    self.c.setopt(pycurl.UPLOAD, 1)
    self.c.setopt(pycurl.PUT, 1)
    self.c.setopt(pycurl.USERAGENT, "pycurl-swift_func-%d" % int(idx))

  def close(self):
    try:
      self.c.close()
    except pycurl.error:
      pass

  def put_file(self, headers, uri, srcfile, size, chunk_size=CHUNK_SIZE):
    assert srcfile is not None or size is not None

    url = '%s://%s:%s/%s/%s/%s/%s' % (self.scheme, self.host, self.port, self.version, self.volume, self.bucket, uri)

    self.c.setopt(pycurl.URL, url)
    if srcfile:
      filesize = os.path.getsize(srcfile)
      fr = FileReader(open(srcfile, 'rb'))
    else:
      filesize = size
      fr = FakeReader(size)
    try:
      self.c.setopt(pycurl.READFUNCTION, fr.read_callback)
      int_headers = dict(headers)
      if filesize <= (1024*1024):
        int_headers["Expect"] = ''
      else:
        int_headers["Expect"] = '100-continue'
      headers_list = [ "%s: %s" % (k, v) for k, v in int_headers.items() ]
      self.c.setopt(pycurl.HTTPHEADER, headers_list)
      buf = cStringIO.StringIO()
      self.c.setopt(pycurl.WRITEFUNCTION, buf.write)
      hdr = cStringIO.StringIO()
      self.c.setopt(pycurl.HEADERFUNCTION, hdr.write)
      self.c.setopt(pycurl.INFILESIZE, filesize)
      conn_retry = 0
      rest_retry = 0
      code_cnt = defaultdict(int)
      code, reason, total_time = 0, '', -1
      while code == 0:
        try:
          self.c.perform()
        except pycurl.error as err:
          if err.args[0] == 7:
            if conn_retry > 0 and (conn_retry % 1000) == 0:
              self.elfd.write("Thread-%d: retried connection for url (%s) %d times\n" % (self.idx, url, conn_retry))
              self.elfd.flush()
            sleep(0.001 * (2 ** int(conn_retry / 1000)))
            continue
          else:
            code, reason = 599, repr(err)
        else:
          conn_retry = 0
          code = int(self.c.getinfo(pycurl.RESPONSE_CODE))
          total_time = self.c.getinfo(pycurl.TOTAL_TIME)
          status_line = hdr.getvalue().splitlines()[0]
          m = re.match(r'HTTP\/\S*\s*\d+\s*(.*?)\s*$', status_line)
          if m:
            reason = m.groups(1)
          else:
            reason = ''
        if code in (503,):
          fr.reset()
          code_cnt[code] += 1
          if rest_retry < 5:
            rest_retry += 1
            sleep(1 * (2 ** rest_retry))
            code, reason = 0, ''
          else:
            break
      if rest_retry > 0:
        self.elfd.write("Thread-%d: PUT exiting with %r for url (%s), after retried %d times (%r)\n" % (self.idx, (code, reason), url, rest_retry, code_cnt))
        self.elfd.flush()
      return PycurlResp(code, reason, total_time)
    finally:
      fr.close()


class Getter(object):
  def __init__(self, scheme, host, port, version, volume, bucket, idx, elfd):
    self.scheme = scheme
    self.host = host
    self.port = port
    self.version = version
    self.volume = volume
    self.bucket = bucket
    self.idx = idx
    self.elfd = elfd
    self.c = pycurl.Curl()
    self.c.setopt(pycurl.USERAGENT, "pycurl-swift_func-%d" % int(idx))

  def close(self):
    try:
      self.c.close()
    except pycurl.error:
      pass

  def get_file(self, headers, uri, srcfile, size, chunk_size=CHUNK_SIZE):
    assert srcfile is not None or size is not None

    url = '%s://%s:%s/%s/%s/%s/%s' % (self.scheme, self.host, self.port, self.version, self.volume, self.bucket, uri)

    self.c.setopt(pycurl.URL, url)
    if srcfile:
      filesize = os.path.getsize(srcfile)
    else:
      filesize = size
    int_headers = dict(headers)
    headers_list = [ "%s: %s" % (k, v) for k, v in int_headers.items() ]
    self.c.setopt(pycurl.HTTPHEADER, headers_list)
    fw = FakeWriter()
    self.c.setopt(pycurl.WRITEFUNCTION, fw.write_callback)
    hdr = cStringIO.StringIO()
    self.c.setopt(pycurl.HEADERFUNCTION, hdr.write)
    conn_retry = 0
    rest_retry = 0
    code_cnt = defaultdict(int)
    code, reason, total_time = 0, '', -1
    while code == 0:
      fw.size = 0
      try:
        self.c.perform()
      except pycurl.error as err:
        if err.args[0] == 7:
          if conn_retry > 0 and (conn_retry % 1000) == 0:
            self.elfd.write("Thread-%d: retried connection for url (%s) %d times\n" % (self.idx, url, conn_retry))
            self.elfd.flush()
          sleep(0.001 * (2 ** int(conn_retry / 1000)))
          continue
        else:
          code, reason = 599, repr(err)
      else:
        conn_retry = 0
        code = int(self.c.getinfo(pycurl.RESPONSE_CODE))
        total_time = self.c.getinfo(pycurl.TOTAL_TIME)
        status_line = hdr.getvalue().splitlines()[0]
        m = re.match(r'HTTP\/\S*\s*\d+\s*(.*?)\s*$', status_line)
        if m:
          reason = m.groups(1)
        else:
          reason = ''
      if code == 200:
        if fw.size != filesize:
          self.elfd.write("Thread-%d: expected file size %d, got %d, for %s" % (self.idx, filesize, fw.size, url))
          self.elfd.flush()
        else:
          break
      elif code in (500, 503):
        code_cnt[code] += 1
        if rest_retry < 5:
          rest_retry += 1
          sleep(1 * (2 ** rest_retry))
          code, reason = 0, ''
        else:
          break
      else:
        break
    if rest_retry > 0:
      self.elfd.write("Thread-%d: GET exiting with %r for url (%s), after retried %d times (%r)\n" % (self.idx, (code, reason), url, rest_retry, code_cnt))
    return PycurlResp(code, reason, total_time)


def get_bucket(headers, scheme, host, version, volume, bucket):
  bucket_url = '%s://%s/%s/%s/%s' % (scheme, host, version, volume, bucket)

  h = httplib2.Http()
  resp, content = h.request(bucket_url, 'GET', headers=headers)

  return resp, content

def delete_file(headers, scheme, host, version, volume, bucket, uri=''):

  file_url = '%s://%s/%s/%s/%s%s' % (scheme, host, version, volume, bucket, uri)
  h = httplib2.Http()
  return h.request(file_url, 'DELETE', headers=headers)

def head(headers, scheme, host, version, volume, bucket, uri):

  uri = '/%s/%s/%s%s' % (version, volume, bucket, uri)
  if scheme == 'https':
    h = httplib.HTTPSConnection(host)
  else:
    assert scheme == 'http'
    h = httplib.HTTPConnection(host)
  h.request('HEAD', uri, headers=headers)
  resp = h.getresponse()
  h.close()

  return resp

def get_preauth_file(host, version, uri, localfile):
  file_url = 'https://%s/%s%s' % (host, version, uri)

  try:
    response = urllib2.urlopen(file_url)
  except Exception, ex:
    return ex

  lf = open(localfile, 'w')
  lf.write(response.read())
  lf.close()

  return response.info()

def clean_container(headers, host, version, volume, bucket):
  """
  gets entire contents of a container
  and deletes each object itteratively
  """
  resp, content = get_bucket(headers, host, version, volume, bucket)
  if not resp['status'] == '200':
    print '%s\n%s' % (str(resp), content)
    return

  l_uris = content.split('\n')
  for uri in sorted(l_uris, reverse=True):
    print 'delete: %s' % uri
    r,c = delete_file(headers, host, version, volume, bucket, '/%s' % uri)
    if not r['status'] == '204':
      print r
      print c
      #sys.exit(1)


if __name__ == "__main__":
  pass
