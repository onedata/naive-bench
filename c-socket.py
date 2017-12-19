#!/usr/bin/env python3
import time, sys, socket
from multiprocessing import Pool, freeze_support, Lock, Process, Manager, Queue, Value
import resource

SERVER = "grafana.logs"
PORT = 2003

def collect_metric(name, value, timestamp):
    sock = socket.socket()
    sock.connect( (SERVER, PORT) )
    payload = "%s %d %d\n" % (name, value, timestamp)
    print(payload)
    sock.send(payload.encode())
    sock.close()

def now():
    return int(time.time())

gprefix = ""
def test_send_data(datacenter, cluster_id, host, thred_id, component, submodule, counter):
  # Initiazlize an aiographite instanc
  # Send data
  c = 0
  inc = 1000
  prefix = create_prefix(datacenter, cluster_id, host, thred_id, component, submodule, counter)

  print("Process {} connecting to CarbonReporter".format(thred_id))
  print("Process {} connected.".format(thred_id))

  while True:
    time.sleep(1)
    c+=inc
    collect_metric(prefix,c,now())

  #reporter.report_now()

def create_prefix(*args):
  prefix = ""
  for i in args:
    if str(i) is not "": prefix += "." + str(i).replace(".","_") 
  return prefix[1:]

# vfs-3496-oneclient-krakow_vfs-3496_svc_dev_onedata_uk_to.1231.oneclient.helpers.s3.total_write_time

def main():
  print("in the main")
  datacenter = "k8s-otc"
  cluster_id =  str(sys.argv[1]) # k8s-2-otc-onedata-0002
  host = str(sys.argv[2]) # test-sd3232
  reporter_id = "thread_id"
  component = "test-example"
  submodule = "0"
  counter = "counter_1"
  prefix = create_prefix(datacenter, cluster_id, host)
  print(prefix)


  # prefix = create_prefix(1000, component, submodule, counter)
  # print(prefix)
  # while True:
  #    time.sleep(1)
  #    test_send_data(1, component, submodule, counter)
  #    #reporter.report_now()

  threads = []
  threadcount = 10
  print("Starting loop")
  for thread_id in range(threadcount):
      child = Process(target=test_send_data, \
                      args=[datacenter, cluster_id, host, thread_id, component, submodule, counter],)
      child.start()
      threads.append(child)


if __name__ == '__main__':
  main()