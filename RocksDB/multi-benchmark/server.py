# receiving table indices and return ev values
import socket
import pickle
import time
import os
import pandas as pd
import sqlite3
from _thread import *
import argparse

import pyrocksdb
import struct
import numpy as np
from array import *
from tqdm import tqdm
import struct
import csv


parser = argparse.ArgumentParser(description="server")
parser.add_argument("--port", type=int, default=8000)

args = parser.parse_args()

def getEV(key):
    # print(key)
    tableNum = key[0]
    idx = key[1]
    # print(tableNum)
    # print(idx)
    select_ev = "SELECT * FROM tab{} where rowid={};".format(tableNum, idx)

    # print(select_ev)

    ev = cursor.execute(select_ev).fetchone()

    # print(type(ev))
    # print(ev)
    return ev


def get_bin_db (db, opts, key):
    # BINARY 
    # get
    opts = pyrocksdb.ReadOptions()
    #start_time = time.time()
    blob = db.get(opts, key)
    #print (blob.data)
    #exit(1)
    # convert to float
    offset = 0*36
    ev = struct.unpack('f'*36, blob.data[4*offset:4*offset+144])
    #print (ev)
    #ev = np.asarray(struct.unpack('f'*36, blob.data[4*offset:4*offset+144]), dtype=np.float32) 
    #finish_time = time.time()
    #latency = int(round(((finish_time - start_time)*1000000),0)) # in us
    return ev
#    return latency, ev

def get_str_db(db, opts, key):
    opts = pyrocksdb.ReadOptions()

    #start_time = time.time()
    blob = db.get(opts, key)
    ev = blob.data
    ev = (ev.decode('UTF-8')).split(",")
    ev = np.asarray(ev, dtype=np.float32)
    #finish_time = time.time()
    #latency = int(round(((finish_time - start_time)*1000000),0)) # in us
    return ev
    #return latency, ev

# establish socket:
HOST = '127.0.0.1'
PORT = args.port
threadCount = 0

# load ev-tables from sqlite db:
connection = sqlite3.connect("/home/cc/ev-tables-sqlite/EV32bit.db")
cursor = connection.cursor()

str_db3 = pyrocksdb.DB()
str_db4 = pyrocksdb.DB()
str_db12 = pyrocksdb.DB()
str_db16 = pyrocksdb.DB()
str_db21 = pyrocksdb.DB()

bin_db3 = pyrocksdb.DB()
bin_db4 = pyrocksdb.DB()
bin_db12 = pyrocksdb.DB()
bin_db16 = pyrocksdb.DB()
bin_db21 = pyrocksdb.DB()

opts = pyrocksdb.Options()

# for multi-thread
opts.IncreaseParallelism()
opts.OptimizeLevelStyleCompaction()
opts.create_if_missing = False

# open strDB
str_s3 = str_db3.open(opts, "/home/cc/fan-rocksdb/str_db/str-ev-table-3.db")
assert(str_s3.ok())
str_s4 = str_db4.open(opts, "/home/cc/fan-rocksdb/str_db/str-ev-table-4.db")
assert(str_s4.ok())
str_s12 = str_db12.open(opts, "/home/cc/fan-rocksdb/str_db/str-ev-table-12.db")
assert(str_s12.ok())
str_s16 = str_db16.open(opts, "/home/cc/fan-rocksdb/str_db/str-ev-table-16.db")
assert(str_s16.ok())
str_s21 = str_db21.open(opts, "/home/cc/fan-rocksdb/str_db/str-ev-table-21.db")
assert(str_s21.ok())

# open binDB
bin_s3 = bin_db3.open(opts, "/home/cc/fan-rocksdb/bin_db/bin-ev-table-3.db")
assert(bin_s3.ok())
bin_s4 = bin_db4.open(opts, "/home/cc/fan-rocksdb/bin_db/bin-ev-table-4.db")
assert(bin_s4.ok())
bin_s12 = bin_db12.open(opts, "/home/cc/fan-rocksdb/bin_db/bin-ev-table-12.db")
assert(bin_s12.ok())
bin_s16 = bin_db16.open(opts, "/home/cc/fan-rocksdb/bin_db/bin-ev-table-16.db")
assert(bin_s16.ok())
bin_s21 = bin_db21.open(opts, "/home/cc/fan-rocksdb/bin_db/bin-ev-table-21.db")
assert(bin_s21.ok())


print("Database is connected!")

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.bind((HOST, PORT))
except s.error as e:
    print(str(e))
print("Server is up!")

s.listen()

def threaded_client(c):
    elapsedTime = 0
    n_request = 0
    

    while True:
        data = c.recv(9216)
        if not data:
            break
        key = pickle.loads(data)
        #print(key[0])
        val = []

        if len(key) == 2:
            val.append(getEV(key))
        elif key[2]=="binDB":
            if str(key[0]) == str(3):
                val = get_bin_db(bin_db3, opts, str(key[1]))
                #latency, val = get_bin_db(bin_db3, opts, str(key[1]))

            elif str(key[0]) == str(4):
                val = get_bin_db(bin_db4, opts, str(key[1]))
            elif str(key[0]) == str(12):
                val = get_bin_db(bin_db12, opts, str(key[1]))
            elif str(key[0]) == str(16):
                val = get_bin_db(bin_db16, opts, str(key[1]))
            elif str(key[0]) == str(21):
                val = get_bin_db(bin_db21, opts, str(key[1]))

        elif key[2]=="strDB":
            if str(key[0]) == str(3):
                val = get_str_db(str_db3, opts, str(key[1]))
            elif str(key[0]) == str(4):
                val = get_str_db(str_db4, opts, str(key[1]))
            elif str(key[0]) == str(12):
                val = get_str_db(str_db12, opts, str(key[1]))
            elif str(key[0]) == str(16):
                val = get_str_db(str_db16, opts, str(key[1]))
            elif str(key[0]) == str(21):
                val = get_str_db(str_db21, opts, str(key[1]))

        c.sendall(pickle.dumps(val))
    c.close()
    #return latency


arrOfLatency = []
while True:
    Client, address = s.accept()
    threaded_client(Client)
    #arrOfLatency.append(threaded_client(Client))
    threadCount += 1
    #print(threadCount)
    #print(arrOfLatency)
s.close()
connection.commit()
connection.close()

bin_db3.close()
bin_db4.close()
bin_db12.close()
bin_db16.close()
bin_db21.close()

str_db3.close()
str_db4.close()
str_db12.close()
str_db16.close()
str_db21.close()