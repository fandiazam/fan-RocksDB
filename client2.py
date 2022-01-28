import pyrocksdb
import time
import os
import pandas as pd
import argparse
import struct
import numpy as np
from array import *
from tqdm import tqdm
import struct
import csv
import random


def get_bin_db (db, opts, key):
    # BINARY 
    # get
    opts = pyrocksdb.ReadOptions()
    start_time = time.time()
    blob = db.get(opts, key)
    #print (blob.data)
    
    # convert to float
    offset = 0*36
    embedding = np.asarray(struct.unpack('f'*36, blob.data[4*offset:4*offset+144]), dtype=np.float32) 
    finish_time = time.time()
    timePerRequest = int(round(((finish_time - start_time)*1000000),0)) # in us
    # counter += 1
    #print(embedding)
    #print("===========================")
    return timePerRequest

def get_csv_db(db, opts, key):
    #db = pyrocksdb.DB()
    #opts = pyrocksdb.Options()
    # for multi-thread
    #opts.IncreaseParallelism()
    #opts.OptimizeLevelStyleCompaction()
    #opts.create_if_missing = False
    #db.open(opts, dbpath)  

    # get
    #embedding = [0]
    opts = pyrocksdb.ReadOptions()
    #print ("Value from EV-Table " + splited_key_value[0] + " & key " + key + " :")

    start_time = time.time()
    blob = db.get(opts, key)
    embedding = blob.data
    embedding = (embedding.decode('UTF-8')).split(",")
    embedding = np.asarray(embedding, dtype=np.float32)
    finish_time = time.time()
    timePerRequest = int(round(((finish_time - start_time)*1000000),0)) # in us
    #print (blob.data)
    #x = txt.split()
    #print(embedding[0])
    #print(embedding)
    #print("===========================")
    return timePerRequest

def printOut(n_request, elapsedTime, cdfFilename):
    print("===== output RocksDB Benchmarking - string DB =====")
    print("Total key read (26 EV-Tables) = ", n_request)
    print("Elapsed time = ", float(round(elapsedTime/1000,2)), " ms")
    IOPS = float(round(n_request/(elapsedTime/1000000),2))
    print("IOPS = ", IOPS)
    print("==================================================")
    print("Output latency data :", cdfFilename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PyrocksDB")
    parser.add_argument("--generate-workload", action="store_true", default=False)
    parser.add_argument("--benchmark-binDB", action="store_true", default=False)
    parser.add_argument("--benchmark-strDB", action="store_true", default=False)
    parser.add_argument("--max-request", type=int, default="")
    parser.add_argument("--cdfFilename", type=str, default="")


    args = parser.parse_args()
    # ================CHANGE THESE!!=============== #
    
    csv_ev_path = "/home/cc/fan-rocksdb/csv_workload"
    bin_ev_path = "/home/cc/fan-rocksdb/bin_workload"
    csv_db_path = "/home/cc/fan-rocksdb/csv_db"
    bin_db_path = "/home/cc/fan-rocksdb/bin_db"

    evTable = [4] # Based on the most #Unique key
    lengths_of_evtables = [2202608]
    
    # ============================================== #
    maxRequest = 1000 #default
    if not (args.max_request == ""):    
        maxRequest = args.max_request

    key_value = []
    totalreq = 0

    #for table in evTable:
        #csvFilename = "ev-table-" + str(table) + ".csv"
        #new_ev_path = os.path.join(csv_ev_path, csvFilename)
        #file = open(new_ev_path)
        #reader = csv.reader(file)
        #lines= len(list(reader))
        #totalreq += lines-1
    for idx in range(0, len(evTable)):
        table = evTable[idx]
        lines = lengths_of_evtables[idx]


        #print(lines-1)
        #exit(1)
        #df = pd.read_csv(new_ev_path, dtype=object, delimiter=',').astype(np.float32)
        #print(len(df))
        randKey = random.sample(range(lines), maxRequest)
        for rKey in randKey: #for value in range (0, (lines-1)):
            key_value.append(str(table) + "-" + str(rKey))
        #print(key_value[:10])
        #exit(1)
    np.random.shuffle(key_value)
    #print(key_value[:20])
    #exit(1)
    #print(len(arrKVRequest))
    #print(totalreq)
    
    print("Done randomize ALL request: total = ", totalreq, 'request')
    elapsedTime = 0
    n_request = 0

    print("===== Sending ", maxRequest, 'request to RocksDB')
    TimePerRequest = [0]

    if args.benchmark_strDB:
        cdfFilename = args.cdfFilename
        with open(cdfFilename, 'w') as f:
            # create the csv writer
            writer = csv.writer(f)
            for idx in tqdm(range(0, maxRequest)):
                kv = key_value[idx]
                db = pyrocksdb.DB()
                opts = pyrocksdb.Options()

                # for multi-thread
                opts.IncreaseParallelism()
                opts.OptimizeLevelStyleCompaction()
                opts.create_if_missing = True

                splited_key_value = kv.split("-")
                #print(splited_key_value)
                dbFilename = "str-ev-table-" + splited_key_value[0] + ".db"
                dbFilename = os.path.join(csv_db_path, dbFilename)
                #print(dbFilename)
                key = splited_key_value[1]
                # open DB
                s = db.open(opts, dbFilename)
                assert(s.ok())
                n_request += 1
                # write into csv
                TimePerRequest[0] = get_csv_db(db, opts, key)
                writer.writerow((TimePerRequest))

                elapsedTime += TimePerRequest[0]

                db.close()
            printOut(n_request, elapsedTime, cdfFilename)
            

            #exit(1)
            
    if args.benchmark_binDB:
        cdfFilename = args.cdfFilename
        #cdfFilename = "single-binDB.csv"
        with open(cdfFilename, 'w') as f:
            # create the csv writer
            writer = csv.writer(f)
            #for kv in tqdm(key_value):
            for idx in tqdm(range(0, maxRequest)):
                kv = key_value[idx]
                db = pyrocksdb.DB()
                opts = pyrocksdb.Options()

                # for multi-thread
                opts.IncreaseParallelism()
                opts.OptimizeLevelStyleCompaction()
                opts.create_if_missing = True

                splited_key_value = kv.split("-")
                #print(splited_key_value)
                dbFilename = "bin-ev-table-" + splited_key_value[0] + ".db"
                dbFilename = os.path.join(bin_db_path, dbFilename)
                #print(dbFilename)
                key = splited_key_value[1]
                # open DB
                s = db.open(opts, dbFilename)
                assert(s.ok())
                n_request += 1

                TimePerRequest[0] = get_bin_db(db, opts, key)
                writer.writerow((TimePerRequest))

                elapsedTime += TimePerRequest[0]

            db.close()
            printOut(n_request, elapsedTime, cdfFilename)


            #exit(1)



