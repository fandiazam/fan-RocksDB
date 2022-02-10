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
    embedding = struct.unpack('f'*36, blob.data[4*offset:4*offset+144])
    #print (embedding2)
    #embedding = np.asarray(struct.unpack('f'*36, blob.data[4*offset:4*offset+144]), dtype=np.float32) 
    #exit(1)
    finish_time = time.time()
    latency = int(round(((finish_time - start_time)*1000000),0)) # in us
    # counter += 1
    #print(embedding)
    #print("===========================")
    return latency

def get_str_db(db, opts, key):
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
    #print ("Value from EV-Table " + splited_table_key[0] + " & key " + key + " :")
    #print("key =", key)

    start_time = time.time()
    blob = db.get(opts, key)
    embedding = blob.data
    #print("Data =", embedding)
    embedding = (embedding.decode('UTF-8')).split(",")
    #print(embedding)
    embedding = np.asarray(embedding, dtype=np.float32)
    finish_time = time.time()
    latency = int(round(((finish_time - start_time)*1000000),0)) # in us
    #print (blob.data)
    #x = txt.split()
    #print(embedding[0])
    #print(embedding)
    #print("===========================")
    return latency

def printOut(n_request, elapsedTime, latencyFile):
    print("===== output RocksDB Benchmarking - string DB =====")
    print("Total key read (26 EV-Tables) = ", n_request)
    print("Elapsed time = ", float(round(elapsedTime/1000,2)), " ms")
    IOPS = float(round(n_request/(elapsedTime/1000000),2))
    print("IOPS = ", IOPS)
    print("==================================================")
    print("Output latency data :", latencyFile)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PyrocksDB")
    parser.add_argument("--generate-workload", action="store_true", default=False)
    parser.add_argument("--benchmark-binDB", action="store_true", default=False)
    parser.add_argument("--benchmark-strDB", action="store_true", default=False)
    parser.add_argument("--max-request", type=int, default="")
    parser.add_argument("--latencyFile", type=str, default="")


    args = parser.parse_args()
    # ================CHANGE THESE!!=============== #
    
    csv_ev_path = "/home/cc/fan-rocksdb/csv_workload"
    bin_ev_path = "/home/cc/fan-rocksdb/bin_workload"
    str_db_path = "/home/cc/fan-rocksdb/str_db"
    bin_db_path = "/home/cc/fan-rocksdb/bin_db"

    evTable = [3, 4, 12, 16, 21] # Based on the most #Unique key #, 4, 12, 16, 21
    lengths_of_evtables = [10131227, 2202608, 8351593, 5461306, 7046547] #, 2202608, 8351593, 5461306, 7046547
    
    # ============================================== #
    maxRequest = 1000 #default
    if not (args.max_request == ""):    
        maxRequest = args.max_request

    table_key = []
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
            table_key.append(str(table) + "-" + str(rKey))
        #print(table_key[:10])
        #exit(1)
    #np.random.shuffle(table_key)
    #print(table_key[:20])
    #exit(1)
    #print(len(arrKVRequest))
    #print(totalreq)
    
    print("Done randomize ALL request: total = ", maxRequest, 'request')
    elapsedTime = 0
    n_request = 0

    print("===== Sending ", maxRequest, 'request to RocksDB')
    arrOfLatency = []

    db3 = pyrocksdb.DB()
    db4 = pyrocksdb.DB()
    db12 = pyrocksdb.DB()
    db16 = pyrocksdb.DB()
    db21 = pyrocksdb.DB()

    opts = pyrocksdb.Options()

    # for multi-thread
    opts.IncreaseParallelism()
    opts.OptimizeLevelStyleCompaction()
    opts.create_if_missing = False



    if args.benchmark_strDB:
        latencyFile = args.latencyFile

        # open DB
        s3 = db3.open(opts, "/home/cc/fan-rocksdb/str_db/str-ev-table-3.db")
        assert(s3.ok())
        s4 = db4.open(opts, "/home/cc/fan-rocksdb/str_db/str-ev-table-4.db")
        assert(s4.ok())
        s12 = db12.open(opts, "/home/cc/fan-rocksdb/str_db/str-ev-table-12.db")
        assert(s12.ok())
        s16 = db16.open(opts, "/home/cc/fan-rocksdb/str_db/str-ev-table-16.db")
        assert(s16.ok())
        s21 = db21.open(opts, "/home/cc/fan-rocksdb/str_db/str-ev-table-21.db")
        assert(s21.ok())

        with open(latencyFile, 'w', newline='') as f:
            # create the csv writer
            #writer = csv.writer(f)


            for idx in tqdm(range(0, maxRequest)):
                #kv = table_key[idx]
                #db = pyrocksdb.DB()
                
                #db3 = pyrocksdb.DB()
                #db4 = pyrocksdb.DB()
                #db12 = pyrocksdb.DB()
                #db16 = pyrocksdb.DB()
                #db21 = pyrocksdb.DB()

                #opts = pyrocksdb.Options()

                # for multi-thread
                #opts.IncreaseParallelism()
                #opts.OptimizeLevelStyleCompaction()
                #opts.create_if_missing = False

                splited_table_key = table_key[idx].split("-")
                #print(splited_table_key)
                dbFilename = "str-ev-table-" + splited_table_key[0] + ".db"
                dbFilename = os.path.join(str_db_path, dbFilename)
                #print(dbFilename)
                key = splited_table_key[1]
                # open DB
                #s = db.open(opts, dbFilename)
                #assert(s.ok())
                n_request += 1
                # write into csv
                #print("splited_table_key =", splited_table_key[0])
                if splited_table_key[0] == str(3):
                    arrOfLatency.append(get_str_db(db3, opts, key))
                elif splited_table_key[0] == str(4):
                    arrOfLatency.append(get_str_db(db4, opts, key))
                elif splited_table_key[0] == str(12):
                    arrOfLatency.append(get_str_db(db12, opts, key))
                elif splited_table_key[0] == str(16):
                    arrOfLatency.append(get_str_db(db16, opts, key))
                elif splited_table_key[0] == str(21):
                    arrOfLatency.append(get_str_db(db21, opts, key))

                #writer.writerow((arrOfLatency))
                #mywriter = csv.writer(file, delimiter=',')
                

            for Time in arrOfLatency:
                elapsedTime += Time
                f.write(str(Time)+"\n")

            db3.close()
            db4.close()
            db12.close()
            db16.close()
            db21.close()

            printOut(n_request, elapsedTime, latencyFile)
            

            #exit(1)
            
    if args.benchmark_binDB:
        latencyFile = args.latencyFile

        # open DB
        s3 = db3.open(opts, "/home/cc/fan-rocksdb/bin_db/bin-ev-table-3.db")
        assert(s3.ok())
        s4 = db4.open(opts, "/home/cc/fan-rocksdb/bin_db/bin-ev-table-4.db")
        assert(s4.ok())
        s12 = db12.open(opts, "/home/cc/fan-rocksdb/bin_db/bin-ev-table-12.db")
        assert(s12.ok())
        s16 = db16.open(opts, "/home/cc/fan-rocksdb/bin_db/bin-ev-table-16.db")
        assert(s16.ok())
        s21 = db21.open(opts, "/home/cc/fan-rocksdb/bin_db/bin-ev-table-21.db")
        assert(s21.ok())

        #latencyFile = "single-binDB.csv"
        with open(latencyFile, 'w') as f:
            # create the csv writer
            #writer = csv.writer(f)
            #for kv in tqdm(table_key):
            for idx in tqdm(range(0, maxRequest)):
                #kv = table_key[idx]
                # db = pyrocksdb.DB()
                
                #opts = pyrocksdb.Options()

                # for multi-thread
                #opts.IncreaseParallelism()
                #opts.OptimizeLevelStyleCompaction()
                #opts.create_if_missing = True

                splited_table_key = table_key[idx].split("-")
                #print(splited_table_key)
                dbFilename = "bin-ev-table-" + splited_table_key[0] + ".db"
                dbFilename = os.path.join(bin_db_path, dbFilename)
                #print(dbFilename)
                key = splited_table_key[1]
                # open DB
                #s = db.open(opts, dbFilename)
                #assert(s.ok())
                n_request += 1

                if splited_table_key[0] == str(3):
                    arrOfLatency.append(get_bin_db(db3, opts, key))
                elif splited_table_key[0] == str(4):
                    arrOfLatency.append(get_bin_db(db4, opts, key))
                elif splited_table_key[0] == str(12):
                    arrOfLatency.append(get_bin_db(db12, opts, key))
                elif splited_table_key[0] == str(16):
                    arrOfLatency.append(get_bin_db(db16, opts, key))
                elif splited_table_key[0] == str(21):
                    arrOfLatency.append(get_bin_db(db21, opts, key))

            for Time in arrOfLatency:
                elapsedTime += Time
                f.write(str(Time)+"\n")

            db3.close()
            db4.close()
            db12.close()
            db16.close()
            db21.close()

            printOut(n_request, elapsedTime, latencyFile)


            #exit(1)



