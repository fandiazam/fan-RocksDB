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

def csv2dict (file_path):
    new_ev_df = pd.read_csv(new_ev_path, dtype=str, delimiter=' ')
    new_ev_df.insert(0, 'key', range(0, 0 + len(new_ev_df)))
    new_ev_df['key'] = new_ev_df['key'].astype(str)
    ev_dict = dict(new_ev_df.values)
    return ev_dict

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

def write_as_binary(df, filePath):
    # bytearray
    columns = df.columns[:-1]
    #print(columns)
    #val = df["0"].iloc[0]
    #print(val)
    #print(bin(np.float16(-117.0).view('H'))[2:].zfill(16))
    #print(bin(np.float16(val).view('H'))[2:].zfill(16))
    #exit(1)

    with open(filePath, 'wb') as f:
        # per row
        for nrow in range(0, df.shape[0]):
            #print ("df.shape[0]")
            #print(df.shape[0])
            #exit(1)
            # per column [0... 1023]
            for col in columns:
                val = df[col].iloc[nrow]
                f.write(val)
                #print(val)
            #exit(1)
    f.close()
    print("===== output file : " + filePath)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PyrocksDB")
    parser.add_argument("--convert-csv2bin", action="store_true", default=False)
    parser.add_argument("--store-csv2db", action="store_true", default=False) #store string value
    parser.add_argument("--store-bin2db", action="store_true", default=False)
    parser.add_argument("--simple-benchmark-binDB", action="store_true", default=False)
    parser.add_argument("--simple-benchmark-strDB", action="store_true", default=False)

    args = parser.parse_args()

    # ================CHANGE THESE!!=============== #
    csv_ev_path = "/home/cc/fan-rocksdb/csv_workload"
    bin_ev_path = "/home/cc/fan-rocksdb/bin_workload"
    str_db_path = "/home/cc/fan-rocksdb/str_db"
    bin_db_path = "/home/cc/fan-rocksdb/bin_db"

    total_ev_table = 26
    # ============================================== #

    if args.convert_csv2bin: # Takes 30 minutes
        for ev_idx in range(0, total_ev_table):
            csvFilename = "ev-table-" + str(ev_idx + 1) + ".csv"
            new_ev_path = os.path.join(csv_ev_path, csvFilename)

            df = pd.read_csv(new_ev_path, dtype=object, delimiter=',').astype(np.float32)
            if 'key' not in df.columns:
                # No Key column, just use the index
                df["key"] = df.index
            df["key"] = df.key.astype(int)
            #print(df.head())
            dbFilename = "ev-table-" + str(ev_idx + 1) + ".bin" 
            outFile = os.path.join(bin_ev_path, dbFilename)
            write_as_binary(df, outFile)
            

        
    if args.store_csv2db:
        for ev_idx in range(0, total_ev_table):
            csvFilename = "ev-table-" + str(ev_idx + 1) + ".csv"
            new_ev_path = os.path.join(csv_ev_path, csvFilename)
            ev_dict = csv2dict(new_ev_path)

            #print(ev_dict)

            db = pyrocksdb.DB()
            opts = pyrocksdb.Options()
            # for multi-thread
            opts.IncreaseParallelism()
            opts.OptimizeLevelStyleCompaction()
            opts.create_if_missing = True
            dbFilename = "str-ev-table-" + str(ev_idx + 1) + ".db" 
            dbFilename = os.path.join(str_db_path, dbFilename)
            #print(dbFilename)
            s = db.open(opts, dbFilename)
            assert(s.ok())

            # put
            opts = pyrocksdb.WriteOptions()
            #print(ev_idx)
            #for nrow in tqdm(range(0, len(ev_dict))):
            for k, v in ev_dict.items():
                db.put(opts, k, v)
            print("===== output file : " + dbFilename)

            #get_csv_db(db, opts, k)
            
            # get
            #opts = pyrocksdb.ReadOptions()
            #print ("Value from EV-Table " + splited_key_value[0] + " & key " + key + " :")
            #blob = db.get(opts, "0")
            #embedding = blob.data
            #embedding = (embedding.decode('UTF-8')).split(",")
            #embedding = np.asarray(embedding, dtype=np.float32)  
            #print (blob.data)
            #print(embedding)
            #exit(1)
            db.close()
            #get_csv_db(str_db_path, key)

        
    if args.store_bin2db:
        for ev_idx in range(0, 26):
            binFilename = "ev-table-" + str(ev_idx + 1) + ".bin"
            new_ev_path = os.path.join(bin_ev_path, binFilename)
            #ev_dict = csv2dict(new_ev_path)

            #print(ev_dict)

            db = pyrocksdb.DB()
            opts = pyrocksdb.Options()
            # for multi-thread
            opts.IncreaseParallelism()
            opts.OptimizeLevelStyleCompaction()
            opts.create_if_missing = True
            dbFilename = "bin-ev-table-" + str(ev_idx + 1) + ".db" 
            dbFilename = os.path.join(bin_db_path, dbFilename)
            #print(dbFilename)
            s = db.open(opts, dbFilename)
            assert(s.ok())

            # put
            with open(new_ev_path, 'rb') as f:
            #new_ev_df = open(filePath, 'rb')
            #new_ev_arr = new_ev_df.to_numpy()
                #number = list(f.read())
                #print(number)

                data = f.read()
                #print(data)
                #print(data[:8])
                #exit(1)
                num_of_indexes = len(data)//144
                #print(num_of_indexes)

                #embedding = []
                # counter = 0
                opts = pyrocksdb.WriteOptions()
                #for nrow in tqdm(range(0, num_of_indexes)):
                for i in range(0, num_of_indexes):
                    # put
                    offset = i*36 # 36 -> dimension
                    v = data[4*offset:4*offset+144]
                    k = str(i)
                    db.put(opts, k, v)
                db.close()
                print("===== output file : " + dbFilename)
            f.close()
                

    if args.simple_benchmark_binDB or args.simple_benchmark_strDB:
        # This is just sample 
        key_value = ["1-1", "2-3", "2-1", "2-4", "1-6"]
        elapsedTime = 0
        n_request = 0
        if args.simple_benchmark_strDB:
            for kv in key_value:
                db = pyrocksdb.DB()
                opts = pyrocksdb.Options()

                # for multi-thread
                opts.IncreaseParallelism()
                opts.OptimizeLevelStyleCompaction()
                opts.create_if_missing = True

                splited_key_value = kv.split("-")
                #print(splited_key_value)
                dbFilename = "str-ev-table-" + splited_key_value[0] + ".db"
                dbFilename = os.path.join(str_db_path, dbFilename)
                #print(dbFilename)
                key = splited_key_value[1]
                # open DB
                s = db.open(opts, dbFilename)
                assert(s.ok())
                n_request += 1
                elapsedTime += get_csv_db(db, opts, key)

            db.close()
            print("==========CSV===========")
            print("Total key read (26 EV-Tables) = ", n_request)
            print("Elapsed time = ", float(round(elapsedTime/1000,2)), " ms")
            IOPS = float(round(n_request/(elapsedTime/1000000),2))
            print("IOPS = ", IOPS)
            #exit(1)
            
        if args.simple_benchmark_binDB:
            for kv in key_value:
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
                elapsedTime += get_bin_db(db, opts, key)

            db.close()
            print("==========BIN===========")
            print("Total key read (26 EV-Tables) = ", n_request)
            print("Elapsed time = ", float(round(elapsedTime/1000,2)), " ms")
            IOPS = float(round(n_request/(elapsedTime/1000000),2))
            print("IOPS = ", IOPS)
            #exit(1)
