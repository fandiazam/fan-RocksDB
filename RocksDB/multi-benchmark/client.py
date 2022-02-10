# send request to workload server and receive table indices
# send table indices to db-server and receive ev values
import socket
import time
import pickle
import argparse

parser = argparse.ArgumentParser(description="client")
parser.add_argument("--port", type=int, default=8000)
parser.add_argument("--workloadPort", type=int, default=8001)

parser.add_argument("--benchmark-binDB", action="store_true", default=False)
parser.add_argument("--benchmark-strDB", action="store_true", default=False)
parser.add_argument("--RocksDB", action="store_true", default=False)
parser.add_argument("--SQLite", action="store_true", default=False)

args = parser.parse_args()

def task(key):
    HOST = '127.0.0.1'
    PORT = args.port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        send_key = pickle.dumps(key)
        #print(send_key)
        s.sendall(send_key)
        # time.sleep(0.1)
        data = s.recv(9216)

        data = pickle.loads(data)
        #print("data")
        #print(data)

        s.close()
    return data

HOST = '127.0.0.1'
WLPORT = args.workloadPort
while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            #key=[]
            s.connect((HOST, WLPORT))
            send_key = pickle.dumps("Ready for new key!")
            s.sendall(send_key)
            # time.sleep(0.1)
            data = s.recv(9216)
            if data is None:
                break

            key = pickle.loads(data)
            if key is None:
                break

            if args.SQLite:
                key = key
            elif args.benchmark_binDB and args.RocksDB:
                #if key is not None:
                key.append("binDB")
            elif args.benchmark_strDB and args.RocksDB:
                key.append("strDB")
            s.close()
            print(key)

            task(key)
s.close()
