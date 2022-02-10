import socket
import random
import time
import pickle
import argparse

parser = argparse.ArgumentParser(description="client")
parser.add_argument("--workloadPort", type=int, default=8001)
parser.add_argument("--numOfGroupKeys", type=int, default=100000)
args = parser.parse_args()

evtable_list = [3, 4, 12, 16, 21]
lengths_of_evtables = [10131227, 2202608, 8351593, 5461306, 7046547]
numOfIters = args.numOfGroupKeys

# for i in range(numOfIters):
#     print('*', end='')
# print()
pointer = -1
# Generate workload:
workload = []
for i in range(numOfIters):
    for idx, j in enumerate(lengths_of_evtables):
        key = random.randrange(0, j)
        workload.append([evtable_list[idx], key])
# print(workload)

def getWorkload():
    global workload, pointer
    if pointer+1 >= len(workload):
        return None
    pointer += 1
    # if pointer % 5 == 0:
    #     print('*', end='')
    # print("pointer:", pointer)
    return workload[pointer]

HOST = '127.0.0.1'
PORT = args.workloadPort
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.bind((HOST, PORT))
except s.error as e:
    print(str(e))

s.listen()

def threaded_client(c):
    while True:
        data = c.recv(9216)
        if not data:
            break
        # print(data)
        c.sendall(pickle.dumps(getWorkload()))
    c.close()

while True:
    Client, address = s.accept()
    threaded_client(Client)
    # threadCount += 1
    # print(threadCount)
s.close()
