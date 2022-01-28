==========================================================================
                             RocksDB Benchmarking
==========================================================================
0. Correct install pyrocksdb
    pip install tensorflow==1.15.2 # pip install tensorflow==2.1.0
    pip install numpy
    pip install pandas
    pip install scikit-learn
    pip install keras==2.1.3

    # [NEW] Now, install the rest of the dependencies
    pip install future
    pip install numpy
    pip install pandas
    pip install scikit-learn
    pip install onnx
    pip install torchviz
    pip install mpi
    pip install torch
    pip install tqdm

    cd ~
    sudo apt-get install build-essential libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev liblz4-dev
    sudo apt-get update
    sudo apt-get install cmake python3-virtualenv python-dev -y

    git clone https://github.com/twmht/python-rocksdb.git --recursive -b pybind11
    cd python-rocksdb
    python setup.py install

    # Installed /home/fandi/anaconda3/envs/dlrm/lib/python3.7/site-packages/python_rocksdb-1.0-py3.7-linux-x86_64.egg
    # Installed /home/cc/anaconda3/lib/python3.9/site-packages/python_rocksdb-1.0-py3.9-linux-x86_64.egg

    python -c 'import pyrocksdb; print("it works!");'

    ssh cc@192.5.86.194


1. Connect to the server
    ssh daniar@192.5.86.174 #dan-5 server
    cd /home/daniar/ev-table/

2. Convert the EV-table to RocksDB and test simple request
    cd mnt/extra
    mkdir rocksdb
    cd rocksdb
    mkdir {csv_workload,bin_workload,csv_db,bin_db}
    cp /mnt/extra/dlrm/weights_and_biases/epoch-0/ev-table/* /mnt/extra/rocksdb/csv_workload/
    cp /home/cc/ev-tables-sqlite/ev-table-32/* /home/cc/fan-rocksdb/csv_workload

    # Raw EV-table (csv file) path  
    # csv_ev_path = /home/daniar/ev-table/csv_file

    # generated databases path
    # db_ev_path = home/daniar/ev-table/db_file

    # Convert the EV-table to DB 
    # by default this command will access the csv_ev_path and db_ev_path 
    
    cd /home/daniar/ev-table/
    python create_ev_table.py --convert-csv2bin
    python create_ev_table.py --store-csv2db
    python create_ev_table.py --store-bin2db
    python create_ev_table.py --simple-benchmark-binDB
    python create_ev_table.py --simple-benchmark-strDB


    # nohup python create_ev_table.py --store-bin2db &

    bash ./multiclient-benchmark.sh

    # oserror errno 98 address already in use socket
    # kill -9 $(ps -A | grep python | awk '{print $1}')

   
    
    # Get the value (just simple request)
    # by default it will request ["1-1", "2-3", "2-1", "2-4", "1-6"]
    # 2-3; "2" for table and "3" for the key

    python create_ev_table.py --get-value  

3. Single client benchmarking
    
    python singleClientBench.py --benchmark-strDB
    python singleClientBench2.py --benchmark-strDB

    python singleClientBench.py --benchmark-strDB --max-request 100000 --cdfFilename single-strDB.csv > log-single-strDB.txt &
    python singleClientBench.py --benchmark-binDB --max-request 100000 --cdfFilename single-binDB.csv > log-single-binDB.txt &


    python singleClientBench.py --benchmark-strDB --max-request 100000 --cdfFilename dummy.csv


4. Multi client benchmark
    nohup bash ./fan-multiclient-benchmark.sh &

