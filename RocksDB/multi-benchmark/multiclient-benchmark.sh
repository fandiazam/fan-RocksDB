#!/bin/bash
function ShowTime()
{
    endTime=`date +'%Y-%m-%d %H:%M:%S'`
    start_seconds=$(date --date="$startTime" +%s);
    end_seconds=$(date --date="$endTime" +%s);
    usedTime=$((end_seconds-start_seconds))
    if [[ $usedTime -ge 2 ]]
    then
        echo -e "\033[31mUsed time:$usedTime s \033[0m"
    elif [[ $usedTime -ge 1 ]]
    then
        echo -e "\033[33mUsed time:$usedTime s  \033[0m"
    else
        echo -e "\033[32mUsed time:$usedTime s \033[0m"
    fi
}


startTime=`date +'%Y-%m-%d %H:%M:%S'`
echo "Start time:$startTime"

workloadPort="8000"
dbPort="8001"
DBType="RocksDB" #"SQLite" or "RocksDB"
dataType="strDB" # "binDB" or "strDB"


# echo ${dbPort}
python3 workload_generation.py --workloadPort=${workloadPort} &
python3 server.py --port=${dbPort} &
sleep 5

python3 client.py --workloadPort=$workloadPort --port=$dbPort --$DBType --benchmark-$dataType > log-client1.txt &
python3 client.py --workloadPort=$workloadPort --port=$dbPort --$DBType --benchmark-$dataType > log-client2.txt &
python3 client.py --workloadPort=$workloadPort --port=$dbPort --$DBType --benchmark-$dataType > log-client3.txt &
python3 client.py --workloadPort=$workloadPort --port=$dbPort --$DBType --benchmark-$dataType > log-client4.txt &
python3 client.py --workloadPort=$workloadPort --port=$dbPort --$DBType --benchmark-$dataType > log-client5.txt &
# python3 client.py --workloadPort=$workloadPort --port=$dbPort &
# python3 client.py --workloadPort=$workloadPort --port=$dbPort &
# python3 client.py --workloadPort=$workloadPort --port=$dbPort &
# python3 client.py --workloadPort=$workloadPort --port=$dbPort &
# python3 client.py --workloadPort=$workloadPort --port=$dbPort &


count=0
# echo $(jobs -p)
for pid in $(jobs -p); do
  
  if ((count == 0));then
    port1=$pid
  fi
  if ((count == 1));then
    port2=$pid
  fi
  if ((count++ >= 2)); then
    wait $pid
  fi
done
kill $port1
kill $port2
echo "done"
sleep 1
ShowTime
