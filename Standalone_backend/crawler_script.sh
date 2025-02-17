#!/bin/bash
kvsWorkers=8  # number of kvs workers to launch
flameWorkers=8  # number of flame workers to launch

rm -r worker*
rm *.jar

javac -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar -d classes --source-path src src/cis5550/jobs/Crawler.java
sleep 1
jar cf crawler.jar classes/cis5550/jobs/Crawler.class
sleep 1

javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar --source-path src -d bin $(find src -name '*.java')

echo "cd $(pwd); java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Coordinator 8000" > kvscoordinator.sh
chmod +x kvscoordinator.sh
open -a Terminal kvscoordinator.sh

sleep 2

for i in `seq 1 $kvsWorkers`
do
    dir=worker$i
    if [ ! -d $dir ]
    then
        mkdir $dir
    fi
    # create id doc to specified key for worker
    # Determine the key range for this worker
    start_key=$((($i - 1) * 26 / $kvsWorkers))

    # Convert the key range to lowercase alphabetical characters
    start_key=$(printf "\\$(printf '%03o' $((97 + $start_key)))")

    # Create the id file with the start key
    echo $start_key > $dir/id

    echo "cd $(pwd); java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker $((8000+$i)) $dir localhost:8000" > kvsworker$i.sh
    chmod +x kvsworker$i.sh
    open -a Terminal kvsworker$i.sh
done

echo "cd $(pwd); java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Coordinator 9000 localhost:8000" > flamecoordinator.sh
chmod +x flamecoordinator.sh
open -a Terminal flamecoordinator.sh

sleep 2

for i in `seq 1 $flameWorkers`
do
    echo "cd $(pwd); java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Worker $((9000+$i)) localhost:9000" > flameworker$i.sh
    chmod +x flameworker$i.sh
    open -a Terminal flameworker$i.sh
done