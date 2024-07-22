#!/bin/bash
kvsWorkers=8  # Number of KVS workers to launch
flameWorkers=8  # Number of Flame workers to launch

# Clean up previous run
# rm -rf worker*
rm -f *.jar
rm -rf bin/*
rm -rf classes/*

# Compile Crawler class
javac -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar -d classes --source-path src src/cis5550/jobs/Crawler.java

# Compile Indexer class
javac -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar -d classes --source-path src src/cis5550/jobs/Indexer.java

# Compile PageRank class
javac -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar -d classes --source-path src src/cis5550/jobs/PageRank.java

# Pause to ensure no race conditions in file system
sleep 1

# Package Crawler class into a jar
jar cf crawler.jar -C classes/ .

# Package Indexer class into a jar
jar cf indexer.jar -C classes/ .

# Package PageRank class into a jar
jar cf pagerank.jar -C classes/ .

# Compile all other Java sources
javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar --source-path src -d bin $(find src -type f -name '*.java' ! -path 'src/cis5550/jobs/Searcher.java')

# Launch KVS Coordinator
echo "cd \"$(pwd)\"; java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Coordinator 8000" > kvscoordinator.sh
chmod +x kvscoordinator.sh
open -a Terminal kvscoordinator.sh

# Give the system time to ensure the coordinator is up
sleep 2

# Launch KVS Workers
for i in `seq 1 $kvsWorkers`
do
    dir="worker$i"
    mkdir -p $dir
    mkdir -p $dir/pt-crawl  # Create pt-crawl folder under each worker directory
    mkdir -p $dir/pt-url  # Create pt-url folder under each worker directory
    mkdir -p $dir/pt-index  # Create pt-index folder under each worker directory
    mkdir -p $dir/pt-pageranks  # Create pt-index folder under each worker directory
    mkdir -p $dir/pt-queue-leftover

    # Generate evenly distributed worker ID
    letterIndex=$((($i - 1) * 26 / $kvsWorkers))
    workerId=$(printf "\x$(printf %x $((97 + $letterIndex)))")
    echo $workerId > "$dir/id"



    echo "cd \"$(pwd)\"; java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker $((8000+$i)) $dir localhost:8000" > "kvsworker$i.sh"
    chmod +x "kvsworker$i.sh"
    open -a Terminal "kvsworker$i.sh"
done

# Launch Flame Coordinator
echo "cd \"$(pwd)\"; java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Coordinator 9000 localhost:8000" > flamecoordinator.sh
chmod +x flamecoordinator.sh
open -a Terminal flamecoordinator.sh

# Wait for the coordinator to initialize
sleep 2

# Launch Flame Workers
for i in `seq 1 $flameWorkers`
do
    echo "cd \"$(pwd)\"; java -cp bin:lib/webserver.jar:lib/kvs.jar:lib/flame.jar cis5550.flame.Worker $((9000+$i)) localhost:9000" > "flameworker$i.sh"
    chmod +x "flameworker$i.sh"
    open -a Terminal "flameworker$i.sh"
done