#!/bin/bash

# Compile Searcher
#javac -cp lib/webserver.jar --source-path src src/cis5550/kvs/Coordinator.java
#javac -cp lib/webserver.jar --source-path src src/cis5550/kvs/Worker.java
javac -cp lib/kvs.jar:lib/json-simple-1.1.1.jar --source-path src src/cis5550/jobs/Searcher.java

# Start KVS Coordinator
java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Coordinator 8000 &
sleep 5 # Pause execution for 5 seconds

# Start KVS Workers
java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8001 worker1 localhost:8000 &
java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8002 worker2 localhost:8000 &
java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8003 worker3 localhost:8000 &
java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8004 worker4 localhost:8000 &
java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8005 worker5 localhost:8000 &
java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8006 worker6 localhost:8000 &
java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8007 worker7 localhost:8000 &
java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8008 worker8 localhost:8000 &
sleep 2 # Pause execution for 2 seconds

# Start Searcher
java -cp lib/json-simple-1.1.1.jar:lib/kvs.jar:src cis5550.jobs.Searcher 8080 localhost:8000
