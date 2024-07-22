cd project_crawl
mkdir worker1;
cd worker1;
echo "a" > id; # adding the id file in it
cd ..; 
sudo java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker 80 worker1 54.234.126.155:80
