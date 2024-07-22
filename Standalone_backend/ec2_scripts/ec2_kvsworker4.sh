sudo dnf install -y java augeas-libs
cd project_crawl
mkdir worker4;
cd worker4;
echo "t" > id; # adding the id file in it
cd ..; 
sudo java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker 80 worker4 54.234.126.155:80