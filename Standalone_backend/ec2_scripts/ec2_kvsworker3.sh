sudo dnf install -y java augeas-libs
cd project_crawl
mkdir worker3;
cd worker3;
echo "n" > id; # adding the id file in it
cd ..; 
sudo java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker 80 worker3 54.234.126.155:80