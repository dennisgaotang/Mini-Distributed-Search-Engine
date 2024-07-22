# CodeCrafters Search Engine README
The CodeCrafters is a distributed search engine which allows user to search webpages based on query words. It utilizes a scalable key-value store, KVS, and a big data processing engine, Flame to support crawling, indexing and ranking funtionalities. It also provides and easy-to-use user-interface. While standalone code is provided, the searcher with its components are fully deployed on EC2 nodes for scalablity and flexibility. You may access our search engine via: http://codecrafters.cis5550.net/

This repo contains two folders: "Standalone_backend" and "Integration". It is read-only, and please try not to push changes under this branch.

## Folders Description
- **Standalone_backend**: contains three flame jobs for preparing the data for searcher: Crawler, Indexer and Pagerank. The code in this folder is for standalone run on local computer. The crawler in this folder is deployed in EC2.
- **Searcher**: contains all the components required to run Search Engine locally, including: Searcher and frontend. Searcher and frontend are deployed together on an EC2 instance. 
- **Integration**: contains all the components with all the parts(including frontend and fine-tuned ranking) integrated, all the components have been run on EC2. 

## Instructions to run codes in the two folders
### For Standalone_backend folder ("cd Standalone_backend"):
1. **To run crawler**: 
   * run command "crawler_script.sh" to launch the kvs and coordinator, you may specify the number of workers in the script for each system.
   * run command "java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler https://en.wikipedia.org/wiki/Wikipedia:Popular_pages blacklist" to start crawling with this seed
   * optional: if you want to terminate the crawling in the middle and relaunch the crawl process with the previous url queue(frontier), you need to:
       * 1. shut down the flamecoordinator and all flameworker terminal process.
       * 2. run command "restart_flame_script.sh"
       * 3. run command "java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 crawler.jar continue blacklist"
       * 4. you may also kill all the terminals and keep all the worker* folder and its data by deleting line 5 "rm -r worker*" in the crawler_script.sh file and run it.
2. **To run Indexer**:
   * run command "bash indexer_script.sh"
   * run command "java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 indexer.jar cis5550.jobs.Indexer"
3. **To run PageRank**:
   * run command "bash pagerank_script.sh"
   * run command "java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank 0.01"

### For Integration folder:
1. **Step 1**: (Terminal 1): Run Coordinators and Workers
   * Option 1: Run Coordinators and Workers with libraries (webserver.jar, kvs.jar, flame.jar)
       * Command: bash script.sh
   * Option 2: Run Coordinators and Workers without libraries (webserver.jar, kvs.jar, flame.jar)
       * Command: bash script2.sh
   #### pt tables description:
   * pt-crawl: crawled urls, their metadata and page content
   * pt-index: indexing of crawled urls (word: [url1: word frequency in url1], [url2: word frequency in url2], ...)
   * pt-pageranks: page ranks of crawled urls
   * pt-url: child urls of crawled urls
   * pt-queue-leftover: url queue to crawl (url frontier record) if the program crashes before finishing crawling

2. **Step 2**: (Terminal 2): Run jobs
   * Option 1: Run Crawler
       * Command: java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler [seed url]
       * Example: java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler https://en.wikipedia.org/wiki/Wikipedia:Popular_pages
       * Result: pt-crawl and pt-url will be populated after running Crawler 
   * Option 2: Run Indexer
       * Command: java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 indexer.jar cis5550.jobs.Indexer
       * Result: pt-index will be populated after running Indexer
   * Option 3: Run PageRank
       * Command: java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank [Optional: threshold value]
       * Example: java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank 0.01
       * Result: pt-pageranks will be populated after running Indexer
       
## Instructions to run Search Engine

Under Searcher folder, run `bash script.sh` to run Search Engine locally.

If it's not working, under Searcher folder:
1. **Step 0**: Download data of 8 workers, place 8 folders under Searcher.
2. **Compile Searcher.**: Run `javac -cp lib/json-simple-1.1.1.jar --source-path src src/cis5550/jobs/Searcher.java`
3. **Start KVS Coordinator**: Run `java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Coordinator 8000`.
4. **Start KVS Workers**: Run `java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8001 worker1 localhost:8000`, 
    `java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8002 worker2 localhost:8000`,
    `java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8003 worker3 localhost:8000`,
    `java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8004 worker4 localhost:8000`,
    `java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8005 worker5 localhost:8000`,
    `java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8006 worker6 localhost:8000`,
    `java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8007 worker7 localhost:8000`,
    `java -cp lib/kvs.jar:lib/webserver.jar cis5550.kvs.Worker 8008 worker8 localhost:8000`.
5. **Start Seacher**: Run `java -cp lib/json-simple-1.1.1.jar:lib/kvs.jar:src cis5550.jobs.Searcher 8080 localhost:8000`. 

You can access the Search Website through localhost:8080, and KVS Workers status through localhost:8000.

The representation video is on https://youtu.be/Kiytvyg1Zbs?feature=shared
