Step 1 (Terminal 1): Run Coordinators and Workers

Option 1: Run Coordinators and Workers with libraries (webserver.jar, kvs.jar, flame.jar)

Command:
bash script.sh

Option 2: Run Coordinators and Workers without libraries (webserver.jar, kvs.jar, flame.jar)

Command:
bash script2.sh

pt tables description:
pt-crawl: crawled urls, their metadata and page content
pt-index: indexing of crawled urls (word: [url1: word frequency in url1], [url2: word frequency in url2], ...)
pt-pageranks: page ranks of crawled urls
pt-url: child urls of crawled urls
pt-queue-leftover: url queue to crawl (url frontier record) if the program crashes before finishing crawling

Step 2 (Terminal 2): Run jobs

Option 1: Run Crawler
Command:
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler [seed url] 

Example:
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler https://en.wikipedia.org/wiki/Wikipedia:Popular_pages 

pt-crawl and pt-url will be populated after running Crawler 

Option 2: Run Indexer

Command: 
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 indexer.jar cis5550.jobs.Indexer
pt-index will be populated after running Indexer

Option 3: Run PageRank
Command: 
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank [Optional: threshold value]

Example:
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank 0.01

pt-pageranks will be populated after running Indexer
