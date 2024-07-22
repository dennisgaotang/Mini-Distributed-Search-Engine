package cis5550.jobs;

import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Crawler {
	public static final Set<String> whiteListHosts = new HashSet<>();
	private static final int MAX_NUM_PAGE_PER_HOST = 200;

    static {
        whiteListHosts.add("en.wikipedia.org");
        // Add more hosts if needed
        whiteListHosts.add("www.nytimes.com");
    }
	
    static final int MAX_URL_LENGTH_ALLOWED = 500;
    static final int NUM_WORKERS = 8;
    static final int MAX_QUEUE_SIZE = 1500 * NUM_WORKERS;
    static final int MAX_NUM_URL_EXTRACTED_FOR_EACH_PAGE = 50; // this is also the sample rate
    static final boolean ENABLE_BLOCK_HOST_WITHOUT_ROBOTS_TXT = true;
	// timeout's for ROBOT, HEAD, GET request
    static final int MAX_ROBOT_CONNECT_TIMEOUT = 1500;
    static final int MAX_ROBOT_READ_TIMEOUT = 1500;
    static final int MAX_HEAD_CONNECT_TIMEOUT = 1000;
    static final int MAX_HEAD_READ_TIMEOUT = 500;
    static final int MAX_GET_CONNECT_TIMEOUT = 1000;
    static final int MAX_GET_READ_TIMEOUT = 500;
	
    static final int PAGE_WORD_LIMIT = 300;
    
	// It should accept up to two arguments. 
	// The first, required argument is the seed URL from which to start the crawl; 
	// the second, optional argument is the name of the blacklist.
	// If you are not implementing EC2, please ignore the second argument if it is present.
	public static void run(FlameContext ctx, String[] args) throws Exception {
		
		if (!(args.length == 1 || args.length == 2)) {
			ctx.output("Error: Crawler.run() received args with length not equals to one.");
		} else {
			ctx.output("OK");
		}
		final String SEED_URL = args[0];
		
		
		FlameRDD urlQueue = null;
		KVSClient kvsInRun = ctx.getKVS();
		if (args[0].equals("continue")) {
			// urlQueue = ctx.fromTable("pt-queue-leftover", (row) -> {return row.get("value");});
		} else {
			// store the args into  a List
			LinkedList<String> list = new LinkedList<String>();
			list.add(args[0]); // adding the seed url
			urlQueue = ctx.parallelize(list);
		}
		
		
		String blackListTableName = args.length == 2? args[1] : null;
		
//		// put the initial blacklist patterns
//		if (blackListTableName != null) {
//			String dummyBlackListPattern = "http*://*/cgi-bin/*";
//			ctx.getKVS().put(blackListTableName, Hasher.hash(dummyBlackListPattern), "pattern", dummyBlackListPattern.getBytes());
//			
//			dummyBlackListPattern = "http*://priv-policy.imrworldwide.com/*";
//			ctx.getKVS().put(blackListTableName, Hasher.hash(dummyBlackListPattern), "pattern", dummyBlackListPattern.getBytes());
//			dummyBlackListPattern = "http*://g%65*";
//			ctx.getKVS().put(blackListTableName, Hasher.hash(dummyBlackListPattern), "pattern", dummyBlackListPattern.getBytes());
//			dummyBlackListPattern = "https://www.miamiherald.com/*";
//			ctx.getKVS().put(blackListTableName, Hasher.hash(dummyBlackListPattern), "pattern", dummyBlackListPattern.getBytes());
//			dummyBlackListPattern = "http*://*.gov/*";
//			ctx.getKVS().put(blackListTableName, Hasher.hash(dummyBlackListPattern), "pattern", dummyBlackListPattern.getBytes());
//			dummyBlackListPattern = "http*://*.org/*";
//			ctx.getKVS().put(blackListTableName, Hasher.hash(dummyBlackListPattern), "pattern", dummyBlackListPattern.getBytes());
//			dummyBlackListPattern = "https://www.pubgalaxy.com/*";
//			ctx.getKVS().put(blackListTableName, Hasher.hash(dummyBlackListPattern), "pattern", dummyBlackListPattern.getBytes());
//			dummyBlackListPattern = "http*://*.org/*";
//			ctx.getKVS().put(blackListTableName, Hasher.hash(dummyBlackListPattern), "pattern", dummyBlackListPattern.getBytes());
//		}
		int numIter = 1;
		while (urlQueue.count() > 0) {
			System.out.println("##### starting iteration " + numIter + ".");
			numIter++;
			System.out.println("calling flatmap on urlQueue of size " + urlQueue.count());
			
			FlameRDD urlQueueTemp = urlQueue.flatMap(urlStr -> {
				try {
					boolean isOriginalSeed = urlStr.equals(SEED_URL);

					System.out.println("\ndeque seed url: " + urlStr);
					if (urlStr == null || urlStr.isEmpty() || urlStr.isBlank() || urlStr.contains("...") || urlStr.contains("..") || urlStr.length() >= MAX_URL_LENGTH_ALLOWED) {
						System.out.println("urlStr voided: contains .. or length>Max");
						return Collections.emptyList();
					}
					System.out.flush();
					KVSClient kvs = ctx.getKVS();
					// getCurrentWorkerKeyRange();
					
					

					// normalized the urlStr before any processing					
					urlStr = simplifiedNormalizeURL(urlStr);
					System.out.println("result of simplifiedNormalizeURL() new urlStr=" + urlStr);
					String urlHashed = Hasher.hash(urlStr);
					String[] urlParts = URLParser.parseURL(urlStr);
					String hostName = urlParts[1];
					String hostNameHashed = Hasher.hash(hostName);
					
					if (urlStr == null) {
						System.out.println("\n !#! ERROR!!! seed string '" + urlStr + "' return null after simplifiedNormalizeURL() adding port number\n");
						return Collections.emptyList();
					}
					
					// check if the host is blacklisted
					if (kvs.existsRow("hosts", hostNameHashed) && kvs.getRow("hosts", hostNameHashed).columns().contains("blocked") ) {
						// if there is blocked column, the hostname is blocked and the reason is the inside content
						System.out.println("hostName = " + hostName + " is blocked for urlStr=" + urlStr);
						return Collections.emptyList();
					}
					
					
//					// basic filter the url
//					if (!filterAndValidateURL(urlStr)) {
//						return Collections.emptyList();
//					}
//					System.out.println("finish filterAndValidateURL()");
					
//					 //one idea to get the fromKey and toKey is to 
//					// filter by blacklist (EC2)
//					if (blackListTableName != null) {
//						kvs.wait();
//    					System.out.println("blackListTableName is not null");
//						Iterator<Row> iter = kvs.scan(blackListTableName); // fromKey, toKey
//    					System.out.println("after calling kvs scan");
//    					
//	                    while (iter != null && iter.hasNext()) {
//	                    	Row r = iter.next();
//	                    	String pattern = r.get("pattern");
//	                    	System.out.println("inside blacklist iter: r.key() = " + r.key() + "; pattern = " + pattern);
//	                    	if (matchesPattern(urlStr, pattern)) {
//								while (iter.hasNext()) {iter.next()}; // clear the iterator
//	                    		return Collections.emptyList();
//	                    	}
//	                    }
//					}
//					
//					//
//					System.out.println("finish blacklist filtering");
					
					
					// check whether the urlStr already be downloaded in pt-crawl
					if (ctx.getKVS().existsRow("pt-crawl", urlHashed)) {
						return Collections.emptyList();
					}
					
					System.out.println("Start robots protocol");
					// steps 2, 3, 4 robots protocol
					
					

					
					
					String robotsFileLink = urlParts[0] + "://" + hostName + "/robots.txt";
					
					// step 2 It checks whether robots.txt has already been downloaded from the relevant host. or has already been blocked
					// If not, it tries to download it now.
					String robotCharset = null;
					if (( !kvs.existsRow("hosts", hostNameHashed) ) || (!kvs.getRow("hosts", hostNameHashed).columns().contains("triedDownloadRobotsTxt"))  ) {
						System.out.println("Try download robots.txt for host:" + hostName);
						// if tried to download put all these default into to hosts table, only responseCode 200 will activate further HEAD request
						// mark tried to download at first
						if (hostName != null && !hostName.isEmpty()) {
							kvs.put("hosts", hostNameHashed, "hostname", hostName);
						}
						kvs.put("hosts", hostNameHashed, "triedDownloadRobotsTxt", "000");
						
							@SuppressWarnings("deprecation")
							URL urlRobotsFile = new URL(robotsFileLink);
							HttpURLConnection robotConnection = (HttpURLConnection) urlRobotsFile.openConnection();
						try {
							// download the robots.txt file
							robotConnection.setInstanceFollowRedirects(false);
							robotConnection.setRequestMethod("GET");
							robotConnection.setRequestProperty("User-Agent", "cis5550-crawler");
							
						    // Set timeout for establishing the connection (in milliseconds)
						    int RobotConnectTimeout = MAX_ROBOT_CONNECT_TIMEOUT; // 0.1 seconds
						    robotConnection.setConnectTimeout(RobotConnectTimeout);
		
						    // Set timeout for reading from the input stream (in milliseconds)
						    int RobotReadTimeout = MAX_ROBOT_READ_TIMEOUT; // 0.2 seconds
						    robotConnection.setReadTimeout(RobotReadTimeout);
						    
						    
						    robotConnection.connect();
							int responseCode = robotConnection.getResponseCode();
							kvs.put("hosts", hostNameHashed, "triedDownloadRobotsTxt", ""+responseCode);
							
							String robotContentType;
							// String robotCharset; moved to the if block front
							
							if (responseCode == 200) {
								// read the Charset from contentType 
								robotContentType = robotConnection.getContentType();
								String robotContentTypePure = null;
				                robotCharset = null;
				                if (robotContentType != null) {
				                    String[] parts = robotContentType.split(";");
				                    robotContentTypePure = parts[0].trim();
				                    for (String part : parts) {
				                        part = part.trim();
				                        if (part.toLowerCase().startsWith("charset=")) {
				                        	robotCharset = part.substring("charset=".length());
				                            break;
				                        }
				                    }
				                }
				                // ending reading the Charset from contentType
				                
								// store the file into the host table
								InputStream inputStream = robotConnection.getInputStream();
						    	ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		
				                int bytesRead;
				                byte[] buffer = new byte[4096]; // Or adjust the buffer size as needed
		
				                while ((bytesRead = inputStream.read(buffer)) != -1) {
				                    outputStream.write(buffer, 0, bytesRead);
				                }
		
				                byte[] pageBytes = outputStream.toByteArray();
				                outputStream.close();
				                inputStream.close();
		
				                
			                    if (robotContentTypePure != null) {
				                	kvs.put("hosts", hostNameHashed, "robotContentType", robotContentTypePure);
			                    }
			                    // save the page in the table
				                if (pageBytes.length != 0) {
				                	String robotsTxtStr = new String(pageBytes, (robotCharset==null || robotCharset=="undef") ? "UTF-8": robotCharset);
				                	if (robotsTxtStr.contains("<script>") || robotsTxtStr.contains(".jpeg")) {
				                		kvs.put("hosts", hostNameHashed, "triedDownloadRobotsTxt", "504"); // HTTP proxy 504 Gateway timeout
									    if (ENABLE_BLOCK_HOST_WITHOUT_ROBOTS_TXT) {
											kvs.put("hosts", hostNameHashed, "blocked", "no_robots.txt");
										} 
									    return Collections.emptyList();
				                	} else {
				                		kvs.put("hosts", hostNameHashed, "robots.txt", robotsTxtStr);
				                	}
				                }
							}
							
							
						} catch (SocketTimeoutException e) {
						    // Handle timeout exception, server down
						    System.err.println("Connection timed out: " + e.getMessage());
//						    // put the current host to blacklist
//						    kvs.put(blackListTableName, hostNameHashed, "pattern", urlParts[0] + "://" + hostName + "/*");
//						    // put the reason
//						    kvs.put(blackListTableName, hostNameHashed, "reason", "GET robots.txt SocketTimeoutException when call connect()".getBytes());
						    
						    kvs.put("hosts", hostNameHashed, "triedDownloadRobotsTxt", "504"); // HTTP proxy 504 Gateway timeout
						    if (ENABLE_BLOCK_HOST_WITHOUT_ROBOTS_TXT) {
								kvs.put("hosts", hostNameHashed, "blocked", "no_robots.txt");
							} 
						    return Collections.emptyList();
						} catch (IOException e) {
						    // Handle other IO exceptions
						    System.err.println("Failed to connect to the URL: " + e.getMessage());
						 // put the current host to blacklist
//						    kvs.put(blackListTableName, hostNameHashed, "pattern", urlParts[0] + "://" + hostName + "/*");
//						    kvs.put(blackListTableName, hostNameHashed, "reason", "GET robots.txt IOException".getBytes());

						    kvs.put("hosts", hostNameHashed, "triedDownloadRobotsTxt", "504"); // HTTP proxy 504 Gateway timeout
						    if (ENABLE_BLOCK_HOST_WITHOUT_ROBOTS_TXT) {
								kvs.put("hosts", hostNameHashed, "blocked", "no_robots.txt");
							} 
						    return Collections.emptyList();
						} catch (Exception e) {

						    kvs.put("hosts", hostNameHashed, "triedDownloadRobotsTxt", "504"); // HTTP proxy 504 Gateway timeout
						    if (ENABLE_BLOCK_HOST_WITHOUT_ROBOTS_TXT) {
								kvs.put("hosts", hostNameHashed, "blocked", "no_robots.txt");
							} 
						    return Collections.emptyList();
						} finally {
							robotConnection.disconnect();
						}
						
					}
					
					// step 3 If the host has a robots.txt file, the crawler checks whether the file allows visiting the current URL. 
					// If not, it returns an empty set.
					double crawlDelay = 0.0;
					boolean isAllowed = true;
					if (kvs.existsRow("hosts", hostNameHashed) && kvs.getRow("hosts", hostNameHashed).columns().contains("triedDownloadRobotsTxt")) {
						String robotsTxtDownloadStatus = kvs.getRow("hosts", hostNameHashed).get("triedDownloadRobotsTxt");
						try {
							if (robotsTxtDownloadStatus != null && robotsTxtDownloadStatus.equals("200") && kvs.getRow("hosts", hostNameHashed).columns().contains("robots.txt")) {
								// no need to parse again if exist column isAllowed and crawlDelay
								if (kvs.getRow("hosts", hostNameHashed).columns().contains("isAllowed") && kvs.getRow("hosts", hostNameHashed).columns().contains("crawlDelay")) {
									try {
										String isAllowedStrTorF = new String(kvs.get("hosts", hostNameHashed, "isAllowed"));
										isAllowed = isAllowedStrTorF.equals("T") ? true: false;
										String crawlDelayStr = new String(kvs.get("hosts", hostNameHashed, "crawlDelay"));
										crawlDelay = Double.parseDouble(crawlDelayStr);
									} catch (Exception e) {
										System.out.println("!@! Reading isAllowed and crawlDelay col from hosts table failed");
										// parse the file again since cannot read the two values in hosts table
										// String robotsTxt = kvs.getRow("hosts", hostNameHashed).get("robots.txt");
										byte[] robotsTxtBytes = kvs.get("hosts", hostNameHashed, "robots.txt");
										String robotsTxt = null;
										try {
											robotsTxt = new String(robotsTxtBytes, (robotCharset==null || robotCharset=="undef") ? "UTF-8": robotCharset);
										} catch (Exception e2){
											System.out.println("robotsTxt create new String failed: robotCharset=" + robotCharset);
											System.out.println(e2.getStackTrace());
											if (ENABLE_BLOCK_HOST_WITHOUT_ROBOTS_TXT) {
												kvs.put("hosts", hostNameHashed, "blocked", "no_robots.txt");
											} 
											return Collections.emptyList();
										}
										RobotsTxtParser parser = new RobotsTxtParser(robotsTxt);
										String userAgent = "cis5550-crawler";
										isAllowed = parser.isAllowed(userAgent, urlParts[3]);
										crawlDelay = parser.getCrawlDelay();
									}
								} else {
									// parse the file the first time download and put the isAllowed and crawlDelay columns in the hosts table
									// String robotsTxt = kvs.getRow("hosts", hostNameHashed).get("robots.txt");
									byte[] robotsTxtBytes = kvs.get("hosts", hostNameHashed, "robots.txt");
									String robotsTxt = null;
									try {
										robotsTxt = new String(robotsTxtBytes, (robotCharset==null || robotCharset=="undef") ? "UTF-8": robotCharset);
									} catch (Exception e){
										System.out.println("robotsTxt create new String failed: robotCharset=" + robotCharset);
										System.out.println(e.getStackTrace());
										if (ENABLE_BLOCK_HOST_WITHOUT_ROBOTS_TXT) {
											kvs.put("hosts", hostNameHashed, "blocked", "no_robots.txt");
										}
										return Collections.emptyList();
									}
									RobotsTxtParser parser = new RobotsTxtParser(robotsTxt);
									String userAgent = "cis5550-crawler";
									isAllowed = parser.isAllowed(userAgent, urlParts[3]);
									crawlDelay = parser.getCrawlDelay();
									
									kvs.put("hosts", hostNameHashed, "isAllowed", isAllowed? "T" : "F");
									kvs.put("hosts", hostNameHashed, "crawlDelay", ""+crawlDelay);
								}
								
								
								System.out.println("verify the userAgent for web robot host=" + hostName + ",isAllowed=" + isAllowed );
								System.out.println("crawlDelay=" + crawlDelay);
								if (!isAllowed) {
									// return an empty set
									return Collections.emptyList();
								}
							} else {
								// had tried to download robots.txt for this host but failed or file not found
								// do nothing
								System.out.println("had tried to download robots.txt for this host but failed or file not found");
								// if a host does not have robots.txt file block this host
								if (ENABLE_BLOCK_HOST_WITHOUT_ROBOTS_TXT) {
									kvs.put("hosts", hostNameHashed, "blocked", "no_robots.txt");
								}
								return Collections.emptyList();
							}
						} catch (Exception e) {
							e.printStackTrace();
							if (ENABLE_BLOCK_HOST_WITHOUT_ROBOTS_TXT) {
								kvs.put("hosts", hostNameHashed, "blocked", "no_robots.txt");
							}
							return Collections.emptyList();
						}
					}
					System.out.println("finish robots protocol");

					
					// step 4 It checks whether at least one second has passed since it last made a request to this host. 
					// If not, it just returns the current URL, to be attempted again in the next round.
					long currentTimeMillis = System.currentTimeMillis();
					long lastAccessSince = 0;
					if (kvs.getRow("hosts", hostNameHashed).columns().contains("lastAccessSince")) {
						lastAccessSince = Long.parseLong(kvs.getRow("hosts", hostNameHashed).get("lastAccessSince"));
					} 
					
					if (currentTimeMillis - lastAccessSince < crawlDelay * 1000) {
						List<String> l = new ArrayList<>();
		                l.add(urlStr);
		                return l;
					}
					System.out.println("finish delay request");

					
					// sending HEAD request
					// set default responseCode
					System.out.println("Start sending HEAD request");
					int responseCode = 404; // default one to prevent the GETrequest happening
					String charset = null; // default one to prevent the GETrequest creating new String page content
					String contentTypePure = "";
					@SuppressWarnings("deprecation")
					URL urlHEAD = new URL(urlStr);
					HttpURLConnection HEADcon = (HttpURLConnection) urlHEAD.openConnection(); // MalformedURLException: Illegal character in URL
																							  // java.net.MalformedURLException: Illegal character in URL
					try {
						HEADcon.setInstanceFollowRedirects(false);
						// HttpURLConnection.setFollowRedirects(false);
						HEADcon.setRequestMethod("HEAD");
						HEADcon.setRequestProperty("User-Agent", "cis5550-crawler");
						
						// Set timeout for establishing the connection (in milliseconds)
					    int HEADconnectTimeout = MAX_HEAD_CONNECT_TIMEOUT; // 0.1 seconds
					    HEADcon.setConnectTimeout(HEADconnectTimeout);
	
					    // Set timeout for reading from the input stream (in milliseconds)
					    int HEADreadTimeout = MAX_HEAD_READ_TIMEOUT; // 0.2 seconds
					    HEADcon.setReadTimeout(HEADreadTimeout);
					    
					    // the first col to establish the row to pt-crawl indicate we have tried to send a HEAD request
		                kvs.put("pt-crawl", urlHashed, "url", urlStr);
						HEADcon.connect();
						
						responseCode = HEADcon.getResponseCode();
						// mark the request time
						kvs.put("hosts", hostNameHashed, "lastAccessSince", ""+System.currentTimeMillis());
						
						// store in pt-crawl

		                kvs.put("pt-crawl", urlHashed, "responseCode", ""+responseCode);
		                System.out.println("set HEAD response code=" + responseCode);
		                
			            // Process response based on status code
		                String contentType;
		                contentTypePure = "";
		                
			            if (responseCode == 200) {
			                // OK (200) response
			                contentType = HEADcon.getContentType();
			                System.out.println("contentType=" + contentType);
			                charset = null;
			                
			                if (contentType != null) {
			                    String[] parts = contentType.split(";");
			                    contentTypePure = parts[0].trim();
			                    for (String part : parts) {
			                        part = part.trim();
			                        if (part.startsWith("charset=")) {
			                            charset = part.substring("charset=".length());
			                            System.out.println("headrequest parsed charset=" + charset);
			                            break;
			                        }
			                    }
			                }
			                int contentLength = HEADcon.getContentLength();
			                // Insert data into pt-crawl table
			                // (Insert response code, content type, and content length into the table)
			                if (contentTypePure != null && !contentTypePure.isEmpty()) {
			                	kvs.put("pt-crawl", urlHashed, "contentType", contentTypePure);
			                }
			                if (contentType != null && !contentType.isEmpty()) {
			                	kvs.put("pt-crawl", urlHashed, "contentTypeFull", contentType); // found error: contentType is null
			                }
	
			                kvs.put("pt-crawl", urlHashed, "length", ""+contentLength);
			                
			                
			            } else if (responseCode == 301 ||
			                    responseCode == 302 ||
			                    responseCode == 303 ||
			                    responseCode == 307 ||
			                    responseCode == 308) {
			                // Redirect response
			            	try {
				            	String newUrl = HEADcon.getHeaderField("Location");
				                System.out.println("Redirected to: " + newUrl);
				                if (newUrl != null) {
				                	newUrl = normalizeURL(urlStr, newUrl);
				                	if (newUrl.equals(urlStr)) {
				                		return Collections.emptyList();
				                	}
				                	System.out.println("redirected link normalized to:" + newUrl);
				                }
				                if (filterAndValidateURL(newUrl)) {
				                	newUrl = trimQueryParameters(newUrl); // cut the parts after ?
				                	List<String> l = new ArrayList<>();
					                l.add(newUrl);
					                return l;
				                } else {
				                	return Collections.emptyList();
				                }
			            	} catch (Exception e) {
			            		// 
			            		e.printStackTrace();
			            		System.out.println("Acceptable Exception: caught error during normalizeURL() or filterAndValidateURL()");
			            		return Collections.emptyList();
			            	}   
			            } else {
			                // Other response codes (not OK or redirect)
			                System.out.println("Response Code: " + responseCode);
			                // Return an empty set
			                HEADcon.disconnect(); // Close the connection
			                return Collections.emptyList();
			            }
			            HEADcon.disconnect(); // Close the connection
			            
					} catch (SocketTimeoutException e) {
					    // Handle timeout exception, server down
					    System.err.println("Connection to HEAD request timed out: " + e.getMessage());
					    e.printStackTrace();
					    // put the current host to blacklist
//					    kvs.put(blackListTableName, urlHashed, "pattern", urlStr);
//					    kvs.put(blackListTableName, urlHashed, "reason", "HEAD request SocketTimeoutException");
					    kvs.put("pt-crawl", urlHashed, "responseCode", "408"); // connection timeout
					    
					    // the head reqeust timeout put the tried url into our 
					    return Collections.emptyList();
					} catch (IOException e) {
					    // Handle other IO exceptions
					    System.err.println("Failed to connect to the URL for HEAD request: " + e.getMessage());
					    e.printStackTrace();
					    kvs.put("pt-crawl", urlHashed, "responseCode", "504"); // HTTP proxy 504 Gateway timeout
					 // put the current host to blacklist
					   // kvs.put(blackListTableName, hostNameHashed, "pattern", urlParts[0] + "://" + hostName + "/*");
					    return Collections.emptyList();
					} catch (Exception e) {
						// Handle other  exceptions
					    System.err.println("Failed to connect to the URL for HEAD request: " + e.getMessage());
					    e.printStackTrace();
					    kvs.put("pt-crawl", urlHashed, "responseCode", "404"); // HTTP proxy 404 Gateway timeout
						return Collections.emptyList();
					} finally {
						HEADcon.disconnect(); // Close the connection
						System.out.println("finish HEAD request");

					}
					
					

					
		            if (responseCode == 200 && contentTypePure.toLowerCase().equals("text/html")) {
		            	System.out.println("Start sending GET request for page");
			            // sending GET request and stored downloaded page
						@SuppressWarnings("deprecation")
						URL url = new URL(urlStr);
						HttpURLConnection GETPageconn = (HttpURLConnection) url.openConnection();
						GETPageconn.setInstanceFollowRedirects(false);
						GETPageconn.setRequestMethod("GET");
						GETPageconn.setRequestProperty("User-Agent", "cis5550-crawler");
						
						// Set timeout for establishing the connection (in milliseconds)
					    int GETconnectTimeout = MAX_GET_CONNECT_TIMEOUT; // 1 seconds
					    GETPageconn.setConnectTimeout(GETconnectTimeout);
	
					    // Set timeout for reading from the input stream (in milliseconds)
					    int GETreadTimeout = MAX_GET_READ_TIMEOUT; // 0.2 seconds
					    GETPageconn.setReadTimeout(GETreadTimeout);
						
						GETPageconn.connect();
						// mark the request time
						kvs.put("hosts", hostNameHashed, "lastAccessSince", ""+System.currentTimeMillis());
						
						Set<String> normalizedUrls = new HashSet<>();
						List<String> finalURLToQueue = new ArrayList<>();
						int GETResponseCode = GETPageconn.getResponseCode();
						kvs.put("pt-crawl", urlHashed, "responseCode", ""+ GETResponseCode);
						
					    if (GETResponseCode == 200) {
					    	try {
					    		// Read the input stream
						    	InputStream inputStream = GETPageconn.getInputStream();
						    	ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

				                int bytesRead;
				                byte[] buffer = new byte[4096]; // Or adjust the buffer size as needed

				                while ((bytesRead = inputStream.read(buffer)) != -1) {
				                    outputStream.write(buffer, 0, bytesRead);
				                }

				                byte[] pageBytes = outputStream.toByteArray();
				                outputStream.close();
				                inputStream.close();

				                GETPageconn.disconnect();
			                    
//			                    // EC Content-seen test, check duplicate page
//			                    Iterator<Row> iter = kvs.scan("pt-crawl");
//			                    while (iter.hasNext()) {
//			                    	Row r = iter.next();
//			                    	if (r.columns().contains("page")) {
//			                    		byte[] anotherPageBytes = r.getBytes("page");
//			                    		int numBytesToCompare = 1000;
//			                    		if (comparebyteArray(pageBytes, anotherPageBytes, numBytesToCompare)) {
//			                    			// found duplicate page content
//			                    			String canonicalPageURL = r.get("url");
//			                    			System.out.println("Found duplicate page content with link: " + urlStr +  " and link: " + canonicalPageURL + "by comparing the first " + numBytesToCompare + "bytes");
//			                    			if (canonicalPageURL != null && !canonicalPageURL.isEmpty()) {
//			                    				kvs.put("pt-crawl", urlHashed, "canonicalURL", canonicalPageURL);
//			                    			}
//			                    			return Collections.emptyList();
//			                    		}
//			                    	}
//			                    }
			                    
			                    
			                    String htmlContentStr = null;
			                    try {
			                    	htmlContentStr = new String(pageBytes, charset==null? "UTF-8": charset);

			                    } catch (Exception e) {
			                    	System.out.println("create new String htmlContentStr failed: charset='" + robotCharset + "'.");
									System.out.println(e.getStackTrace());
									return Collections.emptyList();
			                    	
			                    }
			                    if (htmlContentStr == null || htmlContentStr.isBlank()) {
			                    	System.out.println("Exception: htmlContentStr is null or blank");
			                    	return Collections.emptyList();
			                    }
			                    
			                    // check html content language, if does not have en tag, dont extract urls
			                    String lang = getHTMLContentLang(htmlContentStr);
			                    System.out.println("lang=" + lang);
			                    if (lang == null) {
			                    	return Collections.emptyList();
			                    }
			                    // this is a huge filter in our crawler
//			                    if (lang == null) {
//			                    	// the html content does not contain a lang attrubute: <html lang="en">
//			                    	kvs.put("pt-crawl", urlHashed, "lang", "");
//			                    	return Collections.emptyList();
//			                    } else {
//			                    	kvs.put("pt-crawl", urlHashed, "lang", lang);
//			                    }
			               
			                    
			                    if (lang != null && !(lang.startsWith("en") || lang.startsWith("EN"))) {
			                    	// stop extracting the urls inside
			                    	return Collections.emptyList();
			                    }
			                    	
			                    // clean the page and save the page in the table if there is no duplicate
			                    String cleanedPage = null;
			                    try {
			                    	cleanedPage = processPageText(htmlContentStr, PAGE_WORD_LIMIT);
			                    } catch (Exception e) {
			                    	e.printStackTrace();
			                    	System.out.println("!~~! clean page failed for urlStr=" + urlStr);
			                    	kvs.put("pt-crawl", urlHashed, "page", pageBytes);
			                    }
			                    if (cleanedPage != null) {
			                    	kvs.put("pt-crawl", urlHashed, "page", cleanedPage);
			                    	System.out.println("Success put cleaned page into pt-crawl");
			                    }

			                    // increment the number of web page put for this website
			                    if (kvs.existsRow("hosts", hostNameHashed) && kvs.getRow("hosts", hostNameHashed).columns().contains("num_page_downloaded")) {
			                    	int oldNumPage = Integer.parseInt(kvs.getRow("hosts", hostNameHashed).get("num_page_downloaded"));
			                    	int newNumPage = oldNumPage+1;
			                    	if ((!kvs.getRow("hosts", hostNameHashed).columns().contains("max_num_page_downloaded_reached")) && (newNumPage >= MAX_NUM_PAGE_PER_HOST) && (!whiteListHosts.contains(hostName))) {
			                    		kvs.put("hosts", hostNameHashed, "max_num_page_downloaded_reached", "T");
			                    	}
			                    	kvs.put("hosts", hostNameHashed, "num_page_downloaded", "" + newNumPage);
			                    	
			                    } else {
			                    	kvs.put("hosts", hostNameHashed, "num_page_downloaded", "1");
			                    }
			                    
			                    
			                    // put extra header info of the page to pt-crawl
			                    saveTitleHeaderMetaInfo(kvs, urlHashed, htmlContentStr);

			                    
			                    
			                    // extract url from this website
			                    Set<String> extractedUrls = isOriginalSeed? extractUrlsFromHtmlTakeFirstNURL(htmlContentStr, 10000): extractUrlsFromHtmlTakeFirstNURL(htmlContentStr, MAX_NUM_URL_EXTRACTED_FOR_EACH_PAGE*2);
			                    
			                    
		                    	System.out.println("Extract url part 0");
			                    for (String extractedUrl : extractedUrls) {
			                    	try {
			                    		String normalizedURL = normalizeURL(urlStr, extractedUrl);
				                        if (filterAndValidateURL(normalizedURL)) {
				                        	normalizedURL = trimQueryParameters(normalizedURL); // cut the parts after ?
				                        	normalizedUrls.add(normalizedURL);
				                        }
			                    	} catch (Exception e) {
			                    		continue;
			                    	}
			                    }
		                    	System.out.println("Extract url part 1 finish for loop");
		                    	
		                    	// inside flapmap filtering and sampling for each parent url
		                    	finalURLToQueue = new ArrayList<>(normalizedUrls);
		                    	Collections.shuffle(finalURLToQueue);
		                    	finalURLToQueue.subList(0, Math.min(MAX_NUM_URL_EXTRACTED_FOR_EACH_PAGE, finalURLToQueue.size()));
		                    	
					    	} catch (Exception e) {
					    		System.out.println("!!!!caught error after trying GET request to page content with 200 code.");
					            System.err.println("An Exception(line:470) error occurred: " + e.getMessage());
					            return Collections.emptyList();
					    	}
					    }
					    GETPageconn.disconnect();
					    // save the parent and childs into pt-url
					    String urlCSV = listToCSVString(finalURLToQueue);
					    kvs.put("pt-url", urlHashed, "url", urlStr);
					    kvs.put("pt-url", urlHashed, "url_childs", urlCSV);
					    
					    
					    return finalURLToQueue;
					    // return new ArrayList<>(normalizedUrls);
		            }
		            
		            // if response code != 200
		            return Collections.emptyList();
		            
		            // possible uncaught exception in this block:
		            // 1. java.net.SocketTimeoutException: Read timed out (Crawler.java:417)
		            // 2. java.net.SocketTimeoutException: Connect timed out (Crawler.java:412)
		            // 3. java.net.MalformedURLException: Illegal character in URL (Crawler.java:283)
				} catch (Exception e) {
					System.err.println("!**! The whole block inside flapmap caught exception." + e.getMessage());
					e.printStackTrace();
					return Collections.emptyList();
				}
				// END of flatmap
			});
			System.out.println("END of flatmap");
			
			// statistics
			List<String> newQueueList = urlQueueTemp.collect();
			urlQueueTemp.destroy();
			// filtering queue urls
			// keep each hosts fixed amount of pages
			List<String> newQueueListFiltedPagePerHost = new ArrayList<>();
			for (String url: newQueueList) {
				try {
					String[] urlParts = URLParser.parseURL(url);
					String hostName = urlParts[1];
					String hostNameHashed = Hasher.hash(hostName);
					// kvs.put("hosts", hostNameHashed, "max_num_page_downloaded_reached", " ");
					if (kvsInRun.existsRow("hosts", hostNameHashed) && kvsInRun.getRow("hosts", hostNameHashed).columns().contains("max_num_page_downloaded_reached")) {
						continue; // skip the url with such host because num of pages per host reached
					}
					newQueueListFiltedPagePerHost.add(url);
				} catch (Exception e) {
					continue;
				}
			}
			urlQueueTemp = ctx.parallelize(newQueueListFiltedPagePerHost); // this is the new one
//			try {
//				Map<String, List<String>> hostToUrls= printURLQueueStats(newQueueListFiltedPagePerHost);
//			} catch (Exception e) {
//				e.printStackTrace();
//				System.out.println("printURLQueueStats() encountered error.");
//			}
			
			
			
			
			
			// filter and sample the url queue table
			int oldQueueSize = urlQueue.count();
			int newQueueSizeAfterFlatMap = urlQueueTemp.count();
			
			
			FlameRDD newQueuePossiblySampled = urlQueueTemp;
			if (newQueueSizeAfterFlatMap > MAX_QUEUE_SIZE) {
				System.out.println("Start sampling: newQueueSizeAfterFlatMap=" + newQueueSizeAfterFlatMap + " MAX_QUEUE_SIZE = " + MAX_QUEUE_SIZE);
				double sampleRate = (double)MAX_QUEUE_SIZE / (double)newQueueSizeAfterFlatMap;
				System.out.println("sampleRate = " + sampleRate);
				newQueuePossiblySampled = urlQueueTemp.sample(sampleRate);
				System.out.println("finished sampling ");
				urlQueueTemp.destroy();
			}
			// urlQueue still refers to the old table, clear it in the disk
			urlQueue.destroy();
			// save to table for consistent crawling
			// kvsInRun.delete("pt-queue-leftover");
			// newQueuePossiblySampled.saveAsTable("pt-queue-leftover");
			urlQueue = newQueuePossiblySampled;
			
//			// sample and limit the number of url of each host
//			FlamePairRDD pairRDDGrouped= urlQueueTemp.groupBy((str) -> {
//				// try to test if the url str contains illegal char
//				System.out.println("Groupby encounter input url str=" + str);
//				try {
//					@SuppressWarnings("deprecation")
//					URL dummyURLObject = new URL(str);
//					HttpURLConnection dummyConnection = (HttpURLConnection) dummyURLObject.openConnection();
//				} catch (MalformedURLException e) {
//					return "a_corruptURL";
//				} catch (Exception e) {
//					return "a_corruptURL";
//				}
//				try {
//					// try parsing the url str
//					// might not successfully parse the hostname from url
//					String[] parts = URLParser.parseURL(str);
//				
//					if (parts.length >=2) {
//						return Hasher.hash(parts[1]) ; // return the hostname as groupby key
//					}
//				} catch (Exception e) {
//					System.out.println("#$%! Exception: Groupby parsing url failed");
//					e.printStackTrace();
//					return "a_corruptURL";
//				}
//				
//				return "a_corruptURL";
//			});
//			urlQueueTemp.destroy();
//			
//			FlameRDD newQueueFiltered = pairRDDGrouped.flatMap((pair) -> {
//				String k = pair._1();
//				String V = pair._2();
//				System.out.println("V= " + V);
//				if (k == null || k.isBlank() || k.equals("a_corruptURL") || k == "a_corruptURL") {
//					return Collections.emptyList();
//				}
//
//				// V = comma-separated list of url elements, filter it
//				Set<String> urlsForEachHostNameK = new TreeSet<>();
//				String[] urls = V.split(",");
//				for (String url: urls) {
//					urlsForEachHostNameK.add(url);
//				}
//		        // Convert the TreeSet to an ArrayList
//		        ArrayList<String> urlList = new ArrayList<>(urlsForEachHostNameK);
//		        Collections.shuffle(urlList);
//		        if (urlList.size() > 50) {
//			        // Sample the first 50 elements from the shuffled ArrayList
//			        ArrayList<String> sampledURLList = new ArrayList<>(urlList.subList(0, 50));
//					return sampledURLList;
//		        } else {
//		        	return urlList;
//		        }
//
//			}); // expand csv strings to list of urls
//			pairRDDGrouped.destroy();
//			
//			// sample the RDD to limit the size under MAX_QUEUE_SIZE
//			FlameRDD newQueueFilteredAndSampled = newQueueFiltered;
//			
//			int newQueueSizeAfterSampledByHost = newQueueFiltered.count();
//			final int MAX_QUEUE_SIZE = 10000;
//			boolean hasSampledUnderMax = false;
//			if (newQueueSizeAfterSampledByHost > MAX_QUEUE_SIZE) {
//				double sampleRate = MAX_QUEUE_SIZE / newQueueSizeAfterSampledByHost;
//				newQueueFilteredAndSampled = newQueueFiltered.sample(sampleRate);
//				newQueueFiltered.destroy();
//			}
//			if (hasSampledUnderMax) {
//				
//			}
//			
//			
//			// urlQueue still refers to the old table, clear it in the disk
//			urlQueue.destroy();
//			urlQueue = newQueueFilteredAndSampled;
//			// Thread.sleep(100); // sleep for 0.1 seconds
			
		} // end while loop
		
		List<String> out = urlQueue.collect();
		Collections.sort(out);
	
		String result = "";
		for (String s : out) 
			result = result+(result.equals("") ? "" : "\n")+s;

		ctx.output(result);
		
	}
	
	
	
    private static String listToCSVString(List<String> normalizedUrls) {
		int i = 1;
		int len = normalizedUrls.size();
		StringBuilder sb = new StringBuilder();
		for (String url: normalizedUrls) {
			if (url != null && !url.isBlank()) {
				sb.append(url + (i >= len? "" : ", "));
			}
			i++;
		}
		return sb.toString();
	}


    public static String trimQueryParameters(String url) {
        int index = url.indexOf("?");
        if (index != -1) {
            return url.substring(0, index);
        } else {
            return url;
        }
    }

	private static Map<String, List<String>> printURLQueueStats(List<String> newQueueList) throws Exception {
		Map<String, List<String>> hostToUrls = new HashMap<>();
		for (String url: newQueueList) {
			String[] urlParts = URLParser.parseURL(url);
			String hostName = urlParts[1];
			if (!hostToUrls.containsKey(hostName)) {
				List<String> l = new ArrayList<>();
				l.add(url);
				hostToUrls.put(hostName, l);
			} else {
				List<String> l = hostToUrls.get(hostName);
				l.add(url);
			}
		}
		// print some stats
		System.out.println("total # of hosts = " + hostToUrls.size());
		for (Entry<String, List<String>> e: hostToUrls.entrySet()) {
			List<String> l = e.getValue();
			System.out.print("\n{" + e.getKey() + "(#url= " + l.size() + ")" + ": [");
//			int i = 1;
//			for (String url: l) {
//				System.out.print(url + (i == l.size()? "": ", "));
//				i++;
//			}
			System.out.print("]}\n");
		}
		return hostToUrls;
	}



	// Page clean up: remove non-main-content-body tags (such as script, style) and content inside tags from the page, save only first 300 words and join the words with " "
    private static String processPageText(String page, int wordLimit) {
        String allInOneRegex = "(?s)<script.*?</script>|<style.*?</style>|<meta.*?>|<(link|head|noscript|iframe|footer|nav|header|aside|object|embed)[^>]*>(.*?</\\1>)?|<[^>]*>";
        page = page.replaceAll(allInOneRegex, " ")
                .replaceAll("[\\r\\n\\t`~!@#$%^&*()\\-=+\\[\\]{};:'\",.<>/?\\\\|_]+", " ")
                .replaceAll("[^a-zA-Z0-9\\s]+", " ")
                .trim();
        if (page.isEmpty() || page.isBlank()) {
            return null;
        }
        // Get the first wordLimit number of words from the page
        String[] words = page.split("\\s+", wordLimit + 1);
        if (words.length == (wordLimit + 1)) {
        	 words = Arrays.copyOf(words, words.length - 1);
        }
//        System.out.println("$$$ cleaned page word count=" + words.length);
//        for (String w: words) {
//        	System.out.println("word=" + w);
//        }
        return String.join(" ", words);
    }
    
    // Page clean up: extract content of key tags and save as columns in the url's corresponding row (row key: urlHash)
    private static void saveTitleHeaderMetaInfo (KVSClient kvs, String urlHash, String page) throws IOException {
        Map<String, String> tagMap = extractAllTags(page);
        String title = tagMap.get("title");
        String h1 = tagMap.get("h1");
        String h2 = tagMap.get("h2");
        String h3 = tagMap.get("h3");
        String h4 = tagMap.get("h4");
        String h5 = tagMap.get("h5");
        String h6 = tagMap.get("h6");
        String metaDescription = tagMap.get("metaDescription");
        String metaKeywords = tagMap.get("metaKeywords");

        if (title != null && !title.isEmpty() && !title.isBlank()) {
        	kvs.put("pt-crawl", urlHash, "title", title);
        }
        if (h1 != null && !h1.isEmpty() && !h1.isBlank()) {
        	kvs.put("pt-crawl", urlHash, "h1", h1);
        }
        if (h2 != null && !h2.isEmpty() && !h2.isBlank()) {
        	kvs.put("pt-crawl", urlHash, "h2", h2);
        }
        if (h3 != null && !h3.isEmpty() && !h3.isBlank()) {
        	kvs.put("pt-crawl", urlHash, "h3", h3);
        }
        if (h4 != null && !h4.isEmpty() && !h4.isBlank()) {
        	kvs.put("pt-crawl", urlHash, "h4", h4);
        }
        if (h5 != null && !h5.isEmpty() && !h5.isBlank()) {
        	kvs.put("pt-crawl", urlHash, "h5", h5);
        }
        if (h6 != null && !h6.isEmpty() && !h6.isBlank()) {
        	kvs.put("pt-crawl", urlHash, "h6", h6);
        }
        if (metaDescription != null && !metaDescription.isEmpty() && !metaDescription.isBlank()) {
            kvs.put("pt-crawl", urlHash, "description", metaDescription);
        }
        if (metaKeywords != null && !metaKeywords.isEmpty() && !metaKeywords.isBlank()) {
            kvs.put("pt-crawl", urlHash, "keywords", metaKeywords);
        }
    }
    
    // Page clean up: extract content of key tags
    private static Map<String, String> extractAllTags(String page) {
        Map<String, String> tags = new HashMap<>();

        String[][] tagPatterns = {
                {"title", "<title[^>]*>(.*?)</title>"},
                {"h1", "<h1[^>]*>(.*?)</h1>"},
                {"h2", "<h2[^>]*>(.*?)</h2>"},
                {"h3", "<h3[^>]*>(.*?)</h3>"},
                {"h4", "<h4[^>]*>(.*?)</h4>"},
                {"h5", "<h5[^>]*>(.*?)</h5>"},
                {"h6", "<h6[^>]*>(.*?)</h6>"},
                {"metaDescription", "<meta\\s+name\\s*=\\s*\"description\"\\s+content\\s*=\\s*\"(.*?)\"\\s*>"},
                {"metaKeywords", "<meta\\s+name\\s*=\\s*\"keywords\"\\s+content\\s*=\\s*\"(.*?)\"\\s*>"}
        };

        for (String[] tagPattern : tagPatterns) {
            Pattern pattern = Pattern.compile(tagPattern[1], Pattern.DOTALL| Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(page);
            String content = matcher.find() ? matcher.group(1).trim() : "";
            content = content.replaceAll("<[^>]*>", " ")
                    .replaceAll("[\\r\\n\\t`~!@#$%^&*()\\-=+\\[\\]{};:'\",.<>/?\\\\|_]+", " ")
                    .replaceAll("[^a-zA-Z0-9\\s]+", " ")
                    .trim();
            tags.put(tagPattern[0], content.isEmpty() ? "" : content);
        }
        return tags;
    }
    
    public static boolean containsNonEnglishCharacters(String url) {
    	try {
            // Decode the URL using UTF-8
            String decodedUrl = URLDecoder.decode(url, StandardCharsets.UTF_8.toString());
            // Regular expression to match English characters (basic Latin range)
            String englishRegex = "[\\u0020-\\u007E]+"; // Basic Latin range (ASCII characters)
            // Pattern for matching English characters
            Pattern pattern = Pattern.compile(englishRegex);

            // Check each character in the decoded URL
            for (char c : decodedUrl.toCharArray()) {
                if (!pattern.matcher(String.valueOf(c)).matches()) {
                    return true; // Non-English character found
                }
            }
            return false; // No non-English characters found
        } catch (Exception e) {
            // Handle decoding exception
            e.printStackTrace();
            return true; // Consider as non-English characters on decoding failure
        }
    }
    
	private static boolean filterAndValidateURL(String urlStr) {
		if (urlStr == null) return false;
		if (containsNonEnglishCharacters(urlStr)) {
			// System.out.println("contain nonEnglish char in urlStr= " +  urlStr);
			return false;
		}
			
		urlStr = urlStr.toLowerCase();
		String[] parts = URLParser.parseURL(urlStr);
		if (!(parts[0].equals("http") || parts[0].equals("https"))) {
			return false;
		}
		if (urlStr.endsWith(".jpg")) return false;
		if (urlStr.endsWith(".jpeg")) return false;
		if (urlStr.endsWith(".gif")) return false;
		if (urlStr.endsWith(".png")) return false;
		if (urlStr.endsWith(".txt")) return false;
		
		if (urlStr.toLowerCase().contains("email")) return false;
		if (urlStr.contains("www.majorgeeks.com")) return false;
		if (urlStr.startsWith("https://patents.google.com")) return false;
		// https://support.apple.com
		if (urlStr.startsWith("https://patents.google.com")) return false;
		// https://itunes.apple.com/
		if (urlStr.startsWith("https://itunes.apple.com")) return false;
		// apps.apple.com
		if (urlStr.startsWith("https://apps.apple.com")) return false;
		
		// filter out words with not allowed keywords
		String[] keywords = {
	            "admin", "cart","checkout","favorite","password","register","sendfriend","wishlist", // Checkout- and account-related
	            "cgi-bin","includes","var",																// Script-related
	            "filter","limit", "order", "sort",													// Ordering- and filtering-related
	            "sessionid", "session_id", "sid", "phpsessid",										// Session-related
	            "ajax", "catalog", "dir", "mode", "profile", "search", "pageid", "page_id", "docid", "doc_id" // others

	        };
		String regex = "\\b(" + String.join("|", keywords) + ")\\b";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(urlStr);
		if (matcher.find()) {
			return false;
		}
		
		// other cases
		return true;
	}

	// Method to extract URLs from HTML content
	private static Set<String> extractUrlsFromHtmlTakeFirstNURL(String htmlContent, int n) {
		System.out.println("func extractUrlsFromHtml() called.");
		Set<String> extractedUrls = new HashSet<>();

	    // Initialize flag to indicate if we are inside an anchor tag
	    boolean insideAnchorTag = false;

	    // Initialize StringBuilder to store href attribute value
	    StringBuilder hrefBuilder = new StringBuilder();

	    // Loop through each character in the HTML content
	    for (int i = 0; i < htmlContent.length(); i++) {
	        char currentChar = htmlContent.charAt(i);
	        
	        // Check if current character is "<"
	        if (currentChar == '<') {
	            // Reset hrefBuilder when entering a new tag
	            hrefBuilder.setLength(0);
	            // Set insideAnchorTag flag if this is an anchor tag
	            insideAnchorTag = htmlContent.regionMatches(true, i + 1, "a ", 0, 2);
	        } else if (currentChar == '>') {
	            // Reset insideAnchorTag flag when exiting a tag
	            insideAnchorTag = false;
	        } else if (insideAnchorTag && Character.toLowerCase(currentChar) == 'h' && htmlContent.regionMatches(true, i, "href=", 0, 5)) {
	            // If inside an anchor tag and encountering "href=", extract URL
	            i += 5; // Skip "href="
	            // Find the opening quote character (either ' or ")
	            int quoteIndex = htmlContent.indexOf('"', i);
	            if (quoteIndex == -1) {
	                quoteIndex = htmlContent.indexOf('\'', i);
	            }
	            if (quoteIndex != -1) {
	            	i = quoteIndex + 1; // Move to the character after the opening quote
	                char quoteChar = htmlContent.charAt(quoteIndex);
		            while (i < htmlContent.length() && htmlContent.charAt(i) != quoteChar) {
		                hrefBuilder.append(htmlContent.charAt(i));
		                i++;
		            }
		            // Add extracted URL to the set
		            extractedUrls.add(hrefBuilder.toString());
		            if (extractedUrls.size() == n) {
		            	return extractedUrls;
		            }
	            } else {
	            	continue;
	            }
	            
	        }
	    }

	    return extractedUrls;
	}
	
	// method to see if the HTML content is in what language
	// detecting pattern <html lang="en">
	private static String getHTMLContentLang(String htmlContent) {
		System.out.println("func getHTMLContentLang() called.");
		boolean insideHTMLTag = false;
		String language = null;
		StringBuilder langBuilder = new StringBuilder();
		for (int i = 0; i < htmlContent.length(); i++) {
			char currentChar = htmlContent.charAt(i);
			if (currentChar == '<') {
	            // Reset hrefBuilder when entering a new tag
				langBuilder.setLength(0);
	            // Set insideHTMLTag flag if this is an anchor tag
	            insideHTMLTag = htmlContent.regionMatches(true, i + 1, "html ", 0, 5);
	        } else if (currentChar == '>') {
	        	if (insideHTMLTag) {
	        		return language;
	        	}
	            // Reset insideHTMLTag flag when exiting a tag
	        	insideHTMLTag = false;
	        }  else if (insideHTMLTag && Character.toLowerCase(currentChar) == 'l' && htmlContent.regionMatches(true, i, "lang=", 0, 5)) {
	        	i += 5; // Skip "lang="
	        	// Find the opening quote character (either ' or ")
	            int quoteIndex = htmlContent.indexOf('"', i);
	            if (quoteIndex == -1) {
	                quoteIndex = htmlContent.indexOf('\'', i);
	            }
	            if (quoteIndex != -1) {
	            	i = quoteIndex + 1; // Move to the character after the opening quote
	                char quoteChar = htmlContent.charAt(quoteIndex);
		            while (i < htmlContent.length() && htmlContent.charAt(i) != quoteChar) {
		            	langBuilder.append(htmlContent.charAt(i));
		                i++;
		            }
		            // see if the lang is english
		            language = langBuilder.toString();
		            return language;
	            } else {
	            	continue;
	            }
	        }
		}
		return language;
	}
	
    private static String normalizeURL(String base, String link) {
    	
        // Step 1: Cut off the part after the #
        int hashIndex = link.indexOf('#');
        if (hashIndex >= 0) {
            link = link.substring(0, hashIndex);
        }

        // Step 2: If the link is empty after cutting off the # part, discard it
        if (link.isEmpty()) {
            return null; // Discard empty link
        }

        // Step 3: If the link is relative, normalize it
        if (!link.startsWith("http://") && !link.startsWith("https://")) {
            // If the link starts with "/", it's relative to the base host
            if (link.startsWith("/")) {
                String[] baseParts = URLParser.parseURL(base);
                if (baseParts[0] != null && baseParts[1] != null) {
                    link = baseParts[0] + "://" + baseParts[1] + ":" + (baseParts[2]==null? "" : baseParts[2]) + link;
                }
            } else { // Relative link
                // Remove filename from base URL
                int lastSlashIndex = base.lastIndexOf('/');
                if (lastSlashIndex >= 0) {
                    base = base.substring(0, lastSlashIndex);
                }

                // Remove ".." parts from the link
                while (link.startsWith("../")) {
                    int prevSlashIndex = base.lastIndexOf('/');
                    if (prevSlashIndex >= 0) {
                        base = base.substring(0, prevSlashIndex);
                    }
                    link = link.substring(3); // Remove ".." part
                }

                link = base + "/" + link;
            }
        }

        // Step 4: If the link doesn't have a host part, prepend the host part from the base URL
        if (!link.contains("://")) {
            String[] baseParts = URLParser.parseURL(base);
            if (baseParts[0] != null && baseParts[1] != null) {
                link = baseParts[0] + "://" + baseParts[1] + link;
            }
        }

        // Step 5: If the link doesn't have a port number, add the default port number for the protocol
        String[] linkParts = URLParser.parseURL(link);
        if (linkParts[2] == null) {
            if (linkParts[0].equals("https")) {
                link = linkParts[0] + "://" + linkParts[1] + ":" + "443" + linkParts[3];
            } else if (linkParts[0].equals("http")) {
            	link = linkParts[0] + "://" + linkParts[1] + ":" + "80" + linkParts[3];
            } else {
            	// only supports http or https port
            	return null;
            }
        }
        // make sure all the parts are presents
        linkParts = URLParser.parseURL(link);
        for (int i = 0; i < 4; i++) {
        	if(linkParts[i] == null) return null;
        }

        return link;
    }
    
    private static String simplifiedNormalizeURL(String seedURL) {
    	// Step 1: Cut off the part after the #
        int hashIndex = seedURL.indexOf('#');
        if (hashIndex >= 0) {
        	seedURL = seedURL.substring(0, hashIndex);
        }
    	// Step 2: If the link doesn't have a port number, add the default port number for the protocol
        String[] linkParts = URLParser.parseURL(seedURL);
        if (linkParts[2] == null) {
            if (linkParts[0].equals("https")) {
            	seedURL = linkParts[0] + "://" + linkParts[1] + ":" + "443" + linkParts[3];
            } else if (linkParts[0].equals("http")) {
            	seedURL = linkParts[0] + "://" + linkParts[1] + ":" + "80" + linkParts[3];
            } else {
            	// only supports adding http or https port
            	return null;
            }
        }
        // Step3: check make sure all the parts are presents
        linkParts = URLParser.parseURL(seedURL);
        for (int i = 0; i < 4; i++) {
        	if(linkParts[i] == null) return null;
        }
        return seedURL;
    }
    
    // EC1 helper
    private static boolean comparebyteArray(byte[] pageBytes1, byte[] pageBytes2, int numberOfBytesCompared) {
    	// Compare the lengths of the byte arrays
        if (pageBytes1.length != pageBytes2.length) {
            return false;
        }
        // Compare each byte of the byte arrays
        int shorterArraySize = pageBytes1.length > pageBytes2.length? pageBytes2.length: pageBytes1.length;
        for (int i = 0; i < Math.min(numberOfBytesCompared, shorterArraySize); i++) {
            if (pageBytes1[i] != pageBytes2[i]) {
                return false;
            }
        }
        // If no differences found, the HTML pages are identical
        return true;
    }
    
    // EC2 helper: 
    private static boolean matchesPattern(String candidateUrl, String pattern) {
        // Escape special characters in the pattern and replace '*' with '.*' for regex matching
        String regex = pattern.replaceAll("\\*", ".*");
        // Convert the regex pattern to a compiled Pattern object
        Pattern compiledPattern = Pattern.compile(regex);
        // Create a Matcher to match the candidate URL against the pattern
        Matcher matcher = compiledPattern.matcher(candidateUrl);
        // Check if the candidate URL matches the pattern
        if (matcher.matches()) {
            return true; // Candidate URL matches the pattern
        }
        return false; // Candidate URL does not match any pattern in the blacklist
    }
    
    // class for parsing robots.txt
    private static class RobotsTxtParser {
    	private static double DEFAULT_CRAWL_DELAY = 0.01;
        private Map<String, List<Rule>> rulesMap;
        private double crawlDelay;

        public RobotsTxtParser(String robotsTxtContent) {
        	System.out.println("RobotsTxtParser() called");
            this.rulesMap = new HashMap<>();
            this.crawlDelay = DEFAULT_CRAWL_DELAY; // Default crawl delay

            // Split the content by lines
            String[] lines = robotsTxtContent.split("\n");

            String currentUserAgent = null; // stored in lowercase
            List<Rule> currentRules = null;

            // Parse each line
            for (String line : lines) {
                line = line.trim();
                if (!line.isEmpty()) {
                	if (line.startsWith("#")) {
                		// skip the comment lines
                		continue;
                	}
                    // Check if the line starts with User-agent:
                    if (line.startsWith("User-agent: ")) {
                        // Create a new list for rules
                        currentRules = new ArrayList<>();
                        // Extract the user-agent
                        currentUserAgent = line.substring("User-agent: ".length()).trim().toLowerCase();
                        // Store the rules list for the current user-agent
                        rulesMap.put(currentUserAgent, currentRules);
                    } else if (line.contains(":")) {
                        // Split the line by colon to get the directive and value
                        String[] parts = line.split(":");
                        if (parts.length == 2 && currentRules != null) {
                            String directive = parts[0].trim();
                            String value = parts[1].trim();
                            // Store the directive and value in the rules list for the current user-agent
                            currentRules.add(new Rule(directive, value));
                            // Check if the directive is Crawl-delay
                            if (directive.equalsIgnoreCase("Crawl-delay")) {
                                // Parse and store the crawl delay value
                            	// cut the comments part # 
                            	// example error-prone value= "0.2  # ADDED BY HMS"
                            	String[] delayDoubleWithCommentParts = value.split("#", 2);
                                crawlDelay = Double.parseDouble(delayDoubleWithCommentParts[0].trim());
                                if (crawlDelay < 0) {
                                	crawlDelay = DEFAULT_CRAWL_DELAY;
                                }
                            }
                        }
                    }
                }
            }
//            System.out.println("ruleMap: ");
//            for (Entry<String, List<Rule>> e: rulesMap.entrySet()) {
//            	System.out.println("{" + e.getKey() + "," + e.getValue() + "'");
//            }
            
        }

        // the provided url should start with "/"
        public boolean isAllowed(String userAgent, String url) {
            // Get the rules list for the given user-agent
            List<Rule> userRules = rulesMap.get(userAgent.toLowerCase());
            if (userRules != null) {
                // Apply rules for the given user-agent
                return applyRules(userRules, url);
            }

            // If no specific rules found, check the wildcard rules
            List<Rule> wildcardRules = rulesMap.get("*");
            if (wildcardRules != null) {
                // Apply rules for the wildcard user-agent
                return applyRules(wildcardRules, url);
            }

            // If no rules found, treat as allowed
            return true;
        }

        private boolean applyRules(List<Rule> rules, String url) {
            // Iterate through each rule and apply
            for (Rule rule : rules) {
                if (rule.matches(url)) {
                    return rule.isAllow();
                }
            }
            // If no matching rule found, treat as allowed
            return true;
        }

        public double getCrawlDelay() {
            return crawlDelay;
        }

        private static class Rule {
            private final String directive;
            private final String value;

            public Rule(String directive, String value) {
                this.directive = directive;
                this.value = value;
            }

            public boolean isAllow() {
            	if (directive.equalsIgnoreCase("Allow")) return true;
            	if (directive.equalsIgnoreCase("Disallow")) return false;
                return true;
            }

            public boolean matches(String url) {
                // Check if the URL starts with the value of this rule
                return url.startsWith(value); // noted the case where value is "", 
                // e.g.User-agent: Mediapartners-Google* 
                //     Disallow:
            }
            
            public String toString() {
            	
            	return "(directive=" + this.directive + "; value=" + this.value+ ") ";
            }
        }
    }
}
