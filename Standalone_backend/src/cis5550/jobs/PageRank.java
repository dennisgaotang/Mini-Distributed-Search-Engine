package cis5550.jobs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cis5550.flame.*;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class PageRank {
	public static void run(FlameContext ctx, String[] args) throws Exception {
		try {
			// process args[0] = threshold
			if (args.length > 1) {
				System.out.println("Error. usage: FlameSubmit <server> <jarFile> <className> [optional args of length 1-args[0] = t]");
				return;
			}
			double t = 0.1;
			if (args.length == 1) {
				double thresholdArg = Double.parseDouble(args[0]);
				if (thresholdArg < 0 || thresholdArg > 1) {
					System.out.println("Error, the threshold argument should be in (0.0, 1.0), e.g. 0.1");
					return;
				} else {
					t = thresholdArg;
				}
			}
			System.out.println("Threshold set as " + t);
			
			long startTime = System.currentTimeMillis();
			
			
			// read the crawl results from pt-crawl into RDD
			FlameRDD  rdd = ctx.fromTable("pt-url", (row) -> {
				String rowKey = row.key();
				String url = row.get("url");
				if (!rowKey.equals(Hasher.hash(url))) {
					System.out.println("!**! error unmatch hash in pt-crawl");
				}
				
				String 	url_childs = row.get("	url_childs");
				return rowKey +  "," + url + "," + url_childs;
			});
			System.out.println("************  finish rdd  ***************");
			System.out.println("filtered pt-crawl rdd size = " + rdd.count());
			
			
			
			// turn RDD into PairRDD of (normalized URL, page content)
			// after this, the new PairRDD will have row key as the url, columnKey = hashedURL, value = page content
			FlamePairRDD stateTable = rdd.mapToPair((str) -> {
				String[] parts = str.split(",", 3);
				String hashedUrl = parts[0];
				String url = parts[1];
				String L = parts[2];
				
//				url = simplifiedNormalizeURL(url);
//				String hashedUrl = Hasher.hash(url);
//				if (!hashedUrl.startsWith("a")) {
//					throw new Exception("wrongly created new hashedURL");
//				}
//				Set<String> extractedUrls = extractUrlsFromHtml(page);
//				StringBuilder sb = new StringBuilder();
//				int index = 0;
//				for (String extractedUrl : extractedUrls) {
//					String normalizedURL = normalizeURL(url, extractedUrl);
//					if (normalizedURL == null) {
//						continue;
//					}
//					String hashedPointToURL = Hasher.hash(normalizedURL);
//					sb.append(hashedPointToURL);
//					// sb.append(normalizedURL);
//					if (++index < extractedUrls.size()) {
//						sb.append(",");
//					}
//				}
//				String L = sb.toString();
				
				return new FlamePair(hashedUrl, "1.0,1.0," + L);
				// return new FlamePair(url, "1.0,1.0," + L);
			});
			// stateTable.saveAsTable("stateTable");
			System.out.println("************  finish stateTable initialized  ***************");
			System.out.println("initial stateTable size = " + stateTable.collect().size());
			rdd.destroy(); // clear up the temp memory
			
			
			
			int iterNum = 1;
			while(true) {
				System.out.println("\n--- Start iteration #" + iterNum);
								
				FlamePairRDD transferTable = stateTable.flatMapToPair((p) -> {
					String currentNodeHashedURL = p._1();
					String states = p._2();
					String[] parts = states.split(",", 3);
					double rC = Double.parseDouble(parts[0]);
					double rP = Double.parseDouble(parts[1]);
					
					// int n = parts.length - 2;
					// System.out.println("currentNodeHashedURL=" + currentNodeHashedURL+ " with n=" + n);
					if (parts.length == 2) {
						// no outgoing nodes
						// don't need to compute
						List<FlamePair> lTemp = new ArrayList<>();
						lTemp.add(new FlamePair(currentNodeHashedURL, "0.0"));
						return lTemp;
					} else {
						String L = parts[2];
						String[] childURLs = L.split(", ");
						int n = childURLs.length;
						List<FlamePair> l = new ArrayList<>();
						for (int i = 0; i < childURLs.length; i++) {
							String pointToUrlHashed = Hasher.hash(childURLs[i]);
							double v = 0.85 * rC / n;
							FlamePair newPair = new FlamePair(pointToUrlHashed, String.valueOf(v));
							// System.out.println("newPair = " + newPair);
							l.add(newPair);
						}
						// send 0.0 to itself
						l.add(new FlamePair(currentNodeHashedURL, "0.0"));
						return l;
					}
				});
				System.out.println("************  finish transferTable  ***************");
				// System.out.println("transferTable size = " + transferTable.collect().size());
				// transferTable.saveAsTable("pt-transferTable");
				
				
				
//				// debug view transfer table
//				List<FlamePair> transferTableList = transferTable.collect();
//				System.out.println("************  transferTable  ***************");
//				for (FlamePair p: transferTableList) {
//					System.out.println("* " + p);
//				}
//				System.out.println("************  transferTable end ************");
				
				FlamePairRDD transferTableAggregate = transferTable.foldByKey("0.0", (s1, s2) -> {
					double n1 = Double.parseDouble(s1);
					double n2 = Double.parseDouble(s2);
					double sum = n1 + n2;
					return String.valueOf(sum);
				});
				System.out.println("************  finish transferTableAggregate  ***************");
				// System.out.println("transferTableAggregate size = " + transferTableAggregate.collect().size());
				transferTable.destroy();				
				
				
				FlamePairRDD joinTable = transferTableAggregate.join(stateTable);
				System.out.println("************  finish joinTable  ***************");
				// System.out.println("joinTable size = " + joinTable.collect().size());
				transferTableAggregate.destroy();
				
				
				FlamePairRDD updateStateTable = joinTable.flatMapToPair((p) -> {
					String hashedURL = p._1();
					String states = p._2();
					String[] parts = states.split(",", 4);
					double rIncrement = Double.parseDouble(parts[0]);
					double rC = Double.parseDouble(parts[1]);
					double rP = Double.parseDouble(parts[2]);
					String leftover = parts.length > 3? parts[3] : "";
					// System.out.println(hashedURL + "'s increment = " + rIncrement);
					rP = rC;
					rC = 0.15 + rIncrement;
					FlamePair newPair = new FlamePair(hashedURL, String.valueOf(rC) + "," + String.valueOf(rP) + "," + leftover);
					List<FlamePair> l = new ArrayList<>();
					l.add(newPair);
					return l;
				});
				System.out.println("************  finish updateStateTable  ***************");
				System.out.println("updateStateTable size = " + updateStateTable.collect().size());
				joinTable.destroy();
				
				
				// update the stateTable
				stateTable.destroy();
				stateTable = updateStateTable;
				System.out.println("************  stateTable updated.  ************");
				
				
				// compute the maximum change in ranks across all the pages. 
				// This can be done with a flatMap, where you compute the absolute difference between the old and current ranks for each page, 
				// followed by a fold that computes the maximum of these. If the maximum is below the convergence threshold, exit the loop.
				FlameRDD intermediateRDD = stateTable.flatMap((p) -> {
					List<String> l = new ArrayList<>();
					String hashedURL = p._1();
					String states = p._2();
					String[] parts = states.split(",", 3);
					double rC = Double.parseDouble(parts[0]);
					double rP = Double.parseDouble(parts[1]);
					double diff = Math.abs(rC - rP);
					l.add(String.valueOf(diff));
					return l;
				});
						
				 String maximumDiffOfRank = intermediateRDD.fold("-1.0", (s1, s2) -> {
					double n1 = Double.parseDouble(s1);
					double n2 = Double.parseDouble(s2);
					double max = Math.max(n1, n2);
					return String.valueOf(max);
				});
				 intermediateRDD.destroy();
				System.out.println("************  maximumDiffOfRank updated.  ************");

				
				System.out.println("--- Finished iteration #" + iterNum + "\n");
				if (Double.parseDouble(maximumDiffOfRank) < t) {
					break;
				}
				iterNum++;

			} // end of while loop
			
			stateTable.flatMapToPair((p) -> {
				String hashedURL = p._1();
				// find the original URL by hash
				String url = "";
				try {
					Row r = ctx.getKVS().getRow("pt-url", hashedURL);
					url = r.get("url"); // error
					
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("!**! Row in hashedURL in processed stateTable that's not in pt-crawl: rowKey= " + hashedURL);
				}
				String state = p._2();
				String[] parts = state.split(",");
				String curRank = parts[0];

				ctx.getKVS().put("pt-pageranks", hashedURL, "rank", curRank);
				ctx.getKVS().put("pt-pageranks", hashedURL, "url", url);
				return Collections.emptyList();
			});
			stateTable.destroy(); // destroy in memory stateTable after writing in disk as pt-pageranks
			long endTime = System.currentTimeMillis();
			double elapsedTimeInSecond = ((double)endTime - startTime)/ 1000.0;
			
			System.out.println("###### Congrats! ###### Page-Rank Calculation finished in " + elapsedTimeInSecond + " seconds");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	// Method to extract URLs from HTML content
	private static Set<String> extractUrlsFromHtml(String htmlContent) {
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
	            } else {
	            	continue;
	            }
	            
	        }
	    }

	    return extractedUrls;
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
    
    private static String normalizeURL(String base, String link) {
    	// System.out.println("Try normalizeURL with base='" + base + "'" + " link='" + link + "'");
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
}