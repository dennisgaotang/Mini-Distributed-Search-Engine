

package cis5550.jobs;

import cis5550.external.PorterStemmer;
import cis5550.flame.*;

import java.io.*;
import java.sql.SQLOutput;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class Indexer {

    private static final int MAX_WORD_LEN = 20;
    private static final int MAX_URL_LIST_LEN = 1000;
    private static final int MAX_URL_LEN = 100 * MAX_URL_LIST_LEN;

    private static final int URLS_LEN_CHECK = 50000;
    private static final String URLHASH_URLPAGE_BREAK = "@@@!!!###";

    private static final String URL_PAGE_BREAK = "###%%%&&&";
    private static final String URL_LIST_SPLIT = ", ";

    private static int rowCount = 0;

    private static final Set<String> STOP_WORDS = Set.of(
            "a", "about", "above", "actually", "after", "again", "against", "all", "almost", "also", "although", "always", "am", "an", "and", "any", "are", "aren't", "as", "at",
            "b", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by",
            "c", "can", "couldn't", "could", "come", "comes", "could've",
            "d", "did", "didn't", "do", "does", "doesn't", "doing", "down", "during",
            "e", "each", "either", "else", "enough", "ever", "every",
            "f", "few", "for", "from", "further", "furthermore",
            "g", "get", "gets", "getting", "got",
            "h", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's",
            "however", "http", "https", "htm", "html",
            "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself",
            "j", "just", "k",
            "l", "let", "let's", "like", "likely",
            "m", "may", "maybe", "me", "might", "mine", "more", "most", "mostly", "must", "mustn't", "my", "myself",
            "n", "no", "nor", "not", "now", "of", "off", "often",
            "o", "on", "once", "only", "or", "org", "other", "our", "ours", "ourselves", "out", "over", "own",
            "p", "perhaps",
            "q", "quite",
            "r", "rather", "really", "regarding",
            "s", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
            "t", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very",
            "u", "v", "w", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "will", "with", "won't", "would", "wouldn't",
            "www", "x",
            "yet", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves", "z"
    );

    private static final Set<String> blackListedUrls = Set.of(
//            "https://in.nau.edu:443/university-transit-services/shuttle-information/",
//            "https://threesixty.stthomas.edu:443/dma-camp-justice-for-george/",
//            "https://www.istockphoto.com:443/stock-videos/special-occasions",
//            "https://cisr.mit.edu:443/content/workshop-menu",
//            "https://gitlab.tails.boum.org:443/tails/tails/-/issues/20335",
//            "https://www.bravotv.com:443/summer-house/season-8/episode-6/start-your-engines",
//            "https://www.pinterest.com:443/ideas/cross-stitch-patterns-flowers/933002669632/"
        );


    public static void run(FlameContext context, String[] args) throws Exception {

        try {

            FlameRDD rddUrls = context.fromTable("pt-crawl", row -> {
                rowCount++;
                System.out.println("count: " + rowCount);


                String urlHash = row.key();
                String url = row.get("url");
                String page = row.get("page");

                if (url == null) {
                    return null;
                }

//                System.out.println("rddUrl urlHash: " + urlHash);
//                System.out.println("same as Hasher.hash()? " + Hasher.hash(url).equals(urlHash));
//                System.out.println("url: " + url);
                boolean urlValid = isValidUrl(url);
//                System.out.println("isValidUrl: " + urlValid);
                boolean urlBlackListed = isBlackListed(url);
//                System.out.println("isBlackListed: " + urlBlackListed);

                String responseCode = row.get("responseCode");
                if (url == null || url.isEmpty() || url.isBlank() || !urlValid || urlBlackListed || page == null || page.isEmpty() || page.isBlank() || responseCode == null || !responseCode.equals("200")) {

                    return null;
                }

                page = page.toLowerCase();

                return urlHash + URLHASH_URLPAGE_BREAK + url + URL_PAGE_BREAK + page; // Unique separator for splitting later
            });


            AtomicInteger pairRDDCount = new AtomicInteger();
            FlamePairRDD pairRddUrlPage = rddUrls.mapToPair(s -> {
                pairRDDCount.getAndIncrement();
                System.out.println("pairRDDCount: " + pairRDDCount);
//                System.out.println("original page: " + s);
                try {
                    if (s == null || s.isEmpty() || s.isBlank()) {
                        return null;
                    }
                    String[] parts = s.split(URLHASH_URLPAGE_BREAK, 2);
                    if (parts.length < 2) {  // Make sure both parts exist
                        return null;
                    }
//                    System.out.println("pairRDDUrlPage url: " + parts[0]);
                    return new FlamePair(parts[0], parts[1]);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println("Error processing mapToPair: " + e.getMessage());
                }
                return null;
            });


            rddUrls.destroy();

            AtomicInteger contentProcessCount = new AtomicInteger();

            pairRddUrlPage.flatMap(pair -> {
                contentProcessCount.getAndIncrement();
                System.out.println("contentProcessCount: " + contentProcessCount);

                try {
                    if (pair == null) {
                        return null;
                    }


                    String urlHash = pair._1();
//                    System.out.println("pairRDDURLPage urlHash:" + urlHash);

                    String[] parts = pair._2().split(URL_PAGE_BREAK, 2);

                    String url = parts[0];
//                    System.out.println("pairRDDURLPage url: " + url);

                    url = decodeURL(url);
//                    System.out.println("pairRDDURLPage decode url: " + url);

//                    System.out.println("urlHash = Hasher.hash(decodeURL)? " + urlHash.equals(Hasher.hash(url)));


//                    System.out.println("url: " + url);

                    String page = parts[1];

//                    System.out.println("pairRDDURLPage decoded url:" + url);

                    String[] words = page.split("\\s+");

                    if (words == null || words.length == 0) {
                        return null;
                    }


//                    System.out.println("words: " + Arrays.asList(words));

                    Map<String, Integer> wordCountMap = new HashMap<>();

                    PorterStemmer stemmer = new PorterStemmer();

                    int totalWordCount = 0; // total word count excluding stop words
                    for (int i = 0; i < words.length; i++) {

                        String currentWord = words[i];

                        if (STOP_WORDS.contains(currentWord) || containsDigitOrIsLong(currentWord)) {
                            continue;
                        }

//                System.out.println("original word: " + currentWord);
                        // Reset stemmer for new word
                        stemmer.add(currentWord.toCharArray(), currentWord.length());
                        stemmer.stem();
                        // Get stemmed word
                        String stemmedWord = stemmer.toString();


                        if (stemmedWord.length() > MAX_WORD_LEN) {
                            stemmedWord = stemmedWord.substring(stemmedWord.length() - MAX_WORD_LEN);
                        }

//                System.out.println("stemmed word: " + stemmedWord);

                        // Add stemmed word and its position
                        if (!STOP_WORDS.contains(stemmedWord)) {
                            wordCountMap.put(stemmedWord, wordCountMap.getOrDefault(stemmedWord, 0) + 1);
                            totalWordCount++;
                        }

                    }

//                    System.out.println("total word count: " + totalWordCount);

                    int maxFreq = 0;
                    for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
                        if (entry.getValue() > maxFreq) {
                            maxFreq = entry.getValue();
                        }
                    }

//                    System.out.println("max freq: " + maxFreq);


                    try {
                        updateCrawlRowInfo(context, url, totalWordCount, maxFreq);
                    } catch (IOException e) {
                        System.err.println("Error accessing KVS: " + e.getMessage());

                    }


                    String finalUrl = url;
                    wordCountMap.forEach((word, count) -> {


                        try {
                            updateIndexRowInfo(context, finalUrl, word, count);
                        } catch (IOException e) {
                            System.err.println("Error accessing KVS: " + e.getMessage());
                        }

                    });

                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("flatMap error: " + e.getMessage());
                }

                return null;
            });



            pairRddUrlPage.destroy();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private static void updateCrawlRowInfo(FlameContext context, String url, int totalWordCount, int maxFreq) throws IOException {
        try {
//            System.out.println("url: " + url);
            String urlHash = Hasher.hash(url);
//            System.out.println("urlHash: " + urlHash);

            Row row = context.getKVS().getRow("pt-crawl", urlHash);
            if (row == null) {
//                row = new Row(urlHash);
//                System.out.println("pt-crawl getRow failed: row is null...");
                return;
            }
            if (row.get("totalWordCount") == null) {
                row.put("totalWordCount", String.valueOf(totalWordCount));
            }
            if (row.get("maxFreq") == null) {
                row.put("maxFreq", String.valueOf(maxFreq));
            }
            context.getKVS().putRow("pt-crawl", row);

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to update pt-crawl for url: " + url + "; error: " + e.getMessage());
        }

    }

//    private static void updateIndexRowInfo(FlameContext context, String url, String word, int count) throws IOException {
//        try {
////            System.out.println("pt-index getRow: " + word);
//            Row indexRow = context.getKVS().getRow("pt-index", word);
//            if (indexRow == null) {
//                indexRow = new Row(word);
//                String urls = url + ":" + count;
//                indexRow.put("urls", urls.trim());
//            } else {
//                String urls = indexRow.get("urls");
//                if (urls != null && !urls.contains(url)) {
//                    urls = urls + "," + url + ":" + count;
//                    urls = urls.trim();
//                }
//                indexRow.put("urls", urls);
//            }
//            context.getKVS().putRow("pt-index", indexRow);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.err.println("Failed to update pt-index for word: " + word + "; error: " + e.getMessage());
//        }
//    }


    private static void updateIndexRowInfo(FlameContext context, String url, String word, int count) throws IOException {
        try {
            Row indexRow = context.getKVS().getRow("pt-index", word);
            StringBuilder urlsBuilder;
            if (indexRow == null) {
                indexRow = new Row(word);
                urlsBuilder = new StringBuilder(url + ":" + count);
            } else {
                String urls = indexRow.get("urls");
                urlsBuilder = new StringBuilder(urls);
                if (urls != null && !urls.contains(url)) {
                    if (urlsBuilder.length() > URLS_LEN_CHECK) {
                        Random random = new Random();
                        if (random.nextInt(100) < 95) {  // 95% chance
//                            System.out.println("rand < 95");
                            // Approach 1: Replace last URL if new URL has a higher count
                            int lastCommaIndex = urlsBuilder.lastIndexOf(URL_LIST_SPLIT);
                            if (lastCommaIndex != -1) {
                                String lastUrl = urlsBuilder.substring(lastCommaIndex + URL_LIST_SPLIT.length()).trim();
//                                System.out.println("lastUrl: " + lastUrl);
                                int lastColonIndex = lastUrl.lastIndexOf(":");
                                if (lastColonIndex != -1) {
                                    String lastCountStr = lastUrl.substring(lastColonIndex + 1);
//                                    System.out.println("lastCountStr: " + lastCountStr);
                                    try {
                                        int lastCount = Integer.parseInt(lastCountStr);
                                        if (count > lastCount) {
                                            urlsBuilder.delete(lastCommaIndex + URL_LIST_SPLIT.length(), urlsBuilder.length());
                                            urlsBuilder.append(url).append(":").append(count);
//                                            System.out.println("urlsBuilder: " + urlsBuilder.toString());
                                        }
                                    } catch (NumberFormatException e) {
                                        System.err.println("Error parsing count from URL string: " + lastUrl);
                                    }
                                }
                            }
                        } else {
//                            System.out.println("rand >= 95");

                            // Approach 2: Keep top 200 URLs with the largest counts
                            // Using a min heap to keep the top 200 urls with the largest counts
                            String[] allUrls = urls.split(URL_LIST_SPLIT);
//                            System.out.println("allUrls: " + Arrays.asList(allUrls));
                            PriorityQueue<String> minHeap = new PriorityQueue<>(MAX_URL_LIST_LEN, (a, b) -> {
                                int partsACommaIndex = a.lastIndexOf(":");
                                int partsBCommaIndex = b.lastIndexOf(":");

                                String countA = a.substring(partsACommaIndex + 1);
                                String countB = b.substring(partsBCommaIndex + 1);
                                try {
                                    return Integer.compare(Integer.parseInt(countA), Integer.parseInt(countB));
                                } catch (NumberFormatException e) {

//                                    System.out.println("countA: " + countA);
//                                    System.out.println("countB: " + countB);
                                    System.err.println("Error parsing count from URL string: " + countA);
                                }

                                return -1;
                            });

                            for (String existingUrl : allUrls) {
                                minHeap.offer(existingUrl);
//                                System.out.println("existingUrl: " + existingUrl);
                                if (minHeap.size() > MAX_URL_LIST_LEN) {
                                    minHeap.poll();
                                }
                            }

                            // Add the new url if it has a higher count than the smallest in the heap
                            String lowestCountStr = minHeap.peek();
                            int commaIndex = lowestCountStr.lastIndexOf(":");
                            try{
                                if (minHeap.size() < MAX_URL_LIST_LEN || count > Integer.parseInt(lowestCountStr.substring(commaIndex + 1))) {
                                    minHeap.offer(url + ":" + count);
                                    if (minHeap.size() > MAX_URL_LIST_LEN) {
                                        minHeap.poll();
                                    }
                                }
                            } catch (NumberFormatException e) {
                                System.err.println("Error parsing count from URL string: " + lowestCountStr);
                            }


                            // Rebuild urls string from the heap
                            StringBuilder newUrlsBuilder = new StringBuilder();
                            while (!minHeap.isEmpty()) {
                                if (newUrlsBuilder.length() > 0) newUrlsBuilder.append(URL_LIST_SPLIT);
                                newUrlsBuilder.append(minHeap.poll());
                            }
                            urlsBuilder = newUrlsBuilder;
                        }
                    } else {
                        urlsBuilder.append(URL_LIST_SPLIT).append(url).append(":").append(count);
                    }
                    urlsBuilder.trimToSize();
                }
            }
//            System.out.println("urlsBuilder: " + urlsBuilder.toString());
            indexRow.put("urls", urlsBuilder.toString().trim());
            context.getKVS().putRow("pt-index", indexRow);

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to update pt-index for word: " + word + "; error: " + e.getMessage());
        }
    }



    private static String decodeURL(String url) {
        try {
            return java.net.URLDecoder.decode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return url;
        }
    }
    public static boolean isValidUrl(String url) {
        // Check for multiple occurrences of "amp;" or "%3A" or "%3D" in the URL
        // Check for multiple occurrences of "Special:" in the URL
        // Check for excessive length of the URL
        return url != null
                && url.length() <= MAX_URL_LEN
                && !url.contains(",")
                && !Pattern.compile("%[0-9A-Fa-f]{2}").matcher(url).find()
                && !Pattern.compile("((%3A%2F%2F)|(://)).*?((%3A%2F%2F)|(://))").matcher(url).find();
//                && !Pattern.compile("(amp;.*){2,}").matcher(url).find()
//                && !Pattern.compile("((%3A)|(:)).*?((%3A)|(:)).*?((%3A)|(:))").matcher(url).find()
//                && (Pattern.compile("Special:").matcher(url).results().count() <= 1);
    }

    public static boolean isBlackListed (String url) {


        return url == null || blackListedUrls.contains(url);
    }

    public static boolean containsDigitOrIsLong(String word) {
        // Check if the word's length is greater than 20
        if (word.length() > MAX_WORD_LEN) {
            return true;
        }

        // Check each character to see if it's a digit
        for (int i = 0; i < word.length(); i++) {
            if (Character.isDigit(word.charAt(i))) {
                return true;
            }
        }

        return false;
    }

}