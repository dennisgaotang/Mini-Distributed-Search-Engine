package cis5550.jobs;

import cis5550.kvs.Row;
import cis5550.kvs.KVSClient;
import cis5550.webserver.*;
import cis5550.tools.Hasher;
import cis5550.external.PorterStemmer;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static cis5550.webserver.Server.get;

public class Searcher {
    public static KVSClient clientKVS;
    public static String[] STOPW = { "a", "about", "above", "across",
            "after", "afterwards", "again", "against", "all", "almost", "alone",
            "along", "already", "also", "although", "always", "am", "among",
            "amongst", "amoungst", "amount", "an", "and", "another", "any",
            "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around",
            "as", "at", "back", "be", "became", "because", "become", "becomes",
            "becoming", "been", "before", "beforehand", "behind", "being", "below",
            "beside", "besides", "between", "beyond", "bill", "both", "bottom",
            "but", "by", "call", "can", "cannot", "cant", "co", "con",
            "could", "couldnt", "cry", "de", "describe", "detail", "do", "done",
            "down", "due", "during", "each", "eg", "eight", "either", "eleven",
            "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every",
            "everyone", "everything", "everywhere", "except", "few", "fifteen",
            "fify", "fill", "find", "fire", "first", "five", "for", "former",
            "formerly", "forty", "found", "four", "from", "front", "full",
            "further", "get", "give", "go", "had", "has", "hasnt", "have", "he",
            "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon",
            "hers", "herse", "him", "himse", "his", "how", "however", "hundred",
            "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it",
            "its", "itse", "keep", "last", "latter", "latterly", "least", "less",
            "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill",
            "mine", "more", "moreover", "most", "mostly", "move", "much", "must",
            "my", "myse", "name", "namely", "neither", "never", "nevertheless",
            "next", "nine", "no", "nobody", "none", "noone", "nor", "not",
            "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one",
            "only", "onto", "or", "other", "others", "otherwise", "our", "ours",
            "ourselves", "out", "over", "own", "part", "per", "perhaps", "please",
            "put", "rather", "re", "same", "see", "seem", "seemed", "seeming",
            "seems", "serious", "several", "she", "should", "show", "side", "since",
            "sincere", "six", "sixty", "so", "some", "somehow", "someone",
            "something", "sometime", "sometimes", "somewhere", "still", "such",
            "system", "take", "ten", "than", "that", "the", "their", "them",
            "themselves", "then", "thence", "there", "thereafter", "thereby",
            "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
            "third", "this", "those", "though", "three", "through", "throughout",
            "thru", "thus", "to", "together", "too", "top", "toward", "towards",
            "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
            "very", "via", "was", "we", "well", "were", "what", "whatever", "when",
            "whence", "whenever", "where", "whereafter", "whereas", "whereby",
            "wherein", "whereupon", "wherever", "whether", "which", "while",
            "whither", "who", "whoever", "whole", "whom", "whose", "why", "will",
            "with", "within", "without", "would", "yet", "you", "your", "yours",
            "yourself", "yourselves"};

    static HashSet<String> STOPWORDS = new HashSet<>(Arrays.asList(STOPW));
    private static Map<String, Double> idfCache;
    private static List<String> suggestions = new ArrayList<>();

    private static void initIdfCacheAndSuggestions() throws IOException {
        idfCache = new HashMap<>();
        Iterator<Row> iter = clientKVS.scan("pt-index", null, null);
        while (iter.hasNext()) {
            Row row = iter.next();
            String word = row.key();
            suggestions.add(word);
            String urls = row.get("urls");
            if (urls != null && !urls.isEmpty()) {
                String[] urlArr = urls.split(",");
                double idf = Math.log10(1 + 1500.0 / urlArr.length);
                idfCache.put(word, idf);
            }
        }
    }

    private static int countMatchingWords(String title, String extract, Set<String> stemmedQueryWords) {
        int count = 0;
        PorterStemmer stemmer = new PorterStemmer();
        Set<String> stemmedTitleWords = new HashSet<>();
        Set<String> stemmedExtractWords = new HashSet<>();

        // Stem the words in the title
        for (String word : title.toLowerCase().split("\\s+")) {
            for (char c : word.toCharArray()) {
                stemmer.add(c);
            }
            stemmer.stem();
            stemmedTitleWords.add(stemmer.toString());
            stemmer = new PorterStemmer(); // Reset the stemmer for the next word
        }

        // Stem the words in the extract
        for (String word : extract.toLowerCase().split("\\s+")) {
            for (char c : word.toCharArray()) {
                stemmer.add(c);
            }
            stemmer.stem();
            stemmedExtractWords.add(stemmer.toString());
            stemmer = new PorterStemmer(); // Reset the stemmer for the next word
        }

        // Check for matching stemmed words
        for (String word : stemmedQueryWords) {
            if (stemmedTitleWords.contains(word) || stemmedExtractWords.contains(word)) {
                count++;
            }
        }

        return count;
    }

    public static void main (String[] args) throws IOException {
        if(Integer.parseInt(args[0]) == 443){
            Server.securePort(443);
            Server.port(80);
        } else {
            Server.port(Integer.parseInt(args[0]));
        }
        clientKVS = new KVSClient(args[1]);
        initIdfCacheAndSuggestions();

        get("/search", (req, resp)-> {
            String query_raw = req.queryParams("query");
            if(query_raw == null) {
                return "";
            }

            // Clean and preprocess the query
            String query_cleaned = query_raw.replaceAll("<[^>]+>", " ").replaceAll("[^a-zA-Z0-9\\s]", " ").toLowerCase();
            Set<String> query_words = new HashSet<String> (Arrays.asList(query_cleaned.split("[\\s|\t]+")));
            query_words.removeAll(STOPWORDS);

            // Stem the query words
            PorterStemmer stemmer = new PorterStemmer();
            Set<String> stemmedQueryWords = new HashSet<>();
            for (String word : query_words) {
                for (char c : word.toCharArray()) {
                    stemmer.add(c);
                }
                stemmer.stem();
                String stemmedWord = stemmer.toString();
                stemmedQueryWords.add(stemmedWord);
                stemmer = new PorterStemmer(); // Reset the stemmer for the next word
            }

            // Compute TF-IDF scores
            Map<String, Map<String, Double>> tf = new HashMap<>();
            Map<String, Double> idf = new HashMap<>();
            Map<String, Double> tf_idf = new HashMap<>();

            int td_freq;
            double idf_val;
            Map<String, Double> td_words;

            Row row;
            for (String word : stemmedQueryWords) {
                row = clientKVS.getRow("pt-index", word);

                if (row == null) {
                    System.out.println("WORD DOES NOT EXIST IN INDEX: " + word);
                    continue;
                }

                String tf_urls = row.get("urls");
                if (tf_urls == null || tf_urls.isEmpty()) {
                    System.out.println("NO URLS FOUND FOR WORD: " + word);
                    continue;
                }

                String[] url_arr = tf_urls.split(",");

                // Normalized TF
                for (String url : url_arr) {
                    String[] url_parts = url.split(":");
                    String url_without_count = String.join(":", Arrays.copyOfRange(url_parts, 0, url_parts.length - 1));
                    String url_hash = Hasher.hash(url_without_count);
                    try {
                        td_freq = Integer.parseInt(url_parts[url_parts.length - 1]);
                    } catch (Exception e) {
                        td_freq = 1;
                    }
                    td_words = tf.getOrDefault(url_hash, new HashMap<>());
                    td_words.put(word, 1 + Math.log10(td_freq));
                    tf.put(url_hash, td_words);
                }

                // IDF from cache
                idf_val = idfCache.getOrDefault(word, 0.0);
                idf.put(word, idf_val);
            }

            // TF - IDF with threshold
            double THRESHOLD = 0.4;
            for(Map.Entry<String, Map<String, Double>> ent : tf.entrySet()) {
                double TF_IDF = 0.0;
                for (Map.Entry<String, Double> wordEnt : ent.getValue().entrySet()) {
                    double tf_val = wordEnt.getValue();
                    idf_val = idf.get(wordEnt.getKey());
                    TF_IDF = TF_IDF + tf_val * idf_val ;
                }

                if (TF_IDF > THRESHOLD) {
                    String trimmedUrlHash = ent.getKey().trim();
                    tf_idf.put(trimmedUrlHash, TF_IDF);
                }
            }

            // Assign a fixed PageRank score of 0.15 to all documents
            Map<String, Double> pr = new HashMap<String, Double>();
            for (String url : tf_idf.keySet()) {
                pr.put(url.trim(), 0.15);
            }

            // Combine TF-IDF scores with PageRank scores
            Map<String, Double> tf_idf_pr  = new HashMap<String, Double>();
            double TF_IDF_WEIGHT = 0.85;
            for (Map.Entry<String, Double> ent : tf_idf.entrySet()) {
                String urlHash = ent.getKey();
                double total_score = 0.15;
                if (pr.containsKey(urlHash)) {
                    total_score *= pr.get(urlHash);
                }
                total_score += TF_IDF_WEIGHT * ent.getValue();
                tf_idf_pr.put(urlHash, total_score);
            }

            // Use a priority queue to store and sort the search results
            PriorityQueue<Map.Entry<String, Double>> pq = new PriorityQueue<>(
                    (a, b) -> Double.compare(b.getValue(), a.getValue())
            );
            pq.addAll(tf_idf_pr.entrySet());

            // Response
            Map<String, Object> res = new HashMap<>();
            int res_count = 0;
            Map<Integer, Map<Integer, List<Map<String, String>>>> groupedResults = new HashMap<>();

            while (!pq.isEmpty() && res_count < 500) {
                Map.Entry<String, Double> ent = pq.poll();
                Map<String, String> obj = new HashMap<>();
                Set<String> resultSet = new HashSet<>();
                String urlHash = ent.getKey();
                if (urlHash == null || urlHash.isEmpty()) {
                    System.out.println("Invalid URL hash: " + urlHash);
                    continue;
                }
                Row crawlRow = clientKVS.getRow("pt-crawl", urlHash);
                if (crawlRow == null) {
                    continue;
                }
                String url = crawlRow.get("url");
                if (url == null) {
                    System.out.println("URL NOT FOUND IN pt-crawl ROW: " + urlHash);
                    continue;
                }
                if (resultSet.contains(url)) {
                    System.out.println("Duplicate URL found: " + url);
                    continue;
                }

                obj.put("url", url.trim());
                String title = crawlRow.get("h1");
                if (title == null && url != null) {
                    title = url;
                }
                obj.put("title", title.trim());
                obj.put("pageid", urlHash.substring(0, 15));

                // Add the search result to the corresponding group based on the number of matching words in content and title
                String extract = crawlRow.get("page");
                int matchingWordsContent = countMatchingWords("", extract, stemmedQueryWords);
                int matchingWordsTitle = countMatchingWords(title, "", stemmedQueryWords);
                if (!groupedResults.containsKey(matchingWordsTitle)) {
                    groupedResults.put(matchingWordsTitle, new HashMap<>());
                }
                Map<Integer, List<Map<String, String>>> contentGroups = groupedResults.get(matchingWordsTitle);
                if (!contentGroups.containsKey(matchingWordsContent)) {
                    contentGroups.put(matchingWordsContent, new ArrayList<>());
                }
                contentGroups.get(matchingWordsContent).add(obj);
                resultSet.add(url.trim());
                res_count++;
            }

            // Final sorted search results
            List<Map<String, String>> sortedResults = new ArrayList<>();

            // Iterate over the title groups in descending order of the number of matching words
            for (int titleMatches = stemmedQueryWords.size(); titleMatches >= 0; titleMatches--) {
                if (groupedResults.containsKey(titleMatches)) {
                    Map<Integer, List<Map<String, String>>> contentGroups = groupedResults.get(titleMatches);

                    // Iterate over the content groups in descending order of the number of matching words
                    for (int contentMatches = stemmedQueryWords.size(); contentMatches >= 0; contentMatches--) {
                        if (contentGroups.containsKey(contentMatches)) {
                            List<Map<String, String>> group = contentGroups.get(contentMatches);
                            sortedResults.addAll(group);
                        }
                    }
                }
            }

            // Add the sorted search results to the response map
            for (int i = 0; i < sortedResults.size(); i++) {
                res.put(String.valueOf(i), sortedResults.get(i));
            }

            resp.header("Access-Control-Allow-Origin", "*");
            resp.type("application/json");
            return new JSONObject(res).toString();
        });

        get("/words", (req, resp) -> {
            StringBuilder res = new StringBuilder();
            res.append("[");
            boolean isFirst = true;

            for (String suggestion : suggestions) {
                if (!isFirst) {
                    res.append(",");
                } else {
                    isFirst = false;
                }
                res.append("\"").append(suggestion).append("\"");
            }
            res.append("]");

            resp.header("Access-Control-Allow-Origin", "*");
            resp.type("application/json");
            return res.toString();
        });

        get("/css/:cssFile", new Route() {
            @Override
            public Object handle(Request request, Response response) {
                String path = "src/frontend/css/" + request.params("cssFile");
                response.type("text/css");
                try {
                    String cssString = Files.readString(Path.of(path));
                    response.body(cssString);
                    return cssString;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        });

        get("/javascript/:jsFile", new Route() {
            @Override
            public Object handle(Request request, Response response) {
                String path = "src/frontend/javascript/" + request.params("jsFile");
                response.type("text/javascript");
                try {
                    String jsString = Files.readString(Path.of(path));
                    response.body(jsString);
                    return jsString;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        });

        get("/data/:jsFile", new Route() {
            @Override
            public Object handle(Request request, Response response) {
                String path = "src/frontend/data/" + request.params("jsFile");
                response.type("text/javascript");
                try {
                    String jsString = Files.readString(Path.of(path));
                    response.body(jsString);
                    return jsString;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        });

        get("/config.json", new Route() {
            @Override
            public Object handle(Request request, Response response) {
                response.type("application/json");
                String filePath = "src/frontend/config.json";
                try {
                    String htmlString = Files.readString(Path.of(filePath));
                    response.body(htmlString);
                    return htmlString;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        });


        get("/", new Route() {
            @Override
            public Object handle(Request request, Response response) {
                response.type("text/html");
                String filePath = "src/frontend/index.html";

                try {
                    String htmlString = Files.readString(Path.of(filePath));
                    response.body(htmlString);
                    return htmlString;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            };
        });
    }
}