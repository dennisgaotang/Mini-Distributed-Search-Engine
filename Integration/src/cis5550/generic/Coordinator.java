package cis5550.generic;

import cis5550.tools.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static cis5550.webserver.Server.*;
public class Coordinator {
    private static final Logger logger
            = Logger.getLogger(Coordinator.class);

    protected static Map<String, WorkerEntry> activeWorkers = new ConcurrentHashMap<>();

    protected static int WORKERTIMEOUT = 15;


    protected static void registerRoutes() throws IOException {

        get("/ping", (req, res) -> {
            String id = req.queryParams("id");
            String portStr = req.queryParams("port");
            if (id == null || portStr == null) {
                res.status(400, "Bad Request");
                return "ID and/or port number are missing";
            }

            try {
                int port = Integer.parseInt(portStr);
                String ip = req.ip();
                activeWorkers.put(id, new WorkerEntry(ip, port, Instant.now()));
                res.status(200, "OK");
                return "OK";
            } catch (NumberFormatException e) {
                res.status(400, "Bad Request");
                return "Invalid port number";
            }
        });

        get("/workers", (req, res) -> {
            // Prune inactive workers first
            pruneInactiveWorkers();

            int k = activeWorkers.size();
            StringBuilder response = new StringBuilder(k + "\n");
            activeWorkers.forEach((id, workerEntry) -> response.append(id).append(",").append(workerEntry).append("\n"));

            return response.toString();
        });
    }

    private static void pruneInactiveWorkers() {
        Instant cutoff = Instant.now().minusSeconds(WORKERTIMEOUT);
        activeWorkers.entrySet().removeIf(entry -> entry.getValue().getLastPing().isBefore(cutoff));
    }

    public static String workerTable() {
        List<String> workers = getWorkers(); // Use getWorkers() to retrieve the list of active workers
        StringBuilder sb = new StringBuilder("<html><head><title>KVS Coordinator</title></head><body>");
        sb.append("<h1>Active Workers</h1>");
        sb.append("<table border=\"1\"><tr><th>ID</th><th>Link</th></tr>"); // change 1 !!!

        workers.forEach(worker -> {
            String[] parts = worker.split(",");
            String ip = parts[1].split(":")[0];
            String port = parts[1].split(":")[1];
            String link = "http://" + ip + ":" + port + "/";
            sb.append("<tr><td>").append(parts[0]) // Worker ID
                    .append("</td><td><a href='").append(link).append("'>").append(link).append("</a></td></tr>");
        });

        sb.append("</table></body></html>");
        return sb.toString();
    }

//    public static List<String> getWorkers() {
//        return activeWorkers.entrySet().stream()
//                .map(entry -> entry.getKey() + "," + entry.getValue())
//                .collect(Collectors.toList());
//    }

    public static Vector<String> getWorkers() {
        Vector<String> result = new Vector<>();
        Iterator<String> it = activeWorkers.keySet().iterator();

        while (it.hasNext()) {
            String id = it.next();
            WorkerEntry worker = activeWorkers.get(id);
            result.add(worker.ip + ":" + worker.port);
        }

        return result;
    }

    // WorkerEntry class to store details about each worker
    static class WorkerEntry {
        private String ip;
        private int port;
        private Instant lastPing;

        WorkerEntry(String ip, int port, Instant lastPing) {
            this.ip = ip;
            this.port = port;
            this.lastPing = lastPing;
        }

        public String getIp() {
            return ip;
        }

        public int getPort() {
            return port;
        }

        public Instant getLastPing() {
            return lastPing;
        }

        @Override
        public String toString() {
            return ip + ":" + port;
        }
    }



}
