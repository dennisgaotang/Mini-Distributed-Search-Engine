package cis5550.kvs;

import cis5550.tools.HTTP;
import cis5550.tools.KeyEncoder;
import cis5550.tools.Logger;
import cis5550.webserver.Server;

import java.io.*;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


import static cis5550.webserver.Server.*;

public class Worker extends cis5550.generic.Worker {
    private static final Logger logger
            = Logger.getLogger(Worker.class);

    private static String workerId;
    private static int port;
    private static String coordinatorAddress;
    private static String storageDirectory;

    private static KVSClient kvsClient;

    private static int THREADSLEEPTIME = 5000; //msec

    private static int RANDOMIDLENGTH = 5;

    private static String RANDOMIDLETTERPOOL = "abcdefghijklmnopqrstuvwxyz";

    private static final int PAGE_SIZE = 10;

    private static final Map<String, ConcurrentHashMap<String, Row>> tables = new ConcurrentHashMap<>();

//    private static Map<String, ConcurrentHashMap<String, ConcurrentHashMap<Integer, Row>>> versionSupportTables = new ConcurrentHashMap<>();
//    private static final Map<String, ReadWriteLock> locks = new ConcurrentHashMap<>();
//    private static final Map<String, ConcurrentHashMap<String, AtomicInteger>> versionMap = new ConcurrentHashMap<>();


    public static void main(String[] args) throws  IOException{
        init(args);

        startPingThread(coordinatorAddress, workerId, port);

        setupDataEndpoints();

        // EC
        startWorkerListUpdateTask();


    }


    private static ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private static List<String> workerAddresses = new ArrayList<>();
    private static void startWorkerListUpdateTask() {
        Runnable task = () -> {
            try {
                String result = new String(HTTP.doRequest("GET", "http://" + coordinatorAddress + "/workers", null).body());
                synchronized (workerAddresses) {
                    workerAddresses.clear();
                    String[] lines = result.split("\n");
                    for (int i = 1; i < lines.length; i++) { // Skip the first line as it's the count
                        String[] parts = lines[i].split(",");
                        if (parts.length > 1) {
                            // Assuming the format is ID,Address
                            workerAddresses.add(parts[1]);
                        }
                    }
                }
//                System.out.println("Successfully updated worker list.");
            } catch (IOException e) {
                System.err.println("Failed to update worker list: " + e.getMessage());
            }
        };
        scheduledExecutorService.scheduleAtFixedRate(task, 0, 5, TimeUnit.SECONDS);
    }

    private static void init(String[] args) {
        try {
            parseCommand(args);
            port(port);
            readOrCreateWorkerId(storageDirectory);
            kvsClient = new KVSClient(coordinatorAddress);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }



    private static void parseCommand(String[] args) throws IllegalArgumentException {

        if (args == null || args.length != 3) {
            throw new IllegalArgumentException("Usage: Worker <port> <storageDir> <coordinatorIP:port>");
        }
        try {
            port = Integer.parseInt(args[0].trim());
            storageDirectory = args[1].trim().replaceAll("^[\u201C\u201D\"“”]+|[\u201C\u201D\"“”]+$", "");
            coordinatorAddress = args[2].trim();
//            System.out.println("worker class");
//            System.out.println("port: " + port);
//            System.out.println("storage directory: " + storageDirectory);
//            System.out.println("coordinator ip : port :" + coordinatorAddress);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port number: " + args[0]);
        }
    }

    private static void readOrCreateWorkerId(String storageDir) {
        File storagePath = new File(storageDir);
        if (!storagePath.exists()) {
            storagePath.mkdirs();
        }

        File idFile = new File(storageDir, "id");
        try {
            if (idFile.exists()) {
                try (BufferedReader reader = new BufferedReader(new FileReader(idFile))) {
                    workerId = reader.readLine();
                }
            } else {
                workerId = generateRandomId(RANDOMIDLENGTH);
                try (PrintWriter out = new PrintWriter(new FileWriter(idFile))) {
                    out.println(workerId);
                }
            }
        } catch (IOException e) {
            System.err.println("Error handling ID file: " + e.getMessage());
            System.exit(1);
        }
    }



    private static String generateRandomId(int length) {
        String letters = RANDOMIDLETTERPOOL;
        StringBuilder id = new StringBuilder(length);
        Random rand = new Random();
        for (int i = 0; i < length; i++) {
            id.append(letters.charAt(rand.nextInt(letters.length())));
        }
        return id.toString();
    }


    private void loadPersistentTableNames() {
        File dir = new File(storageDirectory);
        File[] tableDirs = dir.listFiles(File::isDirectory);
        if (tableDirs != null) {
            for (File tableDir : tableDirs) {
                if (tableDir.getName().startsWith("pt-")) {
                    tables.put(tableDir.getName(), new ConcurrentHashMap<>());
                }
            }
        }
    }

    private static void setupDataEndpoints() throws  IOException{


        //GET endpoint
        get("/data/:T/:R/:C", (req, res) -> {
            String table = req.params("T");
            String rowKey = req.params("R");
            String column = req.params("C");

            Row row = getRow(table, rowKey);

            if (row == null) {
                res.status(404, "Not Found");
                res.header("Content-Length", "0");
                return null;
            }

            String data = row.get(column);
            if (data == null) {
                res.status(404, "Not Found");
                res.header("Content-Length", "0");
                return null;
            }

            res.status(200, "OK");
            res.type("text/plain");
            res.body(data);
            return data;

        });

        // user interface -  list of tables
        get("/", (req, res) -> {
            StringBuilder sb = new StringBuilder("<html><body>");
            sb.append("<h1>Tables</h1>");
            sb.append("<table border='1'><tr><th>Table Name</th><th>Row Count</th></tr>");

            // List in-memory tables
            tables.forEach((name, table) -> {
                sb.append("<tr><td><a href='/view/")
                        .append(name).append("'>").append(name)
                        .append("</a></td><td>")
                        .append(table.size()).append("</td></tr>");
            });

            // List persistent tables by reading directory names
            File storageDir = new File(storageDirectory);
            File[] directories = storageDir.listFiles(File::isDirectory);
            if (directories != null) {
                for (File dir : directories) {
                    if (dir.getName().startsWith("pt-")) {
                        // each file in the directory represents a row
                        long rowCount = Optional.ofNullable(dir.list())
                                .map(files -> Arrays.stream(files).count())
                                .orElse(0L);
                        sb.append("<tr><td><a href='/view/")
                                .append(dir.getName()).append("'>").append(dir.getName())
                                .append("</a></td><td>")
                                .append(rowCount).append("</td></tr>");
                    }
                }
            }

            sb.append("</table></body></html>");
            res.status(200, "OK");
            res.type("text/html");
            res.body(sb.toString());
            return sb.toString();
        });


        //list of tables
        get("/tables", (req, res) -> {
            StringBuilder sb = new StringBuilder();
            // Include in-memory tables
            tables.keySet().forEach(tableName -> sb.append(tableName).append("\n"));
            // Include persistent tables by reading the directory names
            File dir = new File(storageDirectory);
            File[] tableDirs = dir.listFiles(File::isDirectory);
            if (tableDirs != null) {
                for (File tableDir : tableDirs) {
                    if (tableDir.getName().startsWith("pt-")) {
                        sb.append(tableDir.getName()).append("\n");
                    }
                }
            }
            res.status(200, "OK");
            res.type("text/plain");
            res.body(sb.toString());
            return sb.toString();
        });

        // user interface -  list of tables

        get("/view/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            String fromRow = req.queryParams("fromRow");

            Map<String, TreeMap<String, String>> rows = new HashMap<>();
            TreeSet<String> allColumns = new TreeSet<>();

            // Load rows from in-memory or persistent table
            if (tables.containsKey(tableName)) {
                tables.get(tableName).forEach((rowKey, row) -> {
                    TreeMap<String, String> columns = new TreeMap<>();
                    row.columns().forEach(column -> {
                        columns.put(column, row.get(column));
                        allColumns.add(column);
                    });
                    rows.put(rowKey, columns);
                });
            }

            // If it's a persistent table, load rows from storage
            File tableDir = new File(storageDirectory, tableName);
            if (tableDir.exists() && tableDir.isDirectory()) {
                for (File rowFile : Objects.requireNonNull(tableDir.listFiles())) {
                    try (FileInputStream fis = new FileInputStream(rowFile)) {
                        Row row = Row.readFrom(fis);
                        TreeMap<String, String> columns = new TreeMap<>();
                        row.columns().forEach(column -> {
                            columns.put(column, row.get(column));
                            allColumns.add(column);
                        });
                        rows.put(row.key(), columns);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            // Sort keys and apply pagination
            List<String> sortedKeys = new ArrayList<>(rows.keySet());
            Collections.sort(sortedKeys);
            int fromIndex = fromRow == null ? 0 : sortedKeys.indexOf(fromRow);
            if (fromIndex < 0) {
                fromIndex = 0;
            }
            int toIndex = Math.min(fromIndex + PAGE_SIZE, sortedKeys.size());

            // Build HTML response
            StringBuilder sb = new StringBuilder("<html><body>");
            sb.append("<h1>Viewing Table: ").append(tableName).append("</h1>");
            sb.append("<table border='1'><tr><th>Row Key</th>");
            allColumns.forEach(column -> sb.append("<th>").append(column).append("</th>"));
            sb.append("</tr>");

            for (int i = fromIndex; i < toIndex; i++) {
                String key = sortedKeys.get(i);
                TreeMap<String, String> columns = rows.get(key);
                sb.append("<tr><td>").append(key).append("</td>");
                allColumns.forEach(column -> sb.append("<td>").append(columns.getOrDefault(column, "")).append("</td>"));
                sb.append("</tr>");
            }
            sb.append("</table>");

            // Next link for pagination
            if (toIndex < sortedKeys.size()) {
                String nextKey = sortedKeys.get(toIndex);
                sb.append("<a href='/view/").append(tableName).append("?fromRow=").append(nextKey).append("'>Next</a>");
            }

            sb.append("</body></html>");
            res.status(200, "OK");
            res.type("text/html");
            res.body(sb.toString());
            return sb.toString();
        });




        // Whole-row read
        get("/data/:tableName/:rowKey", (req, res) -> {
            String tableName = req.params("tableName");
            String rowKey = req.params("rowKey");

            System.out.println("whole row read:");
            System.out.println("table: " + tableName);
            System.out.println("row: " + rowKey);

            Row row = getRow(tableName, rowKey);
            if (row == null) {
                res.status(404, "Not Found");
                res.header("Content-Length", "0");

                return null;
            }
            res.status(200, "OK");
//            res.type("application/octet-stream");
            res.type("text/plain");

            res.bodyAsBytes(row.toByteArray());

            return new String(row.toByteArray());
        });



        // streaming read
        get("/data/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            String startRow = req.queryParams("startRow");
            String endRowExclusive = req.queryParams("endRowExclusive");

            res.type("text/plain"); // Set content type to plain text
            res.status(200, "OK"); // Set HTTP status to 200

            if (tableName.startsWith("pt-")) {
                // Handle persistent table
                File tableDir = new File(storageDirectory, tableName);
                if (!tableDir.exists() || !tableDir.isDirectory()) {
                    res.status(404, "Not Found");
                    res.header("Content-Length", "0");

                    return null;
                }

                File[] rowFiles = tableDir.listFiles();
                if (rowFiles != null) {
                    Arrays.sort(rowFiles); // Ensure files are processed in order

                    for (File rowFile : rowFiles) {
//                        System.out.println("rowfile: " + rowFile.getName());

                        String key = KeyEncoder.decode(rowFile.getName());
//                        System.out.println("key: " + key);
                        if ((startRow == null || key.compareTo(startRow) >= 0) &&
                                (endRowExclusive == null || key.compareTo(endRowExclusive) < 0)) {

                            try (FileInputStream fis = new FileInputStream(rowFile)) {
                                Row row = Row.readFrom(fis);
                                res.write(row.toByteArray());
                                res.write("\n".getBytes());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            } else {
                // Handle in-memory table
                ConcurrentHashMap<String, Row> table = tables.get(tableName);
                if (table == null) {
                    res.status(404, "Not Found");
                    res.header("Content-Length", "0");

                    return null;
                }

                TreeSet<String> sortedKeys = new TreeSet<>(table.keySet());
                for (String key : sortedKeys) {
                    if ((startRow == null || key.compareTo(startRow) >= 0) &&
                            (endRowExclusive == null || key.compareTo(endRowExclusive) < 0)) {
                        Row row = table.get(key);
                        res.write(row.toByteArray());
                        res.write("\n".getBytes());
                    }
                }
            }

            res.write("\n".getBytes());
            return ""; // return an empty string to complete the method without error
        });





        // Row count
        get("/count/:tableName", (req, res) -> {
            String tableName = req.params("tableName");

            if (tableName.startsWith("pt-")) {
                // Handle persistent table
                File tableDir = new File(storageDirectory, tableName);
                if (!tableDir.exists() || !tableDir.isDirectory()) {
                    res.status(404, "Not Found");
                    res.header("Content-Length", "0");
                    return null;
                }

                File[] rowFiles = tableDir.listFiles();
                if (rowFiles != null) {
                    res.status(200, "OK");
                    res.body("" + rowFiles.length);

                    res.type("text/plain");
                    return "" + rowFiles.length;

                }
                res.status(200, "OK");
                res.body("" + 0);

                res.type("text/plain");
                return "" + 0;
            } else {
                // Handle in-memory table
                ConcurrentHashMap<String, Row> table = tables.get(tableName);
                if (table == null) {
                    res.status(404, "Not Found");
                    res.header("Content-Length", "0");

                    return null;
                }
                res.status(200, "OK");
                res.body("" + table.size());

                res.type("text/plain");
                return "" + table.size();


            }



        });

        put("/data/:T", (req, res) -> {
            String table = req.params("T");

            // Deserialize the incoming Row from the request body
            Row row;
            try {
                row = Row.readFrom(new ByteArrayInputStream(req.bodyAsBytes()));
            } catch (Exception e) {
                System.err.println("Failed to deserialize Row: " + e.getMessage());
                res.status(400, "Bad Request");
                return "Invalid row format";
            }

            if (row == null) {
                res.status(400, "Bad Request");
                return "Invalid row data";
            }

            // Attempt to store the Row in the specified table
            try {
                putRow(table, row);
                res.status(200, "OK");
                return "OK";
            } catch (IOException e) {
                System.err.println("Failed to store Row: " + e.getMessage());
                res.status(500, "Internal Server Error");
                return "Error storing row";
            }
        });

        //PUT endpoint
        put("/data/:T/:R/:C", (req, res) -> {

            String table = req.params("T");
            String rowKey = req.params("R");
            String column = req.params("C");

            byte[] value = req.bodyAsBytes();

            // EC: Conditional PUT
//            String ifColumn = req.queryParams("ifcolumn");
//            String equals = req.queryParams("equals");
//            if (ifColumn != null && equals != null) {
//                Row row = getRow(table, rowKey);
//                if (row == null || !conditionalPutCheck(row, ifColumn, equals)) {
//                    res.status(200, "OK");
//                    return "FAIL"; //??????
//                }
//            }

            putRow(table, rowKey, column, value);

            res.status(200, "OK");

//            try {
//
//                forwardPutRequestAsync(table, rowKey, column, value);
//
//
//            } catch (Exception e) {
//                System.out.println(e.getMessage());
//            }

            return "OK";
        });



        //PUT endpoint without replication
        put("/data/:T/:R/:C/noReplicate", (req, res) -> {
            String table = req.params("T");
            String rowKey = req.params("R");
            String column = req.params("C");
            byte[] value = req.bodyAsBytes();

            // Perform the PUT operation without forwarding to other workers
            putRow(table, rowKey, column, value);

            res.status(200, "OK");
            return "OK";
        });

        // Rename
        put("/rename/:oldName", (req, res) -> {
            String oldName = req.params("oldName");
            String newName = req.body().trim();

            // Check if newName already exists
            if (tables.containsKey(newName) || new File(storageDirectory, newName).isDirectory()) {
                res.status(409, "Conflict");
                return "Table " + newName + " already exists.";
            }

            // Check for pt- prefix mismatch
            if (newName.startsWith("pt-") != oldName.startsWith("pt-")) {
                res.status(400, "Bad Request");
                return "Inconsistent table type: persistent and in-memory table naming conflict.";
            }

            // Handle in-memory table rename
            if (!oldName.startsWith("pt-")) {
                ConcurrentHashMap<String, Row> table = tables.remove(oldName);
                if (table == null) {
                    res.status(404, "Not Found");
                    res.header("Content-Length", "0");

                    return null;
                }
                tables.put(newName, table);
            } else {
                // Handle persistent table rename
                File oldDir = new File(storageDirectory, oldName);
                File newDir = new File(storageDirectory, newName);

                if (!oldDir.exists()) {
                    res.status(404, "Not Found");
                    res.header("Content-Length", "0");

                    return null;
                }

                try {
                    Files.move(oldDir.toPath(), newDir.toPath(), StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    e.printStackTrace();
                    res.status(500, "Internal Server Error");
                    return "Failed to rename " + oldName + " to " + newName + ".";
                }
            }

            res.status(200, "OK");
            return "OK";
        });


        //delete
        put("/delete/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            // Check if the table is an in-memory table
            if (!tableName.startsWith("pt-") && tables.containsKey(tableName)) {
                tables.remove(tableName);
                res.status(200, "OK");
                res.header("Content-Length", "0");
                return "OK";
            } else if (tableName.startsWith("pt-")) {
                // Handle persistent tables
                File tableDir = new File(storageDirectory, tableName);
                if (tableDir.exists() && tableDir.isDirectory()) {
                    // Delete all files in the directory and then the directory itself
                    File[] files = tableDir.listFiles();
                    if (files != null) {
                        for (File file : files) {
//                            file.delete();
                            if (!file.delete()) {
                                res.status(500, "Internal Server Error");
                                String message = "Failed to delete file: " + file.getName();
                                res.header("Content-Length", String.valueOf(message.length()));
                                return message;
                            }
                        }
                    }
//                    tableDir.delete();
//                    res.status(200, "OK");
//                    return "OK";
                    if (!tableDir.delete()) {
                        res.status(500, "Internal Server Error");
                        res.header("Content-Length", "0");
                        return null;
                    }
                    res.status(200, "OK");
                    res.header("Content-Length", "0");
                    return "OK";
                } else {
                    // Table does not exist
                    res.status(404, "Not Found");
                    res.header("Content-Length", "0");
                    return null;
                }
            } else {
                // Table does not exist in either in-memory or persistent storage
                res.status(404, "Not Found");
                res.header("Content-Length", "0");
                return null;
            }
        });



    }

    // EC: Conditional PUT
    private static boolean conditionalPutCheck(Row row, String ifColumn, String equalsValue) {
        byte[] columnData = row.getBytes(ifColumn);
        if (columnData == null) {
            return false;
        }
        String columnValue = new String(columnData);
        return columnValue.equals(equalsValue);
    }


    private static Row getRow(String tableName, String rowKey) throws IOException{
        // Check if it's a persistent table
        if (tableName.startsWith("pt-")) {

            System.out.println("get row: " + tableName);
            System.out.println("row key: " + rowKey);
            String encodedKey = KeyEncoder.encode(rowKey);

            System.out.println("encoded key: " + encodedKey);
            File tableDir = new File(storageDirectory, tableName);
            File rowFileDir = tableDir; // Default to the table directory
//            EC: subdirectories
//            if (encodedKey.length() >= 6) {
//                String subDirName = "__" + encodedKey.substring(0, 2);
//                rowFileDir = new File(tableDir, subDirName);
//            }
            File rowFile = new File(rowFileDir, encodedKey);
            if (!rowFile.exists()) {
                return null;
            }
            try (FileInputStream fis = new FileInputStream(rowFile)) {
                return Row.readFrom(fis);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }

        } else {
            // Handle in-memory tables (non-persistent)
            ConcurrentHashMap<String, Row> table = tables.get(tableName);
            if (table != null) {
                return table.get(rowKey);
            }
            return null;


        }
    }


    private static void putRow(String tableName, Row row) throws IOException {
        // Check if the table name indicates a persistent table
        if (tableName.startsWith("pt-")) {

            System.out.println("putRow: " + tableName);
            System.out.println("rowkey: " + row.key());

            File tableDir = new File(storageDirectory, tableName);
            if (!tableDir.exists()) {
                if (!tableDir.mkdirs()) {
                    throw new IOException("Failed to create directory for table: " + tableName);
                }
            }
            String encodedKey = KeyEncoder.encode(row.key());
            System.out.println("encodedKey: " + encodedKey);


            File rowFile = new File(tableDir, encodedKey);

//            // EC: subdirectories
//            String encodedKey = KeyEncoder.encode(row.key());
//            // Determine the directory path based on the key's length
//            File tableDir = new File(storageDirectory, tableName);
////            System.out.println("tableDir" +
////                    ": " + tableDir);
//            File rowFileDir = tableDir; // Default to the table directory
//            if (encodedKey.length() >= 6) {
//                String subDirName = "__" + encodedKey.substring(0, 2);
//                rowFileDir = new File(tableDir, subDirName);
//            }
//            if (!rowFileDir.exists()) {
//                rowFileDir.mkdirs(); // Create the subdirectory if it doesn't exist
//            }
//
//            File rowFile = new File(rowFileDir, encodedKey);


            try (FileOutputStream fos = new FileOutputStream(rowFile)) {
//                System.out.println("row string: " + new String(row.toByteArray()));
                fos.write(row.toByteArray());
            } catch (IOException e) {
                throw new IOException("Failed to write row to disk", e);
            }
        } else {
            // Handle in-memory tables
            tables.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>()).put(row.key(), row);
        }
    }

    private static void putRow(String tableName, String rowKey, String column, byte[] value) throws IOException {
        System.out.println("putRow: " + tableName);
        System.out.println("rowkey: " + rowKey);
        System.out.println("column: " + column);

        if (tableName.startsWith("pt-")) {

//            // EC: subdirectories
//            String encodedKey = KeyEncoder.encode(rowKey);
//            // Determine the directory path based on the key's length
//            File tableDir = new File(storageDirectory, tableName);
////            System.out.println("tableDir" +
////                    ": " + tableDir);
//            File rowFileDir = tableDir; // Default to the table directory
//            if (encodedKey.length() >= 6) {
//                String subDirName = "__" + encodedKey.substring(0, 2);
//                rowFileDir = new File(tableDir, subDirName);
//            }
//            if (!rowFileDir.exists()) {
//                rowFileDir.mkdirs(); // Create the subdirectory if it doesn't exist
//            }
////            System.out.println("encodeKey: " + encodedKey);
////            System.out.println("rowFileDir" +
////                    ": " + rowFileDir);
//
//            File rowFile = new File(rowFileDir, encodedKey);
//            System.out.println("file creation successful? " + rowFile.exists());; // Create the directory if it doesn't exist
//            System.out.println("file path: " + rowFile.getAbsolutePath());; // Create the directory if it doesn't exist

            File tableDir = new File(storageDirectory, tableName);
            if (!tableDir.exists()) {
                if (!tableDir.mkdirs()) {
                    throw new IOException("Failed to create directory for table: " + tableName);
                }
            }

            String encodedKey = KeyEncoder.encode(rowKey);
            System.out.println("encodedKey: " + encodedKey);


            File rowFile = new File(tableDir, encodedKey);

            try (FileOutputStream fos = new FileOutputStream(rowFile, false)) {

                Row row = kvsClient.getRow(tableName, encodedKey);
                if (row == null) {
                    row = new Row(encodedKey);
                }
                row.put(column, value);
                fos.write(row.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
            }


        } else {
            tables.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>());
            Row row = tables.get(tableName).computeIfAbsent(rowKey, k -> new Row(rowKey));
            synchronized (row) {
                row.put(column, value);
            }



        }




    }


    // Example of making replication asynchronous
    private static void forwardPutRequestAsync(String tableName, String rowKey, String column, byte[] value) {
        new Thread(() -> forwardPutRequest(tableName, rowKey, column, value)).start();
    }


    //EC: Replication
    private static void forwardPutRequest(String tableName, String rowKey, String column, byte[] value) {
        try {
            int totalWorkers = kvsClient.numWorkers();
            int currentWorkerIndex = -1;
            for (int i = 0; i < totalWorkers; i++) {
                if (kvsClient.getWorkerID(i).equals(workerId)) {
                    currentWorkerIndex = i;
                    break;
                }
            }

            // Error handling if current worker is not found
            if (currentWorkerIndex == -1) {
                throw new IllegalStateException("Current worker ID not found in the list.");
            }

            //!!!!!! dont forward further /data/:T/:R/:C/noReplicate
            // Calculate and forward to the next N workers
            for (int i = 1; i <= 2; i++) { // Assuming N=2 for next two workers
                int nextWorkerIndex = (currentWorkerIndex + i) % totalWorkers;
                String nextWorkerAddress = kvsClient.getWorkerAddress(nextWorkerIndex);
                // Construct the URL for the next worker
                String targetURL = "http://" + nextWorkerAddress + "/data/" + URLEncoder.encode(tableName, "UTF-8") + "/" + URLEncoder.encode(rowKey, "UTF-8") + "/" + URLEncoder.encode(column, "UTF-8") + "/noReplicate";

                // Forward the PUT request
                HTTP.doRequest("PUT", targetURL, value);
            }
        } catch (Exception e) {
            System.err.println("Error forwarding PUT request: " + e.getMessage());
        }
    }








}
