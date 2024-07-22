package cis5550.webserver;

import cis5550.generic.Worker;
import cis5550.tools.Logger;
import cis5550.tools.SNIInspector;

import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import java.security.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class Server implements Runnable {
    private static final Logger logger
            = Logger.getLogger(Server.class);

    private static final int TIMEOUT = 1000;

    private static final int WAITTIME = 10;

    private static final int BUFFER_SIZE = 1024;

    private static final int NUM_WORKERS = 100; // Number of worker threads
    private static String fileDirectory = null;

    private static Server instance = null;
    private static boolean isRunning = false; // flag


    private static boolean isLocationCalled = false;
    private static int serverPort = 80; // Default port is 80
    private static int sniPort = 5000; // sni port is 5000

    private static List<RouteEntry> routeTable = new ArrayList<>();

    private static Map<String, Set<String>> staticFilesLocations = new HashMap<>(); // EC
    private static String currentHost = null; // EC

    private static int httpsPort = -1; // Default value indicating not set

    public static boolean isHttpsEnabled() {
        return httpsEnabled;
    }

    private static boolean httpsEnabled = false;

    public static void securePort(int port) {
        httpsPort = port;
        httpsEnabled = true;
    }

    private static final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();

    // remove expried sessions
    private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private static void removeExpiredSessions() {
        scheduler.scheduleAtFixedRate(() -> {
            for (Map.Entry<String, Session> entry : sessions.entrySet()) {
                Session session = entry.getValue();
                if (session.isExpired()) {
                    sessions.remove(entry.getKey());
//                    System.out.println("Removed expired session: " + entry.getKey());
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }


    public static ConcurrentHashMap<String, Session> getSessions() {
        return sessions;
    }

    // EC: multi host
    public static void host(String hostname, String keystore, String password) {
        currentHost = hostname; // Set the current host for subsequent route and static file registrations
        // Initialize SSLContext for this host
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(new FileInputStream(keystore), password.toCharArray());
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
            keyManagerFactory.init(keyStore, password.toCharArray());
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), null, new SecureRandom());
            sslContexts.put(hostname, sslContext);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // EC:  host-specific SSL configurations
    private static Map<String, SSLContext> sslContexts = new HashMap<>();


    private static class RouteEntry {
        String method;
        String pathPattern;
        Route handler;
        String host; //EC

        RouteEntry(String method, String pathPattern, Route handler) {
            this.method = method;
            this.pathPattern = pathPattern;
            this.handler = handler;
            this.host = null;
        }

        RouteEntry(String method, String pathPattern, Route handler, String host) {
            this.method = method;
            this.pathPattern = pathPattern;
            this.handler = handler;
            this.host = host;
        }
    }

    // Static class for managing static files
    public static class staticFiles {
        public static void location(String path) {
            isLocationCalled = true;

            if (currentHost != null) {
                Set<String> hostList = staticFilesLocations.get(currentHost);
                if (hostList == null) {
                    hostList = new HashSet<>();
                }
                hostList.add(path);
                staticFilesLocations.put(currentHost, hostList);
            }

            fileDirectory = path;
            initializeServer();
        }
    }

    // Static method to set the port
    public static void port(int port) throws IOException {
        serverPort = port;
    }

    // Static methods for route handling
    public static void get(String path, Route route) throws IOException {
        routeTable.add(new RouteEntry("GET", path, route, currentHost));
        initializeServer();
    }

    public static void post(String path, Route route) throws IOException {
        routeTable.add(new RouteEntry("POST", path, route, currentHost));
        initializeServer();
    }

    public static void put(String path, Route route) throws IOException {
        routeTable.add(new RouteEntry("PUT", path, route, currentHost));
        initializeServer();
    }

    // Helper method to initialize the server
    private static void initializeServer() {
        if (instance == null) {
            instance = new Server();
            if (!isRunning) {
                isRunning = true;
                new Thread(() -> {
                    instance.run();
                }).start();
            }
            removeExpiredSessions();
        }
    }

    private static enum Method {
        GET, HEAD, POST, PUT
    }


    private static BlockingQueue<Socket> socketQueue = new LinkedBlockingQueue<>();



    public static String byteArrayToString(byte[] byteArray) throws IOException {
        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(byteArray);
             BufferedReader reader = new BufferedReader(new InputStreamReader(byteStream))) {
            String line;
            StringBuilder requestBuilder = new StringBuilder();

            while ((line = reader.readLine()) != null && !line.isEmpty()) {
                requestBuilder.append(line).append("\r\n");
            }
            return requestBuilder.toString();
        } catch (IOException e) {
            logger.error("Exception caught when converting byte array to string");
            logger.error(e.getMessage());
        }
        return null;
    }


    private static void sendError(Socket clientSocket, int code) throws IOException {
        String reasonPhrase;
        switch (code) {
            case 400:
                reasonPhrase = "Bad Request";
                break;
            case 403:
                reasonPhrase = "Forbidden";
                break;
            case 404:
                reasonPhrase = "Not Found";
                break;
            case 405:
                reasonPhrase = "Not Allowed";
                break;
            case 500:
                reasonPhrase = "Internal Server Error";
                break;
            case 501:
                reasonPhrase = "Not Implemented";
                break;
            case 505:
                reasonPhrase = "HTTP Version Not Supported";
                break;
            default:
                reasonPhrase = "";
        }
        String response = String.format("HTTP/1.1 %d %s\r\nContent-Type: text/plain\r\n\r\n%d %s",
                code, reasonPhrase, code, reasonPhrase);

        try (PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
            out.print(response);
            out.flush();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }


    private static class Header {
        public String method = null;
        public String url = null;
        public String protocol = null;
        public Map<String, String> content = new HashMap<>();

        public Header(String method, String url, String protocol, Map content) {
            this.method = method;
            this.url = url;
            this.protocol = protocol;
            this.content = content;
        }
    }


    private static byte[] getBody(Socket client, int contentLength, int timeoutMillis) throws IOException {
        if (client == null || contentLength <= 0) {
            throw new IllegalArgumentException("Client socket is null or content length is invalid");
        }

        InputStream in = client.getInputStream();
        byte[] body = new byte[contentLength];
        int bytesRead = 0;
        long endTimeMillis = System.currentTimeMillis() + timeoutMillis;

        while (bytesRead < contentLength && System.currentTimeMillis() < endTimeMillis) {
            int available = in.available();
            if (available > 0) {
                int bytesJustRead = in.read(body, bytesRead, Math.min(contentLength - bytesRead, available));
                if (bytesJustRead == -1) { // End of stream reached
                    break;
                }
                bytesRead += bytesJustRead;
            } else {
                try {
                    Thread.sleep(WAITTIME); // Wait a bit for more data to become available
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore interrupt status
                    throw new IOException("Thread interrupted while reading request body", e);
                }
            }
        }

        if (bytesRead < contentLength) {
            logger.warn("Timeout or end of stream reached before reading expected content length. Bytes read: " + bytesRead + " of " + contentLength);
        }

        return Arrays.copyOf(body, bytesRead); // Return the actual amount of data read, which may be less than contentLength
    }


    public static class DateHandler {

        private static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";

        public static String formatDate(Date date) {
            try {
                SimpleDateFormat formatter = new SimpleDateFormat(HTTP_DATE_FORMAT);
                formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
                return formatter.format(date);
            } catch (Exception e) {
                return null;
            }
        }

        public static Date stringToDate(String date) {
            try {
                SimpleDateFormat formatter = new SimpleDateFormat(HTTP_DATE_FORMAT);
                formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
                return formatter.parse(date);
            } catch (Exception e) {
                return null;
            }
        }

        public static boolean isDateValid(String date) {
            try {
                SimpleDateFormat formatter = new SimpleDateFormat(HTTP_DATE_FORMAT);
                formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
                Date formatedDate = formatter.parse(date);

                // Compare with current time
                return !formatedDate.after(new Date());
            } catch (Exception e) {
                return false;
            }
        }

    }

    private static boolean pathMatch(String path, String pattern, Map<String, String> params) {
        String[] pathParts = path.split("/");
        String[] patternParts = pattern.split("/");
        if (pathParts.length != patternParts.length) {
            return false;
        }
        for (int i = 0; i < patternParts.length; i++) {
            if (patternParts[i].startsWith(":")) {
                String paramName = patternParts[i].substring(1);
                params.put(paramName, pathParts[i]);
            } else if (!patternParts[i].equals(pathParts[i])) {
                return false;
            }
        }
        return true;
    }


    private static void parseQueryParams(String query, Map<String, String> queryParams) {
//        System.out.println("query: " + query);


        if (query == null || query.isEmpty()) {
            return;
        }
        String[] pairs = query.trim().split("&");
        for (String pair : pairs) {
//            System.out.println("pair: " + pair);
            int idx = pair.indexOf("=");
            if (idx > -1 && pair.length() > idx + 1) {
                try {
                    String key = URLDecoder.decode(pair.substring(0, idx).trim(), "UTF-8");
                    String value = URLDecoder.decode(pair.substring(idx + 1).trim(), "UTF-8");
                    queryParams.put(key, value);
//                    System.out.println("key: " + key + "; value: " + value);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException("Error decoding query parameter", e);
                }
            }
        }
    }


    private static boolean handleDynamicRequest(Header header, byte[] body, Socket clientSocket, OutputStream outputStream, Session session) throws IOException {
//        System.out.println("handle dynamic request");

        String method = header.method;
        String requestedFile = header.url;
        String hostHeader = header.content.get("host");

        // EC host
        String host = null;
        if (hostHeader != null) {
            host = hostHeader.split(":")[0]; // Remove port number if present
        }


//        System.out.println("method: ");
//        System.out.println(method);
//        System.out.println("url: ");
//        System.out.println(requestedFile);

        Map<String, String> queryParams = new HashMap<>();
        Map<String, String> params = new HashMap<>();

        String queryString = null;
        String url = requestedFile;
        if (requestedFile.contains("?")) {
            url = requestedFile.substring(0, requestedFile.indexOf('?'));
            queryString = requestedFile.substring(requestedFile.indexOf("?") + 1);
        }

        parseQueryParams(queryString, queryParams);


        String contentType = header.content.get("content-type");
        if (body != null && "application/x-www-form-urlencoded".equals(contentType)) {

            parseQueryParams(byteArrayToString(body), queryParams);
        }


        for (RouteEntry entry : routeTable) {
            if ((staticFilesLocations.get(host) == null || entry.host.equalsIgnoreCase(host)) && entry.method.equalsIgnoreCase(method) && pathMatch(url, entry.pathPattern, params)) {
//
//                for (Map.Entry<String, String> param : queryParams.entrySet()) {
//                    System.out.println(param.getKey());
//                    System.out.println(param.getValue());
//
//                }
//
//                for (Map.Entry<String, String> param : params.entrySet()) {
//                    System.out.println(param.getKey());
//                    System.out.println(param.getValue());
//
//                }

                InetSocketAddress remoteAddr = (InetSocketAddress) clientSocket.getRemoteSocketAddress();
                RequestImpl request = new RequestImpl(header.method, header.url, header.protocol, header.content, queryParams, params, remoteAddr, body, instance);

//                // create session if request does not contain cookie header
//                if (session == null || session.isExpired()) {
//                    request.session();
//                }

//        correction 2!!!


//                System.out.println("is generator called? " + request.isSessionGeneratorCalled());

//                ResponseImpl response = new ResponseImpl(outputStream, instance, request.getCurrentSession(), request.isSessionGeneratorCalled());

                ResponseImpl response = new ResponseImpl(outputStream, instance, request);
                //        correction 3!!!

//                System.out.println("position 7");
//                System.out.println(clientSocket.isClosed());

                try {


                    // Handle dynamic request
                    Object result = entry.handler.handle(request, response);


                    if (!response.isWriteCalled()) {
//                        System.out.println("write not called, using commit");
                        if (result != null) {
                            // If route handler returns a value, consider it as the response body (if not already set)
                            response.body(result.toString());
                        }
                        // Commit the response (write headers and body if applicable)
                        response.commit();

                    }

                    else {
                        clientSocket.close();
                    }

//                    System.out.println("position 8");
//                    System.out.println(clientSocket.isClosed());


                } catch (Exception e) {
//                    System.out.println(e.getMessage());
                    // Handle exceptions by possibly logging and sending a 500 Internal Server Error response
                    System.out.println(("Exception handling request: " + e.getMessage()));
                    if (!response.isWriteCalled()) {
                        System.out.println("write not called, error 500");
                        sendError(clientSocket, 500);
                    } else {
                        System.out.println("error, socket closed");
                        clientSocket.close();
                    }

                    return true;

                }


                return true;

            }
        }

//        System.out.println("position 3");
//        System.out.println(clientSocket.isClosed());

        return false;

    }

    private static boolean handleStaticRequest(Header header, byte[] body, Socket clientSocket, String fileDirectory, OutputStream outputStream, PrintWriter writer, Session session) throws IOException {

//        System.out.println("handle static request");


        String method = header.method;
        String requestedFile = header.url;


        InetSocketAddress remoteAddr = (InetSocketAddress) clientSocket.getRemoteSocketAddress();

        RequestImpl request = new RequestImpl(header.method, header.url, header.protocol, header.content, remoteAddr, body, instance);

//        // create session if request does not contain cookie header
//        if (session == null || session.isExpired()) {
//            request.session();
//        }
//        correction 1!!!

        session = request.getCurrentSession();


//        System.out.println("is generator called? " + request.isSessionGeneratorCalled());


        File file = new File(fileDirectory, requestedFile);

        if (file.isFile()) {
            if (!file.canRead()) {
                sendError(clientSocket, 403);
                return false;
            }


            if (method.equalsIgnoreCase("get") || method.equalsIgnoreCase("head")) {
                // 304 Not Modified
                String date = header.content.get("if-modified-since");
                if (date != null && DateHandler.isDateValid(date)) {


                    Date lastModifiedDate = new Date(file.lastModified());
                    Date requestDate = DateHandler.stringToDate(date);

                    if (!lastModifiedDate.after(requestDate)) {

                        writer.print("HTTP/1.1 304 Not Modified\r\n");
                        writer.print("Date: " + DateHandler.formatDate(new Date()) + "\r\n");
                        writer.print("Content-Type: " + getContentType(requestedFile) + "\r\n");
                        writer.print("Content-Length: " + file.length() + "\r\n");
                        writer.print("Server: localhost\r\n");


                        if (session != null && request.isSessionGeneratorCalled()) {
//                            String cookieHeader = "Set-Cookie: SessionID=" + session.id() + "; Path=/; HttpOnly";
                            // EC cookie secured
                            String cookieHeader = "Set-Cookie: SessionID=" + session.id() +
                                    "; Path=/" +
                                    "; HttpOnly" +
                                    (httpsEnabled ? "; Secure" : "") +
                                    "; SameSite=Lax";

                            outputStream.write((cookieHeader + "\r\n").getBytes());


                        }

                        writer.print("\r\n"); // End of header section
                        writer.flush();
                        return true;
                    }


                }

            }


            // Send HTTP Headers using PrintWriter
            writer.print("HTTP/1.1 200 OK\r\n");
            writer.print("Content-Type: " + getContentType(requestedFile) + "\r\n");
            writer.print("Content-Length: " + file.length() + "\r\n");
            writer.print("Server: localhost\r\n");
            if (session != null && request.isSessionGeneratorCalled()) {
                String cookieHeader = "Set-Cookie: SessionID=" + session.id() + "; Path=/; HttpOnly";
                outputStream.write((cookieHeader + "\r\n").getBytes());
            }
            writer.print("\r\n"); // End of header section

            writer.flush();

//            System.out.println("position 11");
//            System.out.println(clientSocket.isClosed());

            if (method.equalsIgnoreCase("get")) {

                if (!isLocationCalled) {
                    return false;
                }

                // Send file content using OutputStream
                try {
                    FileInputStream fileIn = new FileInputStream(file);
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int count;
                    while ((count = fileIn.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, count);
                    }
                    outputStream.flush();

//                    System.out.println("position 9");
//                    System.out.println(clientSocket.isClosed());

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }


            }

//            System.out.println("position 12");
//            System.out.println(clientSocket.isClosed());
            return true;
        } else {
            sendError(clientSocket, 404);
            System.out.println("404 NOT FOUND");

            return false;
        }
    }

    private static void handleRequest(Header header, byte[] body, Socket clientSocket, String fileDirectory, OutputStream outputStream, PrintWriter writer) throws IOException {

//        System.out.println("handle request");

        if (header == null) {
            return;
        }

        String method = header.method;
        String requestedFile = header.url;


        if (method == null || requestedFile == null) {
            sendError(clientSocket, 400);
            return;
        }

        Session session = getSessionFromCookies(header.content);

        boolean handled = handleDynamicRequest(header, body, clientSocket, outputStream, session);

//        System.out.println("position 10");
//        System.out.println(clientSocket.isClosed());

        if (!handled) {
            handled = handleStaticRequest(header, body, clientSocket, fileDirectory, outputStream, writer, session);
        }

        if (!handled) {
            // No matching route or static file found, send 404
            sendError(clientSocket, 404);
            System.out.println("404 NOT FOUND");
        }

//        System.out.println("position 6");
//        System.out.println(clientSocket.isClosed());
    }

    // Method to parse cookies and retrieve session
    private static Session getSessionFromCookies(Map<String, String> headers) {
        String cookieHeader = headers.get("cookie");
        if (cookieHeader != null && !cookieHeader.isEmpty()) {
            String[] cookies = cookieHeader.split(";");
            for (String cookie : cookies) {
                String[] cookieParts = cookie.trim().split("=");
                if (cookieParts.length == 2 && "SessionID".equals(cookieParts[0])) {
                    String sessionId = cookieParts[1];
                    Session session = sessions.get(sessionId);
                    if (session != null) {
                        // Update last accessed time
                        if (!session.isExpired()) {
                            session.setLastAccessedTime(System.currentTimeMillis());
                        }
                        return session;
                    }
                }
            }
        }
        return null;
    }


    private static Header parseHeader(String request, Socket clientSocket, PrintWriter writer) throws IOException {
        String[] requestLines = request.split("\r\n");
        int count = 0;
        boolean isHostPresent = false;

        while (count < requestLines.length) {

            String[] requestLine = requestLines[count].split(" ");

            // find header start
            if (requestLine.length != 3) {
                sendError(clientSocket, 400);
                count += 1;
                continue;
            }

            // get
            String method = requestLine[0];
            String url = requestLine[1];
            String protocol = requestLine[2];
//            System.out.println("header:");
//            System.out.println(method);
//            System.out.println(url);
//            System.out.println(protocol);


            //check whether it's supported method
            if (method.equalsIgnoreCase("get") || method.equalsIgnoreCase("head") || method.equalsIgnoreCase("put") || method.equalsIgnoreCase("post")) {

                // whether HTTP version is supported
                if (!protocol.equalsIgnoreCase("http/1.1")) {
                    sendError(clientSocket, 505);
                    count += 1;
                    continue;
                }

                if (url.contains("..")) {
                    sendError(clientSocket, 403);
                    count += 1;
                    continue;
                }

                // get the rest of the header
                count += 1;
                HashMap<String, String> map = new HashMap<>();


//                System.out.println("request: ");
                while (count < requestLines.length) {
                    requestLine = requestLines[count].split(":");
                    map.put(requestLine[0].trim().toLowerCase(), requestLine[1].trim());
//                    System.out.println(requestLine[0].trim().toLowerCase());
//                    System.out.println(requestLine[1].trim());
                    if (requestLine[0].equalsIgnoreCase("host")) {
                        isHostPresent = true;
                    }
                    count += 1;
                }

                if (!isHostPresent) {
                    sendError(clientSocket, 400);
                    return null;
                }

                return new Header(method, url, protocol, map);


            } else {

                sendError(clientSocket, 501);
                count += 1;
                continue;
            }


        }


        return null;
    }


    private static String getContentType(String file) {
        if (file.endsWith(".txt")) {
            return "text/plain";
        } else if (file.endsWith(".html")) {
            return "text/html";
        } else if (file.endsWith(".jpg") || file.endsWith(".jpeg")) {
            return "image/jpeg";
        }
        return "application/octet-stream";
    }


    private void processClientRequest(Socket clientSocket) {
//        System.out.println("process client request");
        try {

            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream();
            PrintWriter writer = new PrintWriter(outputStream, true);

            while (!clientSocket.isClosed()) {


                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                int b;
                Queue<Byte> q = new ArrayDeque<>();


                // Read request as bytes and look for double CRLF
                while ((b = inputStream.read()) != -1) {
//                                System.out.println("reading...");

                    buffer.write(b);
                    q.add((byte) b);

                    // Check for double CRLF (bytes 13, 10, 13, 10)
                    if (q.size() == 4) {
                        Byte[] lastFour = q.toArray(new Byte[4]);
                        if (lastFour[0] == 13 && lastFour[1] == 10 &&
                                lastFour[2] == 13 && lastFour[3] == 10) {
                            q.clear();
                            break;
                        }
                        q.poll();
                    }
                }

                if (b == -1 && buffer.size() == 0) {
                    break;
                }


                // Convert headers to string
                String request = byteArrayToString(buffer.toByteArray());

                logger.info(request);

                if (request == null) {
                    continue;
                }

                // Parse request and determine file to serve
                Header header = parseHeader(request, clientSocket, writer);

                if (header == null) {
                    continue;
                }


                // Check whether content length is specified in header
                String contentLength = header.content.get("content-length");
                byte[] body = null;
                if (contentLength != null) {
                    // read content body
                    int length = Integer.parseInt(contentLength);
                    body = getBody(clientSocket, length, TIMEOUT);
                    String bodyStr = byteArrayToString(body);
                    logger.info(bodyStr);

                }


                // Process file request and send response
                handleRequest(header, body, clientSocket, fileDirectory, outputStream, writer);

                // break while loop if write() is called

//                System.out.println("position 5");
//                System.out.println(clientSocket.isClosed());

            }
        } catch (IOException e) {
            logger.error("Error handling client connection", e);
        }
    }

    @Override
    public void run() {

        // Start worker threads
        for (int i = 0; i < NUM_WORKERS; i++) {
            Thread workerThread = new Thread(() -> {
                while (true) {
                    try {
                        // Take a connection from the queue (blocks if the queue is empty)
                        Socket clientSocket = socketQueue.take();
//                        boolean socketClosed = false;

                        // Handle the connection
                        try {
                            // handle connection
                            processClientRequest(clientSocket);
                        } catch (Exception e) {
                            logger.error("Error handling connection: " + e.getMessage());
                        } finally {
                            if (!clientSocket.isClosed()) {
                                try {
                                    // close the socket
                                    clientSocket.close();
                                    logger.info("Client disconnected");
                                } catch (IOException e) {
                                    logger.error("Error closing socket: " + e.getMessage());
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            workerThread.start();
        }


        try {

            if (httpsEnabled) {
                String pwd = "secret";
                KeyStore keyStore = KeyStore.getInstance("JKS");
                keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
                keyManagerFactory.init(keyStore, pwd.toCharArray());
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
                ServerSocketFactory factory = sslContext.getServerSocketFactory();
                ServerSocket serverSocketTLS = factory.createServerSocket(httpsPort);
                logger.info("Server started on HTTPS port " + httpsPort);

                Thread connectionThread = new Thread(() -> {
                    try {
                        while (true) {
                            // accepts a connection
                            Socket securedClientSocket = serverSocketTLS.accept();
                            logger.info("Incoming connection from " + securedClientSocket.getRemoteSocketAddress());
                            logger.info("Client connected");

                            // Enqueue the connection to be handled by a worker thread
                            socketQueue.put(securedClientSocket);
                        }
                    } catch (Exception e) {
                        logger.error(e.toString());
                    }
                });
                connectionThread.start();
            }

            // Open a HTTP ServerSocket on the specified port
            ServerSocket serverSocket = new ServerSocket(serverPort);
            logger.info("Server started on HTTP port " + serverPort);
            Thread connectionThread = new Thread(() -> {
                try {
                    while (true) {
                        // accepts a connection
                        Socket clientSocket = serverSocket.accept();
                        logger.info("Incoming connection from " + clientSocket.getRemoteSocketAddress());
                        logger.info("Client connected");

                        // Enqueue the connection to be handled by a worker thread
                        socketQueue.put(clientSocket);
                    }
                } catch (Exception e) {
                    logger.error(e.toString());
                }
            });
            connectionThread.start();




        } catch (Exception e) {
            logger.error(e.toString());
        }





    }




}

