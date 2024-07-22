package cis5550.webserver;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ResponseImpl implements Response {
    private int statusCode = 200;
    private String reasonPhrase = "OK";


    private final Map<String, List<String>> headers = new HashMap<>();

    private final List<String> contentTypes = new ArrayList<>();


    private byte[] body = null;
    private boolean isWriteCalled = false;

    private boolean isSessionGeneratorCalled = false;

    private final OutputStream outputStream;

    private Server server;

    private Session session;

    private RequestImpl request;
    //        correction 4!!!


    public ResponseImpl(OutputStream outputStream, Server server) {
        this.outputStream = outputStream;
        this.server = server;
    }

//    public ResponseImpl(OutputStream outputStream, Server server, Session session, boolean isSessionGeneratorCalled) {
    public ResponseImpl(OutputStream outputStream, Server server, RequestImpl request) {
        //        correction 5!!!

        this.outputStream = outputStream;
        this.server = server;
//        this.session = session;
//        this.isSessionGeneratorCalled = isSessionGeneratorCalled;
        this.request = request;
        //        correction 6!!!

    }


    @Override
    public void body(String body) {
        // Only set the body if write() has not been called and body is currently unset
        if (!isWriteCalled) {
            this.body = body.getBytes();
        }
    }

    @Override
    public void bodyAsBytes(byte[] body) {
        // Only set the body if write() has not been called and body is currently unset
        if (!isWriteCalled) {
            this.body = body;
        }
    }

    @Override
    public void header(String name, String value) {
        if (!isWriteCalled) {
            if (headers.containsKey(name)) {
                headers.get(name).add(value);
            } else {
                List<String> values = new ArrayList<>();
                values.add(value);
                headers.put(name, values);
            }
        }
    }

    @Override
    public void type(String contentType) {
        if (!isWriteCalled) {
            contentTypes.add(contentType);
            headers.put("Content-Type", contentTypes);
        }
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        if (!isWriteCalled) {
            this.statusCode = statusCode;
            this.reasonPhrase = reasonPhrase;
        }
    }

    @Override
    public void write(byte[] b) throws IOException {

        if (!isWriteCalled) {
//            System.out.println("writing for the 1st time");
            outputStream.write(("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n").getBytes());

//            if (session != null && isSessionGeneratorCalled) {
            Session session = request.getCurrentSession();

            if (session != null && request.isSessionGeneratorCalled()) {

            //        correction 7!!!

//                String cookieHeader = "Set-Cookie: SessionID=" + session.id() + "; Path=/; HttpOnly";
                String cookieHeader = "Set-Cookie: SessionID=" + session.id() +
                        "; Path=/" +
                        "; HttpOnly" +
                        (Server.isHttpsEnabled() ? "; Secure" : "") +
                        "; SameSite=Lax";

                outputStream.write((cookieHeader + "\r\n").getBytes());
//                outputStream.write((cookieHeader + "\r\n").getBytes());

            }

            for (Map.Entry<String, List<String>> header : headers.entrySet()) {

                String name = header.getKey();

                if (name.equalsIgnoreCase("Content-Length")) {
                    continue;
                }

                for (String value : header.getValue()) {
                    outputStream.write((name + ": " + value + "\r\n").getBytes());
//                    System.out.println(name + ": " + value + "\r\n");

                }
            }

            outputStream.write(("Connection: close\r\n").getBytes());
            outputStream.write(("\r\n").getBytes());


            outputStream.flush();
            this.isWriteCalled = true;
        }

//        System.out.println("subsequent writing");
        outputStream.write(b);
        outputStream.flush();
//        System.out.println( b.toString() );


    }

    @Override
    public void redirect(String url, int responseCode) {
        // For extra credit; implement if necessary

        this.status(responseCode, getReasonPhrase(responseCode));

        // Set the "Location" header to the redirect URL
        this.header("Location", url);

        try {

            // Write the status line
            String responseLine = "HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n";
            outputStream.write(responseLine.getBytes());

            // Write headers
            for (Map.Entry<String, List<String>> header : headers.entrySet()) {
                for (String value : header.getValue()) {
                    outputStream.write((header.getKey() + ": " + value + "\r\n").getBytes());
                }
            }

            outputStream.write("\r\n".getBytes());

            outputStream.flush();
            isWriteCalled = true; // Ensure no further writes are allowed

        } catch (IOException e) {

            System.err.println("Error sending redirect: " + e.getMessage());

        }



    }

    private String getReasonPhrase(int statusCode) {

        switch (statusCode) {
            case 301: return "Moved Permanently";
            case 302: return "Found";
            case 303: return "See Other";
            case 307: return "Temporary Redirect";
            case 308: return "Permanent Redirect";
            default: return "";
        }
    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {
        // For extra credit; implement if necessary
    }

    public boolean isWriteCalled() {
        return isWriteCalled;
    }

    public void commit() throws IOException {
//        System.out.println("commit function");
        if (!isWriteCalled) {
            // Write the status line
            outputStream.write(("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n").getBytes());

//            if (session != null && isSessionGeneratorCalled ) {

            Session session = request.getCurrentSession();
            if (session != null && request.isSessionGeneratorCalled()) {

                //        correction 8!!!
//                String cookieHeader = "Set-Cookie: SessionID=" + session.id() + "; Path=/; HttpOnly";
                String cookieHeader = "Set-Cookie: SessionID=" + session.id() +
                        "; Path=/" +
                        "; HttpOnly" +
                        (Server.isHttpsEnabled() ? "; Secure" : "") +
                        "; SameSite=Lax";
                outputStream.write((cookieHeader + "\r\n").getBytes());
            }

            // Write headers
            for (Map.Entry<String, List<String>> header : headers.entrySet()) {
                for (String value : header.getValue()) {
                    outputStream.write((header.getKey() + ": " + value + "\r\n").getBytes());
                }
            }

            // Write a content length if body is set and Content-Length is not manually set
            if (body != null && !headers.containsKey("content-length")) {
                outputStream.write(("Content-Length: " + body.length + "\r\n").getBytes());
            }

            // End of headers
            outputStream.write("\r\n".getBytes());

            // If a body has been set, write it to the stream
            if (body != null) {
                outputStream.write(body);
            }

            outputStream.flush();
            isWriteCalled = true; // Ensure no further writes are allowed
        }
    }




}
