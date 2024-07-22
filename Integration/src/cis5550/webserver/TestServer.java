package cis5550.webserver;

import java.io.IOException;

public class TestServer {

    public static void main(String[] args) throws IOException {
        // Set the HTTPS port
        Server.securePort(443); // Use 443 for deployment on EC2, 8443 for local testing

        Server.get("/", (request, response) -> {
            response.status(200, "OK");
            response.type("text/plain");
            return "Hello World - this is maylaiqm";
        });

    }
}
