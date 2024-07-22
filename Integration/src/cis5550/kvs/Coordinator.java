package cis5550.kvs;

import cis5550.generic.Worker;
import cis5550.tools.Logger;

import java.io.IOException;

import static cis5550.webserver.Server.*;


public class Coordinator extends cis5550.generic.Coordinator {
    private static final Logger logger
            = Logger.getLogger(Coordinator.class);
    private static int port = 8080; // Default port

    public static void main(String[] args) throws IOException{
        System.out.println("KVS Coordinator starting...");
        init(args);
    }

    private static void init(String[] args) throws IOException {
        parseCommand(args);
        port(port);
        registerRoutes();
        System.out.println("KVS Coordinator running on port: " + port);

        get("/", (req, res) -> {
            res.type("text/html");
            return "<html><head><title>KVS Coordinator</title></head><body>" +
                    "<h1>KVS Coordinator</h1>" +
                    workerTable() +
                    "</body></html>";
        });
    }

    private static void parseCommand(String[] args) {
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid Input. Expected a numeric port number, but got: " + args[0]);
                System.exit(1);
            }
        }
    }



}