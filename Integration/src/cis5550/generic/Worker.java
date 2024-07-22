



package cis5550.generic;

import cis5550.tools.HTTP;
import cis5550.tools.Logger;

public class Worker {
    private static final Logger logger
            = Logger.getLogger(Worker.class);

    public static void startPingThread(String coordinatorAddress, String workerId, int port) {
        new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {

//                    Thread.sleep(threadSleepTime);

                    String urlString = "http://" + coordinatorAddress + "/ping?id=" + workerId + "&port=" + port;
//                    System.out.println("urlString: " +urlString);

                    HTTP.doRequest("GET", urlString, null);


                } catch (Exception e) {

                    System.err.println("Error in ping thread: " + e.getMessage());
                }
            }
        }).start();
    }
}

