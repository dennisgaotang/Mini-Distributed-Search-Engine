package cis5550.webserver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionImpl implements Session {

    private static final int MAXACTIVEINTERVAL = 300000; // 300 secs = 300,000 msecs
    private String sessionId;
    private long creationTime;
    private long lastAccessedTime;
    private int maxActiveInterval;
    private Map<String, Object> attributes = new HashMap<>();
    private static ConcurrentHashMap<String, Session> sessions = Server.getSessions();



    public SessionImpl(String sessionId) {
        this.sessionId = sessionId;
        this.creationTime = System.currentTimeMillis();
        this.lastAccessedTime = creationTime;
        this.maxActiveInterval = MAXACTIVEINTERVAL; // default 300 seconds
    }

    @Override
    public String id() {
        return sessionId;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public long lastAccessedTime() {
        return lastAccessedTime;
    }

    @Override
    public void maxActiveInterval(int seconds) {
        this.maxActiveInterval = seconds;
    }

    @Override
    public void invalidate() {
        sessions.remove(this.sessionId);
    }

    @Override
    public Object attribute(String name) {
        return attributes.get(name);
    }

    @Override
    public void attribute(String name, Object value) {
        attributes.put(name, value);
    }

    public void setLastAccessedTime(long lastAccessedTime) {
        this.lastAccessedTime = lastAccessedTime;
    }

    public int getMaxActiveInterval() {
        return maxActiveInterval;
    }


    public boolean isExpired() {
        return (System.currentTimeMillis() - lastAccessedTime) > (maxActiveInterval);
    }

}
