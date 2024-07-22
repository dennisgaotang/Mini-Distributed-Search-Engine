package cis5550.webserver;

import java.security.SecureRandom;
import java.util.*;
import java.net.*;
import java.nio.charset.*;
import java.util.concurrent.ConcurrentHashMap;

// Provided as part of the framework code

class RequestImpl implements Request {
  String method;
  String url;
  String protocol;
  InetSocketAddress remoteAddr;
  Map<String,String> headers;

  Map<String,String> queryParams;
  Map<String,String> params;
  byte bodyRaw[];
  Server server;

  static int sessionIDLength = 120 / 8; // 120 bits = 15 bytes

  private static final ConcurrentHashMap<String, Session> sessions = Server.getSessions();


  public Session getCurrentSession() {
    return currentSession;
  }

  private Session currentSession = null;

  public boolean isSessionGeneratorCalled() {
    return isSessionGeneratorCalled;
  }


  public boolean isSessionGeneratorCalled = false;


  RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String,String> headersArg, Map<String,String> queryParamsArg, Map<String,String> paramsArg, InetSocketAddress remoteAddrArg, byte bodyRawArg[], Server serverArg) {
    method = methodArg;
    url = urlArg;
    remoteAddr = remoteAddrArg;
    protocol = protocolArg;
    headers = headersArg;
    queryParams = queryParamsArg;
    params = paramsArg;
    bodyRaw = bodyRawArg;
    server = serverArg;
  }

  RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String,String> headersArg, InetSocketAddress remoteAddrArg, byte bodyRawArg[], Server serverArg) {
    method = methodArg;
    url = urlArg;
    remoteAddr = remoteAddrArg;
    protocol = protocolArg;
    headers = headersArg;
    bodyRaw = bodyRawArg;
    server = serverArg;
  }

  public String requestMethod() {
  	return method;
  }
  public void setParams(Map<String,String> paramsArg) {
    params = paramsArg;
  }
  public int port() {
  	return remoteAddr.getPort();
  }
  public String url() {
  	return url;
  }
  public String protocol() {
  	return protocol;
  }
  public String contentType() {
  	return headers.get("content-type");
  }
  public String ip() {
  	return remoteAddr.getAddress().getHostAddress();
  }
  public String body() {
    return new String(bodyRaw, StandardCharsets.UTF_8);
  }
  public byte[] bodyAsBytes() {
  	return bodyRaw;
  }
  public int contentLength() {
  	return bodyRaw.length;
  }
  public String headers(String name) {
  	return headers.get(name.toLowerCase());
  }
  public Set<String> headers() {
  	return headers.keySet();
  }
  public String queryParams(String param) {
  	return queryParams.get(param);
  }
  public Set<String> queryParams() {
  	return queryParams.keySet();
  }
  public String params(String param) {
    return params.get(param);
  }
  public Map<String,String> params() {
    return params;
  }

  @Override
  public Session session() {
    if (currentSession != null) {
      return currentSession;
    }

    // Try to retrieve the SessionID cookie from the request
    String sessionIdCookie = headers.get("cookie");
    String sessionId = null;
//    System.out.println("cookie: " + sessionIdCookie);
    Session session = null;
    if (sessionIdCookie != null) {
      for (String cookie : sessionIdCookie.split(";")) {
        if (cookie.trim().startsWith("SessionID=")) {
          sessionId = cookie.split("=")[1].trim();
          session = sessions.get(sessionId);
          if (session != null && !session.isExpired()) {
            return session;
          }
        }
      }
    }

    if (session != null) {
      if(!session.isExpired()) {
        // Update last accessed time and return existing session
        currentSession.setLastAccessedTime(System.currentTimeMillis());
      }
      currentSession = session;

    } else {
      // Create a new session
//      System.out.println("create new session");
      sessionId = generateSessionId();
      currentSession = new SessionImpl(sessionId);
      sessions.put(sessionId, currentSession);
//      System.out.println("sessionID: " + currentSession);
    }

//    System.out.println("current sessionID: " + currentSession);


    return currentSession;
  }

  private String generateSessionId() {

    isSessionGeneratorCalled = true;
    SecureRandom random = new SecureRandom();
    byte[] bytes = new byte[sessionIDLength]; // 120 bits
    random.nextBytes(bytes);
    String id = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    while (sessions.containsKey(id)) {
      id = generateSessionId();
    }
    return id;
  }

}
