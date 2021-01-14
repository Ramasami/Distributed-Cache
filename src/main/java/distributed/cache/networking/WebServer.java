package distributed.cache.networking;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import distributed.cache.actions.inter.OnRequestCallBack;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.Executors;

public class WebServer {
    private static final String STATUS_ENDPOINT = "/status";

    private final int port;
    private HttpServer server;
    private final int poolSize;
    private final OnRequestCallBack[] onRequestCallBack;


    public WebServer(int port, int poolSize, OnRequestCallBack... onRequestCallBack) {
        this.port = port;
        this.poolSize = poolSize;
        this.onRequestCallBack = onRequestCallBack;
    }

    public void startServer() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        HttpContext statusContext = server.createContext(STATUS_ENDPOINT);

        Arrays.stream(onRequestCallBack)
                .forEach(requestCallBack -> {
                    HttpContext context = server.createContext(requestCallBack.getEndPoint());
                    context.setHandler(requestCallBack::handleRequest);
                });

        statusContext.setHandler(this::handleStatusCheckRequest);
        server.setExecutor(Executors.newFixedThreadPool(poolSize));
        server.start();
    }





    private void handleStatusCheckRequest(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("get")) {
            exchange.close();
            return;
        }
        String responseMessage = "Server is alive";
        sendResponse(responseMessage.getBytes(), exchange);
    }

    public static void sendResponse(byte[] responseBytes, HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, responseBytes.length);
        OutputStream outputStream = exchange.getResponseBody();
        outputStream.write(responseBytes);
        outputStream.flush();
        outputStream.close();
    }


    public void stop() {
        server.stop(0);
    }
}
