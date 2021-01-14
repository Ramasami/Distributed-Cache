package distributed.cache.actions.impl;

import com.sun.net.httpserver.HttpExchange;
import distributed.cache.actions.inter.OnRequestCallBack;
import distributed.cache.networking.WebServer;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

public class RequestCallBackImpl implements OnRequestCallBack {

    private final String ENDPOINT;
    private final String METHOD;
    private final Function<byte[],byte[]> function;

    public RequestCallBackImpl(String endpoint, String method, Function<byte[], byte[]> function) {
        ENDPOINT = endpoint;
        METHOD = method.toLowerCase();
        this.function = function;
    }

    private byte[] readAllBytes(InputStream requestBody) throws IOException {
        byte[] message = new byte[requestBody.available()];
        requestBody.read(message);
        return message;
    }

    @Override
    public String getEndPoint() {
        return ENDPOINT;
    }

    @Override
    public void handleRequest(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase(METHOD)) {
            exchange.close();
            return;
        }

        byte [] responseBytes = function.apply(readAllBytes(exchange.getRequestBody()));
        WebServer.sendResponse(responseBytes, exchange);
    }
}
