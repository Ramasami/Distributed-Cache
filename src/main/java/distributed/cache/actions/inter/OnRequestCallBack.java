package distributed.cache.actions.inter;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;

public interface OnRequestCallBack {

    String getEndPoint();
    void handleRequest(HttpExchange exchange) throws IOException;
}
