package distributed.cache.networking;


import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class WebClient {

    private final CloseableHttpAsyncClient httpclient;


    public WebClient() throws IOReactorException {
        ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
        PoolingNHttpClientConnectionManager pool = new PoolingNHttpClientConnectionManager (ioReactor);
        pool.setDefaultMaxPerRoute(10);
        pool.setMaxTotal(10);
        httpclient = HttpAsyncClients.custom().setConnectionManager(pool).build();
        httpclient.start();
    }

    public Future<HttpResponse> sendAsyncTask(String url, byte [] requestPayload) {
        HttpPost post = new HttpPost(url);
        post.setEntity(new NByteArrayEntity(requestPayload));
        return httpclient.execute(post, new FutureCallback<HttpResponse>() {
            @Override
            public void completed(HttpResponse httpResponse) {
                System.out.println("Completed");
            }

            @Override
            public void failed(Exception e) {
                System.out.println("Failed");

            }

            @Override
            public void cancelled() {
                System.out.println("Cancelled");
            }
        });
    }

    public byte[] sendTask(String url, byte [] requestPayload) throws ExecutionException, InterruptedException, IOException {
        HttpPost post = new HttpPost(url);
        post.setEntity(new NByteArrayEntity(requestPayload));
        Future<HttpResponse> result = httpclient.execute(post, new FutureCallback<HttpResponse>() {
            @Override
            public void completed(HttpResponse httpResponse) {
                System.out.println("Completed");
            }

            @Override
            public void failed(Exception e) {
                System.out.println("Failed");

            }

            @Override
            public void cancelled() {
                System.out.println("Cancelled");
            }
        });
         return getOutput(result.get().getEntity().getContent());

    }

    public static byte[] getOutput(InputStream input) throws IOException {
        byte[] responseBytes = new byte[input.available()];
        input.read(responseBytes);
        return responseBytes;
    }
}
