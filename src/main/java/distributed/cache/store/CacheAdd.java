package distributed.cache.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import distributed.cache.consistent.hashing.Node;
import distributed.cache.consistent.hashing.WorkerHash;
import distributed.cache.networking.WebClient;
import org.apache.http.HttpResponse;
import org.apache.http.nio.reactor.IOReactorException;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.function.Function;

public class CacheAdd implements Function<byte[], byte[]> {

    private final WorkerHash workerHash;
    private final String currentWorkerNode;
    private final Cache cache;
    private final ObjectMapper mapper;
    private final WebClient client;

    public CacheAdd(WorkerHash workerHash, String currentWorkerNode, Cache cache) throws IOReactorException {
        this.workerHash = workerHash;
        this.currentWorkerNode = currentWorkerNode;
        this.cache = cache;
        mapper = new ObjectMapper();
        client = new WebClient();
    }

    @Override
    public byte[] apply(byte[] queryBytes) {
        Request request = null;
        try {
            request = mapper.readValue(queryBytes, Request.class);
            assert request.getKey()!=null;
            Node node = workerHash.getAssignedNode(request.getKey());
            if (node.getZnode().equals(currentWorkerNode)) {
                cache.put(request.getKey(), request.getValue());
            } else {
                return client.sendTask(node.getIp() + "/save", queryBytes);
            }
            return "Added".getBytes();
        } catch (Exception e) {
            return "Format Error".getBytes();
        }
    }
}
