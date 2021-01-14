package distributed.cache.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import distributed.cache.consistent.hashing.Node;
import distributed.cache.consistent.hashing.WorkerHash;
import distributed.cache.networking.WebClient;
import org.apache.http.nio.reactor.IOReactorException;

import java.util.function.Function;

public class CacheFetch implements Function<byte[], byte[]> {

    private final WorkerHash workerHash;
    private final String currentWorkerNode;
    private final Cache cache;
    private final ObjectMapper mapper;
    private final WebClient client;

    public CacheFetch(WorkerHash workerHash, String currentWorkerNode, Cache cache) throws IOReactorException {
        this.workerHash = workerHash;
        this.currentWorkerNode = currentWorkerNode;
        this.cache = cache;
        this.client = new WebClient();
        mapper = new ObjectMapper();
    }

    @Override
    public byte[] apply(byte[] queryBytes) {
        Request request = null;
        try {
            request = mapper.readValue(queryBytes, Request.class);
            assert request.getKey()!=null;
            assert request.getValue()==null;
            Node node = workerHash.getAssignedNode(request.getKey());
            if (node.getZnode().equals(currentWorkerNode)) {
                return cache.get(request.getKey()).getBytes();
            } else {
                return client.sendTask(node.getIp() + "/fetch", queryBytes);
            }
        } catch (Exception e) {
            return "Format Error".getBytes();
        }
    }
}
