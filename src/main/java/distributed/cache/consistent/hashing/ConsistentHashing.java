package distributed.cache.consistent.hashing;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConsistentHashing implements Serializable {

    private final int searchSpace;
    private final int pointMultiplier;
    private final Map<Node, List<Long>> nodePositions;
    private final ConcurrentSkipListMap<Long, Node> nodeMappings;
    private static final long serialVersionUID = -179692400113906798L;

    public ConsistentHashing(int searchSpace, int pointMultiplier) {
        this.searchSpace = searchSpace;
        this.pointMultiplier = pointMultiplier;
        this.nodePositions = new ConcurrentHashMap<>();
        this.nodeMappings = new ConcurrentSkipListMap<>();
    }

    public void addNode(Node node) {
        nodePositions.put(node, new CopyOnWriteArrayList<>());
        for (int i = 0; i < pointMultiplier; i++) {
            for (int j = 0; j < node.getWeight(); j++) {
                for(int k = 0; k<3;k++) {
                    final Long point = hashFunction((i * pointMultiplier) + j + k + node.getIp()) % searchSpace;
                    if(nodeMappings.containsKey(point))
                        continue;
                    nodePositions.get(node).add(point);
                    nodeMappings.put(point, node);
                    break;
                }
            }
        }
    }

    private long hashFunction(String s) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
            byte[] messageDigest = md.digest(s.getBytes());
            BigInteger no = new BigInteger(1, messageDigest);
            String hashtext = no.toString(16);
            while (hashtext.length() < 32) {
                hashtext = "0" + hashtext;
            }
            return Math.abs(hashtext.hashCode());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return Math.abs(s.hashCode());
    }

    void removeNode(Node node) {
        for (final Long point : nodePositions.remove(node)) {
            nodeMappings.remove(point);
        }
    }

    public Set<Node> getNodes() {
        return nodePositions.keySet();
    }

    public Node getAssignedNode(String request) {
        final Long key = hashFunction(request);
        final Map.Entry<Long, Node> entry = nodeMappings.higherEntry(key);
        if (entry == null) {
            return nodeMappings.firstEntry().getValue();
        } else {
            return entry.getValue();
        }
    }

    @Override
    public String toString() {
        return "ConsistentHashing{" +
                "searchSpace=" + searchSpace +
                ", pointMultiplier=" + pointMultiplier +
                ", nodePositions=" + nodePositions +
                ", nodeMappings=" + nodeMappings +
                '}';
    }
}


