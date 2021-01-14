package distributed.cache.consistent.hashing;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class LeaderHashing implements Watcher {

    private final ZooKeeper zooKeeper;
    private static final String WORKER_ZNODE = "/workers";
    private static final String CONCURRENT_HASH = "/hash";
    private static final int searchSpace = 100;
    private ConsistentHashing hashing;

    public LeaderHashing(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        this.zooKeeper = zooKeeper;
        createHashingZnode();
    }

    private void createHashingZnode() throws KeeperException, InterruptedException {
        try {
            if (zooKeeper.exists(WORKER_ZNODE, false) == null) {
                zooKeeper.create(WORKER_ZNODE, new byte[]{}, OPEN_ACL_UNSAFE, PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        try {
            if (zooKeeper.exists(CONCURRENT_HASH, false) == null) {
                zooKeeper.create(CONCURRENT_HASH, new byte[]{}, OPEN_ACL_UNSAFE, PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

        Stat stat = zooKeeper.exists(CONCURRENT_HASH, false);
        hashing = (ConsistentHashing) SerializationUtils.deserialize(zooKeeper.getData(CONCURRENT_HASH, false, stat));
        resetHash();
    }

    private synchronized void resetHash() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(WORKER_ZNODE, this);
        hashing = verifyHash(hashing, children);
        zooKeeper.setData(CONCURRENT_HASH, SerializationUtils.serialize(hashing), -1);
        System.out.println("leader new hash: " + hashing);
    }

    private ConsistentHashing verifyHash(ConsistentHashing hashing, List<String> children) {
        Set<String> childrenSet = new HashSet<>(children);
        if (hashing == null)
            hashing = new ConsistentHashing(searchSpace, 3);
        hashing.getNodes()
                .stream()
                .filter(node -> !childrenSet.contains(node.getZnode()))
                .forEach(hashing::removeNode);
        Set<String> nodeSet = hashing.getNodes()
                .stream()
                .map(Node::getZnode)
                .collect(Collectors.toSet());
        ConsistentHashing finalHashing = hashing;
        children.parallelStream()
                .filter(child -> !nodeSet.contains(child))
                .forEach(child -> {
                    try {
                        Stat stat = zooKeeper.exists(WORKER_ZNODE + "/" + child, false);
                        if (stat != null) {
                            String ip = new String(zooKeeper.getData(WORKER_ZNODE + "/" + child, false, stat));
                            finalHashing.addNode(new Node(ip, child));
                        }
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
        return hashing;
    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            resetHash();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
