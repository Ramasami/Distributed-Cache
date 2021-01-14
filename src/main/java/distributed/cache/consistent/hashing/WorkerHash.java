package distributed.cache.consistent.hashing;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class WorkerHash implements Watcher {


    private static final String CONCURRENT_HASH = "/hash";
    private ConsistentHashing hashing;
    private final ZooKeeper zooKeeper;


    public WorkerHash(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        this.zooKeeper = zooKeeper;
        Stat stat;
        try {
            stat = zooKeeper.exists(CONCURRENT_HASH, false);
            if (stat == null) {
                zooKeeper.create(CONCURRENT_HASH, new byte[]{}, OPEN_ACL_UNSAFE, PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        fetchHash();
    }

    public void unregister() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(CONCURRENT_HASH, false);
        zooKeeper.getData(CONCURRENT_HASH, false, stat);
    }

    public ConsistentHashing getHash() throws KeeperException, InterruptedException {
        if(hashing == null)
            fetchHash();
        return hashing;
    }

    private void fetchHash() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(CONCURRENT_HASH, false);
        hashing = (ConsistentHashing) SerializationUtils.deserialize(zooKeeper.getData(CONCURRENT_HASH, this, stat));
        System.out.println("worker: new hash: " + hashing);

    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            fetchHash();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Node getAssignedNode(String request) {
        return hashing.getAssignedNode(request);
    }

}
