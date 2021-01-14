package distributed.cache.network.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class Worker implements Watcher {

    private final ZooKeeper zooKeeper;
    private static final String WORKER_ZNODE ="/workers";
    private String currentZnode = null;
    private final int port;
    private List<String> addresses;

    public Worker(ZooKeeper zooKeeper, int port) throws KeeperException, InterruptedException, UnknownHostException {
        this.zooKeeper = zooKeeper;
        this.port = port;
        createWorkerZnode();
        registerForWorker();
        getUpdatedForWorkers();
    }

    public List<String> getWorkers() {
        if(addresses!=null)
            return addresses;
        return getUpdatedForWorkers();
    }

    private List<String> getUpdatedForWorkers() {
        try {
            List<String> workerZnodes = zooKeeper.getChildren(WORKER_ZNODE,this);
            List<String> children = new ArrayList<>(workerZnodes.size());
            for(String workZnode : workerZnodes) {
                String workerZnodeFullPath = WORKER_ZNODE + "/" + workZnode;
                Stat stat = zooKeeper.exists(workerZnodeFullPath, false);
                if(stat == null)
                    continue;
                byte[] addressBytes = zooKeeper.getData(workerZnodeFullPath, false,stat);
                String address = new String(addressBytes);
                children.add(address);
            }
            addresses = Collections.unmodifiableList(children);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            addresses = Collections.unmodifiableList(new ArrayList<>());
        }
        System.out.println("The cluster addresses are : " + addresses);
        return addresses;
    }

    private byte[] getURL() throws UnknownHostException {
        String url = String.format("http://%s:%d", InetAddress.getLocalHost().getCanonicalHostName(), port);
        return url.getBytes();
    }

    private void registerForWorker() throws KeeperException, InterruptedException, UnknownHostException {
        String path = zooKeeper.create(WORKER_ZNODE + "/n_", getURL(), OPEN_ACL_UNSAFE, EPHEMERAL_SEQUENTIAL);
        System.out.println("znode name " + path);
    }

    private void createWorkerZnode() {
        try {
            if (zooKeeper.exists(WORKER_ZNODE, false) == null) {
                zooKeeper.create(WORKER_ZNODE, new byte[]{}, OPEN_ACL_UNSAFE, PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        getUpdatedForWorkers();
    }

    public void unregisterFromWorker() {
        try {
            zooKeeper.delete(WORKER_ZNODE + "/" + currentZnode, 1);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
