package distributed.cache.network.management;

import distributed.cache.actions.inter.LeaderElectionCallback;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {

    private final ZooKeeper zooKeeper;
    private static final String ELECTION_ZNODE = "/cache_election";
    private String currentZnode = null;
    private final LeaderElectionCallback leaderElectionCallback;

    public LeaderElection(ZooKeeper zooKeeper, LeaderElectionCallback leaderElectionCallback) throws KeeperException, InterruptedException, IOReactorException {
        this.zooKeeper = zooKeeper;
        this.leaderElectionCallback = leaderElectionCallback;
        createElectionZnode();
        registerForElection();
        reelect();
    }

    private void reelect() throws KeeperException, InterruptedException, IOReactorException {
        Stat stat = null;
        String prev = "";
        while (stat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_ZNODE, false);
            Collections.sort(children);
            if (children.get(0).equals(currentZnode)) {
                System.out.println("I am a Leader");
                leaderElectionCallback.onLeader();
                return;
            } else {
                int index = Collections.binarySearch(children, currentZnode) - 1;
                prev = children.get(index);
                stat = zooKeeper.exists(ELECTION_ZNODE + "/" + prev, this);
            }
        }
        leaderElectionCallback.onWorker();
        System.out.println("Watching: " + prev);
    }

    private void registerForElection() throws KeeperException, InterruptedException {
        String path = zooKeeper.create(ELECTION_ZNODE + "/n_", new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("znode name " + path);
        currentZnode = path.replace(ELECTION_ZNODE + "/", "");
    }

    private void createElectionZnode() {
        try {
            if (zooKeeper.exists(ELECTION_ZNODE, false) == null) {
                zooKeeper.create(ELECTION_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void process(WatchedEvent watchedEvent) {
        try {
            reelect();
        } catch (KeeperException | InterruptedException | IOReactorException e) {
            e.printStackTrace();
        }
    }
}
