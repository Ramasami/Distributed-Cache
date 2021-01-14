package distributed.cache.actions.inter;

import org.apache.zookeeper.KeeperException;

public interface LeaderElectionCallback {
    void onLeader() throws KeeperException, InterruptedException;
    void onWorker() throws KeeperException, InterruptedException;
}
