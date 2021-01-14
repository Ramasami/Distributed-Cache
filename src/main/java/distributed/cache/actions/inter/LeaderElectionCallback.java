package distributed.cache.actions.inter;

import org.apache.http.nio.reactor.IOReactorException;
import org.apache.zookeeper.KeeperException;

public interface LeaderElectionCallback {
    void onLeader() throws KeeperException, InterruptedException, IOReactorException;
    void onWorker() throws KeeperException, InterruptedException, IOReactorException;
}
