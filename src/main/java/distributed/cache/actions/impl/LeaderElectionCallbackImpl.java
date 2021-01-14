package distributed.cache.actions.impl;

import distributed.cache.actions.inter.LeaderElectionCallback;
import distributed.cache.consistent.hashing.LeaderHashing;
import distributed.cache.consistent.hashing.WorkerHash;
import distributed.cache.network.management.Worker;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class LeaderElectionCallbackImpl implements LeaderElectionCallback {

    private final boolean shutWorker;
    private final Worker worker;
    private final ZooKeeper zooKeeper;
    private LeaderHashing leaderHashing;
    private WorkerHash workerHash;

    public LeaderElectionCallbackImpl(boolean shutWorker, Worker worker, ZooKeeper zooKeeper) {
        this.shutWorker = shutWorker;
        this.worker = worker;
        this.zooKeeper = zooKeeper;
    }

    @Override
    public void onLeader() throws KeeperException, InterruptedException {
        leaderHashing = new LeaderHashing(zooKeeper);
        if (shutWorker) {
            worker.unregisterFromWorker();
            workerHash.unregister();
        } else {
            onWorker();
        }
    }

    @Override
    public void onWorker() throws KeeperException, InterruptedException {
        if(workerHash==null)
            workerHash = new WorkerHash(zooKeeper);
    }
}
