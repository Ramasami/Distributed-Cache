package distributed.cache.actions.impl;

import distributed.cache.actions.inter.LeaderElectionCallback;
import distributed.cache.consistent.hashing.LeaderHashing;
import distributed.cache.consistent.hashing.WorkerHash;
import distributed.cache.network.management.Worker;
import distributed.cache.networking.WebServer;
import distributed.cache.store.Cache;
import distributed.cache.store.CacheAdd;
import distributed.cache.store.CacheFetch;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class LeaderElectionCallbackImpl implements LeaderElectionCallback {

    private final boolean shutWorker;
    private final Worker worker;
    private final ZooKeeper zooKeeper;
    private LeaderHashing leaderHashing;
    private WorkerHash workerHash;
    private WebServer WorkerServer;
    private CacheFetch cacheFetch;
    private CacheAdd cacheAdd;
    private Cache cache;
    private final int port;

    public LeaderElectionCallbackImpl(boolean shutWorker, Worker worker, ZooKeeper zooKeeper, int port) {
        this.shutWorker = shutWorker;
        this.worker = worker;
        this.zooKeeper = zooKeeper;
        this.port = port;
    }

    @Override
    public void onLeader() throws KeeperException, InterruptedException, IOReactorException {
        leaderHashing = new LeaderHashing(zooKeeper);
        if (shutWorker) {
            worker.unregisterFromWorker();
            if (workerHash != null) {
                workerHash.unregister();
                WorkerServer.stop();
            }
        } else {
            onWorker();
        }
    }

    @Override
    public void onWorker() throws KeeperException, InterruptedException, IOReactorException {
        if (workerHash == null) {
            workerHash = new WorkerHash(zooKeeper);
            cache = new Cache();
            cacheFetch = new CacheFetch(workerHash, worker.getCurrentZnode(), cache);
            cacheAdd = new CacheAdd(workerHash, worker.getCurrentZnode(), cache);
            WorkerServer = new WebServer(port, 5, new RequestCallBackImpl("/fetch", "post", cacheFetch), new RequestCallBackImpl("/save", "post", cacheAdd));
            WorkerServer.startServer();
        }

    }
}
