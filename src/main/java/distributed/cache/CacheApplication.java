package distributed.cache;

import distributed.cache.actions.inter.LeaderElectionCallback;
import distributed.cache.actions.impl.LeaderElectionCallbackImpl;
import distributed.cache.network.management.LeaderElection;
import distributed.cache.network.management.Worker;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Scanner;

public class CacheApplication implements Watcher {


    private static final int SESSION_TIMEOUT = 3000;
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static int port;
    private static boolean shutWorker;

    private static ZooKeeper zooKeeper;



    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        Scanner sc = new Scanner(System.in);
        System.out.print("Enter Port: ");
        port = sc.nextInt();
        CacheApplication cache = new CacheApplication();
        cache.connectToZooKeeper();
        Worker worker = new Worker(zooKeeper,port);
        LeaderElectionCallback leaderElectionCallback = new LeaderElectionCallbackImpl(shutWorker, worker, zooKeeper, port);
        LeaderElection leaderElection = new LeaderElection(zooKeeper,leaderElectionCallback);
        cache.run();

    }

    private void connectToZooKeeper() throws IOException {
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    void run() throws InterruptedException {
        synchronized (CacheApplication.class) {
            CacheApplication.class.wait();
            close();
        }
    }

    private void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
            System.out.println("Successfully connected to Zookeeper");
        } else {
            synchronized (CacheApplication.class) {
                System.out.println("Disconnected from Zookeeper event");
                zooKeeper.notifyAll();
            }
        }
    }
}
