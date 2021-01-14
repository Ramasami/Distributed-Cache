package distributed.cache.consistent.hashing;

import java.io.Serializable;

public class Node implements Serializable {

    private final String ip;
    private final String znode;
    private final int weight;
    private static final long serialVersionUID = -17969240011396798L;

    public Node(String ip, String znode, int weight) {
        this.ip = ip;
        this.znode = znode;
        this.weight = weight;
    }

    public Node(String ip, String znode) {
        this.ip = ip;
        this.znode = znode;
        weight = 1;
    }

    public String getIp() {
        return ip;
    }

    public String getZnode() {
        return znode;
    }

    public int getWeight() {
        return weight;
    }

    @Override
    public String toString() {
        return "Node{" +
                "ip='" + ip + '\'' +
                ", znode='" + znode + '\'' +
                ", weight=" + weight +
                '}';
    }
}
