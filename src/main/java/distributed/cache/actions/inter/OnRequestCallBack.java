package distributed.cache.actions.inter;

public interface OnRequestCallBack {

    String getEndPoint();
    byte[] handleRequest(byte[] input);
}
