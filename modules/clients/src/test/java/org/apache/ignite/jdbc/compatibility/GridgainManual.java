package org.apache.ignite.jdbc.compatibility;

public class GridgainManual implements IgniteCluster {
    @Override
    public String host() {
        return "localhost";
    }

    @Override
    public int port() {
        return 10800;
    }


    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
