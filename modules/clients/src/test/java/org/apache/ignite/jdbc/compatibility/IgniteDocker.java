package org.apache.ignite.jdbc.compatibility;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

public class IgniteDocker implements IgniteCluster {
    private static final String IGNITE_IMAGE = "apacheignite/ignite:latest";

    private static final int THIN_CLIENT_PORT = 10800;

    private final GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(IGNITE_IMAGE))
            .withExposedPorts(THIN_CLIENT_PORT)
            .waitingFor(Wait.forLogMessage(".*node started.*", 1)
                    //.waitingFor(Wait.forLogMessage(".*node started.*", 1)
                    .withStartupTimeout(Duration.ofSeconds(120)));

    void startGrid() {
        container.start();
    }

    @Override
    public String host() {
        return container.getHost();
    }

    @Override
    public int port() {
        return container.getMappedPort(THIN_CLIENT_PORT);
    }


    @Override
    public void start() {
        startGrid();
    }

    @Override
    public void stop() {
        container.close();
    }
}
