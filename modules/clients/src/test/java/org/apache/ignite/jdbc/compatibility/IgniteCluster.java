package org.apache.ignite.jdbc.compatibility;

import org.testcontainers.lifecycle.Startable;

public interface IgniteCluster extends Startable {
    String host();

    int port();
}
