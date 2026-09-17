/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.jdbc.compatibility.thin;

import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.jdbc.compatibility.GridgainManual;
import org.apache.ignite.jdbc.compatibility.IgniteCluster;
import org.apache.ignite.jdbc.compatibility.IgniteDocker;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base class for JDBC thin driver compatibility tests running against an Ignite node in Docker
 * and connecting exclusively through the JDBC thin driver and the Java thin client.
 */
public abstract class JdbcThinAbstractSelfTest {
    /** Ignite cluster running in Docker. */
    protected static final IgniteCluster cluster = new GridgainManual();

    /** JDBC thin driver connection URL pointing at {@link #cluster}. */
    protected static String jdbcUrl;

    /**
     * @throws Exception If failed.
     */
    @BeforeClass
    public static void startCluster() throws Exception {
        cluster.start();

        jdbcUrl = String.format("jdbc:ignite:thin://%s:%d", cluster.host(), cluster.port());
    }

    /**
     *
     */
    @AfterClass
    public static void stopCluster() {
        cluster.stop();
    }

    /**
     * @return JDBC connection to {@link #cluster}.
     * @throws SQLException On error.
     */
    protected Connection createConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    /**
     * @param r Runnable to check support.
     */
    protected void checkNotSupported(final RunnableX r) {
        GridTestUtils.assertThrowsWithCause(r, SQLFeatureNotSupportedException.class);
    }

    /**
     * @param r Runnable to check on closed connection.
     */
    protected void checkConnectionClosed(final RunnableX r) {
        GridTestUtils.assertThrowsAnyCause(null,
            () -> {
                r.run();

                return null;
            }, SQLException.class, "Connection is closed");
    }

    /**
     * @param r Runnable to check on closed statement.
     */
    protected void checkStatementClosed(final RunnableX r) {
        GridTestUtils.assertThrowsAnyCause(null,
            () -> {
                r.run();

                return null;
            }, SQLException.class, "Statement is closed");
    }

    /**
     * @param r Runnable to check on closed result set.
     */
    protected void checkResultSetClosed(final RunnableX r) {
        GridTestUtils.assertThrowsAnyCause(null,
            () -> {
                r.run();

                return null;
            }, SQLException.class, "Result set is closed");
    }

    /**
     * @param conn Connection.
     * @param sql Statement.
     * @param args Arguments.
     * @return Result set.
     * @throws SQLException if failed.
     */
    protected List<List<?>> execute(Connection conn, String sql, Object... args) throws SQLException {
        try (PreparedStatement s = conn.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++)
                s.setObject(i + 1, args[i]);

            if (s.execute()) {
                List<List<?>> res = new ArrayList<>();

                try (ResultSet rs = s.getResultSet()) {
                    ResultSetMetaData meta = rs.getMetaData();

                    int cnt = meta.getColumnCount();

                    while (rs.next()) {
                        List<Object> row = new ArrayList<>(cnt);

                        for (int i = 1; i <= cnt; i++)
                            row.add(rs.getObject(i));

                        res.add(row);
                    }
                }

                return res;
            }
            else
                return Collections.emptyList();
        }
    }
}
