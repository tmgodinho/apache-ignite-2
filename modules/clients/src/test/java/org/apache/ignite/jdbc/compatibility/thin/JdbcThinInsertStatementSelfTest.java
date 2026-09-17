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

import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Statement test.
 *
 * Ported from {@code org.apache.ignite.jdbc.thin.JdbcThinInsertStatementSelfTest} to run against
 * an Ignite node in Docker ({@link IgniteDocker}), using only the JDBC thin driver and the Java
 * thin client.
 */
public class JdbcThinInsertStatementSelfTest extends JdbcThinAbstractDmlStatementSelfTest {
    /** SQL query. */
    private static final String SQL = "insert into Person(_key, id, firstName, lastName, age, data, text) values " +
        "('p1', 1, 'John', 'White', 25, RAWTOHEX('White'), 'John White'), " +
        "('p2', 2, 'Joe', 'Black', 35, RAWTOHEX('Black'), 'Joe Black'), " +
        "('p3', 3, 'Mike', 'Green', 40, RAWTOHEX('Green'), 'Mike Green')";

    /** SQL query. */
    private static final String SQL_PREPARED = "insert into Person(_key, id, firstName, lastName, age, data, text) " +
        "values (?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?)";

    /** Arguments for prepared statement. */
    private final Object[][] args = new Object[][] {
        {"p1", 1, "John", "White", 25, getBytes("White"), "John White"},
        {"p3", 3, "Mike", "Green", 40, getBytes("Green"), "Mike Green"},
        {"p2", 2, "Joe", "Black", 35, getBytes("Black"), "Joe Black"}
    };

    /** Statement. */
    private Statement stmt;

    /** Prepared statement. */
    private PreparedStatement prepStmt;

    /**
     * @throws Exception If failed.
     */
    @Before
    public void beforeInsertTest() throws Exception {
        stmt = conn.createStatement();

        prepStmt = conn.prepareStatement(SQL_PREPARED);

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());

        assertNotNull(prepStmt);
        assertFalse(prepStmt.isClosed());

        int paramCnt = 1;

        for (Object[] arg : args) {
            prepStmt.setString(paramCnt++, (String)arg[0]);
            prepStmt.setInt(paramCnt++, (Integer)arg[1]);
            prepStmt.setString(paramCnt++, (String)arg[2]);
            prepStmt.setString(paramCnt++, (String)arg[3]);
            prepStmt.setInt(paramCnt++, (Integer)arg[4]);

            Blob blob = conn.createBlob();
            blob.setBytes(1, (byte[])arg[5]);
            prepStmt.setBlob(paramCnt++, blob);

            Clob clob = conn.createClob();
            clob.setString(1, (String)arg[6]);
            prepStmt.setClob(paramCnt++, clob);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @After
    public void afterInsertTest() throws Exception {
        try (Statement selStmt = conn.createStatement()) {
            assertTrue(selStmt.execute(SQL_SELECT));

            ResultSet rs = selStmt.getResultSet();

            assert rs != null;

            while (rs.next()) {
                int id = rs.getInt("id");

                switch (id) {
                    case 1:
                        assertEquals("p1", rs.getString("_key"));
                        assertEquals("John", rs.getString("firstName"));
                        assertEquals("White", rs.getString("lastName"));
                        assertEquals(25, rs.getInt("age"));
                        assertEquals("White", str(getBytes(rs.getBlob("data"))));
                        assertEquals("John White", str(rs.getClob("text")));
                        break;

                    case 2:
                        assertEquals("p2", rs.getString("_key"));
                        assertEquals("Joe", rs.getString("firstName"));
                        assertEquals("Black", rs.getString("lastName"));
                        assertEquals(35, rs.getInt("age"));
                        assertEquals("Black", str(getBytes(rs.getBlob("data"))));
                        assertEquals("Joe Black", str(rs.getClob("text")));
                        break;

                    case 3:
                        assertEquals("p3", rs.getString("_key"));
                        assertEquals("Mike", rs.getString("firstName"));
                        assertEquals("Green", rs.getString("lastName"));
                        assertEquals(40, rs.getInt("age"));
                        assertEquals("Green", str(getBytes(rs.getBlob("data"))));
                        assertEquals("Mike Green", str(rs.getClob("text")));
                        break;

                    default:
                        assert false : "Invalid ID: " + id;
                }
            }
        }

        if (stmt != null && !stmt.isClosed())
            stmt.close();

        if (prepStmt != null && !prepStmt.isClosed())
            prepStmt.close();

        assertTrue(prepStmt.isClosed());
        assertTrue(stmt.isClosed());
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testExecuteUpdate() throws SQLException {
        assertEquals(3, stmt.executeUpdate(SQL));
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testPreparedExecuteUpdate() throws SQLException {
        assertEquals(3, prepStmt.executeUpdate());
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testExecute() throws SQLException {
        assertFalse(stmt.execute(SQL));
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testPreparedExecute() throws SQLException {
        assertFalse(prepStmt.execute());
    }

    /**
     * Checks whether it's impossible to insert duplicate in single key statement.
     */
    @Test
    public void testDuplicateSingleKey() throws Exception {
        doTestDuplicate(
            () -> stmt.execute(SQL),
            "insert into Person(_key, id, firstName, lastName, age) values ('p2', 2, 'Joe', 'Black', 35)"
        );
    }

    /**
     * Checks whether it's impossible to insert duplicate in multiple keys statement.
     *
     * The original test populated the duplicate entry directly through the cache API
     * ({@code jcache(0).put(...)}), bypassing SQL entirely. That API is not reachable from this
     * Docker-based, JDBC/thin-client-only environment, so the duplicate is instead populated
     * through the Java thin client using a {@link BinaryObject}, which is the closest equivalent
     * available here and still exercises the same non-SQL insertion path.
     */
    @Test
    public void testDuplicateMultipleKeys() throws Exception {
        doTestDuplicate(() -> {
            try (IgniteClient client = thinClient()) {
                ClientCache<String, BinaryObject> cache = client.<String, BinaryObject>cache(CACHE_NAME)
                    .withKeepBinary();

                BinaryObject val = client.binary().builder(CACHE_NAME)
                    .setField("id", 2)
                    .setField("firstName", "Joe")
                    .setField("lastName", "Black")
                    .setField("age", 35)
                    .setField("data", getBytes("Black"))
                    .setField("text", "Joe Black")
                    .build();

                cache.put("p2", val);
            }
        }, SQL);
    }

    /**
     * @param initClosure Closure that populates the duplicate key before the insert under test.
     * @param sql Insert statement expected to fail because of the duplicate key.
     */
    private void doTestDuplicate(RunnableX initClosure, String sql) throws Exception {
        initClosure.run();

        // The original test also asserted that no "Failed to execute SQL query" message was logged
        // server-side (via a ListeningTestLogger attached to the embedded grid's logger). That check
        // is not portable here since the server runs in a separate Docker container whose logger this
        // process has no access to.
        GridTestUtils.assertThrowsAnyCause(null, () -> stmt.execute(sql), SQLException.class,
            "Failed to INSERT some keys because they are already in cache [keys=[p2]]");

        try (IgniteClient client = thinClient()) {
            ClientCache<String, BinaryObject> cache = client.<String, BinaryObject>cache(CACHE_NAME)
                .withKeepBinary();

            assertEquals(3, cache.getAll(new HashSet<>(Arrays.asList("p1", "p2", "p3"))).size());
        }
    }
}
