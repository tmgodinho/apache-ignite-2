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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Statement test.
 *
 * Ported from {@code org.apache.ignite.jdbc.thin.JdbcThinStatementSelfTest} to run against an
 * Ignite node in Docker ({@link IgniteDocker}), using only the JDBC thin driver and the Java thin
 * client.
 */
@SuppressWarnings({"ThrowableNotThrown"})
public class JdbcThinStatementSelfTest extends JdbcThinAbstractSelfTest {
    /** Name of the "default" cache holding the "Person" and "Test" SQL types. */
    private static final String CACHE_NAME = "default";

    /** SQL query. */
    private static final String SQL = "select * from Person where age > 30";

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /**
     * Creates the "default" cache holding the "Person" and "Test" SQL types via the thin client,
     * and populates it with {@link BinaryObject} values.
     *
     * @throws Exception If failed.
     */
    @BeforeClass
    public static void startCache() throws Exception {
        try (IgniteClient client = Ignition.startClient(new ClientConfiguration()
            .setAddresses(cluster.host() + ":" + cluster.port()))) {

            ClientCache<Object, Object> cache = client.createCache(cacheConfig());

            cache.put("p1", client.binary().builder("Person")
                .setField("id", 1).setField("firstName", "John").setField("lastName", "White")
                .setField("age", 25).build());
            cache.put("p2", client.binary().builder("Person")
                .setField("id", 2).setField("firstName", "Joe").setField("lastName", "Black")
                .setField("age", 35).build());
            cache.put("p3", client.binary().builder("Person")
                .setField("id", 3).setField("firstName", "Mike").setField("lastName", "Green")
                .setField("age", 40).build());

            for (int i = 1; i <= 10; i++)
                cache.put(i, client.binary().builder("Test").setField("val", i).build());
        }
    }

    /**
     */
    @AfterClass
    public static void stopCache() {
        try (IgniteClient client = Ignition.startClient(new ClientConfiguration()
            .setAddresses(cluster.host() + ":" + cluster.port()))) {

            client.destroyCache(CACHE_NAME);
        }
    }

    /**
     * @return Cache configuration for the "default" cache holding the "Person" and "Test" SQL types.
     */
    private static ClientCacheConfiguration cacheConfig() {
        LinkedHashMap<String, String> personFields = new LinkedHashMap<>();

        personFields.put("id", Integer.class.getName());
        personFields.put("firstName", String.class.getName());
        personFields.put("lastName", String.class.getName());
        personFields.put("age", Integer.class.getName());

        QueryEntity personEntity = new QueryEntity()
            .setKeyType(String.class.getName())
            .setValueType("Person")
            .setFields(personFields);

        LinkedHashMap<String, String> testFields = new LinkedHashMap<>();

        testFields.put("val", Integer.class.getName());

        QueryEntity testEntity = new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType("Test")
            .setFields(testFields);

        return new ClientCacheConfiguration()
            .setName(CACHE_NAME)
            .setQueryEntities(personEntity, testEntity);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Before
    public void beforeTest() throws Exception {
        conn = createConnection();

        conn.setSchema('"' + CACHE_NAME + '"');

        stmt = conn.createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.After
    public void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed()) {
            stmt.close();

            assert stmt.isClosed();
        }

        conn.close();

        assert stmt.isClosed();
        assert conn.isClosed();
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testExecuteQuery0() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        assertNotNull(rs);

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assertEquals("Joe", rs.getString("firstName"));
                assertEquals("Black", rs.getString("lastName"));
                assertEquals(35, rs.getInt("age"));
            }
            else if (id == 3) {
                assertEquals("Mike", rs.getString("firstName"));
                assertEquals("Green", rs.getString("lastName"));
                assertEquals(40, rs.getInt("age"));
            }
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(2, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testExecuteQuery1() throws Exception {
        final String sqlText = "select val from test";

        try (ResultSet rs = stmt.executeQuery(sqlText)) {
            assertNotNull(rs);

            assertTrue(rs.next());

            int val = rs.getInt(1);

            assertTrue("Invalid val: " + val, val >= 1 && val <= 10);
        }

        stmt.close();

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.executeQuery(sqlText);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testExecute() throws Exception {
        assertTrue(stmt.execute(SQL));

        assertEquals("Update count must be -1 for SELECT query", -1, stmt.getUpdateCount());

        ResultSet rs = stmt.getResultSet();

        assertNotNull(rs);

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assertEquals("Joe", rs.getString("firstName"));
                assertEquals("Black", rs.getString("lastName"));
                assertEquals(35, rs.getInt("age"));
            }
            else if (id == 3) {
                assertEquals( "Mike", rs.getString("firstName"));
                assertEquals( "Green", rs.getString("lastName"));
                assertEquals(40, rs.getInt("age"));
            }
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(2, cnt);

        assertFalse("Statement has more results.", stmt.getMoreResults());
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testMaxRows() throws Exception {
        stmt.setMaxRows(1);

        assertEquals(1, stmt.getMaxRows());

        ResultSet rs = stmt.executeQuery(SQL);

        assertNotNull(rs);

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assertEquals("Joe", rs.getString("firstName"));
                assertEquals("Black", rs.getString("lastName"));
                assertEquals(35, rs.getInt("age"));
            }
            else if (id == 3) {
                assertEquals( "Mike", rs.getString("firstName"));
                assertEquals( "Green", rs.getString("lastName"));
                assertEquals(40, rs.getInt("age"));
            }
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(1, cnt);

        stmt.setMaxRows(0);

        rs = stmt.executeQuery(SQL);

        assertNotNull(rs);

        cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assertEquals("Joe", rs.getString("firstName"));
                assertEquals("Black", rs.getString("lastName"));
                assertEquals(35, rs.getInt("age"));
            }
            else if (id == 3) {
                assertEquals( "Mike", rs.getString("firstName"));
                assertEquals( "Green", rs.getString("lastName"));
                assertEquals(40, rs.getInt("age"));
            }
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(2, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testCloseResultSet0() throws Exception {
        ResultSet rs0 = stmt.executeQuery(SQL);
        ResultSet rs1 = stmt.executeQuery(SQL);
        ResultSet rs2 = stmt.executeQuery(SQL);

        assertTrue("ResultSet must be implicitly closed after re-execute statement", rs0.isClosed());
        assertTrue("ResultSet must be implicitly closed after re-execute statement", rs1.isClosed());

        assertFalse("Last result set must be available", rs2.isClosed());

        stmt.close();

        assertTrue("ResultSet must be explicitly closed after close statement", rs2.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testCloseResultSet1() throws Exception {
        stmt.execute(SQL);

        ResultSet rs = stmt.getResultSet();

        stmt.close();

        assertTrue("ResultSet must be explicitly closed after close statement", rs.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testCloseResultSetByConnectionClose() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        conn.close();

        assertTrue("Statement must be implicitly closed after close connection", stmt.isClosed());
        assertTrue("ResultSet must be implicitly closed after close connection", rs.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testCloseOnCompletionAfterQuery() throws Exception {
        assertFalse("Invalid default closeOnCompletion", stmt.isCloseOnCompletion());

        ResultSet rs0 = stmt.executeQuery(SQL);

        ResultSet rs1 = stmt.executeQuery(SQL);

        assertTrue("Result set must be closed implicitly", rs0.isClosed());

        assertFalse("Statement must not be closed", stmt.isClosed());

        rs1.close();

        assertFalse("Statement must not be closed", stmt.isClosed());

        ResultSet rs2 = stmt.executeQuery(SQL);

        stmt.closeOnCompletion();

        assertTrue("Invalid closeOnCompletion", stmt.isCloseOnCompletion());

        rs2.close();

        assertTrue("Statement must be closed", stmt.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testCloseOnCompletionBeforeQuery() throws Exception {
        assertFalse("Invalid default closeOnCompletion", stmt.isCloseOnCompletion());

        ResultSet rs0 = stmt.executeQuery(SQL);

        ResultSet rs1 = stmt.executeQuery(SQL);

        assertTrue("Result set must be closed implicitly", rs0.isClosed());

        assertFalse("Statement must not be closed", stmt.isClosed());

        rs1.close();

        assertFalse("Statement must not be closed", stmt.isClosed());

        stmt.closeOnCompletion();

        ResultSet rs2 = stmt.executeQuery(SQL);

        assertTrue("Invalid closeOnCompletion", stmt.isCloseOnCompletion());

        rs2.close();

        assertTrue("Statement must be closed", stmt.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testExecuteQueryMultipleOnlyResultSets() throws Exception {
        assertTrue(conn.getMetaData().supportsMultipleResultSets());

        int stmtCnt = 10;

        StringBuilder sql = new StringBuilder();

        for (int i = 0; i < stmtCnt; ++i)
            sql.append("select ").append(i).append("; ");

        assertTrue(stmt.execute(sql.toString()));

        for (int i = 0; i < stmtCnt - 1; ++i) {
            ResultSet rs = stmt.getResultSet();

            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
            assertFalse(rs.next());

            assertTrue(stmt.getMoreResults());
        }

        ResultSet rs = stmt.getResultSet();

        assertTrue(rs.next());
        assertEquals(stmtCnt - 1, rs.getInt(1));
        assertFalse(rs.next());

        assertFalse(stmt.getMoreResults());
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testExecuteQueryMultipleOnlyDml() throws Exception {
        conn.setSchema(null);

        Statement stmt0 = conn.createStatement();

        int stmtCnt = 10;

        StringBuilder sql = new StringBuilder("drop table if exists test; create table test(ID int primary key, NAME varchar(20)); ");

        for (int i = 0; i < stmtCnt; ++i)
            sql.append("insert into test (ID, NAME) values (" + i + ", 'name_" + i + "'); ");

        assertFalse(stmt0.execute(sql.toString()));

        // DROP TABLE statement
        assertNull(stmt0.getResultSet());
        assertEquals(0, stmt0.getUpdateCount());

        stmt0.getMoreResults();

        // CREATE TABLE statement
        assertNull(stmt0.getResultSet());
        assertEquals(0, stmt0.getUpdateCount());

        for (int i = 0; i < stmtCnt; ++i) {
            assertTrue(stmt0.getMoreResults());

            assertNull(stmt0.getResultSet());
            assertEquals(1, stmt0.getUpdateCount());
        }

        assertFalse(stmt0.getMoreResults());
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testExecuteQueryMultipleMixed() throws Exception {
        conn.setSchema(null);

        Statement stmt0 = conn.createStatement();

        int stmtCnt = 10;

        StringBuilder sql = new StringBuilder("drop table if exists test; create table test(ID int primary key, NAME varchar(20)); ");

        for (int i = 0; i < stmtCnt; ++i) {
            if (i % 2 == 0)
                sql.append(" insert into test (ID, NAME) values (" + i + ", 'name_" + i + "'); ");
            else
                sql.append(" select * from test where id < " + i + "; ");
        }

        assertFalse(stmt0.execute(sql.toString()));

        // DROP TABLE statement
        assertNull(stmt0.getResultSet());
        assertEquals(0, stmt0.getUpdateCount());

        assertTrue("Result set doesn't have more results.", stmt0.getMoreResults());

        // CREATE TABLE statement
        assertNull(stmt0.getResultSet());
        assertEquals(0, stmt0.getUpdateCount());

        boolean notEmptyResult = false;

        for (int i = 0; i < stmtCnt; ++i) {
            assertTrue(stmt0.getMoreResults());

            if (i % 2 == 0) {
                assertNull(stmt0.getResultSet());
                assertEquals(1, stmt0.getUpdateCount());
            }
            else {
                assertEquals(-1, stmt0.getUpdateCount());

                ResultSet rs = stmt0.getResultSet();

                int rowsCnt = 0;

                while (rs.next())
                    rowsCnt++;

                assertTrue(rowsCnt <= (i + 1) / 2);

                if (rowsCnt == (i + 1) / 2)
                    notEmptyResult = true;
            }
        }

        assertTrue(notEmptyResult);

        assertFalse(stmt0.getMoreResults());
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testExecuteUpdate() throws Exception {
        final String sqlText = "update test set val=1 where _key=1";

        assertEquals(1, stmt.executeUpdate(sqlText));

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.executeUpdate(sqlText);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testExecuteUpdateProducesResultSet() throws Exception {
        final String sqlText = "select * from test";

        GridTestUtils.assertThrows(null,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return stmt.executeUpdate(sqlText);
                }
            },
            SQLException.class,
            "Given statement type does not match that declared by JDBC driver"
        );
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testClose() throws Exception {
        String sqlText = "select * from test";

        ResultSet rs = stmt.executeQuery(sqlText);

        assertTrue(rs.next());
        assertFalse(rs.isClosed());

        assertFalse(stmt.isClosed());

        stmt.close();
        stmt.close(); // Closing closed is ok

        assertTrue(stmt.isClosed());

        // Current result set must be closed
        assertTrue(rs.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testGetSetMaxFieldSizeUnsupported() throws Exception {
        assertEquals(0, stmt.getMaxFieldSize());

        GridTestUtils.assertThrows(null,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setMaxFieldSize(100);

                    return null;
                }
            },
            SQLFeatureNotSupportedException.class,
            "Field size limitation is not supported"
        );

        assertEquals(0, stmt.getMaxFieldSize());

        stmt.close();

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.getMaxFieldSize();
            }
        });

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setMaxFieldSize(100);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testGetSetMaxRows() throws Exception {
        assertEquals(0, stmt.getMaxRows());

        GridTestUtils.assertThrows(null,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setMaxRows(-1);

                    return null;
                }
            },
            SQLException.class,
            "Invalid max rows value"
        );

        assertEquals(0, stmt.getMaxRows());

        final int maxRows = 1;

        stmt.setMaxRows(maxRows);

        assertEquals(maxRows, stmt.getMaxRows());

        String sqlText = "select * from test";

        ResultSet rs = stmt.executeQuery(sqlText);

        assertTrue(rs.next());
        assertFalse(rs.next()); //max rows reached

        stmt.close();

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.getMaxRows();
            }
        });

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setMaxRows(maxRows);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-5440")
    public void testSetEscapeProcessing() throws Exception {
        stmt.setEscapeProcessing(false);

        final String sqlText = "select {fn CONVERT(1, SQL_BOOLEAN)}";

        GridTestUtils.assertThrows(null,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return stmt.executeQuery(sqlText);
                }
            },
            SQLException.class,
            "Failed to parse"
        );

        ResultSet rs = stmt.executeQuery(sqlText);

        assertTrue(rs.next());

        assertEquals(true, rs.getBoolean(1));

        stmt.setEscapeProcessing(true);

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setEscapeProcessing(true);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testGetSetQueryTimeout() throws Exception {
        assertEquals(0, stmt.getQueryTimeout());

        // Invalid argument
        GridTestUtils.assertThrows(null,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setQueryTimeout(-1);

                    return null;
                }
            },
            SQLException.class,
            "Invalid timeout value"
        );

        assertEquals(0, stmt.getQueryTimeout());

        final int timeout = 3;

        stmt.setQueryTimeout(timeout);

        assertEquals(timeout, stmt.getQueryTimeout());

        stmt.close();

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.getQueryTimeout();
            }
        });

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setQueryTimeout(timeout);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testMaxFieldSize() throws Exception {
        assertTrue(stmt.getMaxFieldSize() >= 0);

        GridTestUtils.assertThrows(null,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setMaxFieldSize(-1);

                    return null;
                }
            },
            SQLException.class,
            "Invalid field limit"
        );

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setMaxFieldSize(100);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testQueryTimeout() throws Exception {
        assertEquals("Default timeout invalid: " + stmt.getQueryTimeout(), 0, stmt.getQueryTimeout());

        stmt.setQueryTimeout(10);

        assertEquals(10, stmt.getQueryTimeout());

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.getQueryTimeout();
            }
        });

        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setQueryTimeout(10);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testWarningsOnClosedStatement() throws Exception {
        stmt.clearWarnings();

        assertNull(null, stmt.getWarnings());

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.getWarnings();
            }
        });

        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.clearWarnings();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testCursorName() throws Exception {
        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setCursorName("test");
            }
        });

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setCursorName("test");
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testGetMoreResults() throws Exception {
        assertFalse(stmt.getMoreResults());

        stmt.execute("select 1; ");

        ResultSet rs = stmt.getResultSet();

        assertFalse(stmt.getMoreResults());

        assertNull(stmt.getResultSet());

        assertTrue(rs.isClosed());

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.getMoreResults();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testGetMoreResultsKeepCurrent() throws Exception {
        assertFalse(stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
        assertFalse(stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        assertFalse(stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS));

        stmt.execute("select 1; ");

        ResultSet rs = stmt.getResultSet();

        assertFalse(stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));

        assertFalse(rs.isClosed());

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testGetMoreResultsCloseAll() throws Exception {
        assertFalse(stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
        assertFalse(stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        assertFalse(stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS));

        stmt.execute("select 1; ");

        ResultSet rs = stmt.getResultSet();

        assertFalse(stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS));

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            }
        });
    }

    /**
     * Verifies that emty batch can be performed.
     *
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testBatchEmpty() throws Exception {
        assertTrue(conn.getMetaData().supportsBatchUpdates());

        stmt.addBatch("");
        stmt.clearBatch();

        // Just verify that no exception have been thrown.
        stmt.executeBatch();
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testFetchDirection() throws Exception {
        assertEquals(ResultSet.FETCH_FORWARD, stmt.getFetchDirection());

        GridTestUtils.assertThrows(null,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setFetchDirection(ResultSet.FETCH_REVERSE);

                    return null;
                }
            },
            SQLFeatureNotSupportedException.class,
            "Only forward direction is supported."
        );

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setFetchDirection(-1);
            }
        });

        checkStatementClosed(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.getFetchDirection();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testAutogenerated() throws Exception {
        GridTestUtils.assertThrows(null,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.executeUpdate("select 1", -1);

                    return null;
                }
            },
            SQLException.class,
            "Invalid autoGeneratedKeys value");

        GridTestUtils.assertThrows(null,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.execute("select 1", -1);

                    return null;
                }
            },
            SQLException.class,
            "Invalid autoGeneratedKeys value");

        assertFalse(conn.getMetaData().supportsGetGeneratedKeys());

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.getGeneratedKeys();
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.executeUpdate("select 1", Statement.RETURN_GENERATED_KEYS);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.executeUpdate("select 1", new int[] {1, 2});
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.executeUpdate("select 1", new String[] {"a", "b"});
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.execute("select 1", Statement.RETURN_GENERATED_KEYS);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.execute("select 1", new int[] {1, 2});
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.execute("select 1", new String[] {"a", "b"});
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testStatementTypeMismatchSelectForCachedQuery() throws Exception {
        // Put query to cache.
        stmt.executeQuery("select 1;");

        GridTestUtils.assertThrows(null,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.executeUpdate("select 1;");

                    return null;
                }
            },
            SQLException.class,
            "Given statement type does not match that declared by JDBC driver");

        assertNull("Not results expected. Last statement is executed with exception", stmt.getResultSet());
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testStatementTypeMismatchUpdate() throws Exception {
        GridTestUtils.assertThrows(null,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.executeQuery("update test set val=28 where _key=1");

                    return null;
                }
            },
            SQLException.class,
            "Given statement type does not match that declared by JDBC driver");

        ResultSet rs = stmt.executeQuery("select val from test where _key=1");

        boolean next = rs.next();

        assertTrue(next);

        assertEquals("The data must not be updated. " +
            "Because update statement is executed via 'executeQuery' method." +
            " Data [val=" + rs.getInt(1) + ']',
            1,
            rs.getInt(1));
    }

    /**
     * Verifies that a server-side deserialization failure surfaces as a {@link SQLException} with
     * the proper SQL state and cause, instead of e.g. hanging the connection.
     *
     * The original test populated and read from the cache directly through the embedded grid's
     * {@code IgniteCache}/{@code binary()} APIs. Here the DDL-created cache is instead addressed
     * through the Java thin client, which is reachable the same way since the cache is visible
     * cluster-wide once created.
     *
     * @throws SQLException If failed.
     */
    @org.junit.Test
    public void testExceptionOnDeserializeResponse() throws SQLException {
        try (Connection c = createConnection()) {
            execute(c, "CREATE TABLE TEST_DESERIALIZE(id int primary key, name varchar, BINFIELD OTHER) WITH " +
                "\"cache_name=TEST_DESERIALIZE,VALUE_TYPE=TEST_TYPE\"");

            try (IgniteClient client = Ignition.startClient(new ClientConfiguration()
                .setAddresses(cluster.host() + ":" + cluster.port()))) {

                ClientCache<Object, Object> cc = client.cache("TEST_DESERIALIZE");

                BinaryObjectBuilder bobFld = client.binary().builder("TestType");
                bobFld.setField("fld0", 0);

                BinaryObjectBuilder bob = client.binary().builder("TEST_TYPE");
                bob.setField("NAME", "name0");
                bob.setField("BINFIELD", bobFld.build());

                cc.put(0, bob.build());
            }

            try (Statement stmt = c.createStatement()) {
                SQLException ex = (SQLException)GridTestUtils.assertThrows(
                    null,
                    () -> stmt.executeQuery("SELECT * FROM TEST_DESERIALIZE"),
                    SQLException.class,
                    "Serialization error during sending an sql request"
                );

                assertEquals(SqlStateCode.DATA_EXCEPTION, ex.getSQLState());
                assertTrue(X.hasCause(ex, "TestType", BinaryInvalidTypeException.class));

                ResultSet rs = stmt.executeQuery("SELECT id FROM TEST_DESERIALIZE");

                rs.next();
                assertEquals(0, rs.getInt(1));
            }
        }
    }
}
