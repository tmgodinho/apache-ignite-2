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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcThinFeature;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.OTHER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static org.apache.ignite.internal.util.CommonUtils.MAX_ARRAY_SIZE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Prepared statement test.
 *
 * Ported from {@code org.apache.ignite.jdbc.thin.JdbcThinPreparedStatementSelfTest} to run against
 * an Ignite node in Docker ({@link IgniteDocker}), using only the JDBC thin driver and the Java
 * thin client.
 *
 * The original {@code TestObject} fixture (created server-side through {@code CacheConfiguration
 * #setIndexedTypes}) is replaced with a plain SQL table created and populated entirely through
 * ordinary JDBC (DDL + parameterized INSERT), since the Docker-based server has no IndexedTypes
 * classes on its classpath.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinPreparedStatementSelfTest extends JdbcThinAbstractSelfTest {
    /** SQL query. */
    private static final String SQL_PART =
        "select id, boolVal, byteVal, shortVal, intVal, longVal, floatVal, " +
            "doubleVal, bigVal, strVal, arrVal, dateVal, timeVal, tsVal, objVal, blobVal, clobVal " +
            "from TestObject ";

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private PreparedStatement stmt;

    /**
     * Creates and populates the {@code TestObject} table via plain JDBC DDL/DML.
     *
     * @throws Exception If failed.
     */
    @BeforeClass
    public static void createTestObjectTable() throws Exception {
        try (Connection c = DriverManager.getConnection(jdbcUrl)) {
            c.createStatement().execute(
                "CREATE TABLE TestObject(" +
                    "id INT PRIMARY KEY, " +
                    "boolVal BOOLEAN, " +
                    "byteVal TINYINT, " +
                    "shortVal SMALLINT, " +
                    "intVal INT, " +
                    "longVal BIGINT, " +
                    "floatVal REAL, " +
                    "doubleVal DOUBLE, " +
                    "bigVal DECIMAL, " +
                    "strVal VARCHAR, " +
                    "arrVal BINARY, " +
                    // dateVal is declared TIMESTAMP (not DATE) so it preserves full millisecond
                    // precision: a plain SQL DATE column truncates the time-of-day component on
                    // storage, but the "is not distinct from ?" comparisons in testDate() below bind
                    // their parameter via setObject(new java.util.Date(1)) without any such
                    // truncation, so a DATE column would never compare equal to that parameter.
                    "dateVal TIMESTAMP, " +
                    "timeVal TIME, " +
                    "tsVal TIMESTAMP, " +
                    "objVal OTHER, " +
                    "blobVal BINARY, " +
                    "clobVal VARCHAR)"
            );

            try (PreparedStatement ins = c.prepareStatement(
                "INSERT INTO TestObject(id, boolVal, byteVal, shortVal, intVal, longVal, floatVal, " +
                    "doubleVal, bigVal, strVal, arrVal, dateVal, timeVal, tsVal, objVal, blobVal, clobVal) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {

                ins.setInt(1, 1);
                ins.setBoolean(2, true);
                ins.setByte(3, (byte)1);
                ins.setShort(4, (short)1);
                ins.setInt(5, 1);
                ins.setLong(6, 1L);
                ins.setFloat(7, 1.0f);
                ins.setDouble(8, 1.0d);
                ins.setBigDecimal(9, new BigDecimal(1));
                ins.setString(10, "str");
                ins.setBytes(11, new byte[] {1});
                // Use setObject (rather than setDate) to match exactly how testDate() below binds its
                // "dateVal" query parameter, so both the fixture row and the query filter go through the
                // identical java.util.Date -> wire conversion path.
                ins.setObject(12, new Date(1));
                ins.setTime(13, new Time(1));
                ins.setTimestamp(14, new Timestamp(1));
                ins.setObject(15, new TestObjectField(100, "AAAA"));
                ins.setBytes(16, new byte[] {1, 2, 3});
                ins.setString(17, "large str");

                ins.executeUpdate();

                ins.setInt(1, 2);
                ins.setNull(2, BOOLEAN);
                ins.setNull(3, TINYINT);
                ins.setNull(4, SMALLINT);
                ins.setNull(5, INTEGER);
                ins.setNull(6, BIGINT);
                ins.setNull(7, FLOAT);
                ins.setNull(8, DOUBLE);
                ins.setNull(9, OTHER);
                ins.setNull(10, VARCHAR);
                ins.setNull(11, BINARY);
                ins.setNull(12, DATE);
                ins.setNull(13, TIME);
                ins.setNull(14, TIMESTAMP);
                ins.setNull(15, OTHER);
                ins.setNull(16, BLOB);
                ins.setNull(17, CLOB);

                ins.executeUpdate();
            }
        }
    }

    /**
     * Drops the {@code TestObject} table.
     */
    @AfterClass
    public static void dropTestObjectTable() throws Exception {
        try (Connection c = DriverManager.getConnection(jdbcUrl)) {
            c.createStatement().execute("DROP TABLE TestObject");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Before
    public void beforeTest() throws Exception {
        conn = createConnection(false);

        assert conn != null;
        assert !conn.isClosed();
    }

    /**
     * @throws Exception If failed.
     */
    @After
    public void afterTest() throws Exception {
        if (stmt != null) {
            stmt.close();

            assert stmt.isClosed();
        }

        if (conn != null) {
            conn.close();

            assert conn.isClosed();
        }
    }

    /**
     * Create new JDBC connection to the grid.
     *
     * @param keepBinary Whether to keep bin object in binary format.
     * @return New connection.
     */
    private Connection createConnection(boolean keepBinary) throws SQLException {
        String url = keepBinary ? jdbcUrl + "?keepBinary=true" : jdbcUrl;

        return DriverManager.getConnection(url);
    }

    /**
     * Create new JDBC connection to the grid.
     *
     * @param disabledFeatues Features that should be disabled.
     * @return New connection.
     */
    private Connection createConnection(JdbcThinFeature... disabledFeatues) throws SQLException {
        String url = jdbcUrl + "?disabledFeatures=" + Arrays.stream(disabledFeatues)
            .map(JdbcThinFeature::name)
            .collect(Collectors.joining(","));

        return DriverManager.getConnection(url);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRepeatableUsage() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where id = ?");

        stmt.setInt(1, 1);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assertEquals(1, rs.getInt(1));

            cnt++;
        }

        assertEquals(1, cnt);

        cnt = 0;

        rs = stmt.executeQuery();

        while (rs.next()) {
            if (cnt == 0)
                assertEquals(1, rs.getInt(1));

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * Ensure binary object's meta is properly synchronized between connections
     *      - start grid
     *      - from one connection create and fill table such one of the columns was user's object
     *      - from another connection execute query with filter by this object
     *      - verify that result is not empty and returned object is the same as expected
     *
     * @throws SQLException In case of any sql error.
     */
    @Test
    public void testObjectDifferentConnections() throws SQLException {
        final TestObjectField exp = new TestObjectField(42, "BBBB");

        conn.createStatement().execute("CREATE TABLE test(id INT PRIMARY KEY, objVal OTHER)");

        try {
            stmt = conn.prepareStatement("INSERT INTO test(id, objVal) VALUES (?, ?)");

            stmt.setInt(1, exp.a);
            stmt.setObject(2, exp);

            stmt.execute();

            try (Connection anotherConn = createConnection(false);
                 PreparedStatement stmt = anotherConn.prepareStatement("SELECT id, objVal FROM test WHERE id = ?")
            ) {
                stmt.setInt(1, exp.a);

                int cnt = 0;

                ResultSet rs = stmt.executeQuery();

                while (rs.next()) {
                    if (cnt == 0) {
                        Assert.assertTrue("Result's value type mismatch",
                            rs.getObject("objVal") instanceof TestObjectField);

                        Assert.assertEquals("Result's value mismatch", exp, rs.getObject("objVal", TestObjectField.class));
                    }

                    cnt++;
                }

                Assert.assertEquals("There should be exactly 1 result", 1, cnt);
            }
        }
        finally {
            conn.createStatement().execute("DROP TABLE test");
        }
    }

    /**
     * Ensure custom objects can be retrieved as {@link BinaryObject}
     * if keepBinary flag is set to {@code true} on connection
     *      - start grid and create and fill table such one of the columns was user's object
     *      - from another connection with keepBinary flag set to {@code true}
     *      execute query with filter by this object
     *      - verify that result is not empty and returned object is the {@link BinaryObject}
     *
     * @throws SQLException In case of any sql error.
     */
    @Test
    public void testObjectConnectionWithKeepBinaryFlag() throws SQLException {
        try (Connection anotherConn = createConnection(true)) {
            stmt = anotherConn.prepareStatement(SQL_PART + " where objVal is not distinct from ?");

            stmt.setObject(1, new TestObjectField(100, "AAAA"));

            int cnt = 0;

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                if (cnt == 0) {
                    Assert.assertEquals("Result's id mismatch", 1, rs.getInt("id"));

                    Assert.assertTrue(rs.getObject("objVal") instanceof BinaryObject);

                    Assert.assertEquals("Result's value mismatch", Integer.valueOf(100),
                        rs.getObject("objVal", BinaryObject.class).field("a"));
                }

                cnt++;
            }

            Assert.assertEquals("There should be exactly 1 result", 1, cnt);
        }
    }

    /**
     * Ensure custom objects can be retrieved through JdbcThinConnection
     *      - start grid and create and fill table such one of the columns was user's object
     *      - execute query with filter by this object (use both real object and null for param value)
     *      - verify that result is not empty and returned object is the same as expected
     *
     * @throws Exception If failed.
     */
    @Test
    public void testObject() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where objVal is not distinct from ?");

        stmt.setObject(1, new TestObjectField(100, "AAAA"));

        int cnt = 0;

        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            if (cnt == 0) {
                Assert.assertEquals("Result's id mismatch", 1, rs.getInt("id"));

                Assert.assertTrue("Result's value type mismatch",
                    rs.getObject("objVal") instanceof TestObjectField);

                Assert.assertEquals("Result's value mismatch", 100,
                    rs.getObject("objVal", TestObjectField.class).a);
            }

            cnt++;
        }

        Assert.assertEquals("There should be exactly 1 result", 1, cnt);

        stmt.setNull(1, Types.JAVA_OBJECT);

        stmt.execute();

        cnt = 0;

        rs = stmt.getResultSet();

        while (rs.next()) {
            if (cnt == 0) {
                Assert.assertEquals("Result's id mismatch", 2, rs.getInt("id"));

                Assert.assertNull("Result's value should be null", rs.getObject("objVal"));
            }

            cnt++;
        }

        Assert.assertEquals("There should be exactly 1 result", 1, cnt);
    }

    /**
     * Ensure custom object support could be disabled via disabledFeatures connection property
     *      - start grid and create and fill table such one of the columns was user's object
     *      - from another connection with disabledFeatures set to {@link JdbcThinFeature#CUSTOM_OBJECT}
     *      execute query with filter by this object
     *      - verify that exception is thrown when you try to set custom object as statement param
     * @throws SQLException
     */
    @Test
    public void testCustomObjectSupportCanBeDisabled() throws SQLException {
        try (Connection conn = createConnection(JdbcThinFeature.CUSTOM_OBJECT);
            PreparedStatement stmt = conn.prepareStatement(SQL_PART + " where objVal is not distinct from ?")
        ) {
            Throwable t = GridTestUtils.assertThrowsWithCause(
                new RunnableX() {
                    @Override public void runx() throws Exception {
                        stmt.setObject(1, new TestObjectField(100, "AAAA"));
                    }
                },
                SQLException.class
            );

            Assert.assertThat(t.getMessage(), is(containsString("Custom objects are not supported")));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryExecuteException() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where boolVal is not distinct from ?");

        stmt.setBoolean(1, true);

        GridTestUtils.assertThrowsAnyCause(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                stmt.executeQuery("select 1");

                return null;
            }
        }, SQLException.class, "The method 'executeQuery(String)' is called on PreparedStatement instance.");

        GridTestUtils.assertThrowsAnyCause(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                stmt.execute("select 1");

                return null;
            }
        }, SQLException.class, "The method 'execute(String)' is called on PreparedStatement instance.");

        GridTestUtils.assertThrowsAnyCause(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                stmt.execute("select 1", Statement.NO_GENERATED_KEYS);

                return null;
            }
        }, SQLException.class, "The method 'execute(String)' is called on PreparedStatement instance.");

        GridTestUtils.assertThrowsAnyCause(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                stmt.executeUpdate("select 1", Statement.NO_GENERATED_KEYS);

                return null;
            }
        }, SQLException.class, "The method 'executeUpdate(String, int)' is called on PreparedStatement instance.");

        GridTestUtils.assertThrowsAnyCause(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                stmt.executeUpdate("select 1", new int[] {1});

                return null;
            }
        }, SQLException.class, "The method 'executeUpdate(String, int[])' is called on PreparedStatement instance.");

        GridTestUtils.assertThrowsAnyCause(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                stmt.executeUpdate("select 1 as a", new String[]{"a"});

                return null;
            }
        }, SQLException.class, "The method 'executeUpdate(String, String[])' is called on PreparedStatement instance.");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBoolean() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where boolVal is not distinct from ?");

        stmt.setBoolean(1, true);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, BOOLEAN);

        stmt.execute();

        rs = stmt.getResultSet();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testByte() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where byteVal is not distinct from ?");

        stmt.setByte(1, (byte)1);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, TINYINT);

        stmt.execute();

        rs = stmt.getResultSet();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testShort() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where shortVal is not distinct from ?");

        stmt.setShort(1, (short)1);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, SMALLINT);

        stmt.execute();

        rs = stmt.getResultSet();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInteger() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where intVal is not distinct from ?");

        stmt.setInt(1, 1);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, INTEGER);

        stmt.execute();

        rs = stmt.getResultSet();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLong() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where longVal is not distinct from ?");

        stmt.setLong(1, 1L);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, BIGINT);

        stmt.execute();

        rs = stmt.getResultSet();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFloat() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where floatVal is not distinct from ?");

        stmt.setFloat(1, 1.0f);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, FLOAT);

        stmt.execute();

        rs = stmt.getResultSet();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDouble() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where doubleVal is not distinct from ?");

        stmt.setDouble(1, 1.0d);

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, DOUBLE);

        stmt.execute();

        rs = stmt.getResultSet();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBigDecimal() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where bigVal is not distinct from ?");

        stmt.setBigDecimal(1, new BigDecimal(1));

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, OTHER);

        stmt.execute();

        rs = stmt.getResultSet();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testString() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where strVal is not distinct from ?");

        stmt.setString(1, "str");

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, VARCHAR);

        stmt.execute();

        rs = stmt.getResultSet();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testArray() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where arrVal is not distinct from ?");

        stmt.setBytes(1, new byte[] {1});

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, BINARY);

        stmt.execute();

        rs = stmt.getResultSet();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBlob() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where blobVal is not distinct from ?");

        Blob blob = conn.createBlob();

        blob.setBytes(1, new byte[] {1, 2, 3});

        stmt.setBlob(1, blob);

        ResultSet rs = stmt.executeQuery();

        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.next());

        stmt.setNull(1, BLOB);

        rs = stmt.executeQuery();

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertFalse(rs.next());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBlobWrittenViaOutputStream() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where blobVal is not distinct from ?");

        Blob blob = conn.createBlob();

        try (OutputStream out = blob.setBinaryStream(1)) {
            out.write(new byte[] {1, 2, 3});
        }

        stmt.setBlob(1, blob);

        checkStmtExec(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBlobSeeChangesDoneAfterAddToStatement() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where blobVal is not distinct from ?");

        Blob blob = conn.createBlob();

        stmt.setBlob(1, blob);

        try (OutputStream out = blob.setBinaryStream(1)) {
            out.write(new byte[] {1});
            out.write(2);
        }

        blob.setBytes(3, new byte[] {3});

        checkStmtExec(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBlobNull() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where blobVal is not distinct from ?");

        stmt.setBlob(1, (Blob)null);
        checkStmtExec(2);

        stmt.setNull(1, BLOB);
        checkStmtExec(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryStreamKnownLength() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where blobVal is not distinct from ?");

        ByteArrayInputStream stream = new ByteArrayInputStream(new byte[]{1, 2, 3});

        assertThrows(null, () -> {
            stmt.setBinaryStream(1, stream, -1L);
            return null;
        }, SQLException.class, null);

        assertThrows(null, () -> {
            stmt.setBinaryStream(1, stream, -1);
            return null;
        }, SQLException.class, null);

        assertThrows(null, () -> {
            stmt.setBinaryStream(1, stream, (long)MAX_ARRAY_SIZE + 1);
            return null;
        }, SQLFeatureNotSupportedException.class, null);

        assertThrows(null, () -> {
            stmt.setBinaryStream(1, stream, MAX_ARRAY_SIZE + 1);
            return null;
        }, SQLFeatureNotSupportedException.class, null);

        stmt.setBinaryStream(1, stream, 3);
        checkStmtExec(1);

        stream.reset();
        stmt.setBinaryStream(1, stream, 3L);
        checkStmtExec(1);

        stream.reset();
        stmt.setBinaryStream(1, stream, 10L);
        assertThrows(null, () -> stmt.executeQuery(), SQLException.class, null);

        stmt.setBinaryStream(1, null, 0);
        checkStmtExec(2);

        stmt.setBinaryStream(1, null, 0L);
        checkStmtExec(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryStreamUnknownLength() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where blobVal is not distinct from ?");

        ByteArrayInputStream stream = new ByteArrayInputStream(new byte[]{1, 2, 3});

        stmt.setBinaryStream(1, stream);
        checkStmtExec(1);

        stmt.setBinaryStream(1, null);
        checkStmtExec(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryStreamThrows() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where blobVal is not distinct from ?");

        stmt.setBinaryStream(1, new ThrowingInputStream());

        assertThrows(null, () -> stmt.executeQuery(), SQLException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBlobStreamKnownLength() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where blobVal is not distinct from ?");

        ByteArrayInputStream stream = new ByteArrayInputStream(new byte[]{1, 2, 3});

        assertThrows(null, () -> {
            stmt.setBlob(1, stream, -1L);
            return null;
        }, SQLException.class, null);

        assertThrows(null, () -> {
            stmt.setBlob(1, stream, (long)MAX_ARRAY_SIZE + 1);
            return null;
        }, SQLFeatureNotSupportedException.class, null);

        stmt.setBlob(1, stream, 3L);
        checkStmtExec(1);

        stream.reset();
        stmt.setBlob(1, stream, 10L);
        assertThrows(null, () -> stmt.executeQuery(), SQLException.class, null);

        stmt.setBlob(1, null, 0L);
        checkStmtExec(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBlobStreamUnknownLength() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where blobVal is not distinct from ?");

        ByteArrayInputStream stream = new ByteArrayInputStream(new byte[]{1, 2, 3});

        stmt.setBlob(1, stream);
        checkStmtExec(1);

        stmt.setBlob(1, (Blob)null);
        checkStmtExec(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBlobStreamThrows() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where blobVal is not distinct from ?");

        stmt.setBinaryStream(1, new ThrowingInputStream());

        assertThrows(null, () -> stmt.executeQuery(), SQLException.class, null);
    }

    /** */
    private void checkStmtExec(int expectedId) throws SQLException {
        ResultSet rs = stmt.executeQuery();

        assertTrue(rs.next());
        assertEquals(expectedId, rs.getInt("id"));
        assertFalse(rs.next());
    }

    /** */
    static class ThrowingInputStream extends InputStream {
        /** {@inheritDoc} */
        @Override public int read() throws IOException {
            throw new IOException();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClob() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where clobVal is not distinct from ?");

        Clob clob = conn.createClob();

        clob.setString(1, "large str");

        stmt.setClob(1, clob);

        ResultSet rs = stmt.executeQuery();

        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertFalse(rs.next());

        stmt.setNull(1, CLOB);

        rs = stmt.executeQuery();

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertFalse(rs.next());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDate() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where dateVal is not distinct from ?");

        stmt.setObject(1, new Date(1));

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, DATE);

        stmt.execute();

        rs = stmt.getResultSet();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTime() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where timeVal is not distinct from ?");

        stmt.setTime(1, new Time(1));

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, TIME);

        stmt.execute();

        rs = stmt.getResultSet();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTimestamp() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where tsVal is not distinct from ?");

        stmt.setTimestamp(1, new Timestamp(1));

        ResultSet rs = stmt.executeQuery();

        int cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 1;

            cnt++;
        }

        assert cnt == 1;

        stmt.setNull(1, TIMESTAMP);

        stmt.execute();

        rs = stmt.getResultSet();

        cnt = 0;

        while (rs.next()) {
            if (cnt == 0)
                assert rs.getInt("id") == 2;

            cnt++;
        }

        assert cnt == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClearParameter() throws Exception {
        stmt = conn.prepareStatement(SQL_PART + " where boolVal is not distinct from ?");

        stmt.setString(1, "");
        stmt.setLong(2, 1L);
        stmt.setInt(5, 1);

        stmt.clearParameters();

        stmt.setBoolean(1, true);

        ResultSet rs = stmt.executeQuery();

        boolean hasNext = rs.next();

        assert hasNext;

        assert rs.getInt("id") == 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotSupportedTypes() throws Exception {
        stmt = conn.prepareStatement("");

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setArray(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setAsciiStream(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setAsciiStream(1, null, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setAsciiStream(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setCharacterStream(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setCharacterStream(1, null, 0);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setCharacterStream(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setClob(1, (Reader)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setClob(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setNCharacterStream(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setNCharacterStream(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setNClob(1, (NClob)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setNClob(1, (Reader)null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setNClob(1, null, 0L);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setRowId(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setRef(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setSQLXML(1, null);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void runx() throws Exception {
                stmt.setURL(1, new URL("http://test"));
            }
        });
    }

    /**
     * Dummy object represents object field of the {@code TestObject} table's {@code objVal} column.
     */
    @SuppressWarnings("PackageVisibleField")
    private static class TestObjectField implements Serializable {
        /** */
        final int a;

        /** */
        final String b;

        /**
         * @param a A.
         * @param b B.
         */
        private TestObjectField(int a, String b) {
            this.a = a;
            this.b = b;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            TestObjectField that = (TestObjectField)o;

            return a == that.a && !(b != null ? !b.equals(that.b) : that.b != null);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = a;

            res = 31 * res + (b != null ? b.hashCode() : 0);

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestObjectField.class, this);
        }
    }
}
