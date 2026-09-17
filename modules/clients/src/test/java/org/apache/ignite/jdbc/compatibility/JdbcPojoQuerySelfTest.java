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

package org.apache.ignite.jdbc.compatibility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests JDBC queries against binary objects with nested types, using an Ignite node running in Docker.
 *
 * Data is populated via the Java Thin Client. Queries run over the JDBC Thin driver.
 */
public class JdbcPojoQuerySelfTest {
    private static final String TEST_OBJECT = "org.apache.ignite.internal.JdbcTestObject";

    private static final String TEST_OBJECT_2 = "org.apache.ignite.internal.JdbcTestObject2";

    private static final String CACHE_NAME = "default";

    private static final IgniteDocker service = new IgniteDocker();

    private static String jdbcUrl;

    private Connection conn;

    private Statement stmt;

    @BeforeClass
    public static void startGrid() throws Exception {
        service.start();

        jdbcUrl = String.format("jdbc:ignite:thin://%s:%d", service.host(), service.port());

        setupData();
    }

    private static void setupData() {
        String addr = String.format("%s:%d", service.host(), service.port());

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(addr))) {
            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
            fields.put("id", Integer.class.getName());
            fields.put("testObject", TEST_OBJECT_2);

            QueryEntity entity = new QueryEntity()
                .setKeyType(String.class.getName())
                .setValueType(TEST_OBJECT)
                .setFields(fields)
                .setIndexes(Collections.singletonList(new QueryIndex("id")));

            ClientCache<String, BinaryObject> cache = client.createCache(
                new ClientCacheConfiguration()
                    .setName(CACHE_NAME)
                    .setQueryEntities(entity));

            BinaryObject nested = client.binary().builder(TEST_OBJECT_2)
                .setField("id", 1)
                .setField("boolVal", true)
                .build();

            BinaryObject obj = client.binary().builder(TEST_OBJECT)
                .setField("id", 1)
                .setField("testObject", nested)
                .build();

            cache.put("0", obj);
        }
    }

    @AfterClass
    public static void stopGrid() {
        service.stop();
    }

    @Before
    public void openConnection() throws Exception {
        conn = DriverManager.getConnection(jdbcUrl);
        stmt = conn.createStatement();

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());
    }

    @After
    public void closeConnection() throws Exception {
        if (conn != null)
            conn.close();
    }

    @Test
    public void testJdbcQueryTask1() throws Exception {
        ResultSet rs = stmt.executeQuery("select * from \"" + CACHE_NAME + "\".JdbcTestObject");

        assertResultSet(rs);
    }

    @Test
    public void testJdbcQueryTask2() throws Exception {
        stmt.execute("select * from \"" + CACHE_NAME + "\".JdbcTestObject");

        ResultSet rs = stmt.getResultSet();

        assertResultSet(rs);
    }

    private static void assertResultSet(ResultSet rs) throws Exception {
        assertNotNull(rs);

        int cnt = 0;

        while (rs.next()) {
            assertNotNull(rs.getString("id"));
            assertNotNull(rs.getString("testObject"));

            assertTrue(rs.getObject("testObject").toString().contains("id=1"));
            assertTrue(rs.getObject("testObject").toString().contains("boolVal=true"));

            cnt++;
        }

        assertEquals(1, cnt);
    }
}
