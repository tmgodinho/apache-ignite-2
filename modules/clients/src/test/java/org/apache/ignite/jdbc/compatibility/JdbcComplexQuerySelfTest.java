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

import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.junit.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;

/**
 * Tests for complex queries (joins, etc.) against an Ignite node running in Docker.
 */
public class JdbcComplexQuerySelfTest {
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

    /**
     * Creates the "org" and "pers" caches via the thin client, registers their SQL schema
     * through {@link QueryEntity}, and populates them with {@link BinaryObject} values.
     */
    private static void setupData() {
        String thinClientAddr = String.format("%s:%d", service.host(), service.port());
        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(thinClientAddr))) {
            ClientCache<String, BinaryObject> orgCache = client.createCache(orgCacheConfig());

            orgCache.put("o1", client.binary().builder("Organization")
                .setField("id", 1).setField("name", "A").build());
            orgCache.put("o2", client.binary().builder("Organization")
                .setField("id", 2).setField("name", "B").build());

            ClientCache<String, BinaryObject> persCache = client.createCache(persCacheConfig());

            persCache.put("p1", client.binary().builder("Person")
                .setField("id", 1).setField("name", "John White").setField("age", 25).setField("orgId", 1).build());
            persCache.put("p2", client.binary().builder("Person")
                .setField("id", 2).setField("name", "Joe Black").setField("age", 35).setField("orgId", 1).build());
            persCache.put("p3", client.binary().builder("Person")
                .setField("id", 3).setField("name", "Mike Green").setField("age", 40).setField("orgId", 2).build());
        }
    }

    /** */
    private static ClientCacheConfiguration orgCacheConfig() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", Integer.class.getName());
        fields.put("name", String.class.getName());

        QueryEntity entity = new QueryEntity()
            .setKeyType(String.class.getName())
            .setValueType("Organization")
            .setFields(fields);

        return new ClientCacheConfiguration()
            .setName("org")
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setBackups(1)
            .setQueryEntities(entity);
    }

    /** */
    private static ClientCacheConfiguration persCacheConfig() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", Integer.class.getName());
        fields.put("name", String.class.getName());
        fields.put("age", Integer.class.getName());
        fields.put("orgId", Integer.class.getName());

        QueryEntity entity = new QueryEntity()
            .setKeyType(String.class.getName())
            .setValueType("Person")
            .setFields(fields);

        return new ClientCacheConfiguration()
            .setName("pers")
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setBackups(1)
            .setQueryEntities(entity);
    }

    @AfterClass
    public static void stopGrid() {
        service.stop();
    }

    @Before
    public void openConnection() throws Exception {
        conn = DriverManager.getConnection(jdbcUrl);
        stmt = conn.createStatement();
    }

    @After
    public void closeConnection() throws Exception {
        if (conn != null)
            conn.close();
    }

    @Test
    public void testJoin() throws Exception {
        ResultSet rs = stmt.executeQuery(
            "select p.id, p.name, o.name as orgName from \"pers\".Person p, \"org\".Organization o where p.orgId = o.id");

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 1) {
                assert "John White".equals(rs.getString("name"));
                assert "A".equals(rs.getString("orgName"));
            }
            else if (id == 2) {
                assert "Joe Black".equals(rs.getString("name"));
                assert "A".equals(rs.getString("orgName"));
            }
            else if (id == 3) {
                assert "Mike Green".equals(rs.getString("name"));
                assert "B".equals(rs.getString("orgName"));
            }
            else
                assert false : "Wrong ID: " + id;

            cnt++;
        }

        assert cnt == 3;
    }

    @Test
    public void testJoinWithoutAlias() throws Exception {
        ResultSet rs = stmt.executeQuery(
            "select p.id, p.name, o.name from \"pers\".Person p, \"org\".Organization o where p.orgId = o.id");

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt(1);

            if (id == 1) {
                assert "John White".equals(rs.getString("name"));
                assert "John White".equals(rs.getString(2));
                assert "A".equals(rs.getString(3));
            }
            else if (id == 2) {
                assert "Joe Black".equals(rs.getString("name"));
                assert "Joe Black".equals(rs.getString(2));
                assert "A".equals(rs.getString(3));
            }
            else if (id == 3) {
                assert "Mike Green".equals(rs.getString("name"));
                assert "Mike Green".equals(rs.getString(2));
                assert "B".equals(rs.getString(3));
            }
            else
                assert false : "Wrong ID: " + id;

            cnt++;
        }

        assert cnt == 3;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIn() throws Exception {
        ResultSet rs = stmt.executeQuery("select name from \"pers\".Person where age in (25, 35)");

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            assert "John White".equals(rs.getString("name")) ||
                "Joe Black".equals(rs.getString("name"));

            cnt++;
        }

        assert cnt == 2;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBetween() throws Exception {
        ResultSet rs = stmt.executeQuery("select name from \"pers\".Person where age between 24 and 36");

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            assert "John White".equals(rs.getString("name")) ||
                "Joe Black".equals(rs.getString("name"));

            cnt++;
        }

        assert cnt == 2;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCalculatedValue() throws Exception {
        ResultSet rs = stmt.executeQuery("select age * 2 from \"pers\".Person");

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            assert rs.getInt(1) == 50 ||
                rs.getInt(1) == 70 ||
                rs.getInt(1) == 80;

            cnt++;
        }

        assert cnt == 3;
    }
}
