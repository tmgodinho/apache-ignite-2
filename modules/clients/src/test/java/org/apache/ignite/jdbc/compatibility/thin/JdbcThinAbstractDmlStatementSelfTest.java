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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.junit.After;
import org.junit.Before;

import static java.nio.charset.StandardCharsets.UTF_16;
import static org.junit.Assert.assertTrue;

/**
 * Base class for JDBC thin insert/update/delete statement compatibility tests.
 *
 * The "Person" cache/table is created and dropped through the Java thin client, since the
 * server-side node runs in Docker and its {@code IndexedTypes} classes are not available on its
 * classpath - the schema is described entirely through {@link QueryEntity} instead of a POJO.
 */
public abstract class JdbcThinAbstractDmlStatementSelfTest extends JdbcThinAbstractSelfTest {
    /** Name of the cache/table used by the tests. */
    static final String CACHE_NAME = "Person";

    /** SQL SELECT query for verification. */
    static final String SQL_SELECT = "select _key, id, firstName, lastName, age, data, text from Person";

    /** Connection. */
    protected Connection conn;

    /**
     * @throws Exception If failed.
     */
    @Before
    public void beforeTest() throws Exception {
        try (IgniteClient client = thinClient()) {
            client.createCache(cacheConfig());
        }

        conn = createConnection();

        conn.setSchema('"' + CACHE_NAME + '"');
    }

    /**
     * @throws Exception If failed.
     */
    @After
    public void afterTest() throws Exception {
        conn.close();

        assertTrue(conn.isClosed());

        try (IgniteClient client = thinClient()) {
            client.destroyCache(CACHE_NAME);
        }
    }

    /**
     * @return Java thin client connected to {@link #cluster}.
     */
    protected IgniteClient thinClient() {
        return Ignition.startClient(new ClientConfiguration()
            .setAddresses(cluster.host() + ":" + cluster.port()));
    }

    /**
     * @return Cache configuration for the "Person" cache/table.
     */
    protected ClientCacheConfiguration cacheConfig() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("id", Integer.class.getName());
        fields.put("firstName", String.class.getName());
        fields.put("lastName", String.class.getName());
        fields.put("age", Integer.class.getName());
        fields.put("data", byte[].class.getName());
        fields.put("text", String.class.getName());

        QueryEntity entity = new QueryEntity()
            .setKeyType(String.class.getName())
            .setValueType(CACHE_NAME)
            .setFields(fields);

        return new ClientCacheConfiguration()
            .setName(CACHE_NAME)
            .setQueryEntities(entity);
    }

    /**
     * Helper to get test binary data as string UTF-16 encoding to be in sync with the RAWTOHEX function
     * which uses UTF-16 for conversion strings to byte arrays.
     * @param str String.
     * @return Byte array with the UTF-16 encoding.
     */
    static byte[] getBytes(String str) {
        return str.getBytes(UTF_16);
    }

    /**
     * Helper to convert a binary data (which is a string UTF-16 encoding) back to string.
     * @param arr Byte array with the UTF-16 encoding.
     * @return String.
     */
    static String str(byte[] arr) {
        return new String(arr, UTF_16);
    }

    /**
     * @param blob Blob.
     */
    static byte[] getBytes(Blob blob) {
        try {
            return blob.getBytes(1, (int)blob.length());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param clob Clob.
     */
    static String str(Clob clob) {
        try {
            return clob.getSubString(1, (int)clob.length());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
