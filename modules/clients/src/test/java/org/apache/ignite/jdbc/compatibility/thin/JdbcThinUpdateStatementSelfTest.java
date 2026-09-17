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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Ported from {@code org.apache.ignite.jdbc.thin.JdbcThinUpdateStatementSelfTest} to run against
 * an Ignite node in Docker ({@link IgniteDocker}), using only the JDBC thin driver and the Java
 * thin client.
 */
public class JdbcThinUpdateStatementSelfTest extends JdbcThinAbstractUpdateStatementSelfTest {
    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testExecute() throws SQLException {
        conn.createStatement().execute("update Person set firstName = 'Jack' where " +
            "cast(substring(_key, 2, 1) as int) % 2 = 0");

        List<List<?>> rows = execute(conn, "select firstName from Person order by _key");

        assertEquals(Arrays.asList(
            Arrays.asList("John"),
            Arrays.asList("Jack"),
            Arrays.asList("Mike")), rows);
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testExecuteUpdate() throws SQLException {
        conn.createStatement().executeUpdate("update Person set firstName = 'Jack' where " +
            "cast(substring(_key, 2, 1) as int) % 2 = 0");

        List<List<?>> rows = execute(conn, "select firstName from Person order by _key");

        assertEquals(Arrays.asList(
            Arrays.asList("John"),
            Arrays.asList("Jack"),
            Arrays.asList("Mike")), rows);
    }
}
