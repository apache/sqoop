/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.connector.jdbc.oracle.integration;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.sqoop.connector.jdbc.oracle.util.OracleQueries;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test Oracle queries against Oracle database.
 */
public class OracleQueriesTest extends OracleTestCase {

  @Test
  public void testGetCurrentSchema() throws Exception {
    Connection conn = provider.getConnection();

    String schema = OracleQueries.getCurrentSchema(conn);
    Assert.assertEquals(schema.toUpperCase(),
        provider.getConnectionUsername().toUpperCase());

    PreparedStatement stmt =
        conn.prepareStatement("ALTER SESSION SET CURRENT_SCHEMA=SYS");
    stmt.execute();

    schema = OracleQueries.getCurrentSchema(conn);
    Assert.assertEquals(schema, "SYS");
  }

}
