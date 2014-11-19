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

package org.apache.sqoop.manager.oracle;

import org.junit.Assert;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.junit.Test;

import com.cloudera.sqoop.manager.OracleUtils;

/**
 * Test Oracle queries against Oracle database.
 */
public class OraOopOracleQueriesTest extends OraOopTestCase {

  @Test
  public void testGetCurrentSchema() throws Exception {
    Connection conn = getTestEnvConnection();
    try {
      String schema = OraOopOracleQueries.getCurrentSchema(conn);
      Assert.assertEquals(OracleUtils.ORACLE_USER_NAME.toUpperCase(), schema
          .toUpperCase());

      PreparedStatement stmt =
          conn.prepareStatement("ALTER SESSION SET CURRENT_SCHEMA=SYS");
      stmt.execute();

      schema = OraOopOracleQueries.getCurrentSchema(conn);
      Assert.assertEquals("SYS", schema);
    } finally {
      closeTestEnvConnection();
    }
  }

  @Test
  public void testLongBlockId() {
    OraOopOracleDataChunkExtent chunk =
        new OraOopOracleDataChunkExtent("1", 100, 1, 2147483648L, 4294967295L);
    String whereClause = chunk.getWhereClause();
    Assert.assertNotNull(whereClause);
  }

}
