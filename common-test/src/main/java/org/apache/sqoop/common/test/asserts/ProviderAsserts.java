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
package org.apache.sqoop.common.test.asserts;

import org.apache.sqoop.common.test.db.DatabaseProvider;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.test.db.TableName;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.fail;

/**
 * Database provider related asserts.
 */
public class ProviderAsserts {

  private static final Logger LOG = Logger.getLogger(ProviderAsserts.class);

  /**
   * Assert row in the table.
   *
   * @param provider Provider that should be used to query the database
   * @param tableName Table name
   * @param conditions Conditions for identifying the row
   * @param values Values that should be present in the table
   */
  public static void assertRow(DatabaseProvider provider, TableName tableName,  Object []conditions, Object ...values) {
    assertRow(provider, tableName, true, conditions, values);
  }

  /**
   * Assert row in the table.
   *
   * @param provider Provider that should be used to query the database
   * @param tableName Table name
   * @param escapeValues Flag whether the values should be escaped based on their type when using in the generated queries or not
   * @param conditions Conditions for identifying the row
   * @param values Values that should be present in the table
   */
  public static void assertRow(DatabaseProvider provider, TableName tableName, boolean escapeValues, Object []conditions, Object ...values) {
    ResultSet rs = null;
    try {
      rs = provider.getRows(tableName, escapeValues, conditions);

      if(! rs.next()) {
        fail("No rows found.");
      }

      int i = 1;
      for(Object expectedValue : values) {
        Object actualValue = rs.getObject(i);
        assertEquals("Columns do not match on position: " + i, expectedValue.toString(), actualValue.toString());
        i++;
      }

      if(rs.next()) {
        fail("Found more than one row.");
      }
    } catch (SQLException e) {
      LOG.error("Unexpected SQLException", e);
      fail("Unexpected SQLException: " + e);
    } finally {
      provider.closeResultSetWithStatement(rs);
    }
  }

  private ProviderAsserts() {
    // Instantiation is prohibited
  }
}
