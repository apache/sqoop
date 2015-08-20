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

package org.apache.sqoop.integration.repository.mysql;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.sqoop.common.test.db.DatabaseProvider;

public class MySqlTestUtils {

  private DatabaseProvider provider;

  public MySqlTestUtils(DatabaseProvider provider) {
    this.provider = provider;
  }

  public void setDatabase(String database) throws Exception {
    provider.getConnection().setCatalog(database);
  }

  public void assertTableExists(String database, String table) throws Exception {
    DatabaseMetaData md = provider.getConnection().getMetaData();
    ResultSet rs = md.getTables(null, database, table, null);
    while (rs.next()) {
      if (rs.getString(3).equals(table)) {
        return;
      }
    }

    throw new AssertionError("Could not find table '" + table
        + "' part of database '" + database + "'");
  }

  public void assertForeignKey(String database, String table, String column,
      String foreignKeyTable, String foreignKeyColumn) throws Exception {
    DatabaseMetaData md = provider.getConnection().getMetaData();
    ResultSet rs = md.getCrossReference(null, database, table, null, database,
        foreignKeyTable);
    while (rs.next()) {
      if (rs.getString(4).equals(column)
          && rs.getString(8).equals(foreignKeyColumn)) {
        return;
      }
    }

    throw new AssertionError("Could not find '" + table + "." + column
        + "' part of database '" + database + "' with reference to '" + table
        + "." + column + "'");
  }

  public void assertUniqueConstraints(String database, String table,
      String... columns) throws Exception {
    Set<String> columnSet = new TreeSet<String>();
    Map<String, Set<String>> indexColumnMap = new HashMap<String, Set<String>>();

    for (String column : columns) {
      columnSet.add(column);
    }

    DatabaseMetaData md = provider.getConnection().getMetaData();
    ResultSet rs = md.getIndexInfo(null, database, table, true, false);

    // Get map of index => columns
    while (rs.next()) {
      String indexName = rs.getString(6);
      String columnName = rs.getString(9);
      if (!indexColumnMap.containsKey(indexName)) {
        indexColumnMap.put(indexName, new TreeSet<String>());
      }
      indexColumnMap.get(indexName).add(columnName);
    }

    // Validate unique constraints
    for (String index : indexColumnMap.keySet()) {
      if (indexColumnMap.get(index).equals(columnSet)) {
        return;
      }
    }

    throw new AssertionError("Could not find unique constraint on table '"
        + table + "' part of database '" + database
        + "' with reference to columns '" + columnSet + "'");
  }
}
