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

package org.apache.sqoop.manager;

import static org.junit.Assert.assertArrayEquals;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
/**
 * Test methods of the generic SqlManager implementation.
 */
public class TestSqlManager {

  @Test
  public void testFilteringSpecifiedColumnNamesWhenNoneSpecified() {
    SqoopOptions opts = new SqoopOptions();
    SqlManager sqlManager = stubSqlManager(opts);
    String[] allColumnsFromDbTable = { "aaa", "bbb", "ccc", "ddd" };
    assertArrayEquals(new String[]{ "aaa", "bbb", "ccc", "ddd" }, sqlManager.filterSpecifiedColumnNames(allColumnsFromDbTable));
  }

  @Test
  public void testFilteringSpecifiedColumnNamesWhenSubset() {
    SqoopOptions opts = new SqoopOptions();
    String[] cols = { "bbb", "ccc" };
    opts.setColumns(cols);
    SqlManager sqlManager = stubSqlManager(opts);
    String[] allColumnsFromDbTable = { "aaa", "bbb", "ccc", "ddd" };
    assertArrayEquals(new String[]{ "bbb", "ccc" }, sqlManager.filterSpecifiedColumnNames(allColumnsFromDbTable));
  }

  @Test
  public void testFilteringSpecifiedColumnNamesUsesCaseFromArgumentNotDatabase() {
    SqoopOptions opts = new SqoopOptions();
    String[] cols = { "bbb", "ccc" };
    opts.setColumns(cols);
    SqlManager sqlManager = stubSqlManager(opts);
    String[] allColumnsFromDbTable = { "AAA", "BBB", "CCC", "DDD" };
    assertArrayEquals(new String[]{ "bbb", "ccc" }, sqlManager.filterSpecifiedColumnNames(allColumnsFromDbTable));
  }

  private SqlManager stubSqlManager(SqoopOptions opts) {
    SqlManager sqlManager = new SqlManager(opts) {
      @Override
      public Connection getConnection() throws SQLException {
        return null;
      }
      @Override
      public String getDriverClass() {
        return null;
      }
    };
    return sqlManager;
  }
}