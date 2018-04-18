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

package org.apache.sqoop.testutil.adapter;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.mysql.MySQLTestUtils;

import java.sql.SQLException;

public class MySqlDatabaseAdapter implements DatabaseAdapter {
  private MySQLTestUtils mySQLTestUtils = new MySQLTestUtils();

  public SqoopOptions injectConnectionParameters(SqoopOptions options) {
    options.setUsername(mySQLTestUtils.getUserName());
    mySQLTestUtils.addPasswordIfIsSet(options);
    return options;
  }

  public void dropTableIfExists(String tableName, ConnManager manager) throws SQLException {
    mySQLTestUtils.dropTableIfExists(tableName, manager);
  }

  public String getConnectionString() {
    return mySQLTestUtils.getMySqlConnectString();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
