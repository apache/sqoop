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
import org.apache.sqoop.manager.oracle.util.OracleUtils;
import org.apache.sqoop.testutil.SqlUtil;

import java.sql.SQLException;

public class OracleDatabaseAdapter implements DatabaseAdapter {

  @Override
  public SqoopOptions injectConnectionParameters(SqoopOptions options) {
    org.apache.sqoop.manager.oracle.util.OracleUtils.setOracleAuth(options);
    return options;
  }

  @Override
  public void dropTableIfExists(String tableName, ConnManager manager) throws SQLException {
    String dropTableStatement = OracleUtils.getDropTableStatement(tableName);
    SqlUtil.executeStatement(dropTableStatement, manager);
  }

  @Override
  public String getConnectionString() {
    return OracleUtils.CONNECT_STRING;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
