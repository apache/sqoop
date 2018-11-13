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

package org.apache.sqoop.manager.mysql;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.testcategories.thirdpartytest.MysqlTest;
import org.apache.sqoop.testutil.LobAvroImportTestCase;
import org.junit.experimental.categories.Category;

/**
 * Tests BLOB/CLOB import for Avro with MySQL Db.
 */
@Category(MysqlTest.class)
public class MySQLLobAvroImportTest extends LobAvroImportTestCase {

  public static final Log LOG = LogFactory.getLog(
      MySQLLobAvroImportTest.class.getName());

  private MySQLTestUtils mySQLTestUtils = new MySQLTestUtils();

  @Override
  protected Log getLogger() {
    return LOG;
  }

  @Override
  protected String getDbFriendlyName() {
    return "MySQL";
  }

  @Override
  protected String getConnectString() {
    return mySQLTestUtils.getMySqlConnectString();
  }

  @Override
  protected SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions opts = new SqoopOptions(conf);
    opts.setUsername(mySQLTestUtils.getUserName());
    mySQLTestUtils.addPasswordIfIsSet(opts);
    return opts;
  }

  @Override
  protected void dropTableIfExists(String table) throws SQLException {
    mySQLTestUtils.dropTableIfExists(table, getManager());
  }

  @Override
  protected String getBlobType() {
    return "MEDIUMBLOB";
  }
}
