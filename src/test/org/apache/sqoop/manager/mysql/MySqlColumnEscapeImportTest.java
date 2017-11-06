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

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.MySQLTestUtils;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class MySqlColumnEscapeImportTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      MySqlColumnEscapeImportTest.class.getName());
  private MySQLTestUtils mySQLTestUtils = new MySQLTestUtils();

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
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
  protected String dropTableIfExistsCommand(String table) {
    return "DROP TABLE IF EXISTS " + getManager().escapeTableName(table);
  }

  @After
  public void tearDown() {
      try {
        dropTableIfExists(getTableName());
      } catch (SQLException e) {
        LOG.error("Could not delete test table", e);
      }
      super.tearDown();
  }

  protected String [] getArgv() {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--connect");
    args.add(getConnectString());
    args.add("--username");
    args.add(mySQLTestUtils.getUserName());
    mySQLTestUtils.addPasswordIfIsSet(args);
    args.add("--target-dir");
    args.add(getWarehouseDir());
    args.add("--num-mappers");
    args.add("1");
    args.add("--table");
    args.add(getTableName());

    return args.toArray(new String[0]);
  }

  @Test
  public void testEscapeColumnWithDoubleQuote() throws IOException {
    String[] colNames = { "column\"withdoublequote" };
    String[] types = { "VARCHAR(50)"};
    String[] vals = { "'hello, world'"};
    createTableWithColTypesAndNames(colNames, types, vals);
    String[] args = getArgv();
    runImport(args);

    Path warehousePath = new Path(this.getWarehouseDir());
    Path filePath = new Path(warehousePath, "part-m-00000");
    String output = Files.toString(new File(filePath.toString()), Charsets.UTF_8);

    assertEquals("hello, world", output.trim());
  }

}

