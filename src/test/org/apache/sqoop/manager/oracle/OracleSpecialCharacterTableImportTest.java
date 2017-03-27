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

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.OracleUtils;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class OracleSpecialCharacterTableImportTest extends ImportJobTestCase {

  @Parameterized.Parameters(name = "tableName = {0}")
  public static Iterable<? extends Object> tableNameParameters() {
    return Arrays.asList("DOLLAR$","FOO$BAR", "T#1");
  }

  public static final Log LOG = LogFactory.getLog(
      OracleSpecialCharacterTableImportTest.class.getName());

  private final String tableName;

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Override
  protected String getConnectString() {
    return OracleUtils.CONNECT_STRING;
  }

  @Override
  protected SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions opts = new SqoopOptions(conf);
    OracleUtils.setOracleAuth(opts);
    return opts;
  }

  @Override
  protected void dropTableIfExists(String table) throws SQLException {
    OracleUtils.dropTable(table, getManager());
  }

  @After
  public void tearDown() {
    try {
      OracleUtils.dropTable(getTableName(), getManager());
    } catch (SQLException e) {
      LOG.error("Test table could not be dropped", e);
    }
    super.tearDown();
  }

  protected String [] getArgv() {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--connect");
    args.add(getConnectString());
    args.add("--username");
    args.add(OracleUtils.ORACLE_USER_NAME);
    args.add("--password");
    args.add(OracleUtils.ORACLE_USER_PASS);
    args.add("--target-dir");
    args.add(getWarehouseDir());
    args.add("--num-mappers");
    args.add("1");
    args.add("--table");
    args.add(getTableName());

    return args.toArray(new String[0]);
  }

  public OracleSpecialCharacterTableImportTest(String tableName) {
    this.tableName = tableName;
  }

  @Test
  public void testImportWithTableNameContainingSpecialCharacters() throws IOException {
    String [] types = { "VARCHAR(50)"};
    String [] vals = { "'hello, world!'"};
    setCurTableName(tableName);
    createTableWithColTypes(types, vals);
    String[] args = getArgv();
    runImport(args);

    Path warehousePath = new Path(this.getWarehouseDir());
    Path filePath = new Path(warehousePath, "part-m-00000");
    String output = Files.toString(new File(filePath.toString()), Charsets.UTF_8);

    assertEquals("hello, world!", output.trim());
  }

}
