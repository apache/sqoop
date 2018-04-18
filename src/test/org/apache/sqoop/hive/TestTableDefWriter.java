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

package org.apache.sqoop.hive;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.util.SqlTypeMap;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.testutil.HsqldbTestServer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Test Hive DDL statement generation.
 */
public class TestTableDefWriter {

  public static final Log LOG = LogFactory.getLog(
      TestTableDefWriter.class.getName());

  private ConnManager connManager;

  private Configuration conf;

  private SqoopOptions options;

  private TableDefWriter writer;

  private String inputTable;

  private String outputTable;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void before() {
    inputTable = HsqldbTestServer.getTableName();
    outputTable = "outputTable";
    connManager = mock(ConnManager.class);
    conf = new Configuration();
    options = new SqoopOptions();
    when(connManager.getColumnTypes(anyString())).thenReturn(new SqlTypeMap<String, Integer>());
    when(connManager.getColumnNames(anyString())).thenReturn(new String[]{});

    writer = new TableDefWriter(options, connManager, inputTable, outputTable, conf, false);
  }

  // Test getHiveOctalCharCode and expect an IllegalArgumentException.
  private void expectExceptionInCharCode(int charCode) {
    thrown.expect(IllegalArgumentException.class);
    thrown.reportMissingExceptionWithMessage("Expected IllegalArgumentException with out-of-range Hive delimiter");
    TableDefWriter.getHiveOctalCharCode(charCode);
  }

  @Test
  public void testHiveOctalCharCode() {
    assertEquals("\\000", TableDefWriter.getHiveOctalCharCode(0));
    assertEquals("\\001", TableDefWriter.getHiveOctalCharCode(1));
    assertEquals("\\012", TableDefWriter.getHiveOctalCharCode((int) '\n'));
    assertEquals("\\177", TableDefWriter.getHiveOctalCharCode(0177));

    expectExceptionInCharCode(4096);
    expectExceptionInCharCode(0200);
    expectExceptionInCharCode(254);
  }

  @Test
  public void testDifferentTableNames() throws Exception {
    String createTable = writer.getCreateTableStmt();
    String loadData = writer.getLoadDataStmt();

    LOG.debug("Create table stmt: " + createTable);
    LOG.debug("Load data stmt: " + loadData);

    // Assert that the statements generated have the form we expect.
    assertTrue(createTable.indexOf(
        "CREATE TABLE IF NOT EXISTS `outputTable`") != -1);
    assertTrue(loadData.indexOf("INTO TABLE `outputTable`") != -1);
    assertTrue(loadData.indexOf("/" + inputTable + "'") != -1);
  }

  @Test
  public void testDifferentTargetDirs() throws Exception {
    String targetDir = "targetDir";

    // Specify a different target dir from input table name
    options.setTargetDir(targetDir);

    String createTable = writer.getCreateTableStmt();
    String loadData = writer.getLoadDataStmt();

    LOG.debug("Create table stmt: " + createTable);
    LOG.debug("Load data stmt: " + loadData);

    // Assert that the statements generated have the form we expect.
    assertTrue(createTable.indexOf(
        "CREATE TABLE IF NOT EXISTS `" + outputTable + "`") != -1);
    assertTrue(loadData.indexOf("INTO TABLE `" + outputTable + "`") != -1);
    assertTrue(loadData.indexOf("/" + targetDir + "'") != -1);
  }

  @Test
  public void testPartitions() throws Exception {
    options.setHivePartitionKey("ds");
    options.setHivePartitionValue("20110413");

    String createTable = writer.getCreateTableStmt();
    String loadData = writer.getLoadDataStmt();

    assertNotNull(createTable);
    assertNotNull(loadData);
    assertEquals("CREATE TABLE IF NOT EXISTS `outputTable` ( ) "
        + "PARTITIONED BY (ds STRING) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\054' "
        + "LINES TERMINATED BY '\\012' STORED AS TEXTFILE", createTable);
    assertTrue(loadData.endsWith(" PARTITION (ds='20110413')"));
  }

  @Test
  public void testLzoSplitting() throws Exception {
    options.setUseCompression(true);
    options.setCompressionCodec("lzop");

    String createTable = writer.getCreateTableStmt();
    String loadData = writer.getLoadDataStmt();

    assertNotNull(createTable);
    assertNotNull(loadData);
    assertEquals("CREATE TABLE IF NOT EXISTS `outputTable` ( ) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\054' "
        + "LINES TERMINATED BY '\\012' STORED AS "
        + "INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' "
        + "OUTPUTFORMAT "
        + "'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'",
        createTable);
  }

  @Test
  public void testUserMappingNoDecimal() throws Exception {
    options.setMapColumnHive("id=STRING,value=INTEGER");

    Map<String, Integer> colTypes = new SqlTypeMap<String, Integer>();
    colTypes.put("id", Types.INTEGER);
    colTypes.put("value", Types.VARCHAR);

    setUpMockConnManager(HsqldbTestServer.getTableName(), colTypes);

    String createTable = writer.getCreateTableStmt();

    assertNotNull(createTable);

    assertTrue(createTable.contains("`id` STRING"));
    assertTrue(createTable.contains("`value` INTEGER"));

    assertFalse(createTable.contains("`id` INTEGER"));
    assertFalse(createTable.contains("`value` STRING"));
  }

  @Test
  public void testUserMappingWithDecimal() throws Exception {
    options.setMapColumnHive("id=STRING,value2=DECIMAL(13,5),value1=INTEGER,value3=DECIMAL(4,5),value4=VARCHAR(255)");

    Map<String, Integer> colTypes = new SqlTypeMap<String, Integer>();
    colTypes.put("id", Types.INTEGER);
    colTypes.put("value1", Types.VARCHAR);
    colTypes.put("value2", Types.DOUBLE);
    colTypes.put("value3", Types.FLOAT);
    colTypes.put("value4", Types.CHAR);

    setUpMockConnManager(HsqldbTestServer.getTableName(), colTypes);

    String createTable = writer.getCreateTableStmt();

    assertNotNull(createTable);

    assertTrue(createTable.contains("`id` STRING"));
    assertTrue(createTable.contains("`value1` INTEGER"));
    assertTrue(createTable.contains("`value2` DECIMAL(13,5)"));
    assertTrue(createTable.contains("`value3` DECIMAL(4,5)"));
    assertTrue(createTable.contains("`value4` VARCHAR(255)"));

    assertFalse(createTable.contains("`id` INTEGER"));
    assertFalse(createTable.contains("`value1` STRING"));
    assertFalse(createTable.contains("`value2` DOUBLE"));
    assertFalse(createTable.contains("`value3` FLOAT"));
    assertFalse(createTable.contains("`value4` CHAR"));
  }

  @Test
  public void testUserMappingFailWhenCantBeApplied() throws Exception {
    options.setMapColumnHive("id=STRING,value=INTEGER");

    Map<String, Integer> colTypes = new SqlTypeMap<String, Integer>();
    colTypes.put("id", Types.INTEGER);
    setUpMockConnManager(HsqldbTestServer.getTableName(), colTypes);

    thrown.expect(IllegalArgumentException.class);
    thrown.reportMissingExceptionWithMessage("Expected IllegalArgumentException on non applied Hive type mapping");
    writer.getCreateTableStmt();
  }

  @Test
  public void testHiveDatabase() throws Exception {
    options.setHiveDatabaseName("db");

    String createTable = writer.getCreateTableStmt();
    assertNotNull(createTable);
    assertTrue(createTable.contains("`db`.`outputTable`"));

    String loadStmt = writer.getLoadDataStmt();
    assertNotNull(loadStmt);
    assertTrue(createTable.contains("`db`.`outputTable`"));
  }

  @Test
  public void testGetCreateTableStmtDiscardsConnection() throws Exception {
    writer.getCreateTableStmt();

    verify(connManager).discardConnection(true);
  }

  private void setUpMockConnManager(String tableName, Map<String, Integer> typeMap) {
    when(connManager.getColumnTypes(tableName)).thenReturn(typeMap);
    when(connManager.getColumnNames(tableName)).thenReturn(typeMap.keySet().toArray(new String[]{}));
  }

}
