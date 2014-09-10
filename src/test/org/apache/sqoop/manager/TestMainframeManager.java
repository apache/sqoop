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

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.accumulo.AccumuloUtil;
import org.apache.sqoop.hbase.HBaseUtil;
import org.apache.sqoop.tool.MainframeImportTool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.ConnFactory;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.metastore.JobData;
import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.util.ImportException;

/**
 * Test methods of the generic SqlManager implementation.
 */
public class TestMainframeManager extends BaseSqoopTestCase {

  private static final Log LOG = LogFactory.getLog(TestMainframeManager.class
      .getName());

  private ConnManager manager;

  private SqoopOptions opts;

  private ImportJobContext context;

  @Before
  public void setUp() {
    Configuration conf = getConf();
    opts = getSqoopOptions(conf);
    opts.setConnectString("dummy.server");
    opts.setTableName("dummy.pds");
    opts.setConnManagerClassName("org.apache.sqoop.manager.MainframeManager");
    context = new ImportJobContext(getTableName(), null, opts, null);
    ConnFactory f = new ConnFactory(conf);
    try {
      this.manager = f.getManager(new JobData(opts, new MainframeImportTool()));
    } catch (IOException ioe) {
      fail("IOException instantiating manager: "
          + StringUtils.stringifyException(ioe));
    }
  }

  @After
  public void tearDown() {
    try {
      manager.close();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  @Test
  public void testListColNames() {
    String[] colNames = manager.getColumnNames(getTableName());
    assertNotNull("manager should return a column list", colNames);
    assertEquals("Column list should be length 1", 1, colNames.length);
    assertEquals(MainframeManager.DEFAULT_DATASET_COLUMN_NAME, colNames[0]);
  }

  @Test
  public void testListColTypes() {
    Map<String, Integer> types = manager.getColumnTypes(getTableName());
    assertNotNull("manager should return a column types map", types);
    assertEquals("Column types map should be size 1", 1, types.size());
    assertEquals(types.get(MainframeManager.DEFAULT_DATASET_COLUMN_NAME)
        .intValue(), Types.VARCHAR);
  }

  @Test
  public void testImportTableNoHBaseJarPresent() {
    HBaseUtil.setAlwaysNoHBaseJarMode(true);
    opts.setHBaseTable("dummy_table");
    try {
      manager.importTable(context);
      fail("An ImportException should be thrown: "
          + "HBase jars are not present in classpath, cannot import to HBase!");
    } catch (ImportException e) {
      assertEquals(e.toString(),
          "HBase jars are not present in classpath, cannot import to HBase!");
    } catch (IOException e) {
      fail("No IOException should be thrown!");
    } finally {
      opts.setHBaseTable(null);
    }
  }

  @Test
  public void testImportTableNoAccumuloJarPresent() {
    AccumuloUtil.setAlwaysNoAccumuloJarMode(true);
    opts.setAccumuloTable("dummy_table");
    try {
      manager.importTable(context);
      fail("An ImportException should be thrown: "
          + "Accumulo jars are not present in classpath, cannot import to "
          + "Accumulo!");
    } catch (ImportException e) {
      assertEquals(e.toString(),
          "Accumulo jars are not present in classpath, cannot import to "
          + "Accumulo!");
    } catch (IOException e) {
      fail("No IOException should be thrown!");
    } finally {
      opts.setAccumuloTable(null);
    }
  }

  @Test
  public void testListTables() {
    String[] tables = manager.listTables();
    assertNull("manager should not return a list of tables", tables);
  }

  @Test
  public void testListDatabases() {
    String[] databases = manager.listDatabases();
    assertNull("manager should not return a list of databases", databases);
  }

  @Test
  public void testGetPrimaryKey() {
    String primaryKey = manager.getPrimaryKey(getTableName());
    assertNull("manager should not return a primary key", primaryKey);
  }

  @Test
  public void testReadTable() {
    String[] colNames = manager.getColumnNames(getTableName());
    try {
      ResultSet table = manager.readTable(getTableName(), colNames);
      assertNull("manager should not read a table", table);
    } catch (SQLException sqlE) {
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  @Test
  public void testGetConnection() {
    try {
      Connection con = manager.getConnection();
      assertNull("manager should not return a connection", con);
    } catch (SQLException sqlE) {
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  @Test
  public void testGetDriverClass() {
    String driverClass = manager.getDriverClass();
    assertNotNull("manager should return a driver class", driverClass);
    assertEquals("manager should return an empty driver class", "",
        driverClass);
  }
}
