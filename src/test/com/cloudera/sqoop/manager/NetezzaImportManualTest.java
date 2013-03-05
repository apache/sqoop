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
package com.cloudera.sqoop.manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.sqoop.manager.NetezzaManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.util.FileListing;

/**
 * Test the Netezza  implementation.
 *
 * This uses JDBC to import data from an Netezza database into HDFS.
 *
 * Since this requires an Netezza SErver installation, this class is named
 * in such a way that Sqoop's default QA process does not run it. You need to
 * run this manually with -Dtestcase=NetezzaManagerImportManualTest.
 *
 */
public class NetezzaImportManualTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory
      .getLog(NetezzaImportManualTest.class.getName());



  // instance variables populated during setUp, used during tests
  private NetezzaManager manager;


  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  @Override
  protected String getTableName() {
    return NetezzaTestUtils.TABLE_NAME;
  }

  @Before
  public void setUp() {
    super.setUp();

    SqoopOptions options = new SqoopOptions(
        NetezzaTestUtils.getNZConnectString(), getTableName());
    options.setUsername(NetezzaTestUtils.getNZUser());
    options.setPassword(NetezzaTestUtils.getNZPassword());

    manager = new NetezzaManager(options);

    // Drop the existing table, if there is one.
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = manager.getConnection();
      stmt = conn.createStatement();
      stmt.execute("DROP TABLE " + getTableName());
    } catch (SQLException sqlE) {
      LOG.info("Table was not dropped: " + sqlE.getMessage());
    } finally {
      try {
        if (null != stmt) {
          stmt.close();
        }
      } catch (Exception ex) {
        LOG.warn("Exception while closing stmt", ex);
      }
    }

    // Create and populate table
    try {
      conn = manager.getConnection();
      conn.setAutoCommit(false);
      stmt = conn.createStatement();

      // create the database table and populate it with data.
      stmt.executeUpdate("CREATE TABLE " + getTableName() + " ("
          + "id INT NOT NULL, " + "name VARCHAR(24) NOT NULL, "
          + "salary FLOAT, " + "dept VARCHAR(32), " + "PRIMARY KEY (id))");

      stmt.executeUpdate("INSERT INTO " + getTableName() + " VALUES("
          + "1,'Aaron', " + "1000000.00,'engineering')");
      stmt.executeUpdate("INSERT INTO " + getTableName() + " VALUES("
          + "2,'Bob', " + "400.00,'sales')");
      stmt.executeUpdate("INSERT INTO " + getTableName() + " VALUES("
          + "3,'Fred', 15.00," + "'marketing')");
      conn.commit();
    } catch (SQLException sqlE) {
      LOG.error("Encountered SQL Exception: ", sqlE);
      sqlE.printStackTrace();
      fail("SQLException when running test setUp(): " + sqlE);
    } finally {
      try {
        if (null != stmt) {
          stmt.close();
        }
      } catch (Exception ex) {
        LOG.warn("Exception while closing connection/stmt", ex);
      }
    }
  }

  @After
  public void tearDown() {
    super.tearDown();
    try {
      manager.close();
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + sqlE.toString());
      fail("Got SQLException: " + sqlE.toString());
    }
  }

  @Test
  public void testNetezzaImport() throws IOException {

    runNetezzaTest(getExpectedResults());
  }

  private String[] getExpectedResults() {
    return new String[] { "1,Aaron,1000000.0,engineering", "2,Bob,400.0,sales",
        "3,Fred,15.0,marketing", };
  }

  private String[] getArgv() {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--table");
    args.add(getTableName());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(NetezzaTestUtils.getNZConnectString());
    args.add("--username");
    args.add(NetezzaTestUtils.getNZUser());
    args.add("--password");
    args.add(NetezzaTestUtils.getNZPassword());
    args.add("--num-mappers");
    args.add("1");
    return args.toArray(new String[args.size()]);
  }

  private void runNetezzaTest(String[] expectedResults) throws IOException {

    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, getTableName());
    Path filePath = new Path(tablePath, "part-m-00000");

    File tableFile = new File(tablePath.toString());
    if (tableFile.exists() && tableFile.isDirectory()) {
      // remove the directory before running the import.
      FileListing.recursiveDeleteDir(tableFile);
    }

    String[] argv = getArgv();
    try {
      runImport(argv);
    } catch (IOException ioe) {
      LOG.error("Got IOException during import: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    }

    File f = new File(filePath.toString());
    assertTrue("Could not find imported data file", f.exists());
    BufferedReader r = null;
    try {
      // Read through the file and make sure it's all there.
      r = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
      String[] s = new String[3];
      for (int i = 0; i < s.length; ++i) {
        s[i] = r.readLine();
      }
      Arrays.sort(s);
      for (int i = 0; i < expectedResults.length; ++i) {
        assertEquals(expectedResults[i], s[i]);
      }
    } catch (IOException ioe) {
      LOG.error("Got IOException verifying results: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    } finally {
      IOUtils.closeStream(r);
    }
  }

}
