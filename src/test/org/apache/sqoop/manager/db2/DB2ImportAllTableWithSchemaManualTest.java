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
package org.apache.sqoop.manager.db2;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.manager.Db2Manager;
import org.apache.sqoop.tool.ImportAllTablesTool;
import org.apache.sqoop.Sqoop;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.tool.SqoopTool;
import com.cloudera.sqoop.util.FileListing;
import org.apache.sqoop.util.LoggingUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test the DB2 XML data type.
 *
 * This uses JDBC to import data from an DB2 database into HDFS.
 *
 * Since this requires an DB2 Server installation,
 * this class is named in such a way that Sqoop's default QA process does
 * not run it. You need to run this manually with
 * -Dtestcase=DB2ImportAllTableWithSchema

 * You need to put DB2 JDBC driver library (db2jcc.jar) in a location
 * where Sqoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons).
 *
 * To set up your test environment:
 *   Install DB2 Express 9.7 C server.
 *   Create a database SQOOP
 *   Create a login SQOOP with password PASSWORD and grant all
 *   access for database SQOOP to user SQOOP.
 */
public class DB2ImportAllTableWithSchemaManualTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
		  DB2ImportAllTableWithSchemaManualTest.class.getName());

  static final String HOST_URL = System.getProperty(
          "sqoop.test.db2.connectstring.host_url",
          "jdbc:db2://9.30.245.234:60000");

  static final String DATABASE_NAME = System.getProperty(
          "sqoop.test.db2.connectstring.database",
          "TESTDB");
  static final String DATABASE_USER = System.getProperty(
          "sqoop.test.db2.connectstring.username",
          "DB2FENC1");
  static final String DATABASE_PASSWORD = System.getProperty(
          "sqoop.test.db2.connectstring.password",
          "DB2FENC1");

  static final String TABLE_NAME = "TEST.COMPANY";
  static final String TABLE_SCHEMA = "TEST";
  static final String CONNECT_STRING = HOST_URL
              + "/" + DATABASE_NAME;
  static String ExpectedResults =
      "1,doc1";


  String [] extraArgs = { "--",
    "--schema", TABLE_SCHEMA,
  };

  static {
    LOG.info("Using DB2 CONNECT_STRING HOST_URL is : "+HOST_URL);
    LOG.info("Using DB2 CONNECT_STRING: " + CONNECT_STRING);
  }

  // instance variables populated during setUp, used during tests
  private Db2Manager manager;

  protected String getTableName() {
    return  TABLE_NAME;
  }


  @Before
  public void setUp() {
    super.setUp();

    SqoopOptions options = new SqoopOptions(CONNECT_STRING, getTableName());
    options.setUsername(DATABASE_USER);
    options.setPassword(DATABASE_PASSWORD);

    manager = new Db2Manager(options);

    // Drop the existing table, if there is one.
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = manager.getConnection();
      stmt = conn.createStatement();
      stmt.execute("DROP TABLE " + getTableName());
    } catch (SQLException sqlE) {
        LoggingUtils.logAll(LOG, "Table was not dropped: ", sqlE);
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
          + "ID int, "
          + "DOCNAME VARCHAR(20))");

      stmt.executeUpdate("INSERT INTO " + getTableName() + " VALUES("
          + "1,'doc1' "
          + " )");
      conn.commit();
    } catch (SQLException sqlE) {
        LoggingUtils.logAll(LOG, "Encountered SQL Exception: ", sqlE);
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
        LoggingUtils.logAll(LOG, "Got SQLException: ", sqlE);
      }
  }

  @Test
  public void testDb2Import() throws IOException {

    runDb2Test(ExpectedResults);

  }

  private String [] getArgv() {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);
    args.add("--connect");
    args.add(CONNECT_STRING);
    args.add("--username");
    args.add(DATABASE_USER);
    args.add("--password");
    args.add(DATABASE_PASSWORD);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());

    args.add("--m");
    args.add("1");

    for (String arg : extraArgs) {
      args.add(arg);
    }

    return args.toArray(new String[0]);
  }

  private void runDb2Test(String expectedResults) throws IOException {

    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, getTableName());
    Path filePath = new Path(tablePath, "part-m-00000");

    File tableFile = new File(getTableName().toString());
    if (tableFile.exists() && tableFile.isDirectory()) {
      // remove the directory before running the import.
      FileListing.recursiveDeleteDir(tableFile);
    }

    String [] argv = getArgv();
    try {
      runImportAll(argv);
    } catch (IOException ioe) {
        LOG.error("Got IOException during import: " + ioe.getMessage());
      }

    File f = new File(filePath.toString());
    assertTrue("Could not find imported data file", f.exists());
    BufferedReader r = null;
    try {
      // Read through the file and make sure it's all there.
      r = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
      assertEquals(expectedResults, r.readLine());
    } catch (IOException ioe) {
        LOG.error("Got IOException verifying results: " + ioe.getMessage());
      } finally {
          IOUtils.closeStream(r);
        }
  }

  private void runImportAll(SqoopTool tool,String [] argv) throws IOException {
  // run the tool through the normal entry-point.
    int ret;
    try {
      Configuration conf = getConf();
      SqoopOptions opts = getSqoopOptions(conf);
      Sqoop sqoop = new Sqoop(tool, conf, opts);
      ret = Sqoop.runSqoop(sqoop, argv);
      //ret = tool.run(opts);
    } catch (Exception e) {
        LOG.error("Got exception running Sqoop: " + e.toString());
        ret = 1;
      }

  // expect a successful return.
    if (0 != ret) {
      throw new IOException("Failure during job; return status " + ret);
    }
  }

  /** run an import using the default ImportTool. */
  protected void runImportAll(String [] argv) throws IOException {
    runImportAll(new ImportAllTablesTool(), argv);
  }
}
