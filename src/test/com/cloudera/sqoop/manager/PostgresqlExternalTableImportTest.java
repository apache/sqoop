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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.util.FileListing;

public class PostgresqlExternalTableImportTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory
      .getLog(PostgresqlExternalTableImportTest.class.getName());
  static final String HOST_URL = System.getProperty("sqoop.test.postgresql.connectstring.host_url",
      "jdbc:postgresql://localhost/");
  static final String DATABASE_USER = System.getProperty(
      "sqoop.test.postgresql.username", "sqooptest");
  static final String DATABASE_NAME = System.getProperty(
      "sqoop.test.postgresql.database", "sqooptest");
  static final String PASSWORD = System.getProperty("sqoop.test.postgresql.password");

  static final String TABLE_NAME = "EMPLOYEES_PG";
  static final String NULL_TABLE_NAME = "NULL_EMPLOYEES_PG";
  static final String SPECIAL_TABLE_NAME = "EMPLOYEES_PG's";
  static final String DIFFERENT_TABLE_NAME = "DIFFERENT_TABLE";
  static final String SCHEMA_PUBLIC = "public";
  static final String SCHEMA_SPECIAL = "special";
  static final String CONNECT_STRING = HOST_URL + DATABASE_NAME;
  static final String EXTERNAL_TABLE_DIR = "/tmp/external/employees_pg";
  protected Connection connection;

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  public String quoteTableOrSchemaName(String tableName) {
    return "\"" + tableName + "\"";
  }

  private String getDropTableStatement(String tableName, String schema) {
    return "DROP TABLE IF EXISTS " + quoteTableOrSchemaName(schema) + "."
        + quoteTableOrSchemaName(tableName);
  }

  @Before
  public void setUp() {
    super.setUp();

    LOG.debug("Setting up another postgresql test: " + CONNECT_STRING);

    setUpData(TABLE_NAME, SCHEMA_PUBLIC, false);
    setUpData(NULL_TABLE_NAME, SCHEMA_PUBLIC, true);
    setUpData(SPECIAL_TABLE_NAME, SCHEMA_PUBLIC, false);
    setUpData(DIFFERENT_TABLE_NAME, SCHEMA_SPECIAL, false);

    LOG.debug("setUp complete.");
  }

  @After
  public void tearDown() {
    try {
      Statement stmt = connection.createStatement();
      stmt.executeUpdate(getDropTableStatement(TABLE_NAME, SCHEMA_PUBLIC));
      stmt.executeUpdate(getDropTableStatement(NULL_TABLE_NAME, SCHEMA_PUBLIC));
      stmt.executeUpdate(getDropTableStatement(SPECIAL_TABLE_NAME, SCHEMA_PUBLIC));
      stmt.executeUpdate(getDropTableStatement(DIFFERENT_TABLE_NAME, SCHEMA_SPECIAL));
    } catch (SQLException e) {
      LOG.error("Can't clean up the database:", e);
    }

    super.tearDown();

    try {
      connection.close();
    } catch (SQLException e) {
      LOG.error("Ignoring exception in tearDown", e);
    }
  }

  public void setUpData(String tableName, String schema, boolean nullEntry) {
    SqoopOptions options = new SqoopOptions(CONNECT_STRING, tableName);
    options.setUsername(DATABASE_USER);
    options.setPassword(PASSWORD);

    ConnManager manager = null;
    Statement st = null;

    try {
      manager = new PostgresqlManager(options);
      connection = manager.getConnection();
      connection.setAutoCommit(false);
      st = connection.createStatement();

      // Create schema if not exists in dummy way (always create and ignore
      // errors.
      try {
        st.executeUpdate("CREATE SCHEMA " + manager.escapeTableName(schema));
        connection.commit();
      } catch (SQLException e) {
        LOG.info("Couldn't create schema " + schema + " (is o.k. as long as"
            + "the schema already exists.");
        connection.rollback();
      }

      String fullTableName = manager.escapeTableName(schema) + "."
          + manager.escapeTableName(tableName);
      LOG.info("Creating table: " + fullTableName);

      try {
        // Try to remove the table first. DROP TABLE IF EXISTS didn't
        // get added until pg 8.3, so we just use "DROP TABLE" and ignore
        // any exception here if one occurs.
        st.executeUpdate("DROP TABLE " + fullTableName);
      } catch (SQLException e) {
        LOG.info("Couldn't drop table " + schema + "." + tableName + " (ok)");
        // Now we need to reset the transaction.
        connection.rollback();
      }

      st.executeUpdate("CREATE TABLE " + fullTableName + " (" + manager.escapeColName("id")
          + " INT NOT NULL PRIMARY KEY, " + manager.escapeColName("name")
          + " VARCHAR(24) NOT NULL, " + manager.escapeColName("start_date") + " DATE, "
          + manager.escapeColName("Salary") + " FLOAT, " + manager.escapeColName("Fired")
          + " BOOL, " + manager.escapeColName("dept") + " VARCHAR(32))");

      st.executeUpdate("INSERT INTO " + fullTableName
          + " VALUES(1,'Aaron','2009-05-14',1000000.00,TRUE,'engineering')");
      st.executeUpdate("INSERT INTO " + fullTableName
          + " VALUES(2,'Bob','2009-04-20',400.00,TRUE,'sales')");
      st.executeUpdate("INSERT INTO " + fullTableName
          + " VALUES(3,'Fred','2009-01-23',15.00,FALSE,'marketing')");
      if (nullEntry) {
        st.executeUpdate("INSERT INTO " + fullTableName + " VALUES(4,'Mike',NULL,NULL,NULL,NULL)");

      }
      connection.commit();
    } catch (SQLException sqlE) {
      LOG.error("Encountered SQL Exception: " + sqlE);
      sqlE.printStackTrace();
      fail("SQLException when running test setUp(): " + sqlE);
    } finally {
      try {
        if (null != st) {
          st.close();
        }

        if (null != manager) {
          manager.close();
        }
      } catch (SQLException sqlE) {
        LOG.warn("Got SQLException when closing connection: " + sqlE);
      }
    }

    LOG.debug("setUp complete.");
  }

  private String[] getArgv(boolean isDirect, String tableName, String... extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--table");
    args.add(tableName);
    args.add("--external-table-dir");
    args.add(EXTERNAL_TABLE_DIR);
    args.add("--hive-import");
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(CONNECT_STRING);
    args.add("--username");
    args.add(DATABASE_USER);
    args.add("--password");
    args.add(PASSWORD);
    args.add("--where");
    args.add("id > 1");
    args.add("-m");
    args.add("1");

    if (isDirect) {
      args.add("--direct");
    }

    for (String arg : extraArgs) {
      args.add(arg);
    }

    return args.toArray(new String[0]);
  }

  private void doImportAndVerify(boolean isDirect, String[] expectedResults, String tableName,
      String... extraArgs) throws IOException {

    Path tablePath = new Path(EXTERNAL_TABLE_DIR);

    // if importing with merge step, directory should exist and output should be
    // from a reducer
    boolean isMerge = Arrays.asList(extraArgs).contains("--merge-key");
    Path filePath = new Path(tablePath, isMerge ? "part-r-00000" : "part-m-00000");

    File tableFile = new File(tablePath.toString());
    if (tableFile.exists() && tableFile.isDirectory() && !isMerge) {
      // remove the directory before running the import.
      FileListing.recursiveDeleteDir(tableFile);
    }

    String[] argv = getArgv(isDirect, tableName, extraArgs);
    try {
      runImport(argv);
    } catch (IOException ioe) {
      LOG.error("Got IOException during import: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    }

    File f = new File(filePath.toString());
    assertTrue("Could not find imported data file, " + f, f.exists());
    BufferedReader r = null;
    try {
      // Read through the file and make sure it's all there.
      r = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
      for (String expectedLine : expectedResults) {
        assertEquals(expectedLine, r.readLine());
      }
    } catch (IOException ioe) {
      LOG.error("Got IOException verifying results: " + ioe.toString());
      ioe.printStackTrace();
      fail(ioe.toString());
    } finally {
      IOUtils.closeStream(r);
    }
  }

  @Test
  public void testJdbcBasedImport() throws IOException {
    // separator is different to other tests
    // because the CREATE EXTERNAL TABLE DDL is
    // ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    char sep = '\001';
    String[] expectedResults = {
        "2" + sep + "Bob" + sep + "2009-04-20" + sep + "400.0" + sep + "true" + sep + "sales",
        "3" + sep + "Fred" + sep + "2009-01-23" + sep + "15.0" + sep + "false" + sep + "marketing" };
    doImportAndVerify(false, expectedResults, TABLE_NAME);
  }
}
