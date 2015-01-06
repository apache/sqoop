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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.util.FileListing;

/**
 * Test the PostgresqlManager and DirectPostgresqlManager implementations.
 * The former uses the postgres JDBC driver to perform an import;
 * the latter uses pg_dump to facilitate it.
 *
 * Since this requires a Postgresql installation on your local machine to use,
 * this class is named in such a way that Hadoop's default QA process does not
 * run it. You need to run this manually with -Dtestcase=PostgresqlImportTest or
 * -Dthirdparty=true.
 *
 * You need to put Postgresql's JDBC driver library into a location where
 * Hadoop can access it (e.g., $HADOOP_HOME/lib).
 *
 * To configure a postgresql database to allow local connections, put the
 * following in /etc/postgresql/8.3/main/pg_hba.conf:
 *     local  all all trust
 *     host all all 127.0.0.1/32 trust
 *     host all all ::1/128      trust
 *
 * ... and comment out any other lines referencing 127.0.0.1 or ::1.
 *
 * Also in the file /etc/postgresql/8.3/main/postgresql.conf, uncomment
 * the line that starts with listen_addresses and set its value to '*' as
 * follows
 *     listen_addresses = '*'
 *
 * For postgresql 8.1, this may be in /var/lib/pgsql/data, instead.  You may
 * need to restart the postgresql service after modifying this file.
 *
 * You should also create a sqooptest user and database:
 *
 * $ sudo -u postgres psql -U postgres template1
 * template1=&gt; CREATE USER sqooptest;
 * template1=&gt; CREATE DATABASE sqooptest;
 * template1=&gt; GRANT ALL ON DATABASE sqooptest TO sqooptest;
 * template1=&gt; \q
 *
 */
public class PostgresqlImportTest extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      PostgresqlImportTest.class.getName());

  static final String HOST_URL = System.getProperty(
    "sqoop.test.postgresql.connectstring.host_url",
    "jdbc:postgresql://localhost/");
  static final String DATABASE_USER = System.getProperty(
    "sqoop.test.postgresql.connectstring.username",
    "sqooptest");
  static final String DATABASE_NAME = System.getProperty(
    "sqoop.test.postgresql.connectstring.database",
    "sqooptest");
  static final String PASSWORD = System.getProperty(
    "sqoop.test.postgresql.connectstring.password");

  static final String TABLE_NAME = "EMPLOYEES_PG";
  static final String NULL_TABLE_NAME = "NULL_EMPLOYEES_PG";
  static final String SPECIAL_TABLE_NAME = "EMPLOYEES_PG's";
  static final String DIFFERENT_TABLE_NAME = "DIFFERENT_TABLE";
  static final String SCHEMA_PUBLIC = "public";
  static final String SCHEMA_SPECIAL = "special";
  static final String CONNECT_STRING = HOST_URL + DATABASE_NAME;

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
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

  public void setUpData(String tableName, String schema, boolean nullEntry) {
    SqoopOptions options = new SqoopOptions(CONNECT_STRING, tableName);
    options.setUsername(DATABASE_USER);
    options.setPassword(PASSWORD);

    ConnManager manager = null;
    Connection connection = null;
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

      String fullTableName = manager.escapeTableName(schema)
        + "." + manager.escapeTableName(tableName);
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

      st.executeUpdate("CREATE TABLE " + fullTableName + " ("
          + manager.escapeColName("id") + " INT NOT NULL PRIMARY KEY, "
          + manager.escapeColName("name") + " VARCHAR(24) NOT NULL, "
          + manager.escapeColName("start_date") + " DATE, "
          + manager.escapeColName("Salary") + " FLOAT, "
          + manager.escapeColName("Fired") + " BOOL, "
          + manager.escapeColName("dept") + " VARCHAR(32))");

      st.executeUpdate("INSERT INTO " + fullTableName
          + " VALUES(1,'Aaron','2009-05-14',1000000.00,TRUE,'engineering')");
      st.executeUpdate("INSERT INTO " + fullTableName
          + " VALUES(2,'Bob','2009-04-20',400.00,TRUE,'sales')");
      st.executeUpdate("INSERT INTO " + fullTableName
          + " VALUES(3,'Fred','2009-01-23',15.00,FALSE,'marketing')");
      if (nullEntry) {
        st.executeUpdate("INSERT INTO " + fullTableName
          + " VALUES(4,'Mike',NULL,NULL,NULL,NULL)");

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


  private String [] getArgv(boolean isDirect, String tableName,
      String... extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--table");
    args.add(tableName);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(CONNECT_STRING);
    args.add("--username");
    args.add(DATABASE_USER);
    args.add("--where");
    args.add("id > 1");

    if (isDirect) {
      args.add("--direct");
    }

    for (String arg : extraArgs) {
      args.add(arg);
    }

    return args.toArray(new String[0]);
  }

  private void doImportAndVerify(boolean isDirect, String[] expectedResults,
      String tableName, String... extraArgs) throws IOException {

    Path warehousePath = new Path(this.getWarehouseDir());
    Path tablePath = new Path(warehousePath, tableName);

    // if importing with merge step, directory should exist and output should be from a reducer
    boolean isMerge = Arrays.asList(extraArgs).contains("--merge-key");
    Path filePath = new Path(tablePath, isMerge ? "part-r-00000" : "part-m-00000");

    File tableFile = new File(tablePath.toString());
    if (tableFile.exists() && tableFile.isDirectory() && !isMerge) {
      // remove the directory before running the import.
      FileListing.recursiveDeleteDir(tableFile);
    }

    String [] argv = getArgv(isDirect, tableName, extraArgs);
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
    String [] expectedResults = {
      "2,Bob,2009-04-20,400.0,true,sales",
      "3,Fred,2009-01-23,15.0,false,marketing",
    };

    doImportAndVerify(false, expectedResults, TABLE_NAME);
  }

  @Test
  public void testDirectImport() throws IOException {
    String [] expectedResults = {
      "2,Bob,2009-04-20,400,TRUE,sales",
      "3,Fred,2009-01-23,15,FALSE,marketing",
    };

    doImportAndVerify(true, expectedResults, TABLE_NAME);
  }

  @Test
  public void testListTables() throws IOException {
    SqoopOptions options = new SqoopOptions(new Configuration());
    options.setConnectString(CONNECT_STRING);
    options.setUsername(DATABASE_USER);

    ConnManager mgr = new PostgresqlManager(options);
    String[] tables = mgr.listTables();
    Arrays.sort(tables);
    assertTrue(TABLE_NAME + " is not found!",
        Arrays.binarySearch(tables, TABLE_NAME) >= 0);
  }

  @Test
  public void testTableNameWithSpecialCharacter() throws IOException {
    String [] expectedResults = {
        "2,Bob,2009-04-20,400.0,true,sales",
        "3,Fred,2009-01-23,15.0,false,marketing",
    };

    doImportAndVerify(false, expectedResults, SPECIAL_TABLE_NAME);
  }

  @Test
  public void testIncrementalImport() throws IOException {
    String [] expectedResults = { };

    String [] extraArgs = { "--incremental", "lastmodified",
       "--check-column", "start_date",
    };

    doImportAndVerify(false, expectedResults, TABLE_NAME, extraArgs);
  }

  public void testDirectIncrementalImport() throws IOException {
    String [] expectedResults = { };

    String [] extraArgs = { "--incremental", "lastmodified",
            "--check-column", "start_date",
    };

    doImportAndVerify(true, expectedResults, TABLE_NAME, extraArgs);
  }

  public void testDirectIncrementalImportMerge() throws IOException {
    String [] expectedResults = { };

    String [] extraArgs = { "--incremental", "lastmodified",
            "--check-column", "start_date",
    };

    doImportAndVerify(true, expectedResults, TABLE_NAME, extraArgs);

    extraArgs = new String[] { "--incremental", "lastmodified",
            "--check-column", "start_date",
            "--merge-key", "id",
            "--last-value", "2009-04-20"
    };

    doImportAndVerify(true, expectedResults, TABLE_NAME, extraArgs);
  }

 @Test
  public void testDifferentSchemaImport() throws IOException {
    String [] expectedResults = {
      "2,Bob,2009-04-20,400.0,true,sales",
      "3,Fred,2009-01-23,15.0,false,marketing",
    };

    String [] extraArgs = { "--",
      "--schema", SCHEMA_SPECIAL,
    };

    doImportAndVerify(false, expectedResults, DIFFERENT_TABLE_NAME, extraArgs);
  }

  @Test
  public void testDifferentSchemaImportDirect() throws IOException {
    String [] expectedResults = {
      "2,Bob,2009-04-20,400,TRUE,sales",
      "3,Fred,2009-01-23,15,FALSE,marketing",
    };

    String [] extraArgs = { "--",
      "--schema", SCHEMA_SPECIAL,
    };

    doImportAndVerify(true, expectedResults, DIFFERENT_TABLE_NAME, extraArgs);
  }

  @Test
  public void testNullEscapeCharacters() throws Exception {
     String [] expectedResults = {
      "2,Bob,2009-04-20,400,TRUE,sales",
      "3,Fred,2009-01-23,15,FALSE,marketing",
      "4,Mike,\\N,\\N,\\N,\\N",
    };

    String [] extraArgs = {
      "--null-string", "\\\\\\\\N",
      "--null-non-string", "\\\\\\\\N",
    };

    doImportAndVerify(true, expectedResults, NULL_TABLE_NAME, extraArgs);
  }

  @Test
  public void testDifferentBooleanValues() throws Exception {
    String [] expectedResults = {
      "2,Bob,2009-04-20,400,REAL_TRUE,sales",
      "3,Fred,2009-01-23,15,REAL_FALSE,marketing",
    };

    String [] extraArgs = {
      "--",
      "--boolean-true-string", "REAL_TRUE",
      "--boolean-false-string", "REAL_FALSE",
    };

    doImportAndVerify(true, expectedResults, TABLE_NAME, extraArgs);
  }
}
