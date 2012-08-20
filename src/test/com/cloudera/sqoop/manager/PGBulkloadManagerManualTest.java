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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import com.cloudera.sqoop.TestExport;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;


/**
 * Test the PGBulkloadManager implementations.
 * PGBulkloadManager uses both JDBC driver and pg_bulkload to facilitate it.
 *
 * Since this requires a Postgresql installation on your local machine to use,
 * this class is named in such a way that Hadoop's default QA process does not
 * run it.
 *
 * You need to run this manually with -Dtestcase=PGBulkloadManagerManualTest.
 *
 * You need to put Postgresql's JDBC driver library into lib dir.
 *
 * You need to create a sqooptest superuser and database and tablespace,
 * and install pg_bulkload for sqooptest database:
 *
 * $ sudo -u postgres createuser -U postgres -s sqooptest
 * $ sudo -u postgres createdb -U sqooptest sqooptest
 * $ sudo -u postgres mkdir /var/pgdata/stagingtablespace
 * $ psql -U sqooptest
 *        -f /usr/local/share/postgresql/contrib/pg_bulkload.sql sqooptest
 * $ psql -U sqooptest sqooptest
 * sqooptest=# CREATE USER sqooptest;
 * sqooptest=# CREATE DATABASE sqooptest;
 * sqooptest=# CREATE TABLESPACE sqooptest
 *                 LOCATION '/var/pgdata/stagingtablespace';
 * sqooptest=# \q
 *
 */
public class PGBulkloadManagerManualTest extends TestExport {

  public static final Log LOG =
      LogFactory.getLog(PGBulkloadManagerManualTest.class.getName());
  private DBConfiguration dbConf;


  public PGBulkloadManagerManualTest() {
    Configuration conf = getConf();
    DBConfiguration.configureDB(conf,
                                "org.postgresql.Driver",
                                getConnectString(),
                                getUserName(),
                                null, null);
    dbConf = new DBConfiguration(conf);
  }


  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }


  @Override
  protected String getConnectString() {
    return "jdbc:postgresql://localhost:5432/sqooptest";
  }


  protected String getUserName() {
    return "sqooptest";
  }


  @Override
  protected String getTablePrefix() {
    return super.getTablePrefix().toLowerCase();
  }


  @Override
  protected String getTableName() {
    return super.getTableName().toLowerCase();
  }

  @Override
  public String getStagingTableName() {
    return super.getStagingTableName().toLowerCase();
  }


  @Override
  protected Connection getConnection() {
    try {
      Connection conn = dbConf.getConnection();
      conn.setAutoCommit(false);
      PreparedStatement stmt =
          conn.prepareStatement("SET extra_float_digits TO 0");
      stmt.executeUpdate();
      conn.commit();
      return conn;
    } catch (SQLException sqlE) {
      LOG.error("Could not get connection to test server: " + sqlE);
      return null;
    } catch (ClassNotFoundException cnfE) {
      LOG.error("Could not find driver class: " + cnfE);
      return null;
    }
  }


  @Override
  protected String getDropTableStatement(String tableName) {
    return "DROP TABLE IF EXISTS " + tableName;
  }


  @Override
  protected String[] getArgv(boolean includeHadoopFlags,
                             int rowsPerStatement,
                             int statementsPerTx,
                             String... additionalArgv) {
    ArrayList<String> args =
        new ArrayList<String>(Arrays.asList(additionalArgv));
    args.add("--username");
    args.add(getUserName());
    args.add("--connection-manager");
    args.add("org.apache.sqoop.manager.PGBulkloadManager");
    args.add("--staging-table");
    args.add("dummy");
    args.add("--clear-staging-table");
    return super.getArgv(includeHadoopFlags,
                         rowsPerStatement,
                         statementsPerTx,
                         args.toArray(new String[0]));
  }


  @Override
  protected String [] getCodeGenArgv(String... extraArgs) {
    ArrayList<String> args = new ArrayList<String>(Arrays.asList(extraArgs));
    args.add("--username");
    args.add(getUserName());
    return super.getCodeGenArgv(args.toArray(new String[0]));
  }


  @Override
  public void testColumnsExport() throws IOException, SQLException {
    // PGBulkloadManager does not support --columns option.
  }


  public void testMultiReduceExport() throws IOException, SQLException {
    String[] genericargs = newStrArray(null, "-Dmapred.reduce.tasks=2");
    multiFileTestWithGenericArgs(2, 10, 2, genericargs);
  }


  public void testExportWithTablespace() throws IOException, SQLException {
    String[] genericargs =
      newStrArray(null, "-Dpgbulkload.staging.tablespace=sqooptest");
    multiFileTestWithGenericArgs(1, 10, 1, genericargs);
  }


  protected void multiFileTestWithGenericArgs(int numFiles,
                                              int recordsPerMap,
                                              int numMaps,
                                              String[] genericargs,
                                              String... argv)
    throws IOException, SQLException {

    final int TOTAL_RECORDS = numFiles * recordsPerMap;

    try {
      LOG.info("Beginning test: numFiles=" + numFiles + "; recordsPerMap="
               + recordsPerMap + "; numMaps=" + numMaps);
      LOG.info("  with genericargs: ");
      for (String arg : genericargs) {
        LOG.info("    " + arg);
      }

      for (int i = 0; i < numFiles; i++) {
        createTextFile(i, recordsPerMap, false);
      }

      createTable();

      runExport(getArgv(true, 10, 10,
                        newStrArray(newStrArray(genericargs, argv),
                                    "-m", "" + numMaps)));
      verifyExport(TOTAL_RECORDS);
    } finally {
      LOG.info("multi-reduce test complete");
    }
  }
}
