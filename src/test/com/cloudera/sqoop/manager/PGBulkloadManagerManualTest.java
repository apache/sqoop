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
import org.apache.hadoop.mapred.JobConf;
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
  static final String HOST_URL =
    System.getProperty("sqoop.test.postgresql.connectstring.host_url",
                       "jdbc:postgresql://localhost/");
  static final String DATABASE =
    System.getProperty("sqoop.test.postgresql.database", "sqooptest");
  static final String TABLESPACE =
    System.getProperty("sqoop.test.postgresql.tablespace", "sqooptest");
  static final String USERNAME =
    System.getProperty("sqoop.test.postgresql.username", "sqooptest");
  static final String PG_BULKLOAD =
    System.getProperty("sqoop.test.postgresql.pg_bulkload", "pg_bulkload");
  static final String CONNECT_STRING = HOST_URL + DATABASE;

  public PGBulkloadManagerManualTest() {
    JobConf conf = new JobConf(getConf());
    DBConfiguration.configureDB(conf,
                                "org.postgresql.Driver",
                                getConnectString(),
                                getUserName(),
                                (String) null, (Integer) null);
    dbConf = new DBConfiguration(conf);
  }


  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }


  @Override
  protected String getConnectString() {
    return CONNECT_STRING;
  }


  protected String getUserName() {
    return USERNAME;
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
    args.add("-D");
    args.add("pgbulkload.bin=" + PG_BULKLOAD);
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
    multiFileTest(2, 10, 2, "-D", "mapred.reduce.tasks=2");
  }


  public void testMultiReduceExportWithNewProp()
      throws IOException, SQLException {
    multiFileTest(2, 10, 2, "-D", "mapreduce.job.reduces=2");
  }


  public void testExportWithTablespace() throws IOException, SQLException {
    multiFileTest(1, 10, 1,
                  "-D", "pgbulkload.staging.tablespace=" + TABLESPACE);
  }
}
