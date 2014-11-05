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

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.manager.OracleUtils;

import org.apache.sqoop.manager.oracle.util.HadoopFiles;
import org.apache.sqoop.manager.oracle.util.OracleData;

/**
 * Base test case for OraOop to handle common functions.
 */
public abstract class OraOopTestCase {

  private static final OraOopLog LOG = OraOopLogFactory.getLog(
      OraOopTestCase.class.getName());

  private String sqoopGenLibDirectory = System.getProperty("user.dir")
      + "/target/tmp/lib";
  private String sqoopGenSrcDirectory = System.getProperty("user.dir")
      + "/target/tmp/src";
  private String sqoopTargetDirectory = "target/tmp/";
  private String sqoopGenClassName = "org.apache.sqoop.gen.OraOopTestClass";

  private Connection conn;

  protected ClassLoader classLoader;
  {
    classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = OraOopTestCase.class.getClassLoader();
    }
  }

  static {
    Configuration
        .addDefaultResource(OraOopConstants.ORAOOP_SITE_TEMPLATE_FILENAME);
    Configuration.addDefaultResource(OraOopConstants.ORAOOP_SITE_FILENAME);
  }

  protected String getSqoopTargetDirectory() {
    return sqoopTargetDirectory;
  }

  protected void setSqoopTargetDirectory(String newSqoopTargetDirectory) {
    this.sqoopTargetDirectory = newSqoopTargetDirectory;
  }

  protected String getSqoopGenLibDirectory() {
    return sqoopGenLibDirectory;
  }

  protected String getSqoopGenSrcDirectory() {
    return sqoopGenSrcDirectory;
  }

  protected String getSqoopGenClassName() {
    return sqoopGenClassName;
  }

  protected Connection getTestEnvConnection() throws SQLException {
    if (this.conn == null) {
      this.conn =
          DriverManager.getConnection(OracleUtils.CONNECT_STRING,
              OracleUtils.ORACLE_USER_NAME, OracleUtils.ORACLE_USER_PASS);
      this.conn.setAutoCommit(false);
    }
    return this.conn;
  }

  protected void closeTestEnvConnection() {
    try {
      if (this.conn != null) {
        this.conn.close();
      }
    } catch (SQLException e) {
      // Tried to close connection but failed - continue anyway
    }
    this.conn = null;
  }

  protected void createTable(String fileName) {
    try {
      Connection localConn = getTestEnvConnection();
      int parallelProcesses = OracleData.getParallelProcesses(localConn);
      int rowsPerSlave =
          OracleUtils.INTEGRATIONTEST_NUM_ROWS / parallelProcesses;
      try {
        long startTime = System.currentTimeMillis();
        OracleData.createTable(localConn, fileName, parallelProcesses,
            rowsPerSlave);
        LOG.debug("Created and loaded table in "
            + ((System.currentTimeMillis() - startTime) / 1000) + " seconds.");
      } catch (SQLException e) {
        if (e.getErrorCode() == 955) {
          LOG.debug("Table already exists - using existing data");
        } else {
          throw new RuntimeException(e);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected int countTable(String inputTableName, List<String> partitionList) {
    String tableName = inputTableName;
    if (tableName.startsWith("\"\"") && tableName.endsWith("\"\"")) {
      // Table names may be double quoted to work around Sqoop issue
      tableName = tableName.substring(1, tableName.length() - 1);
    }
    String sql = null;
    int numRows = 0;
    if (partitionList != null && partitionList.size() > 0) {
      sql = "SELECT sum(cnt) FROM (";
      int i = 0;
      for (String partition : partitionList) {
        i++;
        if (i > 1) {
          sql += " UNION ALL ";
        }
        sql +=
            "SELECT count(*) cnt FROM " + tableName + " PARTITION(\""
                + partition + "\")";
      }
      sql += ")";
    } else {
      sql = "SELECT count(*) FROM " + tableName;
    }
    try {
      PreparedStatement stmt =
          this.getTestEnvConnection().prepareStatement(sql);
      stmt.execute();
      ResultSet results = stmt.getResultSet();
      results.next();
      numRows = results.getInt(1);
    } catch (SQLException e) {
      throw new RuntimeException("Could not count number of rows in table "
          + tableName, e);
    }
    return numRows;
  }

  protected Configuration getSqoopConf() {
    Configuration sqoopConf = new Configuration();
    return sqoopConf;
  }

  protected int runImport(String tableName, Configuration sqoopConf,
      boolean sequenceFile) {
    Logger rootLogger = Logger.getRootLogger();
    StringWriter stringWriter = new StringWriter();
    Layout layout = new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n");
    WriterAppender writerAppender = new WriterAppender(layout, stringWriter);
    rootLogger.addAppender(writerAppender);

    List<String> sqoopArgs = new ArrayList<String>();

    sqoopArgs.add("import");

    sqoopArgs.add("--direct");

    if (sequenceFile) {
      sqoopArgs.add("--as-sequencefile");
    }

    sqoopArgs.add("--connect");
    sqoopArgs.add(OracleUtils.CONNECT_STRING);

    sqoopArgs.add("--username");
    sqoopArgs.add(OracleUtils.ORACLE_USER_NAME);

    sqoopArgs.add("--password");
    sqoopArgs.add(OracleUtils.ORACLE_USER_PASS);

    sqoopArgs.add("--table");
    sqoopArgs.add(tableName);

    sqoopArgs.add("--target-dir");
    sqoopArgs.add(this.sqoopTargetDirectory);

    sqoopArgs.add("--class-name");
    sqoopArgs.add(getSqoopGenClassName());

    sqoopArgs.add("--bindir");
    sqoopArgs.add(this.sqoopGenLibDirectory);

    sqoopArgs.add("--outdir");
    sqoopArgs.add(this.sqoopGenSrcDirectory);

    if (OracleUtils.NUM_MAPPERS != 0) {
      sqoopArgs.add("--num-mappers");
      sqoopArgs.add(Integer.toString(OracleUtils.NUM_MAPPERS));
    }

    int rowsInTable =
        countTable(tableName, OraOopUtilities.splitOracleStringList(sqoopConf
            .get(OraOopConstants.ORAOOP_IMPORT_PARTITION_LIST)));

    int retCode =
        Sqoop.runTool(sqoopArgs.toArray(new String[sqoopArgs.size()]),
            sqoopConf);
    int rowsImported = 0;
    if (retCode == 0) {
      String logString = stringWriter.toString();
      Pattern pattern =
          Pattern.compile(
              "(INFO mapreduce.ImportJobBase: Retrieved )([0-9]+)( records.)");
      Matcher matcher = pattern.matcher(logString);
      while (matcher.find()) {
        rowsImported = Integer.parseInt(matcher.group(2));
      }
    }
    Assert.assertEquals("Incorrect number of rows imported", rowsInTable,
        rowsImported);
    return retCode;
  }

  protected int runExportFromTemplateTable(String templateTableName,
      String tableName) {
    List<String> sqoopArgs = new ArrayList<String>();

    sqoopArgs.add("export");

    sqoopArgs.add("--direct");

    sqoopArgs.add("--connect");
    sqoopArgs.add(OracleUtils.CONNECT_STRING);

    sqoopArgs.add("--username");
    sqoopArgs.add(OracleUtils.ORACLE_USER_NAME);

    sqoopArgs.add("--password");
    sqoopArgs.add(OracleUtils.ORACLE_USER_PASS);

    sqoopArgs.add("--table");
    sqoopArgs.add(tableName);

    sqoopArgs.add("--export-dir");
    sqoopArgs.add(this.sqoopTargetDirectory);

    sqoopArgs.add("--class-name");
    sqoopArgs.add(getSqoopGenClassName());

    sqoopArgs.add("--bindir");
    sqoopArgs.add(this.sqoopGenLibDirectory);

    sqoopArgs.add("--outdir");
    sqoopArgs.add(this.sqoopGenSrcDirectory);

    Configuration sqoopConf = getSqoopConf();

    sqoopConf.set("oraoop.template.table", templateTableName);
    sqoopConf.setBoolean("oraoop.drop.table", true);
    sqoopConf.setBoolean("oraoop.nologging", true);
    sqoopConf.setBoolean("oraoop.partitioned", false);

    return Sqoop.runTool(sqoopArgs.toArray(new String[sqoopArgs.size()]),
        sqoopConf);
  }

  protected int runCompareTables(Connection connection, String table1,
      String table2) throws SQLException {
    PreparedStatement stmt;
    stmt = connection.prepareStatement(
        "select count(*) from (select * from (select * from "
                + table1
                + " minus select * from "
                + table2
                + ") union all select * from (select * from "
                + table2
                + " minus select * from " + table1 + "))");
    ResultSet results = stmt.executeQuery();
    results.next();
    int numDifferences = results.getInt(1);
    return numDifferences;
  }

  protected void cleanupFolders() throws Exception {
    HadoopFiles.delete(new Path(getSqoopTargetDirectory()), true);
    HadoopFiles.delete(new Path(getSqoopGenSrcDirectory()), true);
    HadoopFiles.delete(new Path(getSqoopGenLibDirectory()), true);
  }

}
