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

package org.apache.sqoop.testutil;

import org.apache.sqoop.ConnFactory;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.metastore.JobData;
import org.apache.sqoop.tool.ImportTool;
import com.google.common.collect.ObjectArrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.sqoop.SqoopJobDataPublisher;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.fail;

/**
 * Class that implements common methods required for tests.
 */
public abstract class BaseSqoopTestCase {

  public static class DummyDataPublisher extends SqoopJobDataPublisher {
    public static String hiveTable;
    public static String storeTable;
    public static String storeType;
    public static String operation;

    @Override
    public void publish(Data data) {
      hiveTable = data.getHiveTable();
      storeTable = data.getStoreTable();
      storeType = data.getStoreType();
      operation = data.getOperation();
    }
  }

  public static final Log LOG = LogFactory.getLog(
      BaseSqoopTestCase.class.getName());

  public static boolean isOnPhysicalCluster() {
    return onPhysicalCluster;
  }
  private static void setOnPhysicalCluster(boolean value) {
    onPhysicalCluster = value;
  }

  private static boolean onPhysicalCluster = false;

  /** Base directory for all temporary data. */
  public static final String TEMP_BASE_DIR;

  /** Where to import table data to in the local filesystem for testing. */
  public static final String LOCAL_WAREHOUSE_DIR;

  // Initializer for the above
  static {
    String tmpDir = System.getProperty("test.build.data", "/tmp/");
    if (!tmpDir.endsWith(File.separator)) {
      tmpDir = tmpDir + File.separator;
    }

    TEMP_BASE_DIR = tmpDir;
    LOCAL_WAREHOUSE_DIR = TEMP_BASE_DIR + "sqoop/warehouse";
  }

  // Used if a test manually sets the table name to be used.
  private String curTableName;

  protected void setCurTableName(String curName) {
    this.curTableName = curName;
  }

  /**
   * Because of how classloading works, we don't actually want to name
   * all the tables the same thing -- they'll actually just use the same
   * implementation of the Java class that was classloaded before. So we
   * use this counter to uniquify table names.
   */
  private static int tableNum = 0;

  /** When creating sequentially-identified tables, what prefix should
   *  be applied to these tables?
   */
  protected String getTablePrefix() {
    return "SQOOP_TABLE_";
  }

  protected String getTableName() {
    if (null != curTableName) {
      return curTableName;
    } else {
      return getTablePrefix() + Integer.toString(tableNum);
    }
  }

  protected String getWarehouseDir() {
    return LOCAL_WAREHOUSE_DIR;
  }

  private String [] colNames;
  protected String [] getColNames() {
    return colNames;
  }

  protected void setColNames(String [] cols) {
    if (null == cols) {
      this.colNames = null;
    } else {
      this.colNames = Arrays.copyOf(cols, cols.length);
    }
  }

  protected HsqldbTestServer getTestServer() {
    return testServer;
  }

  protected ConnManager getManager() {
    return manager;
  }

  protected void setManager(ConnManager manager) {
    this.manager = manager;
  }

  /**
   * @return a connection to the database under test.
   */
  protected Connection getConnection() {
    try {
      return getTestServer().getConnection();
    } catch (SQLException sqlE) {
      LOG.error("Could not get connection to test server: " + sqlE);
      return null;
    }
  }

  // instance variables populated during setUp, used during tests
  private HsqldbTestServer testServer;
  protected ConnManager manager;

  private static boolean isLog4jConfigured = false;

  protected void incrementTableNum() {
    tableNum++;
  }

  /**
   * @return true if we need an in-memory database to run these tests.
   */
  protected boolean useHsqldbTestServer() {
    return true;
  }

  /**
   * @return the connect string to use for interacting with the database.
   * If useHsqldbTestServer is false, you need to override this and provide
   * a different connect string.
   */
  protected String getConnectString() {
    return HsqldbTestServer.getUrl();
  }

  /**
   * @return a Configuration object used to configure tests.
   */
  protected Configuration getConf() {
    return new Configuration();
  }

  /**
   * @return a new SqoopOptions customized for this particular test, but one
   * which has not had any arguments parsed yet.
   */
  protected SqoopOptions getSqoopOptions(Configuration conf) {
    return new SqoopOptions(conf);
  }

  @Before
  public void setUp() {
    // The assumption is that correct HADOOP configuration will have it set to
    // hdfs://namenode
    setOnPhysicalCluster(
        !CommonArgs.LOCAL_FS.equals(System.getProperty(
            CommonArgs.FS_DEFAULT_NAME)));
    incrementTableNum();

    if (!isLog4jConfigured) {
      BasicConfigurator.configure();
      isLog4jConfigured = true;
      LOG.info("Configured log4j with console appender.");
    }

    if (useHsqldbTestServer()) {
      testServer = new HsqldbTestServer();
      try {
        testServer.resetServer();
      } catch (SQLException sqlE) {
        LOG.error("Got SQLException: " + StringUtils.stringifyException(sqlE));
        fail("Got SQLException: " + StringUtils.stringifyException(sqlE));
      } catch (ClassNotFoundException cnfe) {
        LOG.error("Could not find class for db driver: "
            + StringUtils.stringifyException(cnfe));
        fail("Could not find class for db driver: "
            + StringUtils.stringifyException(cnfe));
      }

      manager = testServer.getManager();
    } else {
      Configuration conf = getConf();
      //Need to disable OraOop for existing tests
      conf.set("oraoop.disabled", "true");
      SqoopOptions opts = getSqoopOptions(conf);
      opts.setConnectString(getConnectString());
      opts.setTableName(getTableName());
      ConnFactory f = new ConnFactory(conf);
      try {
        this.manager = f.getManager(new JobData(opts, new ImportTool()));
      } catch (IOException ioe) {
        fail("IOException instantiating manager: "
            + StringUtils.stringifyException(ioe));
      }
    }
  }

  private void guaranteeCleanWarehouse() {
    if (isOnPhysicalCluster()) {
      Path warehousePath = new Path(this.getWarehouseDir());
      try {
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(warehousePath, true);
      } catch (IOException e) {
        LOG.warn(e);
      }
    } else {
      File s = new File(getWarehouseDir());
      if (!s.delete()) {
        LOG.warn("Cannot delete " + s.getPath());
      }
    }
  }

  @After
  public void tearDown() {
    setCurTableName(null); // clear user-override table name.

    try {
      if (null != manager) {
        manager.close();
        manager = null;
      }
    } catch (SQLException sqlE) {
      LOG.error("Got SQLException: " + StringUtils.stringifyException(sqlE));
      fail("Got SQLException: " + StringUtils.stringifyException(sqlE));
    }
    guaranteeCleanWarehouse();
  }

  public static final String BASE_COL_NAME = "DATA_COL";

  protected String getColName(int i) {
    return BASE_COL_NAME + i;
  }

  /**
   * Drop a table if it already exists in the database.
   * @param table the name of the table to drop.
   * @throws SQLException if something goes wrong.
   */
  protected void dropTableIfExists(String table) throws SQLException {
    Connection conn = getManager().getConnection();
    String dropStatement = dropTableIfExistsCommand(table);
    PreparedStatement statement = conn.prepareStatement(dropStatement,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
  }

  protected String dropTableIfExistsCommand(String table) {
    return "DROP TABLE " + manager.escapeTableName(table) + " IF EXISTS";
  }

  protected void createTableWithColTypesAndNames(String[] colNames,
                                                 String[] colTypes,
                                                 String[] vals) {
    createTableWithColTypesAndNames(getTableName(), colNames, colTypes, vals);
  }

  /**
   * Create a table with a set of columns with their names and add a row of values.
   * @param newTableName The name of the new table
   * @param colNames Column names
   * @param colTypes the types of the columns to make
   * @param vals the SQL text for each value to insert
   */
  protected void createTableWithColTypesAndNames(String newTableName,
                                                 String[] colNames,
                                                 String[] colTypes,
                                                 String[] vals) {
    assert colNames != null;
    assert colTypes != null;
    assert colNames.length == colTypes.length;

    Connection conn = null;
    PreparedStatement statement = null;
    String createTableStr = null;
    String columnDefStr = "";

    try {
      try {
        dropTableIfExists(newTableName);

        conn = getManager().getConnection();

        for (int i = 0; i < colTypes.length; i++) {
          columnDefStr += manager.escapeColName(colNames[i].toUpperCase()) + " " + colTypes[i];
          if (i < colTypes.length - 1) {
            columnDefStr += ", ";
          }
        }
        createTableStr = "CREATE TABLE " + manager.escapeTableName(newTableName) + "(" + columnDefStr + ")";
        LOG.info("Creating table: " + createTableStr);
        statement = conn.prepareStatement(
            createTableStr,
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.executeUpdate();
      } catch (SQLException sqlException) {
        fail("Could not create table: "
            + StringUtils.stringifyException(sqlException));
      } finally {
        if (null != statement) {
          try {
            statement.close();
          } catch (SQLException se) {
            // Ignore exception on close.
          }

          statement = null;
        }
      }

      for (int count=0; vals != null && count < vals.length/colTypes.length;
           ++count ) {
        String columnListStr = "";
        String valueListStr = "";
        for (int i = 0; i < colTypes.length; i++) {
          columnListStr += manager.escapeColName(colNames[i].toUpperCase());
          valueListStr += vals[count * colTypes.length + i];
          if (i < colTypes.length - 1) {
            columnListStr += ", ";
            valueListStr += ", ";
          }
        }
        try {
          String insertValsStr = "INSERT INTO " + manager.escapeTableName(newTableName) + "(" + columnListStr + ")"
              + " VALUES(" + valueListStr + ")";
          LOG.info("Inserting values: " + insertValsStr);
          statement = conn.prepareStatement(
              insertValsStr,
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
          statement.executeUpdate();
        } catch (SQLException sqlException) {
          fail("Could not insert into table: "
              + StringUtils.stringifyException(sqlException));
        } finally {
          if (null != statement) {
            try {
              statement.close();
            } catch (SQLException se) {
              // Ignore exception on close.
            }

            statement = null;
          }
        }
      }

      conn.commit();
      this.colNames = colNames;
    } catch (SQLException se) {
      if (null != conn) {
        try {
          conn.close();
        } catch (SQLException connSE) {
          // Ignore exception on close.
        }
      }
      fail("Could not create table: " + StringUtils.stringifyException(se));
    }
  }

  protected void insertIntoTable(String[] colTypes, String[] vals) {
    insertIntoTable(null, colTypes, vals);
  }

  protected void insertIntoTable(String[] columns, String[] colTypes, String[] vals) {
    assert colTypes != null;
    assert colTypes.length == vals.length;

    Connection conn = null;
    PreparedStatement statement = null;

    String[] colNames;
    if (columns == null){
      colNames = new String[vals.length];
      for( int i = 0; i < vals.length; i++) {
        colNames[i] = BASE_COL_NAME + Integer.toString(i);
      }
    }
    else {
      colNames = columns;
    }

    try {
        conn = getManager().getConnection();
        for (int count=0; vals != null && count < vals.length/colTypes.length;
              ++count ) {
         String columnListStr = "";
         String valueListStr = "";
         for (int i = 0; i < colTypes.length; i++) {
           columnListStr += manager.escapeColName(colNames[i].toUpperCase());
           valueListStr += vals[count * colTypes.length + i];
           if (i < colTypes.length - 1) {
             columnListStr += ", ";
             valueListStr += ", ";
           }
         }
         try {
           String insertValsStr = "INSERT INTO " + manager.escapeTableName(getTableName()) + "(" + columnListStr + ")"
               + " VALUES(" + valueListStr + ")";
           LOG.info("Inserting values: " + insertValsStr);
           statement = conn.prepareStatement(
               insertValsStr,
               ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
           statement.executeUpdate();
         } catch (SQLException sqlException) {
           fail("Could not insert into table: "
               + StringUtils.stringifyException(sqlException));
         } finally {
           if (null != statement) {
             try {
               statement.close();
             } catch (SQLException se) {
               // Ignore exception on close.
             }

             statement = null;
           }
         }
       }
    conn.commit();
    this.colNames = colNames;
    } catch (SQLException se) {
      if (null != conn) {
        try {
          conn.close();
        } catch (SQLException connSE) {
          // Ignore exception on close.
       }
      }
      fail("Could not create table: " + StringUtils.stringifyException(se));
    }

  }

  /**
   * update a table with a set of columns values for a given row.
   * @param colTypes the types of the columns to make
   * @param vals the SQL text for each value to insert
   */
  protected void updateTable(String[] colTypes, String[] vals) {
    assert colNames != null;
    assert colNames.length == vals.length;

    Connection conn = null;
    PreparedStatement statement = null;

    String[] colNames = new String[vals.length];
    for( int i = 0; i < vals.length; i++) {
      colNames[i] = BASE_COL_NAME + Integer.toString(i);
    }

    try {
      conn = getManager().getConnection();
      for (int count=0; vals != null && count < vals.length/colNames.length;
           ++count ) {
        String updateStr = "";
        for (int i = 1; i < colNames.length; i++) {
	      updateStr += manager.escapeColName(colNames[i].toUpperCase()) + " = "+vals[count * colNames.length + i];
          if (i < colNames.length - 1) {
            updateStr += ", ";
          }
        }
        updateStr += " WHERE "+colNames[0]+"="+vals[0]+"";
        try {
          String updateValsStr = "UPDATE " + manager.escapeTableName(getTableName()) + " SET " + updateStr;
          LOG.info("updating values: " + updateValsStr);
          statement = conn.prepareStatement(
                      updateValsStr,
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
          statement.executeUpdate();
        } catch (SQLException sqlException) {
          fail("Could not update table: "
              + StringUtils.stringifyException(sqlException));
        } finally {
          if (null != statement) {
            try {
              statement.close();
            } catch (SQLException se) {
              // Ignore exception on close.
            }
            statement = null;
          }
        }
      }

      conn.commit();
      this.colNames = colNames;
    } catch (SQLException se) {
      if (null != conn) {
        try {
          conn.close();
        } catch (SQLException connSE) {
          // Ignore exception on close.
        }
      }
      fail("Could not update table: " + StringUtils.stringifyException(se));
    }
  }

  /**
   * Create a table with a set of columns and add a row of values.
   * @param colTypes the types of the columns to make
   * @param vals the SQL text for each value to insert
   */
  protected void createTableWithColTypes(String [] colTypes, String [] vals) {
    String[] colNames = new String[colTypes.length];
    for( int i = 0; i < colTypes.length; i++) {
      colNames[i] = BASE_COL_NAME + Integer.toString(i);
    }
    createTableWithColTypesAndNames(colNames, colTypes, vals);
  }

  /**
   * Create a table with a single column and put a data element in it.
   * @param colType the type of the column to create
   * @param val the value to insert (reformatted as a string)
   */
  protected void createTableForColType(String colType, String val) {
    String [] types = { colType };
    String [] vals = { val };

    createTableWithColTypes(types, vals);
  }

  protected Path getTablePath() {
    Path warehousePath = new Path(getWarehouseDir());
    Path tablePath = new Path(warehousePath, getTableName());
    return tablePath;
  }

  protected Path getDataFilePath() {
    return new Path(getTablePath(), "part-m-00000");
  }

  protected void removeTableDir() {
    File tableDirFile = new File(getTablePath().toString());
    if (tableDirFile.exists()) {
      // Remove the directory where the table will be imported to,
      // prior to running the MapReduce job.
      if (!DirUtil.deleteDir(tableDirFile)) {
        LOG.warn("Could not delete table directory: "
            + tableDirFile.getAbsolutePath());
      }
    }
  }

  /**
   * Create a new string array with 'moreEntries' appended to the 'entries'
   * array.
   * @param entries initial entries in the array
   * @param moreEntries variable-length additional entries.
   * @return an array containing entries with all of moreEntries appended.
   */
  protected String [] newStrArray(String [] entries, String... moreEntries)  {
    if (null == moreEntries) {
      return entries;
    }

    if (null == entries) {
      entries = new String[0];
    }

    return ObjectArrays.concat(entries, moreEntries, String.class);
  }
}
