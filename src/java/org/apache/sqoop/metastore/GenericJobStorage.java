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
package org.apache.sqoop.metastore;

import java.io.IOException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.sqoop.manager.ConnManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.tool.SqoopTool;
import org.apache.sqoop.manager.DefaultManagerFactory;
import org.apache.sqoop.manager.JdbcDrivers;

import static org.apache.commons.lang3.StringUtils.startsWith;
import static org.apache.sqoop.manager.JdbcDrivers.DB2;
import static org.apache.sqoop.manager.JdbcDrivers.HSQLDB;
import static org.apache.sqoop.manager.JdbcDrivers.MYSQL;
import static org.apache.sqoop.manager.JdbcDrivers.ORACLE;
import static org.apache.sqoop.manager.JdbcDrivers.POSTGRES;
import static org.apache.sqoop.manager.JdbcDrivers.SQLSERVER;

/**
 * JobStorage implementation that uses a database to
 * hold job information.
 */
public class GenericJobStorage extends JobStorage {

  public static final Log LOG = LogFactory.getLog(
      GenericJobStorage.class.getName());

  /** descriptor key identifying the connect string for the metastore. */
  public static final String META_CONNECT_KEY = "metastore.connect.string";

  /** descriptor key identifying the username to use when connecting
   * to the metastore.
   */
  public static final String META_USERNAME_KEY = "metastore.username";

  /** descriptor key identifying the password to use when connecting
   * to the metastore.
   */
  public static final String META_PASSWORD_KEY = "metastore.password";

  /** Default name for the root metadata table. */
  private static final String DEFAULT_ROOT_TABLE_NAME = "SQOOP_ROOT";

  /** Configuration key used to override root table name. */
  public static final String ROOT_TABLE_NAME_KEY =
       "sqoop.root.table.name";

  /** root metadata table key used to define the current schema version. */
  private static final String STORAGE_VERSION_KEY =
      "sqoop.job.storage.version";

  /** The current version number for the schema edition. */
  private static final int CUR_STORAGE_VERSION = 0;

  /** This value represents an invalid version */
  private static final int NO_VERSION = -1;

  /** root metadata table key used to define the job table name. */
  private static final String SESSION_TABLE_KEY =
      "sqoop.job.info.table";

  /** Outdated key for job table data, kept for backward compatibility */
  public static final String HSQLDB_TABLE_KEY = "sqoop.hsqldb.job.info.table";

  /** Outdated key for schema version, kept for backward compatibility */
  private static final String HSQLDB_VERSION_KEY =
          "sqoop.hsqldb.job.storage.version";

  /** Default value for SESSION_TABLE_KEY. */
  private static final String DEFAULT_SESSION_TABLE_NAME =
      "SQOOP_SESSIONS";

  /** Per-job key with propClass 'schema' that defines the set of
   * properties valid to be defined for propClass 'SqoopOptions'. */
  private static final String PROPERTY_SET_KEY =
      "sqoop.property.set.id";

  /** Current value for PROPERTY_SET_KEY. */
  private static final String CUR_PROPERTY_SET_ID = "0";

  // The following are values for propClass in the v0 schema which
  // describe different aspects of the stored metadata.

  /** Property class for properties about the stored data itself. */
  private static final String PROPERTY_CLASS_SCHEMA = "schema";

  /** Property class for properties that are loaded into SqoopOptions. */
  private static final String PROPERTY_CLASS_SQOOP_OPTIONS = "SqoopOptions";

  /** Property class for properties that are loaded into a Configuration. */
  private static final String PROPERTY_CLASS_CONFIG = "config";

  /**
   * Per-job key with propClass 'schema' that specifies the SqoopTool
   * to load.
   */
  private static final String SQOOP_TOOL_KEY = "sqoop.tool";
  private static final Set<JdbcDrivers> SUPPORTED_DRIVERS = EnumSet.of(HSQLDB, MYSQL, ORACLE, POSTGRES, DB2, SQLSERVER);
  private Map<String, String> connectedDescriptor;
  private String metastoreConnectStr;
  private String metastoreUser;
  private String metastorePassword;
  private Connection connection;
  private ConnManager connManager;

  protected Connection getConnection() {
    return this.connection;
  }

  // After connection to the database and initialization of the
  // schema, this holds the name of the job table.
  private String jobTableName;

  protected void setMetastoreConnectStr(String connectStr) {
    this.metastoreConnectStr = connectStr;
  }

  protected void setMetastoreUser(String user) {
    this.metastoreUser = user;
  }

  protected void setMetastorePassword(String pass) {
    this.metastorePassword = pass;
  }

  /**
   * Set the descriptor used to open() this storage.
   */
  protected void setConnectedDescriptor(Map<String, String> descriptor) {
    this.connectedDescriptor = descriptor;
  }

  @Override
  /**
   * Initialize the connection to the database.
   */
  public void open(Map<String, String> descriptor) throws IOException {
    setConnectionParameters(descriptor);
    validateMetastoreConnectionString(metastoreConnectStr);
    setConnectedDescriptor(descriptor);

    init();
  }

  protected void validateMetastoreConnectionString(String metastoreConnectStr) {
    if (!isDbSupported(metastoreConnectStr)) {
      String errorMessage = metastoreConnectStr + " is an invalid connection string or the required RDBMS is not supported." +
          "Supported RDBMSs are: " + SUPPORTED_DRIVERS.toString();
      throw new RuntimeException(errorMessage);
    }
  }

  protected void setConnectionParameters(Map<String, String> descriptor) {
    setMetastoreConnectStr(descriptor.get(META_CONNECT_KEY));
    setMetastoreUser(descriptor.get(META_USERNAME_KEY));
    setMetastorePassword(descriptor.get(META_PASSWORD_KEY));
  }

  protected void init() throws IOException {
    try {
      connManager = createConnManager();
      connection = connManager.getConnection();

      connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      connection.setAutoCommit(false);

      // Initialize the root schema.
      if (!rootTableExists()) {
        createRootTable();
      }

      // Check the schema version.
      String curStorageVerStr = getRootProperty(STORAGE_VERSION_KEY, NO_VERSION);

      // If schema version is not present under the current key,
      // sets it correctly. Present for backward compatibility
      if (curStorageVerStr == null) {
        setRootProperty(STORAGE_VERSION_KEY, NO_VERSION, Integer.toString(CUR_STORAGE_VERSION));
        curStorageVerStr = Integer.toString(CUR_STORAGE_VERSION);
      }
      int actualStorageVer = NO_VERSION;
      try {
        actualStorageVer = Integer.valueOf(curStorageVerStr);
      } catch (NumberFormatException nfe) {
        LOG.warn("Could not interpret as a number: " + curStorageVerStr);
      }
      if (actualStorageVer != CUR_STORAGE_VERSION) {
        LOG.error("Can not interpret metadata schema");
        LOG.error("The metadata schema version is " + curStorageVerStr);
        LOG.error("The highest version supported is " + CUR_STORAGE_VERSION);
        LOG.error("To use this version of Sqoop, "
            + "you must downgrade your metadata schema.");
        throw new IOException("Invalid metadata version.");
      }

      // Initialize the versioned schema.
      initV0Schema();
    } catch (SQLException sqle) {
      if (null != connection) {
        try {
          connection.rollback();
        } catch (SQLException e2) {
          LOG.warn("Error rolling back transaction in error handler: " + e2);
        }
      }

      throw new IOException("Exception creating SQL connection", sqle);
    }
  }

  @Override
  public void close() throws IOException {
      try {
          if (connManager != null) {
            LOG.debug("Closing connection manager");
            connManager.close();
          }
      } catch (SQLException sqlE) {
          throw new IOException("Exception closing connection manager", sqlE);
      } finally {
          this.connection = null;
      }
  }

  @Override
  /** {@inheritDoc} */
  public boolean canAccept(Map<String, String> descriptor) {
    return descriptor.get(META_CONNECT_KEY) != null;
  }

  @Override
  /** {@inheritDoc} */
  public JobData read(String jobName) throws IOException {
    try {
      if (!jobExists(jobName)) {
        LOG.error("Cannot restore job: " + jobName);
        LOG.error("(No such job)");
        throw new IOException("Cannot restore missing job " + jobName);
      }

      LOG.debug("Restoring job: " + jobName);
      Properties schemaProps = getV0Properties(jobName,
          PROPERTY_CLASS_SCHEMA);
      Properties sqoopOptProps = getV0Properties(jobName,
          PROPERTY_CLASS_SQOOP_OPTIONS);
      Properties configProps = getV0Properties(jobName,
          PROPERTY_CLASS_CONFIG);

      // Check that we're not using a saved job from a previous
      // version whose functionality has been deprecated.
      String thisPropSetId = schemaProps.getProperty(PROPERTY_SET_KEY);
      LOG.debug("System property set: " + CUR_PROPERTY_SET_ID);
      LOG.debug("Stored property set: " + thisPropSetId);
      if (!CUR_PROPERTY_SET_ID.equals(thisPropSetId)) {
        LOG.warn("The property set present in this database was written by");
        LOG.warn("an incompatible version of Sqoop. This may result in an");
        LOG.warn("incomplete operation.");
        // TODO(aaron): Should this fail out-right?
      }

      String toolName = schemaProps.getProperty(SQOOP_TOOL_KEY);
      if (null == toolName) {
        // Don't know what tool to create.
        throw new IOException("Incomplete metadata; missing "
            + SQOOP_TOOL_KEY);
      }

      SqoopTool tool = SqoopTool.getTool(toolName);
      if (null == tool) {
        throw new IOException("Error in job metadata: invalid tool "
            + toolName);
      }

      Configuration conf = new Configuration();
      for (Map.Entry<Object, Object> entry : configProps.entrySet()) {
        conf.set(entry.getKey().toString(), entry.getValue().toString());
      }

      SqoopOptions opts = new SqoopOptions();
      opts.setConf(conf);
      opts.loadProperties(sqoopOptProps);

      // Set the job connection information for this job.
      opts.setJobName(jobName);
      opts.setStorageDescriptor(connectedDescriptor);

      return new JobData(opts, tool);
    } catch (SQLException sqlE) {
      throw new IOException("Error communicating with database", sqlE);
    }
  }

  private boolean jobExists(String jobName) throws SQLException {
    PreparedStatement s = connection.prepareStatement(
        "SELECT COUNT(job_name) FROM " + connManager.escapeTableName(this.jobTableName)
        + " WHERE job_name = ? GROUP BY job_name");
    ResultSet rs = null;
    try {
      s.setString(1, jobName);
      rs = s.executeQuery();
      if (rs.next()) {
        return true; // We got a result, meaning the job exists.
      }
    } finally {
      if (null != rs) {
        try {
          rs.close();
        } catch (SQLException sqlE) {
          LOG.warn("Error closing result set: " + sqlE);
        }
      }

      s.close();
    }

    return false; // No result.
  }

  @Override
  /** {@inheritDoc} */
  public void delete(String jobName) throws IOException {
    try {
      if (!jobExists(jobName)) {
        LOG.error("No such job: " + jobName);
      } else {
        LOG.debug("Deleting job: " + jobName);
        PreparedStatement s = connection.prepareStatement("DELETE FROM "
            + connManager.escapeTableName(this.jobTableName) + " WHERE job_name = ?");
        try {
          s.setString(1, jobName);
          s.executeUpdate();
        } finally {
          s.close();
        }
        connection.commit();
      }
    } catch (SQLException sqlEx) {
      try {
        connection.rollback();
      } catch (SQLException e2) {
        LOG.warn("Error rolling back transaction in error handler: " + e2);
      }
      throw new IOException("Error communicating with database", sqlEx);
    }
  }

  @Override
  /** {@inheritDoc} */
  public void create(String jobName, JobData data)
      throws IOException {
    try {
      if (jobExists(jobName)) {
        LOG.error("Cannot create job " + jobName
            + ": it already exists");
        throw new IOException("Job " + jobName + " already exists");
      }
    } catch (SQLException sqlE) {
      throw new IOException("Error communicating with database", sqlE);
    }

    createInternal(jobName, data);
  }

  /**
   * Actually insert/update the resources for this job.
   */
  private void createInternal(String jobName, JobData data)
      throws IOException {
    try {
      LOG.debug("Creating job: " + jobName);

      // Save the name of the Sqoop tool.
      setV0Property(jobName, PROPERTY_CLASS_SCHEMA, SQOOP_TOOL_KEY,
          data.getSqoopTool().getToolName());

      // Save the property set id.
      setV0Property(jobName, PROPERTY_CLASS_SCHEMA, PROPERTY_SET_KEY,
          CUR_PROPERTY_SET_ID);

      // Save all properties of the SqoopOptions.
      Properties props = data.getSqoopOptions().writeProperties();
      setV0Properties(jobName, PROPERTY_CLASS_SQOOP_OPTIONS, props);

      // And save all unique properties of the configuration.
      Configuration saveConf = data.getSqoopOptions().getConf();
      Configuration baseConf = new Configuration();

      for (Map.Entry<String, String> entry : saveConf) {
        String key = entry.getKey();
        String rawVal = saveConf.getRaw(key);
        String baseVal = baseConf.getRaw(key);
        if (baseVal != null && rawVal.equals(baseVal)) {
          continue; // Don't save this; it's set in the base configuration.
        }

        LOG.debug("Saving " + key + " => " + rawVal + " / " + baseVal);
        setV0Property(jobName, PROPERTY_CLASS_CONFIG, key, rawVal);
      }

      connection.commit();
    } catch (SQLException sqlE) {
      try {
        connection.rollback();
      } catch (SQLException sqlE2) {
        LOG.warn("Exception rolling back transaction during error handling: "
            + sqlE2);
      }
      throw new IOException("Error communicating with database", sqlE);
    }
  }

  @Override
  /** {@inheritDoc} */
  public void update(String jobName, JobData data)
      throws IOException {
    try {
      if (!jobExists(jobName)) {
        LOG.error("Cannot update job " + jobName + ": not found");
        throw new IOException("Job " + jobName + " does not exist");
      }
    } catch (SQLException sqlE) {
      throw new IOException("Error communicating with database", sqlE);
    }

    // Since we set properties with update-or-insert, this is the same
    // as create on this system.
    createInternal(jobName, data);
  }

  @Override
  /** {@inheritDoc} */
  public List<String> list() throws IOException {
    ResultSet rs = null;
    try {
      PreparedStatement s = connection.prepareStatement(
          "SELECT DISTINCT job_name FROM " + connManager.escapeTableName(this.jobTableName));
      try {
        rs = s.executeQuery();
        ArrayList<String> jobs = new ArrayList<String>();
        while (rs.next()) {
          jobs.add(rs.getString(1));
        }

        return jobs;
      } finally {
        if (null != rs) {
          try {
            rs.close();
          } catch (SQLException sqlE) {
            LOG.warn("Error closing resultset: " + sqlE);
          }
        }

        if (null != s) {
          s.close();
        }
      }
    } catch (SQLException sqlE) {
      throw new IOException("Error communicating with database", sqlE);
    }
  }

  // Determine the name to use for the root metadata table.
  private String getRootTableName() {
    Configuration conf = getConf();
    return conf.get(ROOT_TABLE_NAME_KEY, DEFAULT_ROOT_TABLE_NAME).toUpperCase();
  }

  private String getEscapedRootTableName() {
    return connManager.escapeTableName(getRootTableName());
  }

  private boolean tableExists(String tableToCheck) throws SQLException {
    String[] tables = connManager.listTables();
    for (String table : tables) {
      if (table.equals(tableToCheck)) {
        return true;
      }
    }
    return false;
  }

  private boolean rootTableExists() throws SQLException {
    String rootTableName = getRootTableName();
    return tableExists(rootTableName);
  }

  private void createRootTable() throws SQLException {
    String rootTableName = getRootTableName();
    LOG.debug("Creating root table: " + rootTableName);

    // TODO: Sanity-check the value of rootTableName to ensure it is
    // not a SQL-injection attack vector.
    Statement s = connection.createStatement();
    try {
      s.executeUpdate("CREATE TABLE " + getEscapedRootTableName() + " ("
          + "version INT NOT NULL, "
          + "propname VARCHAR(128) NOT NULL, "
          + "propval VARCHAR(256), "
          + "CONSTRAINT " + rootTableName + "_unq UNIQUE (version, propname))");
    } finally {
      s.close();
    }

    setRootProperty(STORAGE_VERSION_KEY, NO_VERSION,
        Integer.toString(CUR_STORAGE_VERSION));

    LOG.debug("Saving root table.");
    connection.commit();
  }

  /**
   * Look up a value for the specified version (may be null) in the
   * root metadata table.
   */
  private String getRootProperty(String propertyName, Integer version)
      throws SQLException {
    LOG.debug("Looking up property " + propertyName + " for version "
        + version);
    PreparedStatement s = null;
    ResultSet rs = null;

    try {
      if (null == version) {
        s = connection.prepareStatement(
          "SELECT propval FROM " + getEscapedRootTableName()
          + " WHERE version IS NULL AND propname = ?");
        s.setString(1, propertyName);
      } else {
        s = connection.prepareStatement(
          "SELECT propval FROM " + getEscapedRootTableName() + " WHERE version = ? "
          + " AND propname = ?");
        s.setInt(1, version);
        s.setString(2, propertyName);
      }

      rs = s.executeQuery();
      if (!rs.next()) {
        LOG.debug(" => (no result)");
        return null; // No such result.
      } else {
        String result = rs.getString(1); // Return the only result col.
        LOG.debug(" => " + result);
        return result;
      }
    } finally {
      if (null != rs) {
        try {
          rs.close();
        } catch (SQLException sqlE) {
          LOG.warn("Error closing resultset: " + sqlE);
        }
      }

      if (null != s) {
        s.close();
      }
    }
  }

  /**
   * Set a value for the specified version (may be null) in the root
   * metadata table.
   */
  private void setRootProperty(String propertyName, Integer version,
      String val) throws SQLException {
    LOG.debug("Setting property " + propertyName + " for version "
        + version + " => " + val);

    PreparedStatement s;
    String curVal = getRootProperty(propertyName, version);
    if (null == curVal) {
      // INSERT the row.
      s = connection.prepareStatement("INSERT INTO " + getEscapedRootTableName()
          + " (propval, propname, version) VALUES ( ? , ? , ? )");
    } else {
      // UPDATE an existing row with non-null version.
      s = connection.prepareStatement("UPDATE " + getEscapedRootTableName()
          + " SET propval = ? WHERE  propname = ? AND version = ?");
    }

    try {
      s.setString(1, val);
      s.setString(2, propertyName);
      //Replaces null value with -1 constant, for backward compatibility
      if (null == version) {
       s.setInt(3, NO_VERSION);
      } else {
        s.setInt(3, version);
      }
      s.executeUpdate();
    } finally {
      s.close();
    }
  }

  /**
   * Create the jobs table in the V0 schema.
   */
  private void createJobTable() throws SQLException {
    String curTableName = DEFAULT_SESSION_TABLE_NAME;
    int tableNum = -1;
    while (true) {
      if (tableExists(curTableName)) {
        tableNum++;
        curTableName = DEFAULT_SESSION_TABLE_NAME + "_" + tableNum;
      } else {
        break;
      }
    }

    // curTableName contains a table name that does not exist.
    // Create this table.
    LOG.debug("Creating job storage table: " + curTableName);
    Statement s = connection.createStatement();
    try {
      s.executeUpdate("CREATE TABLE " + connManager.escapeTableName(curTableName) + " ("
          + "job_name VARCHAR(64) NOT NULL, "
          + "propname VARCHAR(128) NOT NULL, "
          + "propval VARCHAR(1024), "
          + "propclass VARCHAR(32) NOT NULL, "
          + "CONSTRAINT " + curTableName + "_unq UNIQUE "
          + "(job_name, propname, propclass))");

      // Then set a property in the root table pointing to it.
      setRootProperty(SESSION_TABLE_KEY, 0, curTableName);
      connection.commit();
    } finally {
      s.close();
    }

    this.jobTableName = curTableName;
  }

  /**
   * Given a root schema that exists,
   * initialize a version-0 key/value storage schema on top of it,
   * if it does not already exist.
   */
  private void initV0Schema() throws SQLException {
    this.jobTableName = getRootProperty(SESSION_TABLE_KEY, 0);

    checkForOldRootProperties();

    if (null == this.jobTableName) {
      createJobTable();
    }
    if (!tableExists(this.jobTableName)) {
      LOG.debug("Could not find job table: " + jobTableName);
      createJobTable();
    }
  }

  /** Checks to see if there is an existing job table under the old root table schema
   *  and reconfigures under the present schema, present for backward compatibility. **/
  private void checkForOldRootProperties() throws SQLException {
    String hsqldbStorageJobTableName = getRootProperty(HSQLDB_TABLE_KEY, 0);
    if(hsqldbStorageJobTableName != null && this.jobTableName == null) {
      this.jobTableName = hsqldbStorageJobTableName;
      setRootProperty(SESSION_TABLE_KEY, 0, jobTableName);
    }
  }

  /**
   * INSERT or UPDATE a single (job, propname, class) to point
   * to the specified property value.
   */
  private void setV0Property(String jobName, String propClass,
      String propName, String propVal) throws SQLException {
    LOG.debug("Job: " + jobName + "; Setting property "
        + propName + " with class " + propClass + " => " + PasswordRedactor.redactValue(propName, propVal));

    PreparedStatement s = null;
    try {
      String curValue = getV0Property(jobName, propClass, propName);
      if (null == curValue) {
        // Property is not yet set.
        s = connection.prepareStatement("INSERT INTO " + connManager.escapeTableName(this.jobTableName)
            + " (propval, job_name, propclass, propname) "
            + "VALUES (?, ?, ?, ?)");
      } else {
        // Overwrite existing property.
        s = connection.prepareStatement("UPDATE " + connManager.escapeTableName(this.jobTableName)
            + " SET propval = ? WHERE job_name = ? AND propclass = ? "
            + "AND propname = ?");
      }

      s.setString(1, propVal);
      s.setString(2, jobName);
      s.setString(3, propClass);
      s.setString(4, propName);

      s.executeUpdate();
    } finally {
      if (null != s) {
        s.close();
      }
    }
  }

  /**
   * Return a string containing the value of a specified property,
   * or null if it is not set.
   */
  private String getV0Property(String jobName, String propClass,
      String propertyName) throws SQLException {
    LOG.debug("Job: " + jobName + "; Getting property "
        + propertyName + " with class " + propClass);

    ResultSet rs = null;
    PreparedStatement s = connection.prepareStatement(
        "SELECT propval FROM " + connManager.escapeTableName(this.jobTableName)
        + " WHERE job_name = ? AND propclass = ? AND propname = ?");

    try {
      s.setString(1, jobName);
      s.setString(2, propClass);
      s.setString(3, propertyName);
      rs = s.executeQuery();

      if (!rs.next()) {
        LOG.debug(" => (no result)");
        return null;
      }

      String result = rs.getString(1);
      LOG.debug(" => " + result);
      return result;
    } finally {
      if (null != rs) {
        try {
          rs.close();
        } catch (SQLException sqlE) {
          LOG.warn("Error closing resultset: " + sqlE);
        }
      }

      s.close();
    }
  }

  /**
   * Get a java.util.Properties containing all propName -&gt; propVal
   * bindings for a given (jobName, propClass).
   */
  private Properties getV0Properties(String jobName, String propClass)
      throws SQLException {
    LOG.debug("Job: " + jobName
        + "; Getting properties with class " + propClass);

    ResultSet rs = null;
    PreparedStatement s = connection.prepareStatement(
        "SELECT propname, propval FROM " + connManager.escapeTableName(this.jobTableName)
        + " WHERE job_name = ? AND propclass = ?");
    try {
      s.setString(1, jobName);
      s.setString(2, propClass);
      rs = s.executeQuery();

      Properties p = new Properties();
      while (rs.next()) {
        p.setProperty(rs.getString(1), rs.getString(2));
      }

      return p;
    } finally {
      if (null != rs) {
        try {
          rs.close();
        } catch (SQLException sqlE) {
          LOG.warn("Error closing result set: " + sqlE);
        }
      }

      s.close();
    }
  }

  private void setV0Properties(String jobName, String propClass,
      Properties properties) throws SQLException {
    LOG.debug("Job: " + jobName
        + "; Setting bulk properties for class " + propClass);

    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      String key = entry.getKey().toString();
      String val = entry.getValue().toString();
      setV0Property(jobName, propClass, key, val);
    }
  }

  private ConnManager createConnManager() {
    SqoopOptions sqoopOptions = new SqoopOptions();
    sqoopOptions.setConnectString(metastoreConnectStr);
    sqoopOptions.setUsername(metastoreUser);
    sqoopOptions.setPassword(metastorePassword);
    JobData jd = new JobData();
    jd.setSqoopOptions(sqoopOptions);
    DefaultManagerFactory dmf = new DefaultManagerFactory();
    return dmf.accept(jd);
  }

  protected boolean isDbSupported(String metaConnectString) {
    for (JdbcDrivers driver : SUPPORTED_DRIVERS) {
      if (startsWith(metaConnectString, driver.getSchemePrefix())) {
        return true;
      }
    }
    return false;
  }

}
