/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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


package com.cloudera.sqoop.metastore.hsqldb;

import java.io.IOException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;

import com.cloudera.sqoop.metastore.SessionData;
import com.cloudera.sqoop.metastore.SessionStorage;

import com.cloudera.sqoop.tool.SqoopTool;

/**
 * SessionStorage implementation that uses an HSQLDB-backed database to
 * hold session information.
 */
public class HsqldbSessionStorage extends SessionStorage {

  public static final Log LOG = LogFactory.getLog(
      HsqldbSessionStorage.class.getName());
  
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


  /** Default name for the root metadata table in HSQLDB. */
  private static final String DEFAULT_ROOT_TABLE_NAME = "SQOOP_ROOT";

  /** Configuration key used to override root table name. */
  public static final String ROOT_TABLE_NAME_KEY =
       "sqoop.hsqldb.root.table.name";

  /** root metadata table key used to define the current schema version. */
  private static final String STORAGE_VERSION_KEY =
      "sqoop.hsqldb.session.storage.version";

  /** The current version number for the schema edition. */
  private static final int CUR_STORAGE_VERSION = 0;

  /** root metadata table key used to define the session table name. */
  private static final String SESSION_TABLE_KEY =
      "sqoop.hsqldb.session.info.table";

  /** Default value for SESSION_TABLE_KEY. */
  private static final String DEFAULT_SESSION_TABLE_NAME =
      "SQOOP_SESSIONS";

  /** Per-session key with propClass 'schema' that defines the set of
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
   * Per-session key with propClass 'schema' that specifies the SqoopTool
   * to load.
   */
  private static final String SQOOP_TOOL_KEY = "sqoop.tool";


  private String metastoreConnectStr;
  private String metastoreUser;
  private String metastorePassword;
  private Connection connection;

  protected Connection getConnection() {
    return this.connection;
  }

  // After connection to the database and initialization of the
  // schema, this holds the name of the session table.
  private String sessionTableName;

  protected void setMetastoreConnectStr(String connectStr) {
    this.metastoreConnectStr = connectStr;
  }

  protected void setMetastoreUser(String user) {
    this.metastoreUser = user;
  }

  protected void setMetastorePassword(String pass) {
    this.metastorePassword = pass;
  }

  private static final String DB_DRIVER_CLASS = "org.hsqldb.jdbcDriver";

  @Override
  /**
   * Initialize the connection to the database.
   */
  public void open(Map<String, String> descriptor) throws IOException {
    setMetastoreConnectStr(descriptor.get(META_CONNECT_KEY));
    setMetastoreUser(descriptor.get(META_USERNAME_KEY));
    setMetastorePassword(descriptor.get(META_PASSWORD_KEY));

    init();
  }

  protected void init() throws IOException {
    try {
      // Load/initialize the JDBC driver.
      Class.forName(DB_DRIVER_CLASS);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Could not load HSQLDB JDBC driver", cnfe);
    }

    try {
      if (null == metastoreUser) {
        this.connection = DriverManager.getConnection(metastoreConnectStr);
      } else {
        this.connection = DriverManager.getConnection(metastoreConnectStr,
            metastoreUser, metastorePassword);
      }

      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      connection.setAutoCommit(false);

      // Initialize the root schema.
      if (!rootTableExists()) {
        createRootTable();
      }

      // Check the schema version.
      String curStorageVerStr = getRootProperty(STORAGE_VERSION_KEY, null);
      int actualStorageVer = -1;
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
    if (null != this.connection) {
      try {
        LOG.debug("Flushing current transaction");
        this.connection.commit();
      } catch (SQLException sqlE) {
        throw new IOException("Exception committing connection", sqlE);
      }

      try {
        LOG.debug("Closing connection");
        this.connection.close();
      } catch (SQLException sqlE) {
        throw new IOException("Exception closing connection", sqlE);
      } finally {
        this.connection = null;
      }
    }
  }

  @Override
  /** {@inheritDoc} */
  public boolean canAccept(Map<String, String> descriptor) {
    // We return true if the desciptor contains a connect string to find
    // the database.
    return descriptor.get(META_CONNECT_KEY) != null;
  }

  @Override
  /** {@inheritDoc} */
  public SessionData read(String sessionName) throws IOException {
    try {
      if (!sessionExists(sessionName)) {
        LOG.error("Cannot restore session: " + sessionName);
        LOG.error("(No such session)");
        throw new IOException("Cannot restore missing session " + sessionName);
      }

      LOG.debug("Restoring session: " + sessionName);
      Properties schemaProps = getV0Properties(sessionName,
          PROPERTY_CLASS_SCHEMA);
      Properties sqoopOptProps = getV0Properties(sessionName,
          PROPERTY_CLASS_SQOOP_OPTIONS);
      Properties configProps = getV0Properties(sessionName,
          PROPERTY_CLASS_CONFIG);

      // Check that we're not using a saved session from a previous
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
        throw new IOException("Error in session metadata: invalid tool "
            + toolName);
      }

      Configuration conf = new Configuration();
      for (Map.Entry<Object, Object> entry : configProps.entrySet()) {
        conf.set(entry.getKey().toString(), entry.getValue().toString());
      }

      SqoopOptions opts = new SqoopOptions();
      opts.setConf(conf);
      opts.loadProperties(sqoopOptProps);

      return new SessionData(opts, tool);
    } catch (SQLException sqlE) {
      throw new IOException("Error communicating with database", sqlE);
    }
  }

  private boolean sessionExists(String sessionName) throws SQLException {
    PreparedStatement s = connection.prepareStatement(
        "SELECT COUNT(session_name) FROM " + this.sessionTableName
        + " WHERE session_name = ? GROUP BY session_name");
    ResultSet rs = null;
    try {
      s.setString(1, sessionName);
      rs = s.executeQuery();
      if (rs.next()) {
        return true; // We got a result, meaning the session exists.
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
  public void delete(String sessionName) throws IOException {
    try {
      if (!sessionExists(sessionName)) {
        LOG.error("No such session: " + sessionName);
      } else {
        LOG.debug("Deleting session: " + sessionName);
        PreparedStatement s = connection.prepareStatement("DELETE FROM "
            + this.sessionTableName + " WHERE session_name = ?");
        try {
          s.setString(1, sessionName);
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
  public void create(String sessionName, SessionData data)
      throws IOException {
    try {
      if (sessionExists(sessionName)) {
        LOG.error("Cannot create session " + sessionName
            + ": it already exists");
        throw new IOException("Session " + sessionName + " already exists");
      }
    } catch (SQLException sqlE) {
      throw new IOException("Error communicating with database", sqlE);
    }

    createInternal(sessionName, data);
  }

  /**
   * Actually insert/update the resources for this session.
   */
  private void createInternal(String sessionName, SessionData data)
      throws IOException {
    try {
      LOG.debug("Creating session: " + sessionName);

      // Save the name of the Sqoop tool.
      setV0Property(sessionName, PROPERTY_CLASS_SCHEMA, SQOOP_TOOL_KEY,
          data.getSqoopTool().getToolName());

      // Save the property set id.
      setV0Property(sessionName, PROPERTY_CLASS_SCHEMA, PROPERTY_SET_KEY,
          CUR_PROPERTY_SET_ID);

      // Save all properties of the SqoopOptions.
      Properties props = data.getSqoopOptions().writeProperties();
      setV0Properties(sessionName, PROPERTY_CLASS_SQOOP_OPTIONS, props);

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
        setV0Property(sessionName, PROPERTY_CLASS_CONFIG, key, rawVal);
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
  public void update(String sessionName, SessionData data)
      throws IOException {
    try {
      if (!sessionExists(sessionName)) {
        LOG.error("Cannot update session " + sessionName + ": not found");
        throw new IOException("Session " + sessionName + " does not exist");
      }
    } catch (SQLException sqlE) {
      throw new IOException("Error communicating with database", sqlE);
    }

    // Since we set properties with update-or-insert, this is the same
    // as create on this system.
    createInternal(sessionName, data);
  }

  @Override
  /** {@inheritDoc} */
  public List<String> list() throws IOException {
    ResultSet rs = null;
    try {
      PreparedStatement s = connection.prepareStatement(
          "SELECT DISTINCT session_name FROM " + this.sessionTableName);
      try {
        rs = s.executeQuery();
        ArrayList<String> sessions = new ArrayList<String>();
        while (rs.next()) {
          sessions.add(rs.getString(1));
        }

        return sessions;
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
    return conf.get(ROOT_TABLE_NAME_KEY, DEFAULT_ROOT_TABLE_NAME);
  }

  private boolean tableExists(String table) throws SQLException {
    LOG.debug("Checking for table: " + table);
    DatabaseMetaData dbmd = connection.getMetaData();
    String [] tableTypes = { "TABLE" };
    ResultSet rs = dbmd.getTables(null, null, null, tableTypes);
    if (null != rs) {
      try {
        while (rs.next()) {
          if (table.equalsIgnoreCase(rs.getString("TABLE_NAME"))) {
            LOG.debug("Found table: " + table);
            return true;
          }
        }
      } finally {
        rs.close();
      }
    }

    LOG.debug("Could not find table.");
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
      s.executeUpdate("CREATE TABLE " + rootTableName + " ("
          + "version INT, "
          + "propname VARCHAR(128) NOT NULL, "
          + "propval VARCHAR(256), "
          + "CONSTRAINT " + rootTableName + "_unq UNIQUE (version, propname))");
    } finally {
      s.close();
    }

    setRootProperty(STORAGE_VERSION_KEY, null,
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
          "SELECT propval FROM " + getRootTableName()
          + " WHERE version IS NULL AND propname = ?");
        s.setString(1, propertyName);
      } else {
        s = connection.prepareStatement(
          "SELECT propval FROM " + getRootTableName() + " WHERE version = ? "
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
      s = connection.prepareStatement("INSERT INTO " + getRootTableName()
          + " (propval, propname, version) VALUES ( ? , ? , ? )");
    } else if (version == null) {
      // UPDATE an existing row with a null version
      s = connection.prepareStatement("UPDATE " + getRootTableName()
          + " SET propval = ? WHERE  propname = ? AND version IS NULL");
    } else {
      // UPDATE an existing row with non-null version.
      s = connection.prepareStatement("UPDATE " + getRootTableName()
          + " SET propval = ? WHERE  propname = ? AND version = ?");
    }

    try {
      s.setString(1, val);
      s.setString(2, propertyName);
      if (null != version) {
        s.setInt(3, version);
      }
      s.executeUpdate();
    } finally {
      s.close();
    }
  }

  /**
   * Create the sessions table in the V0 schema.
   */
  private void createSessionTable() throws SQLException {
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
    LOG.debug("Creating session storage table: " + curTableName);
    Statement s = connection.createStatement();
    try {
      s.executeUpdate("CREATE TABLE " + curTableName + " ("
          + "session_name VARCHAR(64) NOT NULL, "
          + "propname VARCHAR(128) NOT NULL, "
          + "propval VARCHAR(1024), "
          + "propclass VARCHAR(32) NOT NULL, "
          + "CONSTRAINT " + curTableName + "_unq UNIQUE "
          + "(session_name, propname, propclass))");

      // Then set a property in the root table pointing to it.
      setRootProperty(SESSION_TABLE_KEY, 0, curTableName);
      connection.commit();
    } finally {
      s.close();
    }

    this.sessionTableName = curTableName;
  }

  /**
   * Given a root schema that exists,
   * initialize a version-0 key/value storage schema on top of it,
   * if it does not already exist.
   */
  private void initV0Schema() throws SQLException {
    this.sessionTableName = getRootProperty(SESSION_TABLE_KEY, 0);
    if (null == this.sessionTableName) {
      createSessionTable();
    }
    if (!tableExists(this.sessionTableName)) {
      LOG.debug("Could not find session table: " + sessionTableName);
      createSessionTable();
    }
  }

  /**
   * INSERT or UPDATE a single (session, propname, class) to point
   * to the specified property value.
   */
  private void setV0Property(String sessionName, String propClass,
      String propName, String propVal) throws SQLException {
    LOG.debug("Session: " + sessionName + "; Setting property "
        + propName + " with class " + propClass + " => " + propVal);

    PreparedStatement s = null;
    try {
      String curValue = getV0Property(sessionName, propClass, propName);
      if (null == curValue) {
        // Property is not yet set.
        s = connection.prepareStatement("INSERT INTO " + this.sessionTableName
            + " (propval, session_name, propclass, propname) "
            + "VALUES (?, ?, ?, ?)");
      } else {
        // Overwrite existing property.
        s = connection.prepareStatement("UPDATE " + this.sessionTableName
            + " SET propval = ? WHERE session_name = ? AND propclass = ? "
            + "AND propname = ?");
      }

      s.setString(1, propVal);
      s.setString(2, sessionName);
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
  private String getV0Property(String sessionName, String propClass,
      String propertyName) throws SQLException {
    LOG.debug("Session: " + sessionName + "; Getting property "
        + propertyName + " with class " + propClass);

    ResultSet rs = null;
    PreparedStatement s = connection.prepareStatement(
        "SELECT propval FROM " + this.sessionTableName
        + " WHERE session_name = ? AND propclass = ? AND propname = ?");

    try {
      s.setString(1, sessionName);
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
   * bindings for a given (sessionName, propClass).
   */
  private Properties getV0Properties(String sessionName, String propClass)
      throws SQLException {
    LOG.debug("Session: " + sessionName
        + "; Getting properties with class " + propClass);

    ResultSet rs = null;
    PreparedStatement s = connection.prepareStatement(
        "SELECT propname, propval FROM " + this.sessionTableName
        + " WHERE session_name = ? AND propclass = ?");
    try {
      s.setString(1, sessionName);
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

  private void setV0Properties(String sessionName, String propClass,
      Properties properties) throws SQLException {
    LOG.debug("Session: " + sessionName
        + "; Setting bulk properties for class " + propClass);

    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      String key = entry.getKey().toString();
      String val = entry.getValue().toString();
      setV0Property(sessionName, propClass, key, val);
    }
  }
}

