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
package org.apache.sqoop.repository.derby;

import static org.apache.sqoop.repository.derby.DerbySchemaQuery.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.framework.FrameworkManager;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFormType;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.repository.JdbcRepositoryContext;
import org.apache.sqoop.repository.JdbcRepositoryHandler;
import org.apache.sqoop.repository.JdbcRepositoryTransactionFactory;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;

/**
 * JDBC based repository handler for Derby database.
 *
 * Repository implementation for Derby database.
 */
public class DerbyRepositoryHandler extends JdbcRepositoryHandler {

  private static final Logger LOG =
      Logger.getLogger(DerbyRepositoryHandler.class);

  private static final String EMBEDDED_DERBY_DRIVER_CLASSNAME =
          "org.apache.derby.jdbc.EmbeddedDriver";

  private JdbcRepositoryContext repoContext;
  private DataSource dataSource;
  private JdbcRepositoryTransactionFactory txFactory;

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerConnector(MConnector mc, Connection conn) {
    if (mc.hasPersistenceId()) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0011,
        mc.getUniqueName());
    }
    mc.setPersistenceId(getConnectorId(mc, conn));
    insertFormsForConnector(mc, conn);
  }

  /**
   * Helper method to insert the forms from the  into the
   * repository. The job and connector forms within <code>mc</code> will get
   * updated with the id of the forms when this function returns.
   * @param mf The MFramework instance to use to upgrade.
   * @param conn JDBC connection to use for updating the forms
   */
  private void insertFormsForFramework(MFramework mf, Connection conn) {
    PreparedStatement baseFormStmt = null;
    PreparedStatement baseInputStmt = null;
    try{
      baseFormStmt = conn.prepareStatement(STMT_INSERT_FORM_BASE,
        Statement.RETURN_GENERATED_KEYS);

      baseInputStmt = conn.prepareStatement(STMT_INSERT_INPUT_BASE,
        Statement.RETURN_GENERATED_KEYS);

      // Register connector forms
      registerForms(null, null, mf.getConnectionForms().getForms(),
        MFormType.CONNECTION.name(), baseFormStmt, baseInputStmt);

      // Register all jobs
      for (MJobForms jobForms : mf.getAllJobsForms().values()) {
        registerForms(null, jobForms.getType(), jobForms.getForms(),
          MFormType.JOB.name(), baseFormStmt, baseInputStmt);
      }

    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0014, mf.toString(), ex);
    } finally {
      closeStatements(baseFormStmt, baseInputStmt);
    }
  }

  /**
   * Helper method to insert the forms from the MConnector into the
   * repository. The job and connector forms within <code>mc</code> will get
   * updated with the id of the forms when this function returns.
   * @param mc The connector to use for updating forms
   * @param conn JDBC connection to use for updating the forms
   */
  private void insertFormsForConnector (MConnector mc, Connection conn) {
    long connectorId = mc.getPersistenceId();
    PreparedStatement baseFormStmt = null;
    PreparedStatement baseInputStmt = null;
    try{
      baseFormStmt = conn.prepareStatement(STMT_INSERT_FORM_BASE,
        Statement.RETURN_GENERATED_KEYS);

      baseInputStmt = conn.prepareStatement(STMT_INSERT_INPUT_BASE,
        Statement.RETURN_GENERATED_KEYS);

      // Register connector forms
      registerForms(connectorId, null, mc.getConnectionForms().getForms(),
        MFormType.CONNECTION.name(), baseFormStmt, baseInputStmt);

      // Register all jobs
      for (MJobForms jobForms : mc.getAllJobsForms().values()) {
        registerForms(connectorId, jobForms.getType(), jobForms.getForms(),
          MFormType.JOB.name(), baseFormStmt, baseInputStmt);
      }

    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0014,
        mc.toString(), ex);
    } finally {
      closeStatements(baseFormStmt, baseInputStmt);
    }

  }

  private long getConnectorId(MConnector mc, Connection conn) {
    PreparedStatement baseConnectorStmt = null;
    try {
      baseConnectorStmt = conn.prepareStatement(STMT_INSERT_CONNECTOR_BASE,
        Statement.RETURN_GENERATED_KEYS);
      baseConnectorStmt.setString(1, mc.getUniqueName());
      baseConnectorStmt.setString(2, mc.getClassName());
      baseConnectorStmt.setString(3, mc.getVersion());

      int baseConnectorCount = baseConnectorStmt.executeUpdate();
      if (baseConnectorCount != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0012,
          Integer.toString(baseConnectorCount));
      }

      ResultSet rsetConnectorId = baseConnectorStmt.getGeneratedKeys();

      if (!rsetConnectorId.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0013);
      }
      return rsetConnectorId.getLong(1);
    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0014,
        mc.toString(), ex);
    } finally {
      closeStatements(baseConnectorStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void initialize(JdbcRepositoryContext ctx) {
    repoContext = ctx;
    dataSource = repoContext.getDataSource();
    txFactory = repoContext.getTransactionFactory();
    LOG.info("DerbyRepositoryHandler initialized.");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void shutdown() {
    String driver = repoContext.getDriverClass();
    if (driver != null && driver.equals(EMBEDDED_DERBY_DRIVER_CLASSNAME)) {
      // Using embedded derby. Needs explicit shutdown
      String connectUrl = repoContext.getConnectionUrl();
      if (connectUrl.startsWith("jdbc:derby:")) {
        int index = connectUrl.indexOf(';');
        String baseUrl = null;
        if (index != -1) {
          baseUrl = connectUrl.substring(0, index+1);
        } else {
          baseUrl = connectUrl + ";";
        }
        String shutDownUrl = baseUrl + "shutdown=true";

        LOG.debug("Attempting to shutdown embedded Derby using URL: "
            + shutDownUrl);

        try {
          DriverManager.getConnection(shutDownUrl);
        } catch (SQLException ex) {
          // Shutdown for one db instance is expected to raise SQL STATE 45000
          if (ex.getErrorCode() != 45000) {
            throw new SqoopException(
                DerbyRepoError.DERBYREPO_0002, shutDownUrl, ex);
          }
          LOG.info("Embedded Derby shutdown raised SQL STATE "
              + "45000 as expected.");
        }
      } else {
        LOG.warn("Even though embedded Derby driver was loaded, the connect "
            + "URL is of an unexpected form: " + connectUrl + ". Therefore no "
            + "attempt will be made to shutdown embedded Derby instance.");
      }
    }
  }

  /**
   * Detect version of underlying database structures.
   *
   * @param conn JDBC Connection
   * @return
   */
  public int detectVersion(Connection conn) {
    ResultSet rs = null;
    PreparedStatement stmt = null;

    // First release went out without system table, so we have to detect
    // this version differently.
    try {
      rs = conn.getMetaData().getTables(null, null, null, null);

      Set<String> tableNames = new HashSet<String>();
      while(rs.next()) {
        tableNames.add(rs.getString("TABLE_NAME"));
      }
      closeResultSets(rs);

      LOG.debug("Detecting old version of repository");
      boolean foundAll = true;
      for( String expectedTable : DerbySchemaConstants.tablesV1) {
        if(!tableNames.contains(expectedTable)) {
          foundAll = false;
          LOG.debug("Missing table " + expectedTable);
        }
      }

      // If we find all expected tables, then we are on version 1
      if(foundAll && !tableNames.contains(DerbySchemaConstants.TABLE_SQ_SYSTEM_NAME)) {
        return 1;
      }

    } catch (SQLException e) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0041, e);
    } finally {
      closeResultSets(rs);
    }

    // Normal version detection, select and return the version
    try {
      stmt = conn.prepareStatement(STMT_SELECT_SYSTEM);
      stmt.setString(1, DerbyRepoConstants.SYSKEY_VERSION);
      rs = stmt.executeQuery();

      if(!rs.next()) {
        return 0;
      }

      return rs.getInt(1);
    } catch (SQLException e) {
      LOG.info("Can't fetch repository structure version.", e);
      return 0;
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  /**
   * Detect version of the framework
   *
   * @param conn Connection to metadata repository
   * @return Version of the MFramework
   */
  private String detectFrameworkVersion (Connection conn) {
    ResultSet rs = null;
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(DerbySchemaQuery.STMT_SELECT_SYSTEM);
      stmt.setString(1, DerbyRepoConstants.SYSKEY_FRAMEWORK_VERSION);
      rs = stmt.executeQuery();
      if(!rs.next()) {
        return null;
      }
      return rs.getString(1);
    } catch (SQLException e) {
      LOG.info("Can't fetch framework version.", e);
      return null;
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  /**
   * Create or update framework version
   * @param conn Connection to the metadata repository
   * @param mFramework
   */
  private void createOrUpdateFrameworkVersion(Connection conn,
      MFramework mFramework) {
    ResultSet rs = null;
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(STMT_DELETE_SYSTEM);
      stmt.setString(1, DerbyRepoConstants.SYSKEY_FRAMEWORK_VERSION);
      stmt.executeUpdate();
      closeStatements(stmt);

      stmt = conn.prepareStatement(STMT_INSERT_SYSTEM);
      stmt.setString(1, DerbyRepoConstants.SYSKEY_FRAMEWORK_VERSION);
      stmt.setString(2, mFramework.getVersion());
      stmt.executeUpdate();
    } catch (SQLException e) {
      logException(e);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0044, e);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createOrUpdateInternals(Connection conn) {
    int version = detectVersion(conn);

    if(version <= 0) {
      runQuery(QUERY_CREATE_SCHEMA_SQOOP, conn);
      runQuery(QUERY_CREATE_TABLE_SQ_CONNECTOR, conn);
      runQuery(QUERY_CREATE_TABLE_SQ_FORM, conn);
      runQuery(QUERY_CREATE_TABLE_SQ_INPUT, conn);
      runQuery(QUERY_CREATE_TABLE_SQ_CONNECTION, conn);
      runQuery(QUERY_CREATE_TABLE_SQ_JOB, conn);
      runQuery(QUERY_CREATE_TABLE_SQ_CONNECTION_INPUT, conn);
      runQuery(QUERY_CREATE_TABLE_SQ_JOB_INPUT, conn);
      runQuery(QUERY_CREATE_TABLE_SQ_SUBMISSION, conn);
      runQuery(QUERY_CREATE_TABLE_SQ_COUNTER_GROUP, conn);
      runQuery(QUERY_CREATE_TABLE_SQ_COUNTER, conn);
      runQuery(QUERY_CREATE_TABLE_SQ_COUNTER_SUBMISSION, conn);
    }
    if(version <= 1) {
      runQuery(QUERY_CREATE_TABLE_SQ_SYSTEM, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_ENABLED, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_ENABLED, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_CREATION_USER, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_UPDATE_USER, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_CREATION_USER, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_UPDATE_USER, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_SUBMISSION_ADD_COLUMN_CREATION_USER, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_SUBMISSION_ADD_COLUMN_UPDATE_USER, conn);
    }

    ResultSet rs = null;
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(STMT_DELETE_SYSTEM);
      stmt.setString(1, DerbyRepoConstants.SYSKEY_VERSION);
      stmt.executeUpdate();

      closeStatements(stmt);

      stmt = conn.prepareStatement(STMT_INSERT_SYSTEM);
      stmt.setString(1, DerbyRepoConstants.SYSKEY_VERSION);
      stmt.setString(2, "" + DerbyRepoConstants.VERSION);
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOG.error("Can't persist the repository version", e);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean haveSuitableInternals(Connection conn) {
    int version = detectVersion(conn);

    if(version != DerbyRepoConstants.VERSION) {
      return false;
    }

    // TODO(jarcec): Verify that all structures are present (e.g. something like corruption validation)
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MConnector findConnector(String shortName, Connection conn) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking up connector: " + shortName);
    }
    MConnector mc = null;
    PreparedStatement baseConnectorFetchStmt = null;
    PreparedStatement formFetchStmt = null;
    PreparedStatement inputFetchStmt = null;
    try {
      baseConnectorFetchStmt = conn.prepareStatement(STMT_FETCH_BASE_CONNECTOR);
      baseConnectorFetchStmt.setString(1, shortName);
      ResultSet rsetBaseConnector = baseConnectorFetchStmt.executeQuery();

      if (!rsetBaseConnector.next()) {
        LOG.debug("No connector found by name: " + shortName);
        return null;
      }

      long connectorId = rsetBaseConnector.getLong(1);
      String connectorName = rsetBaseConnector.getString(2);
      String connectorClassName = rsetBaseConnector.getString(3);
      String connectorVersion = rsetBaseConnector.getString(4);

      formFetchStmt = conn.prepareStatement(STMT_FETCH_FORM_CONNECTOR);
      formFetchStmt.setLong(1, connectorId);
      inputFetchStmt = conn.prepareStatement(STMT_FETCH_INPUT);

      List<MForm> connectionForms = new ArrayList<MForm>();
      Map<MJob.Type, List<MForm>> jobForms =
        new HashMap<MJob.Type, List<MForm>>();

      loadForms(connectionForms, jobForms, formFetchStmt, inputFetchStmt, 1);

      mc = new MConnector(connectorName, connectorClassName, connectorVersion,
        new MConnectionForms(connectionForms),
        convertToJobList(jobForms));
      mc.setPersistenceId(connectorId);

      if (rsetBaseConnector.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0005, shortName);
      }
    } catch (SQLException ex) {
      logException(ex, shortName);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0004, shortName, ex);
    } finally {
      closeStatements(baseConnectorFetchStmt,
        formFetchStmt, inputFetchStmt);
    }

    LOG.debug("Looking up connector: " + shortName + ", found: " + mc);
    return mc;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerFramework(MFramework mf, Connection conn) {
    if (mf.hasPersistenceId()) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0011,
        "Framework metadata");
    }

    PreparedStatement baseFormStmt = null;
    PreparedStatement baseInputStmt = null;
    try {
      baseFormStmt = conn.prepareStatement(STMT_INSERT_FORM_BASE,
          Statement.RETURN_GENERATED_KEYS);
      baseInputStmt = conn.prepareStatement(STMT_INSERT_INPUT_BASE,
          Statement.RETURN_GENERATED_KEYS);

      // Register connector forms
      registerForms(null, null, mf.getConnectionForms().getForms(),
        MFormType.CONNECTION.name(), baseFormStmt, baseInputStmt);

      // Register all jobs
      for (MJobForms jobForms : mf.getAllJobsForms().values()) {
        registerForms(null, jobForms.getType(), jobForms.getForms(),
          MFormType.JOB.name(), baseFormStmt, baseInputStmt);
      }

      // We're using hardcoded value for framework metadata as they are
      // represented as NULL in the database.
      mf.setPersistenceId(1);
    } catch (SQLException ex) {
      logException(ex, mf);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0014, ex);
    } finally {
      closeStatements(baseFormStmt, baseInputStmt);
    }
    createOrUpdateFrameworkVersion(conn, mf);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MFramework findFramework(Connection conn) {
    LOG.debug("Looking up framework metadata");
    MFramework mf = null;
    PreparedStatement formFetchStmt = null;
    PreparedStatement inputFetchStmt = null;
    try {
      formFetchStmt = conn.prepareStatement(STMT_FETCH_FORM_FRAMEWORK);
      inputFetchStmt = conn.prepareStatement(STMT_FETCH_INPUT);

      List<MForm> connectionForms = new ArrayList<MForm>();
      Map<MJob.Type, List<MForm>> jobForms =
        new HashMap<MJob.Type, List<MForm>>();

      loadForms(connectionForms, jobForms, formFetchStmt, inputFetchStmt, 1);

      // Return nothing If there aren't any framework metadata
      if(connectionForms.isEmpty() && jobForms.isEmpty()) {
        return null;
      }

      mf = new MFramework(new MConnectionForms(connectionForms),
        convertToJobList(jobForms), detectFrameworkVersion(conn));

      // We're using hardcoded value for framework metadata as they are
      // represented as NULL in the database.
      mf.setPersistenceId(1);

    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0004,
        "Framework metadata", ex);
    } finally {
      if (formFetchStmt != null) {
        try {
          formFetchStmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close form fetch statement", ex);
        }
      }
      if (inputFetchStmt != null) {
        try {
          inputFetchStmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close input fetch statement", ex);
        }
      }
    }

    LOG.debug("Looking up framework metadta found: " + mf);

    // Returned loaded framework metadata
    return mf;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String validationQuery() {
    return "values(1)"; // Yes, this is valid derby SQL
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createConnection(MConnection connection, Connection conn) {
    PreparedStatement stmt = null;
    int result;
    try {
      stmt = conn.prepareStatement(STMT_INSERT_CONNECTION,
        Statement.RETURN_GENERATED_KEYS);
      stmt.setString(1, connection.getName());
      stmt.setLong(2, connection.getConnectorId());
      stmt.setBoolean(3, connection.getEnabled());
      stmt.setString(4, connection.getCreationUser());
      stmt.setTimestamp(5, new Timestamp(connection.getCreationDate().getTime()));
      stmt.setString(6, connection.getLastUpdateUser());
      stmt.setTimestamp(7, new Timestamp(connection.getLastUpdateDate().getTime()));

      result = stmt.executeUpdate();
      if (result != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0012,
          Integer.toString(result));
      }

      ResultSet rsetConnectionId = stmt.getGeneratedKeys();

      if (!rsetConnectionId.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0013);
      }

      long connectionId = rsetConnectionId.getLong(1);

      createInputValues(STMT_INSERT_CONNECTION_INPUT,
        connectionId,
        connection.getConnectorPart().getForms(),
        conn);
      createInputValues(STMT_INSERT_CONNECTION_INPUT,
        connectionId,
        connection.getFrameworkPart().getForms(),
        conn);

      connection.setPersistenceId(connectionId);

    } catch (SQLException ex) {
      logException(ex, connection);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0019, ex);
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateConnection(MConnection connection, Connection conn) {
    PreparedStatement deleteStmt = null;
    PreparedStatement updateStmt = null;
    try {
      // Firstly remove old values
      deleteStmt = conn.prepareStatement(STMT_DELETE_CONNECTION_INPUT);
      deleteStmt.setLong(1, connection.getPersistenceId());
      deleteStmt.executeUpdate();

      // Update CONNECTION table
      updateStmt = conn.prepareStatement(STMT_UPDATE_CONNECTION);
      updateStmt.setString(1, connection.getName());
      updateStmt.setString(2, connection.getLastUpdateUser());
      updateStmt.setTimestamp(3, new Timestamp(new Date().getTime()));

      updateStmt.setLong(4, connection.getPersistenceId());
      updateStmt.executeUpdate();

      // And reinsert new values
      createInputValues(STMT_INSERT_CONNECTION_INPUT,
        connection.getPersistenceId(),
        connection.getConnectorPart().getForms(),
        conn);
      createInputValues(STMT_INSERT_CONNECTION_INPUT,
        connection.getPersistenceId(),
        connection.getFrameworkPart().getForms(),
        conn);

    } catch (SQLException ex) {
      logException(ex, connection);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0021, ex);
    } finally {
      closeStatements(deleteStmt, updateStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean existsConnection(long id, Connection conn) {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_CONNECTION_CHECK);
      stmt.setLong(1, id);
      rs = stmt.executeQuery();

      // Should be always valid in query with count
      rs.next();

      return rs.getLong(1) == 1;
    } catch (SQLException ex) {
      logException(ex, id);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0025, ex);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  @Override
  public boolean inUseConnection(long connectionId, Connection conn) {
    PreparedStatement stmt = null;
    ResultSet rs = null;

    try {
      stmt = conn.prepareStatement(STMT_SELECT_JOBS_FOR_CONNECTION_CHECK);
      stmt.setLong(1, connectionId);
      rs = stmt.executeQuery();

      // Should be always valid in case of count(*) query
      rs.next();

      return rs.getLong(1) != 0;

    } catch (SQLException e) {
      logException(e, connectionId);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0032, e);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  @Override
  public void enableConnection(long connectionId, boolean enabled, Connection conn) {
    PreparedStatement enableConn = null;

    try {
      enableConn = conn.prepareStatement(STMT_ENABLE_CONNECTION);
      enableConn.setBoolean(1, enabled);
      enableConn.setLong(2, connectionId);
      enableConn.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, connectionId);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0042, ex);
    } finally {
      closeStatements(enableConn);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteConnection(long id, Connection conn) {
    PreparedStatement dltConn = null;

    try {
      deleteConnectionInputs(id, conn);
      dltConn = conn.prepareStatement(STMT_DELETE_CONNECTION);
      dltConn.setLong(1, id);
      dltConn.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, id);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0022, ex);
    } finally {
      closeStatements(dltConn);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteConnectionInputs(long id, Connection conn) {
    PreparedStatement dltConnInput = null;
    try {
      dltConnInput = conn.prepareStatement(STMT_DELETE_CONNECTION_INPUT);
      dltConnInput.setLong(1, id);
      dltConnInput.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, id);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0022, ex);
    } finally {
      closeStatements(dltConnInput);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MConnection findConnection(long id, Connection conn) {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_CONNECTION_SINGLE);
      stmt.setLong(1, id);

      List<MConnection> connections = loadConnections(stmt, conn);

      if(connections.size() != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0024, "Couldn't find"
          + " connection with id " + id);
      }

      // Return the first and only one connection object
      return connections.get(0);

    } catch (SQLException ex) {
      logException(ex, id);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0023, ex);
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MConnection> findConnections(Connection conn) {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_CONNECTION_ALL);

      return loadConnections(stmt, conn);

    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0023, ex);
    } finally {
      closeStatements(stmt);
    }
  }


  /**
   *
   * {@inheritDoc}
   *
   */
  @Override
  public List<MConnection> findConnectionsForConnector(long connectorID, Connection conn) {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_CONNECTION_FOR_CONNECTOR);
      stmt.setLong(1, connectorID);

      return loadConnections(stmt, conn);

    } catch (SQLException ex) {
      logException(ex, connectorID);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0023, ex);
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateConnector(MConnector mConnector, Connection conn) {
    PreparedStatement updateConnectorStatement = null;
    PreparedStatement deleteForm = null;
    PreparedStatement deleteInput = null;
    try {
      updateConnectorStatement = conn.prepareStatement(STMT_UPDATE_CONNECTOR);
      deleteInput = conn.prepareStatement(STMT_DELETE_INPUTS_FOR_CONNECTOR);
      deleteForm = conn.prepareStatement(STMT_DELETE_FORMS_FOR_CONNECTOR);
      updateConnectorStatement.setString(1, mConnector.getUniqueName());
      updateConnectorStatement.setString(2, mConnector.getClassName());
      updateConnectorStatement.setString(3, mConnector.getVersion());
      updateConnectorStatement.setLong(4, mConnector.getPersistenceId());

      if (updateConnectorStatement.executeUpdate() != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0038);
      }
      deleteInput.setLong(1, mConnector.getPersistenceId());
      deleteForm.setLong(1, mConnector.getPersistenceId());
      deleteInput.executeUpdate();
      deleteForm.executeUpdate();

    } catch (SQLException e) {
      logException(e, mConnector);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0038, e);
    } finally {
      closeStatements(updateConnectorStatement, deleteForm, deleteInput);
    }
    insertFormsForConnector(mConnector, conn);

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateFramework(MFramework mFramework, Connection conn) {
    PreparedStatement deleteForm = null;
    PreparedStatement deleteInput = null;
    try {
      deleteInput = conn.prepareStatement(STMT_DELETE_FRAMEWORK_INPUTS);
      deleteForm = conn.prepareStatement(STMT_DELETE_FRAMEWORK_FORMS);

      deleteInput.executeUpdate();
      deleteForm.executeUpdate();

    } catch (SQLException e) {
      logException(e, mFramework);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0044, e);
    } finally {
      closeStatements(deleteForm, deleteInput);
    }
    createOrUpdateFrameworkVersion(conn, mFramework);
    insertFormsForFramework(mFramework, conn);

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createJob(MJob job, Connection conn) {
    PreparedStatement stmt = null;
    int result;
    try {
      stmt = conn.prepareStatement(STMT_INSERT_JOB,
        Statement.RETURN_GENERATED_KEYS);
      stmt.setString(1, job.getName());
      stmt.setLong(2, job.getConnectionId());
      stmt.setString(3, job.getType().name());
      stmt.setBoolean(4, job.getEnabled());
      stmt.setString(5, job.getCreationUser());
      stmt.setTimestamp(6, new Timestamp(job.getCreationDate().getTime()));
      stmt.setString(7, job.getLastUpdateUser());
      stmt.setTimestamp(8, new Timestamp(job.getLastUpdateDate().getTime()));

      result = stmt.executeUpdate();
      if (result != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0012,
          Integer.toString(result));
      }

      ResultSet rsetJobId = stmt.getGeneratedKeys();

      if (!rsetJobId.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0013);
      }

      long jobId = rsetJobId.getLong(1);

      createInputValues(STMT_INSERT_JOB_INPUT,
                        jobId,
                        job.getConnectorPart().getForms(),
                        conn);
      createInputValues(STMT_INSERT_JOB_INPUT,
                        jobId,
                        job.getFrameworkPart().getForms(),
                        conn);

      job.setPersistenceId(jobId);

    } catch (SQLException ex) {
      logException(ex, job);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0026, ex);
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateJob(MJob job, Connection conn) {
    PreparedStatement deleteStmt = null;
    PreparedStatement updateStmt = null;
    try {
      // Firstly remove old values
      deleteStmt = conn.prepareStatement(STMT_DELETE_JOB_INPUT);
      deleteStmt.setLong(1, job.getPersistenceId());
      deleteStmt.executeUpdate();

      // Update job table
      updateStmt = conn.prepareStatement(STMT_UPDATE_JOB);
      updateStmt.setString(1, job.getName());
      updateStmt.setString(2, job.getLastUpdateUser());
      updateStmt.setTimestamp(3, new Timestamp(new Date().getTime()));

      updateStmt.setLong(4, job.getPersistenceId());
      updateStmt.executeUpdate();

      // And reinsert new values
      createInputValues(STMT_INSERT_JOB_INPUT,
                        job.getPersistenceId(),
                        job.getConnectorPart().getForms(),
                        conn);
      createInputValues(STMT_INSERT_JOB_INPUT,
                        job.getPersistenceId(),
                        job.getFrameworkPart().getForms(),
                        conn);

    } catch (SQLException ex) {
      logException(ex, job);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0027, ex);
    } finally {
      closeStatements(deleteStmt, updateStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean existsJob(long id, Connection conn) {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_JOB_CHECK);
      stmt.setLong(1, id);
      rs = stmt.executeQuery();

      // Should be always valid in query with count
      rs.next();

      return rs.getLong(1) == 1;
    } catch (SQLException ex) {
      logException(ex, id);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0029, ex);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  @Override
  public boolean inUseJob(long jobId, Connection conn) {
    MSubmission submission = findSubmissionLastForJob(jobId, conn);

    // We have no submissions and thus job can't be in use
    if(submission == null) {
      return false;
    }

    // We can't remove running job
    if(submission.getStatus().isRunning()) {
      return true;
    }

    return false;
  }

  @Override
  public void enableJob(long jobId, boolean enabled, Connection conn) {
    PreparedStatement enableConn = null;

    try {
      enableConn = conn.prepareStatement(STMT_ENABLE_JOB);
      enableConn.setBoolean(1, enabled);
      enableConn.setLong(2, jobId);
      enableConn.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, jobId);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0043, ex);
    } finally {
      closeStatements(enableConn);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteJob(long id, Connection conn) {
    PreparedStatement dlt = null;
    try {
      deleteJobInputs(id, conn);
      dlt = conn.prepareStatement(STMT_DELETE_JOB);
      dlt.setLong(1, id);
      dlt.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, id);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0028, ex);
    } finally {
      closeStatements(dlt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteJobInputs(long id, Connection conn) {
    PreparedStatement dltInput = null;
    try {
      dltInput = conn.prepareStatement(STMT_DELETE_JOB_INPUT);
      dltInput.setLong(1, id);
      dltInput.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, id);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0028, ex);
    } finally {
      closeStatements(dltInput);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MJob findJob(long id, Connection conn) {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_JOB_SINGLE);
      stmt.setLong(1, id);

      List<MJob> jobs = loadJobs(stmt, conn);

      if(jobs.size() != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0030, "Couldn't find"
          + " job with id " + id);
      }

      // Return the first and only one connection object
      return jobs.get(0);

    } catch (SQLException ex) {
      logException(ex, id);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0031, ex);
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MJob> findJobs(Connection conn) {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_JOB_ALL);

      return loadJobs(stmt, conn);

    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0031, ex);
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MJob> findJobsForConnector(long connectorId, Connection conn) {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_ALL_JOBS_FOR_CONNECTOR);
      stmt.setLong(1, connectorId);
      return loadJobs(stmt, conn);

    } catch (SQLException ex) {
      logException(ex, connectorId);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0031, ex);
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createSubmission(MSubmission submission, Connection conn) {
    PreparedStatement stmt = null;
    int result;
    try {
      stmt = conn.prepareStatement(STMT_INSERT_SUBMISSION,
        Statement.RETURN_GENERATED_KEYS);
      stmt.setLong(1, submission.getJobId());
      stmt.setString(2, submission.getStatus().name());
      stmt.setString(3, submission.getCreationUser());
      stmt.setTimestamp(4, new Timestamp(submission.getCreationDate().getTime()));
      stmt.setString(5, submission.getLastUpdateUser());
      stmt.setTimestamp(6, new Timestamp(submission.getLastUpdateDate().getTime()));
      stmt.setString(7, submission.getExternalId());
      stmt.setString(8, submission.getExternalLink());
      stmt.setString(9, submission.getExceptionInfo());
      stmt.setString(10, submission.getExceptionStackTrace());

      result = stmt.executeUpdate();
      if (result != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0012,
          Integer.toString(result));
      }

      ResultSet rsetSubmissionId = stmt.getGeneratedKeys();

      if (!rsetSubmissionId.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0013);
      }

      long submissionId = rsetSubmissionId.getLong(1);

      if(submission.getCounters() != null) {
        createSubmissionCounters(submissionId, submission.getCounters(), conn);
      }

      // Save created persistence id
      submission.setPersistenceId(submissionId);

    } catch (SQLException ex) {
      logException(ex, submission);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0034, ex);
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean existsSubmission(long submissionId, Connection conn) {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_SUBMISSION_CHECK);
      stmt.setLong(1, submissionId);
      rs = stmt.executeQuery();

      // Should be always valid in query with count
      rs.next();

      return rs.getLong(1) == 1;
    } catch (SQLException ex) {
      logException(ex, submissionId);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0033, ex);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateSubmission(MSubmission submission, Connection conn) {
    PreparedStatement stmt = null;
    PreparedStatement deleteStmt = null;
    try {
      //  Update properties in main table
      stmt = conn.prepareStatement(STMT_UPDATE_SUBMISSION);
      stmt.setString(1, submission.getStatus().name());
      stmt.setString(2, submission.getLastUpdateUser());
      stmt.setTimestamp(3, new Timestamp(submission.getLastUpdateDate().getTime()));
      stmt.setString(4, submission.getExceptionInfo());
      stmt.setString(5, submission.getExceptionStackTrace());

      stmt.setLong(6, submission.getPersistenceId());
      stmt.executeUpdate();

      // Delete previous counters
      deleteStmt = conn.prepareStatement(STMT_DELETE_COUNTER_SUBMISSION);
      deleteStmt.setLong(1, submission.getPersistenceId());
      deleteStmt.executeUpdate();

      // Reinsert new counters if needed
      if(submission.getCounters() != null) {
        createSubmissionCounters(submission.getPersistenceId(), submission.getCounters(), conn);
      }

    } catch (SQLException ex) {
      logException(ex, submission);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0035, ex);
    } finally {
      closeStatements(stmt, deleteStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void purgeSubmissions(Date threshold, Connection conn) {
     PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(STMT_PURGE_SUBMISSIONS);
      stmt.setTimestamp(1, new Timestamp(threshold.getTime()));
      stmt.executeUpdate();

    } catch (SQLException ex) {
      logException(ex, threshold);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0036, ex);
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MSubmission> findSubmissionsUnfinished(Connection conn) {
    List<MSubmission> submissions = new LinkedList<MSubmission>();
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_SUBMISSION_UNFINISHED);

      for(SubmissionStatus status : SubmissionStatus.unfinished()) {
        stmt.setString(1, status.name());
        rs = stmt.executeQuery();

        while(rs.next()) {
          submissions.add(loadSubmission(rs, conn));
        }

        rs.close();
        rs = null;
      }
    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0037, ex);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }

    return submissions;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MSubmission> findSubmissions(Connection conn) {
    List<MSubmission> submissions = new LinkedList<MSubmission>();
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_SUBMISSIONS);
      rs = stmt.executeQuery();

      while(rs.next()) {
        submissions.add(loadSubmission(rs, conn));
      }

      rs.close();
      rs = null;
    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0039, ex);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }

    return submissions;
  }

  @Override
  public List<MSubmission> findSubmissionsForJob(long jobId, Connection conn) {
    List<MSubmission> submissions = new LinkedList<MSubmission>();
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_SUBMISSIONS_FOR_JOB);
      stmt.setLong(1, jobId);
      rs = stmt.executeQuery();

      while(rs.next()) {
        submissions.add(loadSubmission(rs, conn));
      }

      rs.close();
      rs = null;
    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0040, ex);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }

    return submissions;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MSubmission findSubmissionLastForJob(long jobId, Connection conn) {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_SUBMISSIONS_FOR_JOB);
      stmt.setLong(1, jobId);
      stmt.setMaxRows(1);
      rs = stmt.executeQuery();

      if(!rs.next()) {
        return null;
      }

      return loadSubmission(rs, conn);
    } catch (SQLException ex) {
      logException(ex, jobId);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0040, ex);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  /**
   * Stores counters for given submission in repository.
   *
   * @param submissionId Submission id
   * @param counters Counters that should be stored
   * @param conn Connection to derby repository
   * @throws SQLException
   */
  private void createSubmissionCounters(long submissionId, Counters counters, Connection conn) throws SQLException {
    PreparedStatement stmt = null;

    try {
      stmt = conn.prepareStatement(STMT_INSERT_COUNTER_SUBMISSION);

      for(CounterGroup group : counters) {
        long groupId = getCounterGroupId(group, conn);

        for(Counter counter: group) {
          long counterId = getCounterId(counter, conn);

          stmt.setLong(1, groupId);
          stmt.setLong(2, counterId);
          stmt.setLong(3, submissionId);
          stmt.setLong(4, counter.getValue());

          stmt.executeUpdate();
        }
      }
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * Resolves counter group database id.
   *
   * @param group Given group
   * @param conn Connection to metastore
   * @return Id
   * @throws SQLException
   */
  private long getCounterGroupId(CounterGroup group, Connection conn) throws SQLException {
    PreparedStatement select = null;
    PreparedStatement insert = null;
    ResultSet rsSelect = null;
    ResultSet rsInsert = null;

    try {
      select = conn.prepareStatement(STMT_SELECT_COUNTER_GROUP);
      select.setString(1, group.getName());

      rsSelect = select.executeQuery();

      if(rsSelect.next()) {
        return rsSelect.getLong(1);
      }

      insert = conn.prepareStatement(STMT_INSERT_COUNTER_GROUP, Statement.RETURN_GENERATED_KEYS);
      insert.setString(1, group.getName());
      insert.executeUpdate();

      rsInsert = insert.getGeneratedKeys();

      if (!rsInsert.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0013);
      }

      return rsInsert.getLong(1);
    } finally {
      closeResultSets(rsSelect, rsInsert);
      closeStatements(select, insert);
    }
  }

  /**
   * Resolves counter id.
   *
   * @param counter Given counter
   * @param conn connection to metastore
   * @return Id
   * @throws SQLException
   */
  private long getCounterId(Counter counter, Connection conn) throws SQLException {
    PreparedStatement select = null;
    PreparedStatement insert = null;
    ResultSet rsSelect = null;
    ResultSet rsInsert = null;

    try {
      select = conn.prepareStatement(STMT_SELECT_COUNTER);
      select.setString(1, counter.getName());

      rsSelect = select.executeQuery();

      if(rsSelect.next()) {
        return rsSelect.getLong(1);
      }

      insert = conn.prepareStatement(STMT_INSERT_COUNTER, Statement.RETURN_GENERATED_KEYS);
      insert.setString(1, counter.getName());
      insert.executeUpdate();

      rsInsert = insert.getGeneratedKeys();

      if (!rsInsert.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0013);
      }

      return rsInsert.getLong(1);
    } finally {
      closeResultSets(rsSelect, rsInsert);
      closeStatements(select, insert);
    }
  }

  /**
   * Create MSubmission structure from result set.
   *
   * @param rs Result set, only active row will be fetched
   * @param conn Connection to metastore
   * @return Created MSubmission structure
   * @throws SQLException
   */
  private MSubmission loadSubmission(ResultSet rs, Connection conn) throws SQLException {
    MSubmission submission = new MSubmission();

    submission.setPersistenceId(rs.getLong(1));
    submission.setJobId(rs.getLong(2));
    submission.setStatus(SubmissionStatus.valueOf(rs.getString(3)));
    submission.setCreationUser(rs.getString(4));
    submission.setCreationDate(rs.getTimestamp(5));
    submission.setLastUpdateUser(rs.getString(6));
    submission.setLastUpdateDate(rs.getTimestamp(7));
    submission.setExternalId(rs.getString(8));
    submission.setExternalLink(rs.getString(9));
    submission.setExceptionInfo(rs.getString(10));
    submission.setExceptionStackTrace(rs.getString(11));

    Counters counters = loadCountersSubmission(rs.getLong(1), conn);
    submission.setCounters(counters);

    return submission;
  }

  private Counters loadCountersSubmission(long submissionId, Connection conn) throws SQLException {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_COUNTER_SUBMISSION);
      stmt.setLong(1, submissionId);
      rs = stmt.executeQuery();

      Counters counters = new Counters();

      while(rs.next()) {
        String groupName = rs.getString(1);
        String counterName = rs.getString(2);
        long value = rs.getLong(3);

        CounterGroup group = counters.getCounterGroup(groupName);
        if(group == null) {
          group = new CounterGroup(groupName);
          counters.addCounterGroup(group);
        }

        group.addCounter(new Counter(counterName, value));
      }

      if(counters.isEmpty()) {
        return null;
      } else {
        return counters;
      }
    } finally {
      closeStatements(stmt);
      closeResultSets(rs);
    }
  }

  private List<MConnection> loadConnections(PreparedStatement stmt,
                                            Connection conn)
                                            throws SQLException {
    List<MConnection> connections = new ArrayList<MConnection>();
    ResultSet rsConnection = null;
    PreparedStatement formConnectorFetchStmt = null;
    PreparedStatement formFrameworkFetchStmt = null;
    PreparedStatement inputFetchStmt = null;

    try {
      rsConnection = stmt.executeQuery();

      formConnectorFetchStmt = conn.prepareStatement(STMT_FETCH_FORM_CONNECTOR);
      formFrameworkFetchStmt = conn.prepareStatement(STMT_FETCH_FORM_FRAMEWORK);
      inputFetchStmt = conn.prepareStatement(STMT_FETCH_CONNECTION_INPUT);

      while(rsConnection.next()) {
        long id = rsConnection.getLong(1);
        String name = rsConnection.getString(2);
        long connectorId = rsConnection.getLong(3);
        boolean enabled = rsConnection.getBoolean(4);
        String creationUser = rsConnection.getString(5);
        Date creationDate = rsConnection.getTimestamp(6);
        String updateUser = rsConnection.getString(7);
        Date lastUpdateDate = rsConnection.getTimestamp(8);

        formConnectorFetchStmt.setLong(1, connectorId);

        inputFetchStmt.setLong(1, id);
        //inputFetchStmt.setLong(2, XXX); // Will be filled by loadForms
        inputFetchStmt.setLong(3, id);

        List<MForm> connectorConnForms = new ArrayList<MForm>();
        List<MForm> frameworkConnForms = new ArrayList<MForm>();

        Map<MJob.Type, List<MForm>> connectorJobForms
          = new HashMap<MJob.Type, List<MForm>>();
        Map<MJob.Type, List<MForm>> frameworkJobForms
          = new HashMap<MJob.Type, List<MForm>>();

        loadForms(connectorConnForms, connectorJobForms,
          formConnectorFetchStmt, inputFetchStmt, 2);
        loadForms(frameworkConnForms, frameworkJobForms,
          formFrameworkFetchStmt, inputFetchStmt, 2);

        MConnection connection = new MConnection(connectorId,
          new MConnectionForms(connectorConnForms),
          new MConnectionForms(frameworkConnForms));

        connection.setPersistenceId(id);
        connection.setName(name);
        connection.setCreationUser(creationUser);
        connection.setCreationDate(creationDate);
        connection.setLastUpdateUser(updateUser);
        connection.setLastUpdateDate(lastUpdateDate);
        connection.setEnabled(enabled);

        connections.add(connection);
      }
    } finally {
      closeResultSets(rsConnection);
      closeStatements(formConnectorFetchStmt,
        formFrameworkFetchStmt, inputFetchStmt);
    }

    return connections;
  }

  private List<MJob> loadJobs(PreparedStatement stmt,
                              Connection conn)
                              throws SQLException {
    List<MJob> jobs = new ArrayList<MJob>();
    ResultSet rsJob = null;
    PreparedStatement formConnectorFetchStmt = null;
    PreparedStatement formFrameworkFetchStmt = null;
    PreparedStatement inputFetchStmt = null;

    try {
      rsJob = stmt.executeQuery();

      formConnectorFetchStmt = conn.prepareStatement(STMT_FETCH_FORM_CONNECTOR);
      formFrameworkFetchStmt = conn.prepareStatement(STMT_FETCH_FORM_FRAMEWORK);
      inputFetchStmt = conn.prepareStatement(STMT_FETCH_JOB_INPUT);

      while(rsJob.next()) {
        long connectorId = rsJob.getLong(1);
        long id = rsJob.getLong(2);
        String name = rsJob.getString(3);
        long connectionId = rsJob.getLong(4);
        String stringType = rsJob.getString(5);
        boolean enabled = rsJob.getBoolean(6);
        String createBy = rsJob.getString(7);
        Date creationDate = rsJob.getTimestamp(8);
        String updateBy = rsJob.getString(9);
        Date lastUpdateDate = rsJob.getTimestamp(10);

        MJob.Type type = MJob.Type.valueOf(stringType);

        formConnectorFetchStmt.setLong(1, connectorId);

        inputFetchStmt.setLong(1, id);
        //inputFetchStmt.setLong(1, XXX); // Will be filled by loadForms
        inputFetchStmt.setLong(3, id);

        List<MForm> connectorConnForms = new ArrayList<MForm>();
        List<MForm> frameworkConnForms = new ArrayList<MForm>();

        Map<MJob.Type, List<MForm>> connectorJobForms
          = new HashMap<MJob.Type, List<MForm>>();
        Map<MJob.Type, List<MForm>> frameworkJobForms
          = new HashMap<MJob.Type, List<MForm>>();

        loadForms(connectorConnForms, connectorJobForms,
          formConnectorFetchStmt, inputFetchStmt, 2);
        loadForms(frameworkConnForms, frameworkJobForms,
          formFrameworkFetchStmt, inputFetchStmt, 2);

        MJob job = new MJob(connectorId, connectionId, type,
          new MJobForms(type, connectorJobForms.get(type)),
          new MJobForms(type, frameworkJobForms.get(type)));

        job.setPersistenceId(id);
        job.setName(name);
        job.setCreationUser(createBy);
        job.setCreationDate(creationDate);
        job.setLastUpdateUser(updateBy);
        job.setLastUpdateDate(lastUpdateDate);
        job.setEnabled(enabled);

        jobs.add(job);
      }
    } finally {
      closeResultSets(rsJob);
      closeStatements(formConnectorFetchStmt,
        formFrameworkFetchStmt, inputFetchStmt);
    }

    return jobs;
  }

  /**
   * Register forms in derby database. This method will insert the ids
   * generated by the repository into the forms passed in itself.
   *
   * Use given prepared statements to create entire form structure in database.
   *
   * @param connectorId
   * @param forms
   * @param type
   * @param baseFormStmt
   * @param baseInputStmt
   * @throws SQLException
   */
  private void registerForms(Long connectorId, MJob.Type jobType,
      List<MForm> forms, String type, PreparedStatement baseFormStmt,
      PreparedStatement baseInputStmt)
          throws SQLException {
    short formIndex = 0;
    for (MForm form : forms) {
      if(connectorId == null) {
        baseFormStmt.setNull(1, Types.BIGINT);
      } else {
        baseFormStmt.setLong(1, connectorId);
      }
      if(jobType == null) {
        baseFormStmt.setNull(2, Types.VARCHAR);
      } else {
        baseFormStmt.setString(2, jobType.name());
      }
      baseFormStmt.setString(3, form.getName());
      baseFormStmt.setString(4, type);
      baseFormStmt.setShort(5, formIndex++);

      int baseFormCount = baseFormStmt.executeUpdate();
      if (baseFormCount != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0015,
          Integer.toString(baseFormCount));
      }
      ResultSet rsetFormId = baseFormStmt.getGeneratedKeys();
      if (!rsetFormId.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0016);
      }

      long formId = rsetFormId.getLong(1);
      form.setPersistenceId(formId);

      // Insert all the inputs
      List<MInput<?>> inputs = form.getInputs();
      registerFormInputs(formId, inputs, baseInputStmt);
    }
  }

  /**
   * Save given inputs to the database.
   *
   * Use given prepare statement to save all inputs into repository.
   *
   * @param formId Identifier for corresponding form
   * @param inputs List of inputs that needs to be saved
   * @param baseInputStmt Statement that we can utilize
   * @throws SQLException In case of any failure on Derby side
   */
  private void registerFormInputs(long formId, List<MInput<?>> inputs,
      PreparedStatement baseInputStmt) throws SQLException {
    short inputIndex = 0;
    for (MInput<?> input : inputs) {
      baseInputStmt.setString(1, input.getName());
      baseInputStmt.setLong(2, formId);
      baseInputStmt.setShort(3, inputIndex++);
      baseInputStmt.setString(4, input.getType().name());
      baseInputStmt.setBoolean(5, input.isSensitive());
      // String specific column(s)
      if (input.getType().equals(MInputType.STRING)) {
        MStringInput	strInput = (MStringInput) input;
        baseInputStmt.setShort(6, strInput.getMaxLength());
      } else {
        baseInputStmt.setNull(6, Types.INTEGER);
      }
      // Enum specific column(s)
      if(input.getType() == MInputType.ENUM) {
        baseInputStmt.setString(7, StringUtils.join(((MEnumInput)input).getValues(), ","));
      } else {
        baseInputStmt.setNull(7, Types.VARCHAR);
      }

      int baseInputCount = baseInputStmt.executeUpdate();
      if (baseInputCount != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0017,
          Integer.toString(baseInputCount));
      }

      ResultSet rsetInputId = baseInputStmt.getGeneratedKeys();
      if (!rsetInputId.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0018);
      }

      long inputId = rsetInputId.getLong(1);
      input.setPersistenceId(inputId);
    }
  }

  /**
   * Execute given query on database.
   *
   * @param query Query that should be executed
   */
  private void runQuery(String query, Connection conn) {
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      if (stmt.execute(query)) {
        ResultSet rset = stmt.getResultSet();
        int count = 0;
        while (rset.next()) {
          count++;
        }
        LOG.info("QUERY(" + query + ") produced unused resultset with "+ count + " rows");
      } else {
        int updateCount = stmt.getUpdateCount();
        LOG.info("QUERY(" + query + ") Update count: " + updateCount);
      }
    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0003, query, ex);
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * Load forms and corresponding inputs from Derby database.
   *
   * Use given prepared statements to load all forms and corresponding inputs
   * from Derby.
   *
   * @param connectionForms List of connection forms that will be filled up
   * @param jobForms Map with job forms that will be filled up
   * @param formFetchStmt Prepared statement for fetching forms
   * @param inputFetchStmt Prepare statement for fetching inputs
   * @throws SQLException In case of any failure on Derby side
   */
  public void loadForms(List<MForm> connectionForms,
                        Map<MJob.Type, List<MForm>> jobForms,
                        PreparedStatement formFetchStmt,
                        PreparedStatement inputFetchStmt,
                        int formPosition) throws SQLException {

    // Get list of structures from database
    ResultSet rsetForm = formFetchStmt.executeQuery();
    while (rsetForm.next()) {
      long formId = rsetForm.getLong(1);
      Long formConnectorId = rsetForm.getLong(2);
      String operation = rsetForm.getString(3);
      String formName = rsetForm.getString(4);
      String formType = rsetForm.getString(5);
      int formIndex = rsetForm.getInt(6);
      List<MInput<?>> formInputs = new ArrayList<MInput<?>>();

      MForm mf = new MForm(formName, formInputs);
      mf.setPersistenceId(formId);

      inputFetchStmt.setLong(formPosition, formId);

      ResultSet rsetInput = inputFetchStmt.executeQuery();
      while (rsetInput.next()) {
        long inputId = rsetInput.getLong(1);
        String inputName = rsetInput.getString(2);
        long inputForm = rsetInput.getLong(3);
        short inputIndex = rsetInput.getShort(4);
        String inputType = rsetInput.getString(5);
        boolean inputSensitivity = rsetInput.getBoolean(6);
        short inputStrLength = rsetInput.getShort(7);
        String inputEnumValues = rsetInput.getString(8);
        String value = rsetInput.getString(9);

        MInputType mit = MInputType.valueOf(inputType);

        MInput input = null;
        switch (mit) {
        case STRING:
          input = new MStringInput(inputName, inputSensitivity, inputStrLength);
          break;
        case MAP:
          input = new MMapInput(inputName, inputSensitivity);
          break;
        case BOOLEAN:
          input = new MBooleanInput(inputName, inputSensitivity);
          break;
        case INTEGER:
          input = new MIntegerInput(inputName, inputSensitivity);
          break;
        case ENUM:
          input = new MEnumInput(inputName, inputSensitivity, inputEnumValues.split(","));
          break;
        default:
          throw new SqoopException(DerbyRepoError.DERBYREPO_0006,
              "input-" + inputName + ":" + inputId + ":"
              + "form-" + inputForm + ":" + mit.name());
        }

        // Set persistent ID
        input.setPersistenceId(inputId);

        // Set value
        if(value == null) {
          input.setEmpty();
        } else {
          input.restoreFromUrlSafeValueString(value);
        }

        if (mf.getInputs().size() != inputIndex) {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0009,
            "form: " + mf
            + "; input: " + input
            + "; index: " + inputIndex
            + "; expected: " + mf.getInputs().size()
          );
        }

        mf.getInputs().add(input);
      }

      if (mf.getInputs().size() == 0) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0008,
          "connector-" + formConnectorId
          + "; form: " + mf
        );
      }

      MFormType mft = MFormType.valueOf(formType);
      switch (mft) {
      case CONNECTION:
        if (connectionForms.size() != formIndex) {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0010,
            "connector-" + formConnectorId
            + "; form: " + mf
            + "; index: " + formIndex
            + "; expected: " + connectionForms.size()
          );
        }
        connectionForms.add(mf);
        break;
      case JOB:
        MJob.Type jobType = MJob.Type.valueOf(operation);
        if (!jobForms.containsKey(jobType)) {
          jobForms.put(jobType, new ArrayList<MForm>());
        }

        if (jobForms.get(jobType).size() != formIndex) {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0010,
            "connector-" + formConnectorId
            + "; form: " + mf
            + "; index: " + formIndex
            + "; expected: " + jobForms.get(jobType).size()
          );
        }
        jobForms.get(jobType).add(mf);
        break;
      default:
        throw new SqoopException(DerbyRepoError.DERBYREPO_0007,
            "connector-" + formConnectorId + ":" + mf);
      }
    }
  }

  public List<MJobForms> convertToJobList(Map<MJob.Type, List<MForm>> l) {
    List<MJobForms> ret = new ArrayList<MJobForms>();

    for (Map.Entry<MJob.Type, List<MForm>> entry : l.entrySet()) {
      MJob.Type type = entry.getKey();
      List<MForm> forms = entry.getValue();

      ret.add(new MJobForms(type, forms));
    }

    return ret;
  }

  private void createInputValues(String query,
                                 long id,
                                 List<MForm> forms,
                                 Connection conn) throws SQLException {
    PreparedStatement stmt = null;
    int result;

    try {
      stmt = conn.prepareStatement(query);

      for (MForm form : forms) {
        for (MInput input : form.getInputs()) {
          // Skip empty values as we're not interested in storing those in db
          if (input.isEmpty()) {
            continue;
          }
          stmt.setLong(1, id);
          stmt.setLong(2, input.getPersistenceId());
          stmt.setString(3, input.getUrlSafeValueString());

          result = stmt.executeUpdate();
          if (result != 1) {
            throw new SqoopException(DerbyRepoError.DERBYREPO_0020,
              Integer.toString(result));
          }
        }
      }
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * Close all given Results set.
   *
   * Any occurring exception is silently ignored and logged.
   *
   * @param resultSets Result sets to close
   */
  private void closeResultSets(ResultSet ... resultSets) {
    if(resultSets == null) {
      return;
    }
    for (ResultSet rs : resultSets) {
      if(rs != null) {
        try {
          rs.close();
        } catch(SQLException ex) {
          LOG.error("Exception during closing result set", ex);
        }
      }
    }
  }

  /**
   * Close all given statements.
   *
   * Any occurring exception is silently ignored and logged.
   *
   * @param stmts Statements to close
   */
  private void closeStatements(Statement... stmts) {
    if(stmts == null) {
      return;
    }
    for (Statement stmt : stmts) {
      if(stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          LOG.error("Exception during closing statement", ex);
        }
      }
    }
  }

  /**
   * Log exception and all String variant of arbitrary number of objects.
   *
   * This method is useful to log SQLException with all objects that were
   * used in the query generation to see where is the issue.
   *
   * @param throwable Arbitrary throwable object
   * @param objects Arbitrary array of associated objects
   */
  private void logException(Throwable throwable, Object ...objects) {
    LOG.error("Exception in repository operation", throwable);
    LOG.error("Associated objects: "+ objects.length);
    for(Object object : objects) {
      LOG.error("\t" + object.getClass().getSimpleName() + ": " + object.toString());
    }
  }
}
