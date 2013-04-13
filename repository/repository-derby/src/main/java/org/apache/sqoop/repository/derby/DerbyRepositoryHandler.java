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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
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
import org.apache.sqoop.utils.StringUtils;

/**
 * JDBC based repository handler for Derby database.
 *
 * Repository implementation for Derby database.
 */
public class DerbyRepositoryHandler implements JdbcRepositoryHandler {

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

    PreparedStatement baseConnectorStmt = null;
    PreparedStatement baseFormStmt = null;
    PreparedStatement baseInputStmt = null;
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

      long connectorId = rsetConnectorId.getLong(1);
      mc.setPersistenceId(connectorId);

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
      closeStatements(baseConnectorStmt, baseFormStmt, baseInputStmt);
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
   * {@inheritDoc}
   */
  @Override
  public void createSchema() {
    runQuery(QUERY_CREATE_SCHEMA_SQOOP);
    runQuery(QUERY_CREATE_TABLE_SQ_CONNECTOR);
    runQuery(QUERY_CREATE_TABLE_SQ_FORM);
    runQuery(QUERY_CREATE_TABLE_SQ_INPUT);
    runQuery(QUERY_CREATE_TABLE_SQ_CONNECTION);
    runQuery(QUERY_CREATE_TABLE_SQ_JOB);
    runQuery(QUERY_CREATE_TABLE_SQ_CONNECTION_INPUT);
    runQuery(QUERY_CREATE_TABLE_SQ_JOB_INPUT);
    runQuery(QUERY_CREATE_TABLE_SQ_SUBMISSION);
    runQuery(QUERY_CREATE_TABLE_SQ_COUNTER_GROUP);
    runQuery(QUERY_CREATE_TABLE_SQ_COUNTER);
    runQuery(QUERY_CREATE_TABLE_SQ_COUNTER_SUBMISSION);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean schemaExists() {
    Connection connection = null;
    Statement stmt = null;
    try {
      connection = dataSource.getConnection();
      stmt = connection.createStatement();
      ResultSet rset = stmt.executeQuery(QUERY_SYSSCHEMA_SQOOP);

      if (!rset.next()) {
        LOG.warn("Schema for SQOOP does not exist");
        return false;
      }
      String sqoopSchemaId = rset.getString(1);
      LOG.debug("SQOOP schema ID: " + sqoopSchemaId);

      connection.commit();
    } catch (SQLException ex) {
      if (connection != null) {
        try {
          connection.rollback();
        } catch (SQLException ex2) {
          LOG.error("Unable to rollback transaction", ex2);
        }
      }
      throw new SqoopException(DerbyRepoError.DERBYREPO_0001, ex);
    } finally {
      closeStatements(stmt);
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close connection", ex);
        }
      }
    }

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
      throw new SqoopException(DerbyRepoError.DERBYREPO_0014,
          mf.toString(), ex);
    } finally {
      closeStatements(baseFormStmt, baseInputStmt);
    }
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
        convertToJobList(jobForms));

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
      stmt.setTimestamp(3, new Timestamp(connection.getCreationDate().getTime()));
      stmt.setTimestamp(4, new Timestamp(connection.getLastUpdateDate().getTime()));

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
      updateStmt.setTimestamp(2, new Timestamp(new Date().getTime()));

      updateStmt.setLong(3, connection.getPersistenceId());
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
      throw new SqoopException(DerbyRepoError.DERBYREPO_0032, e);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteConnection(long id, Connection conn) {
    PreparedStatement dltConn = null;
    PreparedStatement dltConnInput = null;
    try {
      dltConnInput = conn.prepareStatement(STMT_DELETE_CONNECTION_INPUT);
      dltConn = conn.prepareStatement(STMT_DELETE_CONNECTION);

      dltConnInput.setLong(1, id);
      dltConn.setLong(1, id);

      dltConnInput.executeUpdate();
      dltConn.executeUpdate();

    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0022, ex);
    } finally {
      closeStatements(dltConn, dltConnInput);
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
      throw new SqoopException(DerbyRepoError.DERBYREPO_0023, ex);
    } finally {
      closeStatements(stmt);
    }
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
      stmt.setTimestamp(4, new Timestamp(job.getCreationDate().getTime()));
      stmt.setTimestamp(5, new Timestamp(job.getLastUpdateDate().getTime()));

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
      updateStmt.setTimestamp(2, new Timestamp(new Date().getTime()));

      updateStmt.setLong(3, job.getPersistenceId());
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

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteJob(long id, Connection conn) {
    PreparedStatement dlt = null;
    PreparedStatement dltInput = null;
    try {
      dltInput = conn.prepareStatement(STMT_DELETE_JOB_INPUT);
      dlt = conn.prepareStatement(STMT_DELETE_JOB);

      dltInput.setLong(1, id);
      dlt.setLong(1, id);

      dltInput.executeUpdate();
      dlt.executeUpdate();

    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0028, ex);
    } finally {
      closeStatements(dlt, dltInput);
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
      stmt.setTimestamp(3, new Timestamp(submission.getCreationDate().getTime()));
      stmt.setTimestamp(4, new Timestamp(submission.getLastUpdateDate().getTime()));
      stmt.setString(5, submission.getExternalId());
      stmt.setString(6, submission.getExternalLink());
      stmt.setString(7, submission.getExceptionInfo());
      stmt.setString(8, submission.getExceptionStackTrace());

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
      stmt.setTimestamp(2, new Timestamp(submission.getLastUpdateDate().getTime()));
      stmt.setString(3, submission.getExceptionInfo());
      stmt.setString(4, submission.getExceptionStackTrace());

      stmt.setLong(5, submission.getPersistenceId());
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
  public MSubmission findSubmissionLastForJob(long jobId, Connection conn) {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_SUBMISSION_LAST_FOR_JOB);
      stmt.setLong(1, jobId);
      stmt.setMaxRows(1);
      rs = stmt.executeQuery();

      if(!rs.next()) {
        return null;
      }

      return loadSubmission(rs, conn);
    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0037, ex);
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
    submission.setCreationDate(rs.getTimestamp(4));
    submission.setLastUpdateDate(rs.getTimestamp(5));
    submission.setExternalId(rs.getString(6));
    submission.setExternalLink(rs.getString(7));
    submission.setExceptionInfo(rs.getString(8));
    submission.setExceptionStackTrace(rs.getString(9));

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
        Date creationDate = rsConnection.getTimestamp(4);
        Date lastUpdateDate = rsConnection.getTimestamp(5);

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
        connection.setCreationDate(creationDate);
        connection.setLastUpdateDate(lastUpdateDate);

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
        Date creationDate = rsJob.getTimestamp(6);
        Date lastUpdateDate = rsJob.getTimestamp(7);

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
        job.setCreationDate(creationDate);
        job.setLastUpdateDate(lastUpdateDate);

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
   * Register forms in derby database.
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
   * Passed query will be executed in it's own transaction
   *
   * @param query Query that should be executed
   */
  private void runQuery(String query) {
    Connection connection = null;
    Statement stmt = null;
    try {
      connection = dataSource.getConnection();
      stmt = connection.createStatement();
      if (stmt.execute(query)) {
        ResultSet rset = stmt.getResultSet();
        int count = 0;
        while (rset.next()) {
          count++;
        }
        LOG.info("QUERY(" + query + ") produced unused resultset with "
            + count + " rows");
      } else {
        int updateCount = stmt.getUpdateCount();
        LOG.info("QUERY(" + query + ") Update count: " + updateCount);
      }
      connection.commit();
    } catch (SQLException ex) {
      try {
        connection.rollback();
      } catch (SQLException ex2) {
        LOG.error("Unable to rollback transaction", ex2);
      }
      throw new SqoopException(DerbyRepoError.DERBYREPO_0003,
          query, ex);
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close statement", ex);
        }
        if (connection != null) {
          try {
            connection.close();
          } catch (SQLException ex) {
            LOG.error("Unable to close connection", ex);
          }
        }
      }
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
   */  private void closeStatements(Statement... stmts) {
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
}
