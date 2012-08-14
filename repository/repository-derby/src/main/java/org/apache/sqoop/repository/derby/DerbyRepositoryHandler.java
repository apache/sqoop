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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFormType;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.repository.JdbcRepositoryContext;
import org.apache.sqoop.repository.JdbcRepositoryHandler;
import org.apache.sqoop.repository.JdbcRepositoryTransactionFactory;

/**
 * JDBC based repository handler for Derby database.
 *
 * Repository implementation for Derby database.
 */
public class DerbyRepositoryHandler implements JdbcRepositoryHandler {

  private static final Logger LOG =
      Logger.getLogger(DerbyRepositoryHandler.class);

  private static final String SCHEMA_SQOOP = "SQOOP";

  private static final String QUERY_SYSSCHEMA_SQOOP =
      "SELECT SCHEMAID FROM SYS.SYSSCHEMAS WHERE SCHEMANAME = '"
          + SCHEMA_SQOOP + "'";

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

      int baseConnectorCount = baseConnectorStmt.executeUpdate();
      if (baseConnectorCount != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0012,
            new Integer(baseConnectorCount).toString());
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
      registerForms(connectorId, null, mc.getConnection().getForms(),
        MFormType.CONNECTION.name(), baseFormStmt, baseInputStmt);

      // Register all jobs
      for (MJob job : mc.getJobs().values()) {
        registerForms(connectorId, job.getType(), job.getForms(),
          MFormType.JOB.name(), baseFormStmt, baseInputStmt);
      }

    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0014,
          mc.toString(), ex);
    } finally {
      if (baseConnectorStmt != null) {
        try {
          baseConnectorStmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close base connector statement", ex);
        }
      }
      if (baseFormStmt != null) {
        try {
          baseFormStmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close base form statement", ex);
        }
      }
      if (baseInputStmt != null) {
        try {
          baseInputStmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close base input statement", ex);
        }
      }
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
        int index = connectUrl.indexOf(";");
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
        LOG.warn("Even though embedded Derby drvier was loaded, the connect "
            + "URL is of an unexpected form: " + connectUrl + ". Therfore no "
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
      ResultSet  rset = stmt.executeQuery(QUERY_SYSSCHEMA_SQOOP);

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
      if (stmt != null) {
        try {
          stmt.close();
        } catch(SQLException ex) {
          LOG.error("Unable to  close schema lookup stmt", ex);
        }
      }
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

      formFetchStmt = conn.prepareStatement(STMT_FETCH_FORM_CONNECTOR);
      formFetchStmt.setLong(1, connectorId);
      inputFetchStmt = conn.prepareStatement(STMT_FETCH_INPUT);

      List<MForm> connectionForms = new ArrayList<MForm>();
      Map<MJob.Type, List<MForm>> jobForms =
        new HashMap<MJob.Type, List<MForm>>();

      loadForms(shortName, connectionForms, jobForms,
        formFetchStmt, inputFetchStmt);

      mc = new MConnector(connectorName, connectorClassName,
        new MConnection(connectionForms),
        convertToJobList(jobForms));
      mc.setPersistenceId(connectorId);

      if (rsetBaseConnector.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0005, shortName);
      }
    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0004, shortName, ex);
    } finally {
      if (baseConnectorFetchStmt != null) {
        try {
          baseConnectorFetchStmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close base connector fetch statement", ex);
        }
      }
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
      registerForms(null, null, mf.getConnection().getForms(),
        MFormType.CONNECTION.name(), baseFormStmt, baseInputStmt);

      // Register all jobs
      for (MJob job : mf.getJobs().values()) {
        registerForms(null, job.getType(), job.getForms(),
          MFormType.JOB.name(), baseFormStmt, baseInputStmt);
      }

      // We're using hardcoded value for framework metadata as they are
      // represented as NULL in the database.
      mf.setPersistenceId(1);
    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0014,
          mf.toString(), ex);
    } finally {
      if (baseFormStmt != null) {
        try {
          baseFormStmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close base form statement", ex);
        }
      }
      if (baseInputStmt != null) {
        try {
          baseInputStmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close base input statement", ex);
        }
      }
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

      loadForms("Framework metadata", connectionForms, jobForms,
        formFetchStmt, inputFetchStmt);

      // Return nothing If there aren't any framework metadata
      if(connectionForms.isEmpty() && jobForms.isEmpty()) {
        return null;
      }

      mf = new MFramework(new MConnection(connectionForms),
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
            new Integer(baseFormCount).toString());
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
      if (input.getType().equals(MInputType.STRING)) {
        MStringInput	strInput = (MStringInput) input;
        baseInputStmt.setBoolean(5, strInput.isMasked());
        baseInputStmt.setShort(6, strInput.getMaxLength());
      }
      int baseInputCount = baseInputStmt.executeUpdate();
      if (baseInputCount != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0017,
            new Integer(baseInputCount).toString());
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
   * @param connectorName Connector name for purpose of printing errors
   * @param connectionForms List of connection forms that will be filled up
   * @param jobForms Map with job forms that will be filled up
   * @param formFetchStmt Prepared statement for fetching forms
   * @param inputFetchStmt Prepare statement for fetching inputs
   * @throws SQLException In case of any failure on Derby side
   */
  public void loadForms(String connectorName,
                        List<MForm> connectionForms,
                        Map<MJob.Type, List<MForm>> jobForms,
                        PreparedStatement formFetchStmt,
                        PreparedStatement inputFetchStmt) throws SQLException {

    // Get list of structures from database
    ResultSet rsetForm = formFetchStmt.executeQuery();
    while (rsetForm.next()) {
      long formId = rsetForm.getLong(1);
      long formConnectorId = rsetForm.getLong(2);
      String operation = rsetForm.getString(3);
      String formName = rsetForm.getString(4);
      String formType = rsetForm.getString(5);
      int formIndex = rsetForm.getInt(6);
      List<MInput<?>> formInputs = new ArrayList<MInput<?>>();

      MForm mf = new MForm(formName, formInputs);
      mf.setPersistenceId(formId);

      inputFetchStmt.setLong(1, formId);

      ResultSet rsetInput = inputFetchStmt.executeQuery();
      while (rsetInput.next()) {
        long inputId = rsetInput.getLong(1);
        String inputName = rsetInput.getString(2);
        long inputForm = rsetInput.getLong(3);
        short inputIndex = rsetInput.getShort(4);
        String inputType = rsetInput.getString(5);
        boolean inputStrMask = rsetInput.getBoolean(6);
        short inputStrLength = rsetInput.getShort(7);

        MInputType mit = MInputType.valueOf(inputType);

        MInput input = null;
        switch (mit) {
        case STRING:
          input = new MStringInput(inputName, inputStrMask, inputStrLength);
          break;
        case MAP:
          input = new MMapInput(inputName);
          break;
        default:
          throw new SqoopException(DerbyRepoError.DERBYREPO_0006,
              "input-" + inputName + ":" + inputId + ":"
              + "form-" + inputForm + ":" + mit.name());
        }
        input.setPersistenceId(inputId);

        if (mf.getInputs().size() != inputIndex) {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0009,
              "form: " + mf + "; input: " + input);
        }

        mf.getInputs().add(input);
      }

      if (mf.getInputs().size() == 0) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0008,
            "connector-" + formConnectorId + ":" + mf);
      }

      MFormType mft = MFormType.valueOf(formType);
      switch (mft) {
      case CONNECTION:
        if (connectionForms.size() != formIndex) {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0010,
              "connector: " + connectorName + "; form: " + mf);
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
              "connector: " + connectorName + "; form: " + mf);
        }
        jobForms.get(jobType).add(mf);
        break;
      default:
        throw new SqoopException(DerbyRepoError.DERBYREPO_0007,
            "connector-" + formConnectorId + ":" + mf);
      }
    }
  }

  public List<MJob> convertToJobList(Map<MJob.Type, List<MForm>> l) {
    List<MJob> ret = new ArrayList<MJob>();

    for (Map.Entry<MJob.Type, List<MForm>> entry : l.entrySet()) {
      MJob.Type type = entry.getKey();
      List<MForm> forms = entry.getValue();

      ret.add(new MJob(type, forms));
    }

    return ret;
  }
}
