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

import static org.apache.sqoop.repository.derby.DerbySchemaCreateQuery.*;
import static org.apache.sqoop.repository.derby.DerbySchemaInsertUpdateDeleteSelectQuery.*;
import static org.apache.sqoop.repository.derby.DerbySchemaUpgradeQuery.*;
import java.net.URL;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.DirectionError;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.SupportedDirections;
import org.apache.sqoop.connector.ConnectorHandler;
import org.apache.sqoop.connector.ConnectorManagerUtils;
import org.apache.sqoop.driver.Driver;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConfigType;
import org.apache.sqoop.model.MConfigurableType;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.repository.JdbcRepositoryContext;
import org.apache.sqoop.repository.JdbcRepositoryHandler;
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

  /**
   * Unique name of HDFS Connector.
   * HDFS Connector was originally part of the Sqoop driver, but now is its
   * own connector. This constant is used to pre-register the HDFS Connector
   * so that jobs that are being upgraded can reference the HDFS Connector.
   */
  private static final String CONNECTOR_HDFS = "hdfs-connector";

  private JdbcRepositoryContext repoContext;

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerConnector(MConnector mc, Connection conn) {
    if (mc.hasPersistenceId()) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0011,
        mc.getUniqueName());
    }
    mc.setPersistenceId(insertAndGetConnectorId(mc, conn));
    insertConfigsForConnector(mc, conn);
  }

  /**
   * Helper method to insert the configs from the  into the
   * repository.
   * @param mDriver The driver instance to use to upgrade.
   * @param conn JDBC link to use for updating the configs
   */
  private void insertConfigsForDriver(MDriver mDriver, Connection conn) {
    PreparedStatement baseConfigStmt = null;
    PreparedStatement baseInputStmt = null;
    try{
      baseConfigStmt = conn.prepareStatement(STMT_INSERT_INTO_CONFIG,
        Statement.RETURN_GENERATED_KEYS);

      baseInputStmt = conn.prepareStatement(STMT_INSERT_INTO_INPUT,
        Statement.RETURN_GENERATED_KEYS);

      // Register the job config type, since driver config is per job
      registerConfigs(null, null, mDriver.getDriverConfig().getConfigs(),
        MConfigType.JOB.name(), baseConfigStmt, baseInputStmt, conn);

    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0014, mDriver.toString(), ex);
    } finally {
      closeStatements(baseConfigStmt, baseInputStmt);
    }
  }

  /**
   * Helper method to insert the configs from the MConnector into the
   * repository. The job and connector configs within <code>mc</code> will get
   * updated with the id of the configs when this function returns.
   * @param mc The connector to use for updating configs
   * @param conn JDBC connection to use for inserting the configs
   */
  private void insertConfigsForConnector (MConnector mc, Connection conn) {
    long connectorId = mc.getPersistenceId();
    PreparedStatement baseConfigStmt = null;
    PreparedStatement baseInputStmt = null;
    try{
      baseConfigStmt = conn.prepareStatement(STMT_INSERT_INTO_CONFIG,
        Statement.RETURN_GENERATED_KEYS);

      baseInputStmt = conn.prepareStatement(STMT_INSERT_INTO_INPUT,
        Statement.RETURN_GENERATED_KEYS);

      // Register link type config
      registerConfigs(connectorId, null /* No direction for LINK type config*/, mc.getLinkConfig().getConfigs(),
      MConfigType.LINK.name(), baseConfigStmt, baseInputStmt, conn);

      // Register both from/to job type config for connector
      if (mc.getSupportedDirections().isDirectionSupported(Direction.FROM)) {
        registerConfigs(connectorId, Direction.FROM, mc.getFromConfig().getConfigs(),
            MConfigType.JOB.name(), baseConfigStmt, baseInputStmt, conn);
      }
      if (mc.getSupportedDirections().isDirectionSupported(Direction.TO)) {
        registerConfigs(connectorId, Direction.TO, mc.getToConfig().getConfigs(),
            MConfigType.JOB.name(), baseConfigStmt, baseInputStmt, conn);
      }
    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0014,
        mc.toString(), ex);
    } finally {
      closeStatements(baseConfigStmt, baseInputStmt);
    }

  }

  private void insertConnectorDirection(Long connectorId, Direction direction, Connection conn)
      throws SQLException {
    PreparedStatement stmt = null;

    try {
      stmt = conn.prepareStatement(STMT_INSERT_SQ_CONNECTOR_DIRECTIONS);
      stmt.setLong(1, connectorId);
      stmt.setLong(2, getDirection(direction, conn));

      if (stmt.executeUpdate() != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0049);
      }
    } finally {
      closeStatements(stmt);
    }
  }

  private void insertConnectorDirections(Long connectorId, SupportedDirections directions, Connection conn)
      throws SQLException {
    if (directions.isDirectionSupported(Direction.FROM)) {
      insertConnectorDirection(connectorId, Direction.FROM, conn);
    }

    if (directions.isDirectionSupported(Direction.TO)) {
      insertConnectorDirection(connectorId, Direction.TO, conn);
    }
  }

  private long insertAndGetConnectorId(MConnector mc, Connection conn) {
    PreparedStatement baseConnectorStmt = null;
    try {
      baseConnectorStmt = conn.prepareStatement(STMT_INSERT_INTO_CONFIGURABLE,
          Statement.RETURN_GENERATED_KEYS);
      baseConnectorStmt.setString(1, mc.getUniqueName());
      baseConnectorStmt.setString(2, mc.getClassName());
      baseConnectorStmt.setString(3, mc.getVersion());
      baseConnectorStmt.setString(4, mc.getType().name());

      int baseConnectorCount = baseConnectorStmt.executeUpdate();
      if (baseConnectorCount != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0012,
            Integer.toString(baseConnectorCount));
      }

      ResultSet rsetConnectorId = baseConnectorStmt.getGeneratedKeys();

      if (!rsetConnectorId.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0013);
      }
      // connector configurable also have directions
      insertConnectorDirections(rsetConnectorId.getLong(1), mc.getSupportedDirections(), conn);
      return rsetConnectorId.getLong(1);
    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0014, mc.toString(), ex);
    } finally {
      closeStatements(baseConnectorStmt);
    }
  }

  private long insertAndGetDriverId(MDriver mDriver, Connection conn) {
    PreparedStatement baseDriverStmt = null;
    try {
      baseDriverStmt = conn.prepareStatement(STMT_INSERT_INTO_CONFIGURABLE,
          Statement.RETURN_GENERATED_KEYS);
      baseDriverStmt.setString(1, mDriver.getUniqueName());
      baseDriverStmt.setString(2, Driver.getClassName());
      baseDriverStmt.setString(3, mDriver.getVersion());
      baseDriverStmt.setString(4, mDriver.getType().name());

      int baseDriverCount = baseDriverStmt.executeUpdate();
      if (baseDriverCount != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0012, Integer.toString(baseDriverCount));
      }

      ResultSet rsetDriverId = baseDriverStmt.getGeneratedKeys();

      if (!rsetDriverId.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0013);
      }
      return rsetDriverId.getLong(1);
    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0050, mDriver.toString(), ex);
    } finally {
      closeStatements(baseDriverStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void initialize(JdbcRepositoryContext ctx) {
    repoContext = ctx;
    repoContext.getDataSource();
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
            + "URL is of an unexpected config: " + connectUrl + ". Therefore no "
            + "attempt will be made to shutdown embedded Derby instance.");
      }
    }
  }

  /**
   * Detect version of underlying database structures
   *
   * @param conn JDBC Connection
   * @return
   */
  public int detectRepositoryVersion(Connection conn) {
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

      LOG.debug("Detecting existing version of repository");
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
      // NOTE: Since we can different types of version stored in system table, we renamed the
      // key name for the repository version from "version" to "repository.version" for clarity
      stmt = conn.prepareStatement(STMT_SELECT_DEPRECATED_OR_NEW_SYSTEM_VERSION);
      stmt.setString(1, DerbyRepoConstants.SYSKEY_DERBY_REPOSITORY_VERSION);
      stmt.setString(2, DerbyRepoConstants.SYSKEY_VERSION);
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
   * {@inheritDoc}
   */
  @Override
  public void createOrUpgradeRepository(Connection conn) {

    int repositoryVersion = detectRepositoryVersion(conn);
    if(repositoryVersion <= 0) {
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
    if(repositoryVersion <= 1) {
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
    if(repositoryVersion <= 2) {
      runQuery(QUERY_UPGRADE_TABLE_SQ_SUBMISSION_MODIFY_COLUMN_SQS_EXTERNAL_ID_VARCHAR_50, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONNECTOR_MODIFY_COLUMN_SQC_VERSION_VARCHAR_64, conn);
    }
    if(repositoryVersion <= 3) {
      // Schema modifications
      runQuery(QUERY_CREATE_TABLE_SQ_DIRECTION, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_RENAME_COLUMN_SQF_OPERATION_TO_SQF_DIRECTION, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_RENAME_COLUMN_SQB_CONNECTION_TO_SQB_FROM_CONNECTION, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_SQB_TO_CONNECTION, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_REMOVE_CONSTRAINT_SQB_SQN, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_CONSTRAINT_SQB_SQN_FROM, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_CONSTRAINT_SQB_SQN_TO, conn);

      // Data modifications only for non-fresh install.
      if (repositoryVersion > 0) {
        // Register HDFS connector
        updateJobRepositorySchemaAndData(conn, registerHdfsConnector(conn));
      }

      // Wait to remove SQB_TYPE (IMPORT/EXPORT) until we update data.
      // Data updates depend on knowledge of the type of job.
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_REMOVE_COLUMN_SQB_TYPE, conn);

      // SQOOP-1498 rename entities
      renameEntitiesForConnectionAndForm(conn);
      // Change direction from VARCHAR to BIGINT + foreign key.
      updateDirections(conn, insertDirections(conn));
      renameConnectorToConfigurable(conn);

      // Add unique constraints on job and links for version 4 onwards
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_UNIQUE_CONSTRAINT_NAME, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_LINK_ADD_UNIQUE_CONSTRAINT_NAME, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONFIGURABLE_ADD_UNIQUE_CONSTRAINT_NAME, conn);
    }

    // last step upgrade the repository version to the latest value in the code
    upgradeRepositoryVersion(conn);
  }

  // SQOOP-1498 refactoring related upgrades for table and column names
  void renameEntitiesForConnectionAndForm(Connection conn) {
    // LINK
    // drop the constraint before rename
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_1, conn);
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_2, conn);
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_3, conn);
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_4, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_TO_SQ_LINK, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_1, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_2, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_3, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_4, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_5, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_6, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_7, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_8, conn);

    // rename constraints
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONNECTOR_CONSTRAINT, conn);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_LINK_CONNECTOR_CONSTRAINT, conn);

    LOG.info("LINK TABLE altered");

    // LINK_INPUT
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_TO_SQ_LINK_INPUT, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_COLUMN_1, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_COLUMN_2, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_COLUMN_3, conn);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_LINK_INPUT_CONSTRAINT, conn);

    LOG.info("LINK_INPUT TABLE altered");

    // CONFIG
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_FORM_CONSTRAINT, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_TO_SQ_CONFIG, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_1, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_2, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_3, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_4, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_5, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_6, conn);
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_FORM_CONNECTOR_CONSTRAINT, conn);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_CONFIG_CONNECTOR_CONSTRAINT, conn);

    LOG.info("CONFIG TABLE altered");

    // INPUT
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_INPUT_FORM_COLUMN, conn);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_INPUT_CONSTRAINT, conn);

    LOG.info("INPUT TABLE altered and constraints added");

    // JOB

    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_COLUMN_1, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_COLUMN_2, conn);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_JOB_CONSTRAINT_FROM, conn);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_JOB_CONSTRAINT_TO, conn);

    LOG.info("JOB TABLE altered and constraints added");
  }

  private void renameConnectorToConfigurable(Connection conn) {
    // SQ_CONNECTOR to SQ_CONFIGURABLE upgrade
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONFIG_CONNECTOR_CONSTRAINT, conn);
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_LINK_CONSTRAINT, conn);
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTOR_DIRECTION_CONSTRAINT, conn);

    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTOR_TO_SQ_CONFIGURABLE, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONFIG_COLUMN_1, conn);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_LINK_COLUMN_1, conn);
    runQuery(QUERY_UPGRADE_TABLE_SQ_CONFIGURABLE_ADD_COLUMN_SQC_TYPE, conn);

    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_CONFIG_CONFIGURABLE_CONSTRAINT, conn);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_LINK_CONFIGURABLE_CONSTRAINT, conn);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_CONNECTOR_DIRECTION_CONSTRAINT, conn);

    LOG.info("CONNECTOR TABLE altered and constraints added for CONFIGURABLE");
  }

  private void upgradeRepositoryVersion(Connection conn) {
    // remove the deprecated sys version
    runQuery(STMT_DELETE_SYSTEM, conn, DerbyRepoConstants.SYSKEY_VERSION);
    runQuery(STMT_DELETE_SYSTEM, conn, DerbyRepoConstants.SYSKEY_DERBY_REPOSITORY_VERSION);
    runQuery(STMT_INSERT_SYSTEM, conn, DerbyRepoConstants.SYSKEY_DERBY_REPOSITORY_VERSION, ""
        + DerbyRepoConstants.LATEST_DERBY_REPOSITORY_VERSION);
  }
  /**
   * Insert directions: FROM and TO.
   * @param conn
   * @return Map<Direction, Long> direction ID => Direction
   */
  protected Map<Direction, Long> insertDirections(Connection conn) {
    // Add directions
    Map<Direction, Long> directionMap = new TreeMap<Direction, Long>();
    PreparedStatement insertDirectionStmt = null;
    try {
      // Insert directions and get IDs.
      for (Direction direction : Direction.values()) {
        insertDirectionStmt = conn.prepareStatement(STMT_INSERT_DIRECTION, Statement.RETURN_GENERATED_KEYS);
        insertDirectionStmt.setString(1, direction.toString());
        if (insertDirectionStmt.executeUpdate() != 1) {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0046, "Could not add directions FROM and TO.");
        }

        ResultSet directionId = insertDirectionStmt.getGeneratedKeys();
        if (directionId.next()) {
          if (LOG.isInfoEnabled()) {
            LOG.info("Loaded direction: " + directionId.getLong(1));
          }

          directionMap.put(direction, directionId.getLong(1));
        } else {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0047, "Could not get ID of direction " + direction);
        }
      }
    } catch (SQLException e) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0000, e);
    } finally {
      closeStatements(insertDirectionStmt);
    }

    return directionMap;
  }

  /**
   * Add normalized M2M for SQ_CONNECTOR and SQ_CONFIG for Direction.
   * 1. Remember all ID => direction for configs.
   * 2. Drop SQF_DIRECTION (varhchar).
   * 3. Add new M2M tables for SQ_CONNECTOR and SQ_CONFIG.
   * 4. Add directions via updating SQ_CONFIG with proper Direction IDs.
   * 5. Make sure all connectors have all supported directions.
   * @param conn
   */
  protected void updateDirections(Connection conn, Map<Direction, Long> directionMap) {
    // Remember directions
    Statement fetchFormsStmt = null,
              fetchConnectorsStmt = null;
    List<Long> connectorIds = new LinkedList<Long>();
    List<Long> configIds = new LinkedList<Long>();
    List<String> directions = new LinkedList<String>();
    try {
      fetchFormsStmt = conn.createStatement();
      ResultSet rs = fetchFormsStmt.executeQuery(STMT_FETCH_CONFIG_DIRECTIONS);
      while (rs.next()) {
        configIds.add(rs.getLong(1));
        directions.add(rs.getString(2));
      }
      rs.close();
    } catch (SQLException e) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0000, e);
    } finally {
      closeStatements(fetchFormsStmt);
    }

    // Change Schema
    runQuery(QUERY_UPGRADE_TABLE_SQ_CONFIG_DROP_COLUMN_SQ_CFG_DIRECTION_VARCHAR, conn);
    runQuery(QUERY_CREATE_TABLE_SQ_CONNECTOR_DIRECTIONS, conn);
    runQuery(QUERY_CREATE_TABLE_SQ_CONFIG_DIRECTIONS, conn);

    // Add directions back
    while (!configIds.isEmpty() && !directions.isEmpty()) {
      Long configId = configIds.remove(0);
      String directionString = directions.remove(0);
      if (directionString != null && !directionString.isEmpty()) {
        Direction direction = Direction.valueOf(directionString);
        runQuery(STMT_INSERT_SQ_CONFIG_DIRECTIONS, conn, configId, directionMap.get(direction));
      }
    }

    // Add connector directions
    try {
      fetchConnectorsStmt = conn.createStatement();
      ResultSet rs = fetchConnectorsStmt.executeQuery(STMT_SELECT_CONNECTOR_ALL);
      while (rs.next()) {
        connectorIds.add(rs.getLong(1));
      }
      rs.close();
    } catch (SQLException e) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0000, e);
    } finally {
      closeStatements(fetchConnectorsStmt);
    }

    for (Long connectorId : connectorIds) {
      for (Long directionId : directionMap.values()) {
        runQuery(STMT_INSERT_SQ_CONNECTOR_DIRECTIONS, conn, connectorId, directionId);
      }
    }
  }


  /**
   * Upgrade job data from IMPORT/EXPORT to FROM/TO.
   * Since the framework is no longer responsible for HDFS,
   * the HDFS connector/link must be added.
   * Also, the framework configs are moved around such that
   * they belong to the added HDFS connector. Any extra configs
   * are removed.
   * NOTE: Connector configs should have a direction (FROM/TO),
   * but framework configs should not.
   *
   * Here's a brief list describing the data migration process.
   * 1. Change SQ_CONFIG.SQ_CFG_DIRECTION from IMPORT to FROM.
   * 2. Change SQ_CONFIG.SQ_CFG_DIRECTION from EXPORT to TO.
   * 3. Change EXPORT to TO in newly existing SQ_CFG_DIRECTION.
   *    This should affect connectors only since Connector configs
   *    should have had a value for SQ_CFG_OPERATION.
   * 4. Change IMPORT to FROM in newly existing SQ_CFG_DIRECTION.
   *    This should affect connectors only since Connector configs
   *    should have had a value for SQ_CFG_OPERATION.
   * 5. Add HDFS connector for jobs to reference.
   * 6. Set 'input' and 'output' configs connector.
   *    to HDFS connector.
   * 7. Throttling config was originally the second config in
   *    the framework. It should now be the first config.
   * 8. Remove the EXPORT throttling config and ensure all of
   *    its dependencies point to the IMPORT throttling config.
   *    Then make sure the throttling config does not have a direction.
   *    Framework configs should not have a direction.
   * 9. Create an HDFS link to reference and update
   *    jobs to reference that link. IMPORT jobs
   *    should have TO HDFS connector, EXPORT jobs should have
   *    FROM HDFS connector.
   * 10. Update 'table' config names to 'fromJobConfig' and 'toTable'.
   *     Also update the relevant inputs as well.
   * @param conn
   */
  // NOTE: This upgrade code happened before the SQOOP-1498 renaming, hence it uses the form/connection
  // tables instead of the latest config/link tables
  private void updateJobRepositorySchemaAndData(Connection conn, long connectorId) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Updating existing data for generic connectors.");
    }

    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_SQF_OPERATION_TO_SQF_DIRECTION, conn,
        Direction.FROM.toString(), "IMPORT");
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_SQF_OPERATION_TO_SQF_DIRECTION, conn,
        Direction.TO.toString(), "EXPORT");

    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_CONNECTOR_HDFS_FORM_DIRECTION, conn,
        Direction.FROM.toString(),
        "input");
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_CONNECTOR_HDFS_FORM_DIRECTION, conn,
        Direction.TO.toString(),
        "output");

    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_CONNECTOR, conn,
        new Long(connectorId), "input", "output");

    runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_INPUT_UPDATE_THROTTLING_FORM_INPUTS, conn,
        "IMPORT", "EXPORT");
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_REMOVE_EXTRA_FORM_INPUTS, conn,
        "throttling", "EXPORT");
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_REMOVE_EXTRA_DRIVER_FORM, conn,
        "throttling", "EXPORT");
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_DIRECTION_TO_NULL, conn,
        "throttling");
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_DRIVER_INDEX, conn,
        new Long(0), "throttling");

    Long connectionId = createHdfsConnection(conn, connectorId);
    runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_SQB_TO_CONNECTION_COPY_SQB_FROM_CONNECTION, conn,
        "EXPORT");
    runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_SQB_FROM_CONNECTION, conn,
        new Long(connectionId), "EXPORT");
    runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_SQB_TO_CONNECTION, conn,
        new Long(connectionId), "IMPORT");

    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_SQF_NAME, conn,
        "fromJobConfig", "table", Direction.FROM.toString());
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_TABLE_INPUT_NAMES, conn,
        Direction.FROM.toString().toLowerCase(), "fromJobConfig", Direction.FROM.toString());
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_SQF_NAME, conn,
        "toJobConfig", "table", Direction.TO.toString());
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_TABLE_INPUT_NAMES, conn,
        Direction.TO.toString().toLowerCase(), "toJobConfig", Direction.TO.toString());

    if (LOG.isTraceEnabled()) {
      LOG.trace("Updated existing data for generic connectors.");
    }
  }

  /**
   * Pre-register HDFS Connector so that config upgrade will work.
   * NOTE: This should be used only in the upgrade path
   */
  @Deprecated
  protected long registerHdfsConnector(Connection conn) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Begin HDFS Connector pre-loading.");
    }

    List<URL> connectorConfigs = ConnectorManagerUtils.getConnectorConfigs();

    if (LOG.isInfoEnabled()) {
      LOG.info("Connector configs: " + connectorConfigs);
    }

    ConnectorHandler handler = null;
    for (URL url : connectorConfigs) {
      handler = new ConnectorHandler(url);

      if (handler.getConnectorConfigurable().getPersistenceId() != -1) {
        return handler.getConnectorConfigurable().getPersistenceId();
      }

      if (handler.getUniqueName().equals(CONNECTOR_HDFS)) {
        try {
          PreparedStatement baseConnectorStmt = conn.prepareStatement(
              STMT_INSERT_INTO_CONFIGURABLE_WITHOUT_SUPPORTED_DIRECTIONS,
              Statement.RETURN_GENERATED_KEYS);
          baseConnectorStmt.setString(1, handler.getConnectorConfigurable().getUniqueName());
          baseConnectorStmt.setString(2, handler.getConnectorConfigurable().getClassName());
          baseConnectorStmt.setString(3, "0");
          if (baseConnectorStmt.executeUpdate() == 1) {
            ResultSet rsetConnectorId = baseConnectorStmt.getGeneratedKeys();
            if (rsetConnectorId.next()) {
              if (LOG.isInfoEnabled()) {
                LOG.info("HDFS Connector pre-loaded: " + rsetConnectorId.getLong(1));
              }
              return rsetConnectorId.getLong(1);
            }
          }
        } catch (SQLException e) {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0013);
        }

        break;
      }
    }

    return -1L;
  }

  /**
   * Create an HDFS connection ( used only in version 2).
   * Intended to be used when moving HDFS connector out of the sqoop driver
   * to its own connector.
   *
   * NOTE: Should be used only in the upgrade path!
   */
  @Deprecated
  private Long createHdfsConnection(Connection conn, Long connectorId) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Creating HDFS link.");
    }

    PreparedStatement stmt = null;
    int result;
    try {
      stmt = conn.prepareStatement(STMT_INSERT_CONNECTION,
          Statement.RETURN_GENERATED_KEYS);
      stmt.setString(1, CONNECTOR_HDFS);
      stmt.setLong(2, connectorId);
      stmt.setBoolean(3, true);
      stmt.setNull(4, Types.VARCHAR);
      stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
      stmt.setNull(6, Types.VARCHAR);
      stmt.setTimestamp(7, new Timestamp(System.currentTimeMillis()));

      result = stmt.executeUpdate();
      if (result != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0012,
            Integer.toString(result));
      }
      ResultSet rsetConnectionId = stmt.getGeneratedKeys();

      if (!rsetConnectionId.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0013);
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("Created HDFS connection.");
      }

      return rsetConnectionId.getLong(1);
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
  public boolean isRespositorySuitableForUse(Connection conn) {
    // TODO(jarcec): Verify that all structures are present (e.g. something like corruption validation)
    // NOTE: At this point is is just checking if the repo version matches the version
    // in the upgraded code
    return detectRepositoryVersion(conn) == DerbyRepoConstants.LATEST_DERBY_REPOSITORY_VERSION;
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
    PreparedStatement connectorFetchStmt = null;
    try {
      connectorFetchStmt = conn.prepareStatement(STMT_SELECT_FROM_CONFIGURABLE);
      connectorFetchStmt.setString(1, shortName);

      List<MConnector> connectors = loadConnectors(connectorFetchStmt, conn);

      if (connectors.size() == 0) {
        LOG.debug("No connector found by name: " + shortName);
        return null;
      } else if (connectors.size() == 1) {
        LOG.debug("Looking up connector: " + shortName + ", found: " + mc);
        return connectors.get(0);
      } else {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0005, shortName);
      }

    } catch (SQLException ex) {
      logException(ex, shortName);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0004, shortName, ex);
    } finally {
      closeStatements(connectorFetchStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MConnector> findConnectors(Connection conn) {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_CONFIGURABLE_ALL_FOR_TYPE);
      stmt.setString(1, MConfigurableType.CONNECTOR.name());

      return loadConnectors(stmt,conn);
    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0045, ex);
    } finally {
      closeStatements(stmt);
    }
  }

   /**
   * {@inheritDoc}
   */
  @Override
  public void registerDriver(MDriver mDriver, Connection conn) {
    if (mDriver.hasPersistenceId()) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0011, mDriver.getUniqueName());
    }
    mDriver.setPersistenceId(insertAndGetDriverId(mDriver, conn));
    insertConfigsforDriver(mDriver, conn);
  }

  private void insertConfigsforDriver(MDriver mDriver, Connection conn) {
    PreparedStatement baseConfigStmt = null;
    PreparedStatement baseInputStmt = null;
    try {
      baseConfigStmt = conn.prepareStatement(STMT_INSERT_INTO_CONFIG,
          Statement.RETURN_GENERATED_KEYS);
      baseInputStmt = conn.prepareStatement(STMT_INSERT_INTO_INPUT,
          Statement.RETURN_GENERATED_KEYS);

      // Register a driver config as a job type with no owner/connector and direction
      registerConfigs(mDriver.getPersistenceId(), null /* no direction*/, mDriver.getDriverConfig().getConfigs(),
        MConfigType.JOB.name(), baseConfigStmt, baseInputStmt, conn);

    } catch (SQLException ex) {
      logException(ex, mDriver);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0014, ex);
    } finally {
      closeStatements(baseConfigStmt, baseInputStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MDriver findDriver(String shortName, Connection conn) {
    LOG.debug("Looking up Driver and config ");
    PreparedStatement driverFetchStmt = null;
    PreparedStatement driverConfigFetchStmt = null;
    PreparedStatement driverConfigInputFetchStmt = null;

    MDriver mDriver;
    try {
      driverFetchStmt = conn.prepareStatement(STMT_SELECT_FROM_CONFIGURABLE);
      driverFetchStmt.setString(1, shortName);

      ResultSet rsDriverSet = driverFetchStmt.executeQuery();
      if (!rsDriverSet.next()) {
        return null;
      }
      Long driverId = rsDriverSet.getLong(1);
      String driverVersion = rsDriverSet.getString(4);

      driverConfigFetchStmt = conn.prepareStatement(STMT_SELECT_CONFIG_FOR_CONFIGURABLE);
      driverConfigFetchStmt.setLong(1, driverId);

      driverConfigInputFetchStmt = conn.prepareStatement(STMT_SELECT_INPUT);
      List<MConfig> driverConfigs = new ArrayList<MConfig>();
      loadDriverConfigs(driverConfigs, driverConfigFetchStmt, driverConfigInputFetchStmt, 1);

      if (driverConfigs.isEmpty()) {
        return null;
      }
      mDriver = new MDriver(new MDriverConfig(driverConfigs), driverVersion);
      mDriver.setPersistenceId(driverId);

    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0004, "Driver", ex);
    } finally {
      if (driverConfigFetchStmt != null) {
        try {
          driverConfigFetchStmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close driver config fetch statement", ex);
        }
      }
      if (driverConfigInputFetchStmt != null) {
        try {
          driverConfigInputFetchStmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close driver input fetch statement", ex);
        }
      }
      if (driverFetchStmt != null) {
        try {
          driverFetchStmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close driver fetch statement", ex);
        }
      }
    }

    LOG.debug("Looked up Driver and config");
    return mDriver;
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
  public void createLink(MLink link, Connection conn) {
    PreparedStatement stmt = null;
    int result;
    try {
      stmt = conn.prepareStatement(STMT_INSERT_LINK,
        Statement.RETURN_GENERATED_KEYS);
      stmt.setString(1, link.getName());
      stmt.setLong(2, link.getConnectorId());
      stmt.setBoolean(3, link.getEnabled());
      stmt.setString(4, link.getCreationUser());
      stmt.setTimestamp(5, new Timestamp(link.getCreationDate().getTime()));
      stmt.setString(6, link.getLastUpdateUser());
      stmt.setTimestamp(7, new Timestamp(link.getLastUpdateDate().getTime()));

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

      createInputValues(STMT_INSERT_LINK_INPUT,
        connectionId,
        link.getConnectorLinkConfig().getConfigs(),
        conn);
      link.setPersistenceId(connectionId);

    } catch (SQLException ex) {
      logException(ex, link);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0019, ex);
    } finally {
      closeStatements(stmt);
    }
  }
  /**
   * {@inheritDoc}
   */
  @Override
  public void updateLink(MLink link, Connection conn) {
    PreparedStatement deleteStmt = null;
    PreparedStatement updateStmt = null;
    try {
      // Firstly remove old values
      deleteStmt = conn.prepareStatement(STMT_DELETE_LINK_INPUT);
      deleteStmt.setLong(1, link.getPersistenceId());
      deleteStmt.executeUpdate();

      // Update LINK_CONFIG table
      updateStmt = conn.prepareStatement(STMT_UPDATE_LINK);
      updateStmt.setString(1, link.getName());
      updateStmt.setString(2, link.getLastUpdateUser());
      updateStmt.setTimestamp(3, new Timestamp(new Date().getTime()));

      updateStmt.setLong(4, link.getPersistenceId());
      updateStmt.executeUpdate();

      // And reinsert new values
      createInputValues(STMT_INSERT_LINK_INPUT,
        link.getPersistenceId(),
        link.getConnectorLinkConfig().getConfigs(),
        conn);

    } catch (SQLException ex) {
      logException(ex, link);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0021, ex);
    } finally {
      closeStatements(deleteStmt, updateStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean existsLink(long id, Connection conn) {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_LINK_CHECK_BY_ID);
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
  public boolean inUseLink(long connectionId, Connection conn) {
    PreparedStatement stmt = null;
    ResultSet rs = null;

    try {
      stmt = conn.prepareStatement(STMT_SELECT_JOBS_FOR_LINK_CHECK);
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
  public void enableLink(long connectionId, boolean enabled, Connection conn) {
    PreparedStatement enableConn = null;

    try {
      enableConn = conn.prepareStatement(STMT_ENABLE_LINK);
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
  public void deleteLink(long id, Connection conn) {
    PreparedStatement dltConn = null;

    try {
      deleteLinkInputs(id, conn);
      dltConn = conn.prepareStatement(STMT_DELETE_LINK);
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
  public void deleteLinkInputs(long id, Connection conn) {
    PreparedStatement dltConnInput = null;
    try {
      dltConnInput = conn.prepareStatement(STMT_DELETE_LINK_INPUT);
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
  public MLink findLink(long linkId, Connection conn) {
    PreparedStatement linkFetchStmt = null;
    try {
      linkFetchStmt = conn.prepareStatement(STMT_SELECT_LINK_SINGLE);
      linkFetchStmt.setLong(1, linkId);

      List<MLink> links = loadLinks(linkFetchStmt, conn);

      if(links.size() != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0024, "Couldn't find"
          + " link with id " + linkId);
      }

      // Return the first and only one link object with the given id
      return links.get(0);

    } catch (SQLException ex) {
      logException(ex, linkId);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0023, ex);
    } finally {
      closeStatements(linkFetchStmt);
    }
  }

  @Override
  public MLink findLink(String linkName, Connection conn) {
    PreparedStatement linkFetchStmt = null;
    try {
      linkFetchStmt = conn.prepareStatement(STMT_SELECT_LINK_SINGLE_BY_NAME);
      linkFetchStmt.setString(1, linkName);

      List<MLink> links = loadLinks(linkFetchStmt, conn);

      if (links.size() != 1) {
        return null;
      }

      // Return the first and only one link object with the given name
      return links.get(0);

    } catch (SQLException ex) {
      logException(ex, linkName);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0023, ex);
    } finally {
      closeStatements(linkFetchStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MLink> findLinks(Connection conn) {
    PreparedStatement linksFetchStmt = null;
    try {
      linksFetchStmt = conn.prepareStatement(STMT_SELECT_LINK_ALL);

      return loadLinks(linksFetchStmt, conn);

    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0023, ex);
    } finally {
      closeStatements(linksFetchStmt);
    }
  }


  /**
   *
   * {@inheritDoc}
   *
   */
  @Override
  public List<MLink> findLinksForConnector(long connectorId, Connection conn) {
    PreparedStatement linkByConnectorFetchStmt = null;
    try {
      linkByConnectorFetchStmt = conn.prepareStatement(STMT_SELECT_LINK_FOR_CONNECTOR_CONFIGURABLE);
      linkByConnectorFetchStmt.setLong(1, connectorId);
      return loadLinks(linkByConnectorFetchStmt, conn);
    } catch (SQLException ex) {
      logException(ex, connectorId);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0023, ex);
    } finally {
      closeStatements(linkByConnectorFetchStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void upgradeConnectorAndConfigs(MConnector mConnector, Connection conn) {
    updateConnectorAndDeleteConfigs(mConnector, conn);
    insertConfigsForConnector(mConnector, conn);
  }

  private void updateConnectorAndDeleteConfigs(MConnector mConnector, Connection conn) {
    PreparedStatement updateConnectorStatement = null;
    PreparedStatement deleteConfig = null;
    PreparedStatement deleteInput = null;
    try {
      updateConnectorStatement = conn.prepareStatement(STMT_UPDATE_CONFIGURABLE);
      deleteInput = conn.prepareStatement(STMT_DELETE_INPUTS_FOR_CONFIGURABLE);
      deleteConfig = conn.prepareStatement(STMT_DELETE_CONFIGS_FOR_CONFIGURABLE);
      updateConnectorStatement.setString(1, mConnector.getUniqueName());
      updateConnectorStatement.setString(2, mConnector.getClassName());
      updateConnectorStatement.setString(3, mConnector.getVersion());
      updateConnectorStatement.setString(4, mConnector.getType().name());
      updateConnectorStatement.setLong(5, mConnector.getPersistenceId());

      if (updateConnectorStatement.executeUpdate() != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0038);
      }
      deleteInput.setLong(1, mConnector.getPersistenceId());
      deleteConfig.setLong(1, mConnector.getPersistenceId());
      deleteInput.executeUpdate();
      deleteConfig.executeUpdate();

    } catch (SQLException e) {
      logException(e, mConnector);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0038, e);
    } finally {
      closeStatements(updateConnectorStatement, deleteConfig, deleteInput);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void upgradeDriverAndConfigs(MDriver mDriver, Connection conn) {
    updateDriverAndDeleteConfigs(mDriver, conn);
    insertConfigsForDriver(mDriver, conn);
  }

  private void updateDriverAndDeleteConfigs(MDriver mDriver, Connection conn) {
    PreparedStatement updateDriverStatement = null;
    PreparedStatement deleteConfig = null;
    PreparedStatement deleteInput = null;
    try {
      updateDriverStatement = conn.prepareStatement(STMT_UPDATE_CONFIGURABLE);
      deleteInput = conn.prepareStatement(STMT_DELETE_INPUTS_FOR_CONFIGURABLE);
      deleteConfig = conn.prepareStatement(STMT_DELETE_CONFIGS_FOR_CONFIGURABLE);
      updateDriverStatement.setString(1, mDriver.getUniqueName());
      updateDriverStatement.setString(2, Driver.getClassName());
      updateDriverStatement.setString(3, mDriver.getVersion());
      updateDriverStatement.setString(4, mDriver.getType().name());
      updateDriverStatement.setLong(5, mDriver.getPersistenceId());

      if (updateDriverStatement.executeUpdate() != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0038);
      }
      deleteInput.setLong(1, mDriver.getPersistenceId());
      deleteConfig.setLong(1, mDriver.getPersistenceId());
      deleteInput.executeUpdate();
      deleteConfig.executeUpdate();

    } catch (SQLException e) {
      logException(e, mDriver);
      throw new SqoopException(DerbyRepoError.DERBYREPO_0044, e);
    } finally {
      closeStatements(updateDriverStatement, deleteConfig, deleteInput);
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
      stmt = conn.prepareStatement(STMT_INSERT_JOB, Statement.RETURN_GENERATED_KEYS);
      stmt.setString(1, job.getName());
      stmt.setLong(2, job.getLinkId(Direction.FROM));
      stmt.setLong(3, job.getLinkId(Direction.TO));
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

      // from config for the job
      createInputValues(STMT_INSERT_JOB_INPUT,
                        jobId,
                        job.getJobConfig(Direction.FROM).getConfigs(),
                        conn);
      // to config for the job
      createInputValues(STMT_INSERT_JOB_INPUT,
                        jobId,
                        job.getJobConfig(Direction.TO).getConfigs(),
                        conn);
      // driver config per job
      createInputValues(STMT_INSERT_JOB_INPUT,
                        jobId,
                        job.getDriverConfig().getConfigs(),
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
                        job.getJobConfig(Direction.FROM).getConfigs(),
                        conn);
      createInputValues(STMT_INSERT_JOB_INPUT,
                        job.getPersistenceId(),
                        job.getJobConfig(Direction.TO).getConfigs(),
                        conn);
      createInputValues(STMT_INSERT_JOB_INPUT,
          job.getPersistenceId(),
          job.getDriverConfig().getConfigs(),
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
      stmt = conn.prepareStatement(STMT_SELECT_JOB_CHECK_BY_ID);
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
      stmt = conn.prepareStatement(STMT_SELECT_JOB_SINGLE_BY_ID);
      stmt.setLong(1, id);

      List<MJob> jobs = loadJobs(stmt, conn);

      if(jobs.size() != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0030, "Couldn't find"
          + " job with id " + id);
      }

      // Return the first and only one link object
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
  public MJob findJob(String name, Connection conn) {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(STMT_SELECT_JOB_SINGLE_BY_NAME);
      stmt.setString(1, name);

      List<MJob> jobs = loadJobs(stmt, conn);

      if (jobs.size() != 1) {
        return null;
      }

      // Return the first and only one link object
      return jobs.get(0);

    } catch (SQLException ex) {
      logException(ex, name);
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
      stmt = conn.prepareStatement(STMT_SELECT_JOB);

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
      stmt = conn.prepareStatement(STMT_SELECT_ALL_JOBS_FOR_CONNECTOR_CONFIGURABLE);
      stmt.setLong(1, connectorId);
      stmt.setLong(2, connectorId);
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
   * @param conn Connection to database
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
   * @param conn Connection to database
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
   * @param conn Connection to database
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

      while (rs.next()) {
        String groupName = rs.getString(1);
        String counterName = rs.getString(2);
        long value = rs.getLong(3);

        CounterGroup group = counters.getCounterGroup(groupName);
        if (group == null) {
          group = new CounterGroup(groupName);
          counters.addCounterGroup(group);
        }

        group.addCounter(new Counter(counterName, value));
      }

      if (counters.isEmpty()) {
        return null;
      } else {
        return counters;
      }
    } finally {
      closeStatements(stmt);
      closeResultSets(rs);
    }
  }

  private Long getDirection(Direction direction, Connection conn) throws SQLException {
    PreparedStatement directionStmt = null;
    ResultSet rs = null;

    try {
      directionStmt = conn.prepareStatement(STMT_SELECT_SQD_ID_BY_SQD_NAME);
      directionStmt.setString(1, direction.toString());
      rs = directionStmt.executeQuery();

      rs.next();
      return rs.getLong(1);
    } finally {
      if (rs != null) {
        closeResultSets(rs);
      }
      if (directionStmt != null) {
        closeStatements(directionStmt);
      }
    }
  }

  private Direction getDirection(long directionId, Connection conn) throws SQLException {
    PreparedStatement directionStmt = null;
    ResultSet rs = null;

    try {
      directionStmt = conn.prepareStatement(STMT_SELECT_SQD_NAME_BY_SQD_ID);
      directionStmt.setLong(1, directionId);
      rs = directionStmt.executeQuery();

      rs.next();
      return Direction.valueOf(rs.getString(1));
    } finally {
      if (rs != null) {
        closeResultSets(rs);
      }
      if (directionStmt != null) {
        closeStatements(directionStmt);
      }
    }
  }

  private SupportedDirections findConnectorSupportedDirections(long connectorId, Connection conn) throws SQLException {
    PreparedStatement connectorDirectionsStmt = null;
    ResultSet rs = null;

    boolean from = false, to = false;

    try {
      connectorDirectionsStmt = conn.prepareStatement(STMT_SELECT_SQ_CONNECTOR_DIRECTIONS);
      connectorDirectionsStmt.setLong(1, connectorId);
      rs = connectorDirectionsStmt.executeQuery();

      while(rs.next()) {
        switch(getDirection(rs.getLong(2), conn)) {
          case FROM:
            from = true;
            break;

          case TO:
            to = true;
            break;
        }
      }
    } finally {
      if (rs != null) {
        closeResultSets(rs);
      }
      if (connectorDirectionsStmt != null) {
        closeStatements(connectorDirectionsStmt);
      }
    }

    return new SupportedDirections(from, to);
  }

  private List<MConnector> loadConnectors(PreparedStatement stmt, Connection conn) throws SQLException {
    List<MConnector> connectors = new ArrayList<MConnector>();
    ResultSet rsConnectors = null;
    PreparedStatement connectorConfigFetchStmt = null;
    PreparedStatement connectorConfigInputFetchStmt = null;

    try {
      rsConnectors = stmt.executeQuery();
      connectorConfigFetchStmt = conn.prepareStatement(STMT_SELECT_CONFIG_FOR_CONFIGURABLE);
      connectorConfigInputFetchStmt = conn.prepareStatement(STMT_SELECT_INPUT);

      while(rsConnectors.next()) {
        long connectorId = rsConnectors.getLong(1);
        String connectorName = rsConnectors.getString(2);
        String connectorClassName = rsConnectors.getString(3);
        String connectorVersion = rsConnectors.getString(4);

        connectorConfigFetchStmt.setLong(1, connectorId);

        List<MConfig> linkConfig = new ArrayList<MConfig>();
        List<MConfig> fromConfig = new ArrayList<MConfig>();
        List<MConfig> toConfig = new ArrayList<MConfig>();

        loadConnectorConfigTypes(linkConfig, fromConfig, toConfig, connectorConfigFetchStmt,
            connectorConfigInputFetchStmt, 1, conn);

        SupportedDirections supportedDirections
            = findConnectorSupportedDirections(connectorId, conn);
        MFromConfig fromJobConfig = null;
        MToConfig toJobConfig = null;
        if (supportedDirections.isDirectionSupported(Direction.FROM)) {
          fromJobConfig = new MFromConfig(fromConfig);
        }
        if (supportedDirections.isDirectionSupported(Direction.TO)) {
          toJobConfig = new MToConfig(toConfig);
        }
        MConnector mc = new MConnector(connectorName, connectorClassName, connectorVersion,
                                       new MLinkConfig(linkConfig), fromJobConfig, toJobConfig);
        mc.setPersistenceId(connectorId);

        connectors.add(mc);
      }
    } finally {
      closeResultSets(rsConnectors);
      closeStatements(connectorConfigFetchStmt, connectorConfigInputFetchStmt);
    }
    return connectors;
  }

  private List<MLink> loadLinks(PreparedStatement linkFetchStmt, Connection conn) throws SQLException {
    List<MLink> links = new ArrayList<MLink>();
    ResultSet rsConnection = null;
    PreparedStatement connectorLinkConfigFetchStatement = null;
    PreparedStatement connectorLinkConfigInputStatement = null;

    try {
      rsConnection = linkFetchStmt.executeQuery();
      connectorLinkConfigFetchStatement = conn.prepareStatement(STMT_SELECT_CONFIG_FOR_CONFIGURABLE);
      connectorLinkConfigInputStatement = conn.prepareStatement(STMT_FETCH_LINK_INPUT);

      while(rsConnection.next()) {
        long id = rsConnection.getLong(1);
        String name = rsConnection.getString(2);
        long connectorId = rsConnection.getLong(3);
        boolean enabled = rsConnection.getBoolean(4);
        String creationUser = rsConnection.getString(5);
        Date creationDate = rsConnection.getTimestamp(6);
        String updateUser = rsConnection.getString(7);
        Date lastUpdateDate = rsConnection.getTimestamp(8);

        connectorLinkConfigFetchStatement.setLong(1, connectorId);
        connectorLinkConfigInputStatement.setLong(1, id);
        connectorLinkConfigInputStatement.setLong(3, id);

        List<MConfig> connectorLinkConfig = new ArrayList<MConfig>();
        List<MConfig> fromConfig = new ArrayList<MConfig>();
        List<MConfig> toConfig = new ArrayList<MConfig>();

        loadConnectorConfigTypes(connectorLinkConfig, fromConfig, toConfig, connectorLinkConfigFetchStatement,
            connectorLinkConfigInputStatement, 2, conn);
        MLink link = new MLink(connectorId, new MLinkConfig(connectorLinkConfig));

        link.setPersistenceId(id);
        link.setName(name);
        link.setCreationUser(creationUser);
        link.setCreationDate(creationDate);
        link.setLastUpdateUser(updateUser);
        link.setLastUpdateDate(lastUpdateDate);
        link.setEnabled(enabled);

        links.add(link);
      }
    } finally {
      closeResultSets(rsConnection);
      closeStatements(connectorLinkConfigFetchStatement, connectorLinkConfigInputStatement);
    }

    return links;
  }

  private List<MJob> loadJobs(PreparedStatement stmt,
                              Connection conn)
                              throws SQLException {
    List<MJob> jobs = new ArrayList<MJob>();
    ResultSet rsJob = null;
    PreparedStatement fromConfigFetchStmt = null;
    PreparedStatement toConfigFetchStmt = null;
    PreparedStatement driverConfigfetchStmt = null;
    PreparedStatement jobInputFetchStmt = null;

    try {
      rsJob = stmt.executeQuery();
      // Note: Job does not hold a explicit reference to the driver since every
      // job has the same driver
      long driverId = this.findDriver(MDriver.DRIVER_NAME, conn).getPersistenceId();
      fromConfigFetchStmt  = conn.prepareStatement(STMT_SELECT_CONFIG_FOR_CONFIGURABLE);
      toConfigFetchStmt = conn.prepareStatement(STMT_SELECT_CONFIG_FOR_CONFIGURABLE);
      driverConfigfetchStmt = conn.prepareStatement(STMT_SELECT_CONFIG_FOR_CONFIGURABLE);
      jobInputFetchStmt = conn.prepareStatement(STMT_FETCH_JOB_INPUT);

      while(rsJob.next()) {
        long fromConnectorId = rsJob.getLong(1);
        long toConnectorId = rsJob.getLong(2);
        long id = rsJob.getLong(3);
        String name = rsJob.getString(4);
        long fromLinkId = rsJob.getLong(5);
        long toLinkId = rsJob.getLong(6);
        boolean enabled = rsJob.getBoolean(7);
        String createBy = rsJob.getString(8);
        Date creationDate = rsJob.getTimestamp(9);
        String updateBy = rsJob.getString(10);
        Date lastUpdateDate = rsJob.getTimestamp(11);

        fromConfigFetchStmt.setLong(1, fromConnectorId);
        toConfigFetchStmt.setLong(1,toConnectorId);
        driverConfigfetchStmt.setLong(1, driverId);

        jobInputFetchStmt.setLong(1, id);
        jobInputFetchStmt.setLong(3, id);

        // FROM entity configs
        List<MConfig> fromConnectorLinkConfig = new ArrayList<MConfig>();
        List<MConfig> fromConnectorFromJobConfig = new ArrayList<MConfig>();
        List<MConfig> fromConnectorToJobConfig = new ArrayList<MConfig>();

        loadConnectorConfigTypes(fromConnectorLinkConfig, fromConnectorFromJobConfig, fromConnectorToJobConfig,
            fromConfigFetchStmt, jobInputFetchStmt, 2, conn);

        // TO entity configs
        List<MConfig> toConnectorLinkConfig = new ArrayList<MConfig>();
        List<MConfig> toConnectorFromJobConfig = new ArrayList<MConfig>();
        List<MConfig> toConnectorToJobConfig = new ArrayList<MConfig>();

        // ?? dont we need 2 different driver configs for the from/to?
        List<MConfig> driverConfig = new ArrayList<MConfig>();

        loadConnectorConfigTypes(toConnectorLinkConfig, toConnectorFromJobConfig, toConnectorToJobConfig,
            toConfigFetchStmt, jobInputFetchStmt, 2, conn);

        loadDriverConfigs(driverConfig, driverConfigfetchStmt, jobInputFetchStmt, 2);

        MJob job = new MJob(
          fromConnectorId, toConnectorId,
          fromLinkId, toLinkId,
          new MFromConfig(fromConnectorFromJobConfig),
          new MToConfig(toConnectorToJobConfig),
          new MDriverConfig(driverConfig));

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
      closeStatements(fromConfigFetchStmt, toConfigFetchStmt, driverConfigfetchStmt, jobInputFetchStmt);
    }

    return jobs;
  }

  private void registerConfigDirection(Long configId, Direction direction, Connection conn)
      throws SQLException {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(STMT_INSERT_SQ_CONFIG_DIRECTIONS);
      stmt.setLong(1, configId);
      stmt.setLong(2, getDirection(direction, conn));
      if (stmt.executeUpdate() != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0048);
      }
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * Register configs in derby database. This method will insert the ids
   * generated by the repository into the configs passed in itself.
   *
   * Use given prepared statements to create entire config structure in database.
   *
   * @param configurableId
   * @param configs
   * @param type
   * @param baseConfigStmt
   * @param baseInputStmt
   * @param conn
   * @return short number of configs registered.
   * @throws SQLException
   */
  private short registerConfigs(Long configurableId, Direction direction,
      List<MConfig> configs, String type, PreparedStatement baseConfigStmt,
      PreparedStatement baseInputStmt, Connection conn)
          throws SQLException {
    short configIndex = 0;

    for (MConfig config : configs) {
      if (configurableId == null) {
        baseConfigStmt.setNull(1, Types.BIGINT);
      } else {
        baseConfigStmt.setLong(1, configurableId);
      }

      baseConfigStmt.setString(2, config.getName());
      baseConfigStmt.setString(3, type);
      baseConfigStmt.setShort(4, configIndex++);

      int baseConfigCount = baseConfigStmt.executeUpdate();
      if (baseConfigCount != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0015,
          Integer.toString(baseConfigCount));
      }
      ResultSet rsetConfigId = baseConfigStmt.getGeneratedKeys();
      if (!rsetConfigId.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0016);
      }

      long configId = rsetConfigId.getLong(1);
      config.setPersistenceId(configId);

      if (direction != null) {
        registerConfigDirection(configId, direction, conn);
      }

      // Insert all the inputs
      List<MInput<?>> inputs = config.getInputs();
      registerConfigInputs(configId, inputs, baseInputStmt);
    }
    return configIndex;
  }

  /**
   * Save given inputs to the database.
   *
   * Use given prepare statement to save all inputs into repository.
   *
   * @param configId Identifier for corresponding config
   * @param inputs List of inputs that needs to be saved
   * @param baseInputStmt Statement that we can utilize
   * @throws SQLException In case of any failure on Derby side
   */
  private void registerConfigInputs(long configId, List<MInput<?>> inputs,
      PreparedStatement baseInputStmt) throws SQLException {
    short inputIndex = 0;
    for (MInput<?> input : inputs) {
      baseInputStmt.setString(1, input.getName());
      baseInputStmt.setLong(2, configId);
      baseInputStmt.setShort(3, inputIndex++);
      baseInputStmt.setString(4, input.getType().name());
      baseInputStmt.setBoolean(5, input.isSensitive());
      // String specific column(s)
      if (input.getType().equals(MInputType.STRING)) {
        MStringInput  strInput = (MStringInput) input;
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
  private void runQuery(String query, Connection conn, Object... args) {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(query);

      for (int i = 0; i < args.length; ++i) {
        if (args[i] instanceof String) {
          stmt.setString(i + 1, (String)args[i]);
        } else if (args[i] instanceof Long) {
          stmt.setLong(i + 1, (Long) args[i]);
        } else {
          stmt.setObject(i + 1, args[i]);
        }
      }

      if (stmt.execute()) {
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
   * Load configs and corresponding inputs from Derby database.
   *
   * Use given prepared statements to load all configs and corresponding inputs
   * from Derby.
   *
   * @param driverConfig List of driver configs that will be filled up
   * @param configFetchStatement Prepared statement for fetching configs
   * @param inputFetchStmt Prepare statement for fetching inputs
   * @param configPosition position of the config
   * @throws SQLException In case of any failure on Derby side
   */
  public void loadDriverConfigs(List<MConfig> driverConfig,
                                 PreparedStatement configFetchStatement,
                                 PreparedStatement inputFetchStmt,
                                 int configPosition) throws SQLException {

    // Get list of structures from database
    ResultSet rsetConfig = configFetchStatement.executeQuery();
    while (rsetConfig.next()) {
      long configId = rsetConfig.getLong(1);
      Long fromConnectorId = rsetConfig.getLong(2);
      String configName = rsetConfig.getString(3);
      String configTYpe = rsetConfig.getString(4);
      int configIndex = rsetConfig.getInt(5);
      List<MInput<?>> configInputs = new ArrayList<MInput<?>>();

      MConfig mDriverConfig = new MConfig(configName, configInputs);
      mDriverConfig.setPersistenceId(configId);

      inputFetchStmt.setLong(configPosition, configId);

      ResultSet rsetInput = inputFetchStmt.executeQuery();
      while (rsetInput.next()) {
        long inputId = rsetInput.getLong(1);
        String inputName = rsetInput.getString(2);
        long inputConfig = rsetInput.getLong(3);
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
              + "config-" + inputConfig + ":" + mit.name());
        }

        // Set persistent ID
        input.setPersistenceId(inputId);

        // Set value
        if(value == null) {
          input.setEmpty();
        } else {
          input.restoreFromUrlSafeValueString(value);
        }

        if (mDriverConfig.getInputs().size() != inputIndex) {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0009,
            "config: " + mDriverConfig
            + "; input: " + input
            + "; index: " + inputIndex
            + "; expected: " + mDriverConfig.getInputs().size()
          );
        }

        mDriverConfig.getInputs().add(input);
      }

      if (mDriverConfig.getInputs().size() == 0) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0008,
          "owner-" + fromConnectorId
          + "; config: " + mDriverConfig
        );
      }

      MConfigType configType = MConfigType.valueOf(configTYpe);
      switch (configType) {
      case JOB:
        if (driverConfig.size() != configIndex) {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0010,
            "owner-" + fromConnectorId
            + "; config: " + configType
            + "; index: " + configIndex
            + "; expected: " + driverConfig.size()
          );
        }
        driverConfig.add(mDriverConfig);
        break;
      //added for connector upgrade path
      case CONNECTION:
        // do nothing since we do not support it
        break;
      default:
        throw new SqoopException(DerbyRepoError.DERBYREPO_0007,
            "connector-" + fromConnectorId + ":" + configType);
      }
    }
  }

  private Direction findConfigDirection(long configId, Connection conn) throws SQLException {
    PreparedStatement stmt = null;
    ResultSet rs = null;

    try {
      stmt = conn.prepareStatement(STMT_SELECT_SQ_CONFIG_DIRECTIONS);
      stmt.setLong(1, configId);
      rs = stmt.executeQuery();
      rs.next();
      return getDirection(rs.getLong(2), conn);
    } finally {
      if (rs != null) {
        closeResultSets(rs);
      }
      if (stmt != null) {
        closeStatements(stmt);
      }
    }
  }

  /**
   * Load configs and corresponding inputs related to a connector
   *
   * Use given prepared statements to load all configs and corresponding inputs
   * from Derby.
   *
   * @param linkConfig List of link configs that will be filled up
   * @param fromConfig FROM job configs that will be filled up
   * @param toConfig TO job configs that will be filled up
   * @param configFetchStmt Prepared statement for fetching configs
   * @param inputFetchStmt Prepare statement for fetching inputs
   * @param conn Connection object that is used to find config direction.
   * @throws SQLException In case of any failure on Derby side
   */
  public void loadConnectorConfigTypes(List<MConfig> linkConfig, List<MConfig> fromConfig,
      List<MConfig> toConfig, PreparedStatement configFetchStmt, PreparedStatement inputFetchStmt,
      int configPosition, Connection conn) throws SQLException {

    // Get list of structures from database
    ResultSet rsetConfig = configFetchStmt.executeQuery();
    while (rsetConfig.next()) {
      long configId = rsetConfig.getLong(1);
      Long configConnectorId = rsetConfig.getLong(2);
      String configName = rsetConfig.getString(3);
      String configType = rsetConfig.getString(4);
      int configIndex = rsetConfig.getInt(5);
      List<MInput<?>> configInputs = new ArrayList<MInput<?>>();

      MConfig config = new MConfig(configName, configInputs);
      config.setPersistenceId(configId);

      inputFetchStmt.setLong(configPosition, configId);

      ResultSet rsetInput = inputFetchStmt.executeQuery();
      while (rsetInput.next()) {
        long inputId = rsetInput.getLong(1);
        String inputName = rsetInput.getString(2);
        long inputConfig = rsetInput.getLong(3);
        short inputIndex = rsetInput.getShort(4);
        String inputType = rsetInput.getString(5);
        boolean inputSensitivity = rsetInput.getBoolean(6);
        short inputStrLength = rsetInput.getShort(7);
        String inputEnumValues = rsetInput.getString(8);
        String value = rsetInput.getString(9);

        MInputType mit = MInputType.valueOf(inputType);

        MInput<?> input = null;
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
                    + "config-" + inputConfig + ":" + mit.name());
        }

        // Set persistent ID
        input.setPersistenceId(inputId);

        // Set value
        if(value == null) {
          input.setEmpty();
        } else {
          input.restoreFromUrlSafeValueString(value);
        }

        if (config.getInputs().size() != inputIndex) {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0009,
              "config: " + config
                  + "; input: " + input
                  + "; index: " + inputIndex
                  + "; expected: " + config.getInputs().size()
          );
        }

        config.getInputs().add(input);
      }

      if (config.getInputs().size() == 0) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0008,
            "connector-" + configConnectorId
                + "; config: " + config
        );
      }

      MConfigType mConfigType = MConfigType.valueOf(configType);
      switch (mConfigType) {
        //added for connector upgrade path
        case CONNECTION:
        case LINK:
          if (linkConfig.size() != configIndex) {
            throw new SqoopException(DerbyRepoError.DERBYREPO_0010,
                "connector-" + configConnectorId
                    + "; config: " + config
                    + "; index: " + configIndex
                    + "; expected: " + linkConfig.size()
            );
          }
          linkConfig.add(config);
          break;
        case JOB:
          Direction type = findConfigDirection(configId, conn);
          List<MConfig> jobConfigs;
          switch(type) {
            case FROM:
              jobConfigs = fromConfig;
              break;

            case TO:
              jobConfigs = toConfig;
              break;

            default:
              throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
          }

          if (jobConfigs.size() != configIndex) {
            throw new SqoopException(DerbyRepoError.DERBYREPO_0010,
                "connector-" + configConnectorId
                    + "; config: " + config
                    + "; index: " + configIndex
                    + "; expected: " + jobConfigs.size()
            );
          }

          jobConfigs.add(config);
          break;
        default:
          throw new SqoopException(DerbyRepoError.DERBYREPO_0007,
              "connector-" + configConnectorId + ":" + config);
      }
    }
  }

  private void createInputValues(String query,
                                 long id,
                                 List<MConfig> configs,
                                 Connection conn) throws SQLException {
    PreparedStatement stmt = null;
    int result;

    try {
      stmt = conn.prepareStatement(query);

      for (MConfig config : configs) {
        for (MInput input : config.getInputs()) {
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