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

import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.SCHEMA_SQOOP;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorHandler;
import org.apache.sqoop.connector.ConnectorManagerUtils;
import org.apache.sqoop.driver.Driver;
import org.apache.sqoop.error.code.DerbyRepoError;
import org.apache.sqoop.model.MConfigType;
import org.apache.sqoop.model.MConfigurableType;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.repository.JdbcRepositoryContext;
import org.apache.sqoop.repository.common.CommonRepoConstants;
import org.apache.sqoop.repository.common.CommonRepositoryHandler;
import org.apache.sqoop.repository.common.CommonRepositorySchemaConstants;

/**
 * JDBC based repository handler for Derby database.
 *
 * Repository implementation for Derby database.
 */
public class DerbyRepositoryHandler extends CommonRepositoryHandler {

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
  public String name() {
    return "Derby";
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
                DerbyRepoError.DERBYREPO_0001, shutDownUrl, ex);
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
      if(foundAll && !tableNames.contains(CommonRepositorySchemaConstants.TABLE_SQ_SYSTEM_NAME)) {
        return 1;
      }

    } catch (SQLException e) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0006, e);
    } finally {
      closeResultSets(rs);
    }

    // Normal version detection, select and return the version
    try {
      // NOTE: Since we can different types of version stored in system table, we renamed the
      // key name for the repository version from "version" to "repository.version" for clarity
      stmt = conn.prepareStatement(STMT_SELECT_DEPRECATED_OR_NEW_SYSTEM_VERSION);
      stmt.setString(1, CommonRepoConstants.SYSKEY_VERSION);
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
    if (repositoryVersion == DerbyRepoConstants.LATEST_DERBY_REPOSITORY_VERSION) {
      return;
    }

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
      migrateFromUnnamedConstraintsToNamedConstraints(conn);
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

      // force register HDFS-connector as a first class citizen in the connector list
      // and re-associate old frameworks configs and connections/links with the new hdfs connector
      // Data modifications only for non-fresh install hence the > 0 check

      if (repositoryVersion > 0) {
        LOG.info("Force registering the HDFS connector as a new configurable");
        long hdfsConnectorId = registerHdfsConnector(conn);
        LOG.info("Finished Force registering the HDFS connector as a new configurable");

        LOG.info("Updating config and inputs for the hdfs connector.");
        updateJobConfigInputForHdfsConnector(conn, hdfsConnectorId);
        LOG.info("Finished Updating config and inputs for the hdfs connector.");
      }

      // Wait to remove SQB_TYPE (IMPORT/EXPORT) until we update all the job data.
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

      // force register the driver as a first class citizen and re-associate the old framework configs with the new driver Id
      // Data modifications only for non-fresh install hence the > 0 check
      if (repositoryVersion > 0) {
        LOG.info("Force registering the Driver as a new configurable");
        long driverId = registerDriver(conn);
        LOG.info("Finished Force registering of the driver as a new configurable");

        LOG.info("Updating config and inputs for the driver.");
        updateDriverConfigInput(conn, driverId);
        LOG.info("Finished Updating config and inputs for the driver.");
      }

      // Update generic jdbc connector
      if (repositoryVersion > 0) {
        DerbyUpgradeGenericJdbcConnectorConfigAndInputNames derbyUpgradeGenericJdbcConnectorConfigAndInputNames
            = new DerbyUpgradeGenericJdbcConnectorConfigAndInputNames(this, conn);
        derbyUpgradeGenericJdbcConnectorConfigAndInputNames.execute();
      }
    }
    // 1.99.5 release changes
    if (repositoryVersion < 5) {
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONFIG_ADD_UNIQUE_CONSTRAINT_NAME_TYPE_AND_CONFIGURABLE_ID, conn);
      runQuery(QUERY_UPGRADE_TABLE_SQ_INPUT_ADD_UNIQUE_CONSTRAINT_NAME_TYPE_AND_CONFIG_ID, conn);
      // submission table column rename
      runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_SUBMISSION_COLUMN_1, conn);
      runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_SUBMISSION_COLUMN_2, conn);
      // SQOOP-1804
      runQuery(QUERY_UPGRADE_TABLE_SQ_INPUT_ADD_COLUMN_SQI_EDITABLE, conn);
      // create a new table for SQ_INPUT relationships
      runQuery(QUERY_CREATE_TABLE_SQ_INPUT_RELATION, conn);
    }
    // 1.99.6 release changes
    if (repositoryVersion < 6) {
      runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_LINK_INPUT_CONSTRAINT_2, conn);
    }

    // last step upgrade the repository version to the latest value in the code
    upgradeRepositoryVersion(conn);
  }

  /**
   * In reality, this method simply drops all constrains on particular tables
   * and creates new constraints. Pre-1.99.3 these will be unnamed,
   * Post-1.99.3 these will be named.
   *
   * @param conn
   */
  private void migrateFromUnnamedConstraintsToNamedConstraints(Connection conn) {
    // Get unnamed constraints
    PreparedStatement fetchUnnamedConstraintsStmt = null;
    Statement dropUnnamedConstraintsStmt = null;
    Map<String, List<String>> autoConstraintNameMap = new TreeMap<String, List<String>>();

    try {
      fetchUnnamedConstraintsStmt = conn.prepareStatement(STMT_FETCH_TABLE_FOREIGN_KEYS);
      for (String tableName : new String[] {
          DerbySchemaConstants.TABLE_SQ_FORM_NAME,
          CommonRepositorySchemaConstants.TABLE_SQ_INPUT_NAME,
          DerbySchemaConstants.TABLE_SQ_CONNECTION_NAME,
          CommonRepositorySchemaConstants.TABLE_SQ_JOB_NAME,
          DerbySchemaConstants.TABLE_SQ_CONNECTION_INPUT_NAME,
          CommonRepositorySchemaConstants.TABLE_SQ_JOB_INPUT_NAME,
          CommonRepositorySchemaConstants.TABLE_SQ_SUBMISSION_NAME,
          CommonRepositorySchemaConstants.TABLE_SQ_COUNTER_SUBMISSION_NAME
      }) {
        fetchUnnamedConstraintsStmt.setString(1, tableName);

        if (!autoConstraintNameMap.containsKey(tableName)) {
          autoConstraintNameMap.put(tableName, new ArrayList<String>());
        }

        List<String> autoConstraintNames = autoConstraintNameMap.get(tableName);

        if (fetchUnnamedConstraintsStmt.execute()) {
          LOG.info("QUERY(" + STMT_FETCH_TABLE_FOREIGN_KEYS + ") with args: [" + tableName + "]");
          ResultSet rs = fetchUnnamedConstraintsStmt.getResultSet();

          while (rs.next()) {
            autoConstraintNames.add(rs.getString(1));
          }

          rs.close();
        }
      }
    } catch (SQLException e) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0000, e);
    } finally {
      closeStatements(fetchUnnamedConstraintsStmt);
    }

    // Drop constraints
    for (String tableName : autoConstraintNameMap.keySet()) {
      for (String constraintName : autoConstraintNameMap.get(tableName)) {
        String query = DerbySchemaUpgradeQuery.getDropConstraintQuery(
            SCHEMA_SQOOP, tableName, constraintName);
        try {
          dropUnnamedConstraintsStmt = conn.createStatement();
          dropUnnamedConstraintsStmt.execute(query);
        } catch (SQLException e) {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0000, e);
        } finally {
          LOG.info("QUERY(" + query + ")");
          closeStatements(dropUnnamedConstraintsStmt);
        }
      }
    }

    // Create named constraints
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQF_SQC, conn);
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQI_SQF, conn);
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQN_SQC, conn);
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQB_SQN, conn);
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQNI_SQN, conn);
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQNI_SQI, conn);
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQBI_SQB, conn);
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQBI_SQI, conn);
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQS_SQB, conn);
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQRS_SQG, conn);
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQRS_SQR, conn);
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_ADD_CONSTRAINT_SQRS_SQS, conn);
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
    runQuery(STMT_DELETE_SYSTEM, conn, CommonRepoConstants.SYSKEY_VERSION);
    runQuery(STMT_INSERT_SYSTEM, conn, CommonRepoConstants.SYSKEY_VERSION, ""
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
          throw new SqoopException(DerbyRepoError.DERBYREPO_0007, "Could not add directions FROM and TO.");
        }

        ResultSet directionId = insertDirectionStmt.getGeneratedKeys();
        if (directionId.next()) {
          if (LOG.isInfoEnabled()) {
            LOG.info("Loaded direction: " + directionId.getLong(1));
          }

          directionMap.put(direction, directionId.getLong(1));
        } else {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0008, "Could not get ID of direction " + direction);
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
   * Since Sqoop is no longer responsible for HDFS,
   * the HDFS connector/link must be added.
   * NOTE: Connector configs should have a direction (FROM/TO),
   * but link configs should not.
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
   * 6. Set 'fromJobConfig' and 'toJobConfig' configs for HDFS connector.
   * 7. Add 'linkConfig' and 'linkConfig.uri' to the configs for the hdfs
   * 8. Remove the EXPORT throttling config and ensure all of
   *    its dependencies point to the IMPORT throttling config.
   *    Then make sure the throttling config does not have a direction.
   * 9. Create an HDFS link to reference and update
   *    jobs to reference that link. IMPORT jobs
   *    should have TO HDFS connector, EXPORT jobs should have
   *    FROM HDFS connector.
   * 10. Update 'table' config names to 'fromJobConfig' and 'toJobConfig'.
   *     Also update the relevant inputs as well.
   * @param conn
   * @param hdfsConnectorId
   */
  // NOTE: This upgrade code happened before the SQOOP-1498 renaming, hence it uses the form/connection
  // tables instead of the latest config/link tables
  @Deprecated
  private void updateJobConfigInputForHdfsConnector(Connection conn, long hdfsConnectorId) {

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
        new Long(hdfsConnectorId), "input", "output");
    //update the names of the configs
    // 1. input ==> fromJobConfig
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_CONNECTOR_HDFS_FORM_NAME, conn,
       "fromJobConfig",
        "input");
    // 2. output ===> toJobConfig
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_CONNECTOR_HDFS_FORM_NAME, conn,
        "toJobConfig",
        "output");

    // create the link config
    Long linkFormId = createHdfsLinkForm(conn, hdfsConnectorId);
    // create the link config input
    runQuery(STMT_INSERT_INTO_INPUT_WITH_FORM, conn, "linkConfig.uri", linkFormId, 0, MInputType.STRING.name(), false, 255, null);

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

    Long connectionId = createHdfsConnection(conn, hdfsConnectorId);
    runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_SQB_TO_CONNECTION_COPY_SQB_FROM_CONNECTION, conn,
        "EXPORT");
    runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_SQB_FROM_CONNECTION, conn,
        new Long(connectionId), "EXPORT");
    runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_SQB_TO_CONNECTION, conn,
        new Long(connectionId), "IMPORT");

    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_TABLE_FROM_JOB_INPUT_NAMES, conn,
        "fromJobConfig", "fromJobConfig", Direction.FROM.toString());
    runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_UPDATE_TABLE_TO_JOB_INPUT_NAMES, conn,
        "toJobConfig", "toJobConfig", Direction.TO.toString());
  }

  // NOTE: This upgrade code happened after the SQOOP-1498 renaming, hence it
  // uses the configurable and config
  @Deprecated
  private void updateDriverConfigInput(Connection conn, long driverId) {

    // update configs and inputs for driver
    // update the name from throttling ==> throttlingConfig config and associate
    // it with the driverId
    runQuery(QUERY_UPGRADE_TABLE_SQ_CONFIG_NAME_FOR_DRIVER, conn,
        "throttlingConfig", "throttling");

    runQuery(QUERY_UPGRADE_TABLE_SQ_CONFIG_CONFIGURABLE_ID_FOR_DRIVER, conn,
        driverId, "throttlingConfig");

    // nuke security.maxConnections
    runQuery(QUERY_UPGRADE_TABLE_SQ_INPUT_REMOVE_SECURITY_CONFIG_INPUT_FOR_DRIVER, conn,
        "security.maxConnections");

    // nuke the security config since 1.99.3 we do not use it
    runQuery(QUERY_UPGRADE_TABLE_SQ_CONFIG_REMOVE_SECURITY_CONFIG_FOR_DRIVER, conn,
        "security");

    // update throttling.extractors ==> throttlingConfig.numExtractors
    runQuery(QUERY_UPGRADE_TABLE_SQ_INPUT_UPDATE_CONFIG_INPUT_FOR_DRIVER, conn,
         "throttlingConfig.numExtractors", "throttling.extractors");

   // update throttling.loaders ==> throttlingConfig.numLoaders
    runQuery(QUERY_UPGRADE_TABLE_SQ_INPUT_UPDATE_CONFIG_INPUT_FOR_DRIVER, conn,
         "throttlingConfig.numLoaders", "throttling.loaders");

  }

  /**
   * Pre-register Driver since the 1.99.3 release NOTE: This should be used only
   * in the upgrade path
   */
  @Deprecated
  protected long registerDriver(Connection conn) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Begin Driver loading.");
    }

    PreparedStatement baseDriverStmt = null;
    try {
      baseDriverStmt = conn.prepareStatement(STMT_INSERT_INTO_CONFIGURABLE,
          Statement.RETURN_GENERATED_KEYS);
      baseDriverStmt.setString(1, MDriver.DRIVER_NAME);
      baseDriverStmt.setString(2, Driver.getClassName());
      baseDriverStmt.setString(3, "1");
      baseDriverStmt.setString(4, MConfigurableType.DRIVER.name());

      int baseDriverCount = baseDriverStmt.executeUpdate();
      if (baseDriverCount != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0003, Integer.toString(baseDriverCount));
      }

      ResultSet rsetDriverId = baseDriverStmt.getGeneratedKeys();

      if (!rsetDriverId.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0004);
      }
      return rsetDriverId.getLong(1);
    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0009, ex);
    } finally {
      closeStatements(baseDriverStmt);
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
      PreparedStatement baseConnectorStmt = null;
      if (handler.getUniqueName().equals(CONNECTOR_HDFS)) {
        try {
          baseConnectorStmt = conn.prepareStatement(
              STMT_INSERT_INTO_CONNECTOR_WITHOUT_SUPPORTED_DIRECTIONS,
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
          throw new SqoopException(DerbyRepoError.DERBYREPO_0004);
        } finally {
          closeStatements(baseConnectorStmt);
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
        throw new SqoopException(DerbyRepoError.DERBYREPO_0003,
            Integer.toString(result));
      }
      ResultSet rsetConnectionId = stmt.getGeneratedKeys();

      if (!rsetConnectionId.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0004);
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("Created HDFS connection.");
      }

      return rsetConnectionId.getLong(1);
    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0005, ex);
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * We are creating the LINK FORM for HDFS and later it the schema will
   * be renamed to LINK CONFIG
   * NOTE: Should be used only in the upgrade path!
   */
  @Deprecated
  private Long createHdfsLinkForm(Connection conn, Long connectorId) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Creating HDFS link.");
    }

    PreparedStatement stmt = null;
    int result;
    try {
      short index = 0;
      stmt = conn.prepareStatement(STMT_INSERT_INTO_FORM, Statement.RETURN_GENERATED_KEYS);
      stmt.setLong(1, connectorId);
      stmt.setString(2, "linkConfig");
      // it could also be set to the deprecated "CONNECTION"
      stmt.setString(3, MConfigType.LINK.name());
      stmt.setShort(4, index);
      result = stmt.executeUpdate();
      if (result != 1) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0003, Integer.toString(result));
      }
      ResultSet rsetFormId = stmt.getGeneratedKeys();

      if (!rsetFormId.next()) {
        throw new SqoopException(DerbyRepoError.DERBYREPO_0004);
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("Created HDFS connector link FORM.");
      }
      return rsetFormId.getLong(1);
    } catch (SQLException ex) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0005, ex);
    } finally {
      closeStatements(stmt);
    }
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
  public boolean isRepositorySuitableForUse(Connection conn) {
    // TODO(jarcec): Verify that all structures are present (e.g. something like corruption validation)
    // NOTE: At this point is is just checking if the repo version matches the version
    // in the upgraded code
    return detectRepositoryVersion(conn) == DerbyRepoConstants.LATEST_DERBY_REPOSITORY_VERSION;
  }
}