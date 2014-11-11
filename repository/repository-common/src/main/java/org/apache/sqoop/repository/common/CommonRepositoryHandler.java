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
package org.apache.sqoop.repository.common;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.DirectionError;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.SupportedDirections;
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
import org.apache.sqoop.repository.JdbcRepositoryHandler;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Set of methods required from each JDBC based repository.
 */
public abstract class CommonRepositoryHandler extends JdbcRepositoryHandler {

  private static final Logger LOG =
      Logger.getLogger(CommonRepositoryHandler.class);

  /**
   * Name to be used during logging
   *
   * @return String
   */
  public abstract String name();

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
      connectorFetchStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_FROM_CONFIGURABLE);
      connectorFetchStmt.setString(1, shortName);

      List<MConnector> connectors = loadConnectors(connectorFetchStmt, conn);

      if (connectors.size() == 0) {
        LOG.debug("No connector found by name: " + shortName);
        return null;
      } else if (connectors.size() == 1) {
        LOG.debug("Looking up connector: " + shortName + ", found: " + mc);
        return connectors.get(0);
      } else {
        throw new SqoopException(CommonRepositoryError.COMMON_0002, shortName);
      }

    } catch (SQLException ex) {
      logException(ex, shortName);
      throw new SqoopException(CommonRepositoryError.COMMON_0001, shortName, ex);
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
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_CONFIGURABLE_ALL_FOR_TYPE);
      stmt.setString(1, MConfigurableType.CONNECTOR.name());

      return loadConnectors(stmt, conn);
    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(CommonRepositoryError.COMMON_0041, ex);
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerConnector(MConnector mc, Connection conn) {
    if (mc.hasPersistenceId()) {
      throw new SqoopException(CommonRepositoryError.COMMON_0008,
          mc.getUniqueName());
    }
    mc.setPersistenceId(insertAndGetConnectorId(mc, conn));
    insertConfigsForConnector(mc, conn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MJob> findJobsForConnector(long connectorId, Connection conn) {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_ALL_JOBS_FOR_CONNECTOR_CONFIGURABLE);
      stmt.setLong(1, connectorId);
      stmt.setLong(2, connectorId);
      return loadJobs(stmt, conn);

    } catch (SQLException ex) {
      logException(ex, connectorId);
      throw new SqoopException(CommonRepositoryError.COMMON_0028, ex);
    } finally {
      closeStatements(stmt);
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
      updateConnectorStatement = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_UPDATE_CONFIGURABLE);
      deleteInput = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_DELETE_INPUTS_FOR_CONFIGURABLE);
      deleteConfig = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_DELETE_CONFIGS_FOR_CONFIGURABLE);
      updateConnectorStatement.setString(1, mConnector.getUniqueName());
      updateConnectorStatement.setString(2, mConnector.getClassName());
      updateConnectorStatement.setString(3, mConnector.getVersion());
      updateConnectorStatement.setString(4, mConnector.getType().name());
      updateConnectorStatement.setLong(5, mConnector.getPersistenceId());

      if (updateConnectorStatement.executeUpdate() != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0035);
      }
      deleteInput.setLong(1, mConnector.getPersistenceId());
      deleteConfig.setLong(1, mConnector.getPersistenceId());
      deleteInput.executeUpdate();
      deleteConfig.executeUpdate();

    } catch (SQLException e) {
      logException(e, mConnector);
      throw new SqoopException(CommonRepositoryError.COMMON_0035, e);
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
      updateDriverStatement = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_UPDATE_CONFIGURABLE);
      deleteInput = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_DELETE_INPUTS_FOR_CONFIGURABLE);
      deleteConfig = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_DELETE_CONFIGS_FOR_CONFIGURABLE);
      updateDriverStatement.setString(1, mDriver.getUniqueName());
      updateDriverStatement.setString(2, Driver.getClassName());
      updateDriverStatement.setString(3, mDriver.getVersion());
      updateDriverStatement.setString(4, mDriver.getType().name());
      updateDriverStatement.setLong(5, mDriver.getPersistenceId());

      if (updateDriverStatement.executeUpdate() != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0035);
      }
      deleteInput.setLong(1, mDriver.getPersistenceId());
      deleteConfig.setLong(1, mDriver.getPersistenceId());
      deleteInput.executeUpdate();
      deleteConfig.executeUpdate();

    } catch (SQLException e) {
      logException(e, mDriver);
      throw new SqoopException(CommonRepositoryError.COMMON_0040, e);
    } finally {
      closeStatements(updateDriverStatement, deleteConfig, deleteInput);
    }
  }

  /**
   * Helper method to insert the configs from the  into the
   * repository.
   *
   * @param mDriver The driver instance to use to upgrade.
   * @param conn    JDBC link to use for updating the configs
   */
  private void insertConfigsForDriver(MDriver mDriver, Connection conn) {
    PreparedStatement baseConfigStmt = null;
    PreparedStatement baseInputStmt = null;
    try {
      baseConfigStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_INTO_CONFIG,
          Statement.RETURN_GENERATED_KEYS);

      baseInputStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_INTO_INPUT,
          Statement.RETURN_GENERATED_KEYS);

      // Register the job config type, since driver config is per job
      registerConfigs(null, null, mDriver.getDriverConfig().getConfigs(),
          MConfigType.JOB.name(), baseConfigStmt, baseInputStmt, conn);

    } catch (SQLException ex) {
      throw new SqoopException(CommonRepositoryError.COMMON_0011, mDriver.toString(), ex);
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
      driverFetchStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_FROM_CONFIGURABLE);
      driverFetchStmt.setString(1, shortName);

      ResultSet rsDriverSet = driverFetchStmt.executeQuery();
      if (!rsDriverSet.next()) {
        return null;
      }
      Long driverId = rsDriverSet.getLong(1);
      String driverVersion = rsDriverSet.getString(4);

      driverConfigFetchStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_CONFIG_FOR_CONFIGURABLE);
      driverConfigFetchStmt.setLong(1, driverId);

      driverConfigInputFetchStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_INPUT);
      List<MConfig> driverConfigs = new ArrayList<MConfig>();
      loadDriverConfigs(driverConfigs, driverConfigFetchStmt, driverConfigInputFetchStmt, 1);

      if (driverConfigs.isEmpty()) {
        return null;
      }
      mDriver = new MDriver(new MDriverConfig(driverConfigs), driverVersion);
      mDriver.setPersistenceId(driverId);

    } catch (SQLException ex) {
      throw new SqoopException(CommonRepositoryError.COMMON_0001, "Driver", ex);
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
  public void registerDriver(MDriver mDriver, Connection conn) {
    if (mDriver.hasPersistenceId()) {
      throw new SqoopException(CommonRepositoryError.COMMON_0008, mDriver.getUniqueName());
    }
    mDriver.setPersistenceId(insertAndGetDriverId(mDriver, conn));
    insertConfigsforDriver(mDriver, conn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createLink(MLink link, Connection conn) {
    PreparedStatement stmt = null;
    int result;
    try {
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_LINK,
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
        throw new SqoopException(CommonRepositoryError.COMMON_0009,
            Integer.toString(result));
      }

      ResultSet rsetConnectionId = stmt.getGeneratedKeys();

      if (!rsetConnectionId.next()) {
        throw new SqoopException(CommonRepositoryError.COMMON_0010);
      }

      long connectionId = rsetConnectionId.getLong(1);

      createInputValues(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_LINK_INPUT,
          connectionId,
          link.getConnectorLinkConfig().getConfigs(),
          conn);
      link.setPersistenceId(connectionId);

    } catch (SQLException ex) {
      logException(ex, link);
      throw new SqoopException(CommonRepositoryError.COMMON_0016, ex);
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
      deleteStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_DELETE_LINK_INPUT);
      deleteStmt.setLong(1, link.getPersistenceId());
      deleteStmt.executeUpdate();

      // Update LINK_CONFIG table
      updateStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_UPDATE_LINK);
      updateStmt.setString(1, link.getName());
      updateStmt.setString(2, link.getLastUpdateUser());
      updateStmt.setTimestamp(3, new Timestamp(new Date().getTime()));

      updateStmt.setLong(4, link.getPersistenceId());
      updateStmt.executeUpdate();

      // And reinsert new values
      createInputValues(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_LINK_INPUT,
          link.getPersistenceId(),
          link.getConnectorLinkConfig().getConfigs(),
          conn);

    } catch (SQLException ex) {
      logException(ex, link);
      throw new SqoopException(CommonRepositoryError.COMMON_0018, ex);
    } finally {
      closeStatements(deleteStmt, updateStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean existsLink(long linkId, Connection conn) {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_LINK_CHECK_BY_ID);
      stmt.setLong(1, linkId);
      rs = stmt.executeQuery();

      // Should be always valid in query with count
      rs.next();

      return rs.getLong(1) == 1;
    } catch (SQLException ex) {
      logException(ex, linkId);
      throw new SqoopException(CommonRepositoryError.COMMON_0022, ex);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean inUseLink(long linkId, Connection conn) {
    PreparedStatement stmt = null;
    ResultSet rs = null;

    try {
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_JOBS_FOR_LINK_CHECK);
      stmt.setLong(1, linkId);
      rs = stmt.executeQuery();

      // Should be always valid in case of count(*) query
      rs.next();

      return rs.getLong(1) != 0;

    } catch (SQLException e) {
      logException(e, linkId);
      throw new SqoopException(CommonRepositoryError.COMMON_0029, e);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void enableLink(long linkId, boolean enabled, Connection conn) {
    PreparedStatement enableConn = null;

    try {
      enableConn = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_ENABLE_LINK);
      enableConn.setBoolean(1, enabled);
      enableConn.setLong(2, linkId);
      enableConn.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, linkId);
      throw new SqoopException(CommonRepositoryError.COMMON_0038, ex);
    } finally {
      closeStatements(enableConn);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteLink(long linkId, Connection conn) {
    PreparedStatement dltConn = null;

    try {
      deleteLinkInputs(linkId, conn);
      dltConn = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_DELETE_LINK);
      dltConn.setLong(1, linkId);
      dltConn.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, linkId);
      throw new SqoopException(CommonRepositoryError.COMMON_0019, ex);
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
      dltConnInput = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_DELETE_LINK_INPUT);
      dltConnInput.setLong(1, id);
      dltConnInput.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, id);
      throw new SqoopException(CommonRepositoryError.COMMON_0019, ex);
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
      linkFetchStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_LINK_SINGLE);
      linkFetchStmt.setLong(1, linkId);

      List<MLink> links = loadLinks(linkFetchStmt, conn);

      if (links.size() != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0021, "Couldn't find"
            + " link with id " + linkId);
      }

      // Return the first and only one link object with the given id
      return links.get(0);

    } catch (SQLException ex) {
      logException(ex, linkId);
      throw new SqoopException(CommonRepositoryError.COMMON_0020, ex);
    } finally {
      closeStatements(linkFetchStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MLink findLink(String linkName, Connection conn) {
    PreparedStatement linkFetchStmt = null;
    try {
      linkFetchStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_LINK_SINGLE_BY_NAME);
      linkFetchStmt.setString(1, linkName);

      List<MLink> links = loadLinks(linkFetchStmt, conn);

      if (links.size() != 1) {
        return null;
      }

      // Return the first and only one link object with the given name
      return links.get(0);

    } catch (SQLException ex) {
      logException(ex, linkName);
      throw new SqoopException(CommonRepositoryError.COMMON_0020, ex);
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
      linksFetchStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_LINK_ALL);

      return loadLinks(linksFetchStmt, conn);

    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(CommonRepositoryError.COMMON_0020, ex);
    } finally {
      closeStatements(linksFetchStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MLink> findLinksForConnector(long connectorId, Connection conn) {
    PreparedStatement linkByConnectorFetchStmt = null;
    try {
      linkByConnectorFetchStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_LINK_FOR_CONNECTOR_CONFIGURABLE);
      linkByConnectorFetchStmt.setLong(1, connectorId);
      return loadLinks(linkByConnectorFetchStmt, conn);
    } catch (SQLException ex) {
      logException(ex, connectorId);
      throw new SqoopException(CommonRepositoryError.COMMON_0020, ex);
    } finally {
      closeStatements(linkByConnectorFetchStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void createJob(MJob job, Connection conn) {
    PreparedStatement stmt = null;
    int result;
    try {
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_JOB, Statement.RETURN_GENERATED_KEYS);
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
        throw new SqoopException(CommonRepositoryError.COMMON_0009,
            Integer.toString(result));
      }

      ResultSet rsetJobId = stmt.getGeneratedKeys();

      if (!rsetJobId.next()) {
        throw new SqoopException(CommonRepositoryError.COMMON_0010);
      }

      long jobId = rsetJobId.getLong(1);

      // from config for the job
      createInputValues(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_JOB_INPUT,
          jobId,
          job.getJobConfig(Direction.FROM).getConfigs(),
          conn);
      // to config for the job
      createInputValues(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_JOB_INPUT,
          jobId,
          job.getJobConfig(Direction.TO).getConfigs(),
          conn);
      // driver config per job
      createInputValues(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_JOB_INPUT,
          jobId,
          job.getDriverConfig().getConfigs(),
          conn);

      job.setPersistenceId(jobId);

    } catch (SQLException ex) {
      logException(ex, job);
      throw new SqoopException(CommonRepositoryError.COMMON_0023, ex);
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
      deleteStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_DELETE_JOB_INPUT);
      deleteStmt.setLong(1, job.getPersistenceId());
      deleteStmt.executeUpdate();

      // Update job table
      updateStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_UPDATE_JOB);
      updateStmt.setString(1, job.getName());
      updateStmt.setString(2, job.getLastUpdateUser());
      updateStmt.setTimestamp(3, new Timestamp(new Date().getTime()));

      updateStmt.setLong(4, job.getPersistenceId());
      updateStmt.executeUpdate();

      // And reinsert new values
      createInputValues(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_JOB_INPUT,
          job.getPersistenceId(),
          job.getJobConfig(Direction.FROM).getConfigs(),
          conn);
      createInputValues(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_JOB_INPUT,
          job.getPersistenceId(),
          job.getJobConfig(Direction.TO).getConfigs(),
          conn);
      createInputValues(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_JOB_INPUT,
          job.getPersistenceId(),
          job.getDriverConfig().getConfigs(),
          conn);

    } catch (SQLException ex) {
      logException(ex, job);
      throw new SqoopException(CommonRepositoryError.COMMON_0024, ex);
    } finally {
      closeStatements(deleteStmt, updateStmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean existsJob(long jobId, Connection conn) {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_JOB_CHECK_BY_ID);
      stmt.setLong(1, jobId);
      rs = stmt.executeQuery();

      // Should be always valid in query with count
      rs.next();

      return rs.getLong(1) == 1;
    } catch (SQLException ex) {
      logException(ex, jobId);
      throw new SqoopException(CommonRepositoryError.COMMON_0026, ex);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean inUseJob(long jobId, Connection conn) {
    MSubmission submission = findLastSubmissionForJob(jobId, conn);

    // We have no submissions and thus job can't be in use
    if (submission == null) {
      return false;
    }

    // We can't remove running job
    if (submission.getStatus().isRunning()) {
      return true;
    }

    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void enableJob(long jobId, boolean enabled, Connection conn) {
    PreparedStatement enableConn = null;

    try {
      enableConn = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_ENABLE_JOB);
      enableConn.setBoolean(1, enabled);
      enableConn.setLong(2, jobId);
      enableConn.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, jobId);
      throw new SqoopException(CommonRepositoryError.COMMON_0039, ex);
    } finally {
      closeStatements(enableConn);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteJobInputs(long id, Connection conn) {
    PreparedStatement dltInput = null;
    try {
      dltInput = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_DELETE_JOB_INPUT);
      dltInput.setLong(1, id);
      dltInput.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, id);
      throw new SqoopException(CommonRepositoryError.COMMON_0025, ex);
    } finally {
      closeStatements(dltInput);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteJob(long jobId, Connection conn) {
    PreparedStatement dlt = null;
    try {
      deleteJobInputs(jobId, conn);
      dlt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_DELETE_JOB);
      dlt.setLong(1, jobId);
      dlt.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, jobId);
      throw new SqoopException(CommonRepositoryError.COMMON_0025, ex);
    } finally {
      closeStatements(dlt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MJob findJob(long jobId, Connection conn) {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_JOB_SINGLE_BY_ID);
      stmt.setLong(1, jobId);

      List<MJob> jobs = loadJobs(stmt, conn);

      if (jobs.size() != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0027, "Couldn't find"
            + " job with id " + jobId);
      }

      // Return the first and only one link object
      return jobs.get(0);

    } catch (SQLException ex) {
      logException(ex, jobId);
      throw new SqoopException(CommonRepositoryError.COMMON_0028, ex);
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
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_JOB_SINGLE_BY_NAME);
      stmt.setString(1, name);

      List<MJob> jobs = loadJobs(stmt, conn);

      if (jobs.size() != 1) {
        return null;
      }

      // Return the first and only one link object
      return jobs.get(0);

    } catch (SQLException ex) {
      logException(ex, name);
      throw new SqoopException(CommonRepositoryError.COMMON_0028, ex);
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
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_JOB);

      return loadJobs(stmt, conn);

    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(CommonRepositoryError.COMMON_0028, ex);
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
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_SUBMISSION,
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
        throw new SqoopException(CommonRepositoryError.COMMON_0009,
            Integer.toString(result));
      }

      ResultSet rsetSubmissionId = stmt.getGeneratedKeys();

      if (!rsetSubmissionId.next()) {
        throw new SqoopException(CommonRepositoryError.COMMON_0010);
      }

      long submissionId = rsetSubmissionId.getLong(1);

      if(submission.getCounters() != null) {
        createSubmissionCounters(submissionId, submission.getCounters(), conn);
      }

      // Save created persistence id
      submission.setPersistenceId(submissionId);

    } catch (SQLException ex) {
      logException(ex, submission);
      throw new SqoopException(CommonRepositoryError.COMMON_0031, ex);
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
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_SUBMISSION_CHECK);
      stmt.setLong(1, submissionId);
      rs = stmt.executeQuery();

      // Should be always valid in query with count
      rs.next();

      return rs.getLong(1) == 1;
    } catch (SQLException ex) {
      logException(ex, submissionId);
      throw new SqoopException(CommonRepositoryError.COMMON_0030, ex);
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
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_UPDATE_SUBMISSION);
      stmt.setString(1, submission.getStatus().name());
      stmt.setString(2, submission.getLastUpdateUser());
      stmt.setTimestamp(3, new Timestamp(submission.getLastUpdateDate().getTime()));
      stmt.setString(4, submission.getExceptionInfo());
      stmt.setString(5, submission.getExceptionStackTrace());

      stmt.setLong(6, submission.getPersistenceId());
      stmt.executeUpdate();

      // Delete previous counters
      deleteStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_DELETE_COUNTER_SUBMISSION);
      deleteStmt.setLong(1, submission.getPersistenceId());
      deleteStmt.executeUpdate();

      // Reinsert new counters if needed
      if(submission.getCounters() != null) {
        createSubmissionCounters(submission.getPersistenceId(), submission.getCounters(), conn);
      }

    } catch (SQLException ex) {
      logException(ex, submission);
      throw new SqoopException(CommonRepositoryError.COMMON_0032, ex);
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
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_PURGE_SUBMISSIONS);
      stmt.setTimestamp(1, new Timestamp(threshold.getTime()));
      stmt.executeUpdate();

    } catch (SQLException ex) {
      logException(ex, threshold);
      throw new SqoopException(CommonRepositoryError.COMMON_0033, ex);
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MSubmission> findUnfinishedSubmissions(Connection conn) {
    List<MSubmission> submissions = new LinkedList<MSubmission>();
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_SUBMISSION_UNFINISHED);

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
      throw new SqoopException(CommonRepositoryError.COMMON_0034, ex);
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
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_SUBMISSIONS);
      rs = stmt.executeQuery();

      while(rs.next()) {
        submissions.add(loadSubmission(rs, conn));
      }

      rs.close();
      rs = null;
    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(CommonRepositoryError.COMMON_0036, ex);
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
  public List<MSubmission> findSubmissionsForJob(long jobId, Connection conn) {
    List<MSubmission> submissions = new LinkedList<MSubmission>();
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_SUBMISSIONS_FOR_JOB);
      stmt.setLong(1, jobId);
      rs = stmt.executeQuery();

      while(rs.next()) {
        submissions.add(loadSubmission(rs, conn));
      }

      rs.close();
      rs = null;
    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(CommonRepositoryError.COMMON_0037, ex);
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
  public MSubmission findLastSubmissionForJob(long jobId, Connection conn) {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_SUBMISSIONS_FOR_JOB);
      stmt.setLong(1, jobId);
      stmt.setMaxRows(1);
      rs = stmt.executeQuery();

      if(!rs.next()) {
        return null;
      }

      return loadSubmission(rs, conn);
    } catch (SQLException ex) {
      logException(ex, jobId);
      throw new SqoopException(CommonRepositoryError.COMMON_0037, ex);
    } finally {
      closeResultSets(rs);
      closeStatements(stmt);
    }
  }

  private void insertConnectorDirection(Long connectorId, Direction direction, Connection conn)
      throws SQLException {
    PreparedStatement stmt = null;

    try {
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_SQ_CONNECTOR_DIRECTIONS);
      stmt.setLong(1, connectorId);
      stmt.setLong(2, getDirection(direction, conn));

      if (stmt.executeUpdate() != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0043);
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
      baseConnectorStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_INTO_CONFIGURABLE,
          Statement.RETURN_GENERATED_KEYS);
      baseConnectorStmt.setString(1, mc.getUniqueName());
      baseConnectorStmt.setString(2, mc.getClassName());
      baseConnectorStmt.setString(3, mc.getVersion());
      baseConnectorStmt.setString(4, mc.getType().name());

      int baseConnectorCount = baseConnectorStmt.executeUpdate();
      if (baseConnectorCount != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0009,
            Integer.toString(baseConnectorCount));
      }

      ResultSet rsetConnectorId = baseConnectorStmt.getGeneratedKeys();

      if (!rsetConnectorId.next()) {
        throw new SqoopException(CommonRepositoryError.COMMON_0010);
      }
      // connector configurable also have directions
      insertConnectorDirections(rsetConnectorId.getLong(1), mc.getSupportedDirections(), conn);
      return rsetConnectorId.getLong(1);
    } catch (SQLException ex) {
      throw new SqoopException(CommonRepositoryError.COMMON_0011, mc.toString(), ex);
    } finally {
      closeStatements(baseConnectorStmt);
    }
  }

  /**
   * Helper method to insert the configs from the MConnector into the
   * repository. The job and connector configs within <code>mc</code> will get
   * updated with the id of the configs when this function returns.
   * @param mc The connector to use for updating configs
   * @param conn JDBC connection to use for inserting the configs
   */
  private void insertConfigsForConnector(MConnector mc, Connection conn) {
    long connectorId = mc.getPersistenceId();
    PreparedStatement baseConfigStmt = null;
    PreparedStatement baseInputStmt = null;
    try{
      baseConfigStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_INTO_CONFIG,
          Statement.RETURN_GENERATED_KEYS);

      baseInputStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_INTO_INPUT,
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
      throw new SqoopException(CommonRepositoryError.COMMON_0011,
          mc.toString(), ex);
    } finally {
      closeStatements(baseConfigStmt, baseInputStmt);
    }
  }

  private long insertAndGetDriverId(MDriver mDriver, Connection conn) {
    PreparedStatement baseDriverStmt = null;
    try {
      baseDriverStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_INTO_CONFIGURABLE,
          Statement.RETURN_GENERATED_KEYS);
      baseDriverStmt.setString(1, mDriver.getUniqueName());
      baseDriverStmt.setString(2, Driver.getClassName());
      baseDriverStmt.setString(3, mDriver.getVersion());
      baseDriverStmt.setString(4, mDriver.getType().name());

      int baseDriverCount = baseDriverStmt.executeUpdate();
      if (baseDriverCount != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0009, Integer.toString(baseDriverCount));
      }

      ResultSet rsetDriverId = baseDriverStmt.getGeneratedKeys();

      if (!rsetDriverId.next()) {
        throw new SqoopException(CommonRepositoryError.COMMON_0010);
      }
      return rsetDriverId.getLong(1);
    } catch (SQLException ex) {
      throw new SqoopException(CommonRepositoryError.COMMON_0044, mDriver.toString(), ex);
    } finally {
      closeStatements(baseDriverStmt);
    }
  }

  private void insertConfigsforDriver(MDriver mDriver, Connection conn) {
    PreparedStatement baseConfigStmt = null;
    PreparedStatement baseInputStmt = null;
    try {
      baseConfigStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_INTO_CONFIG,
          Statement.RETURN_GENERATED_KEYS);
      baseInputStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_INTO_INPUT,
          Statement.RETURN_GENERATED_KEYS);

      // Register a driver config as a job type with no owner/connector and direction
      registerConfigs(mDriver.getPersistenceId(), null /* no direction*/, mDriver.getDriverConfig().getConfigs(),
          MConfigType.JOB.name(), baseConfigStmt, baseInputStmt, conn);

    } catch (SQLException ex) {
      logException(ex, mDriver);
      throw new SqoopException(CommonRepositoryError.COMMON_0011, ex);
    } finally {
      closeStatements(baseConfigStmt, baseInputStmt);
    }
  }

  /**
   * Stores counters for given submission in repository.
   *
   * @param submissionId Submission id
   * @param counters Counters that should be stored
   * @param conn Connection to derby repository
   * @throws java.sql.SQLException
   */
  private void createSubmissionCounters(long submissionId, Counters counters, Connection conn) throws SQLException {
    PreparedStatement stmt = null;

    try {
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_COUNTER_SUBMISSION);

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
   * @throws java.sql.SQLException
   */
  private long getCounterGroupId(CounterGroup group, Connection conn) throws SQLException {
    PreparedStatement select = null;
    PreparedStatement insert = null;
    ResultSet rsSelect = null;
    ResultSet rsInsert = null;

    try {
      select = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_COUNTER_GROUP);
      select.setString(1, group.getName());

      rsSelect = select.executeQuery();

      if(rsSelect.next()) {
        return rsSelect.getLong(1);
      }

      insert = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_COUNTER_GROUP, Statement.RETURN_GENERATED_KEYS);
      insert.setString(1, group.getName());
      insert.executeUpdate();

      rsInsert = insert.getGeneratedKeys();

      if (!rsInsert.next()) {
        throw new SqoopException(CommonRepositoryError.COMMON_0010);
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
   * @throws java.sql.SQLException
   */
  private long getCounterId(Counter counter, Connection conn) throws SQLException {
    PreparedStatement select = null;
    PreparedStatement insert = null;
    ResultSet rsSelect = null;
    ResultSet rsInsert = null;

    try {
      select = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_COUNTER);
      select.setString(1, counter.getName());

      rsSelect = select.executeQuery();

      if(rsSelect.next()) {
        return rsSelect.getLong(1);
      }

      insert = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_COUNTER, Statement.RETURN_GENERATED_KEYS);
      insert.setString(1, counter.getName());
      insert.executeUpdate();

      rsInsert = insert.getGeneratedKeys();

      if (!rsInsert.next()) {
        throw new SqoopException(CommonRepositoryError.COMMON_0010);
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
   * @throws java.sql.SQLException
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
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_COUNTER_SUBMISSION);
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
      directionStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_SQD_ID_BY_SQD_NAME);
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
      directionStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_SQD_NAME_BY_SQD_ID);
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
      connectorDirectionsStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_SQ_CONNECTOR_DIRECTIONS);
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
      connectorConfigFetchStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_CONFIG_FOR_CONFIGURABLE);
      connectorConfigInputFetchStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_INPUT);

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

  private List<MLink> loadLinks(PreparedStatement stmt,
                                Connection conn)
      throws SQLException {
    List<MLink> links = new ArrayList<MLink>();
    ResultSet rsConnection = null;
    PreparedStatement connectorConfigFetchStatement = null;
    PreparedStatement connectorConfigInputStatement = null;

    try {
      rsConnection = stmt.executeQuery();

      //
      connectorConfigFetchStatement = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_CONFIG_FOR_CONFIGURABLE);
      connectorConfigInputStatement = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_FETCH_LINK_INPUT);

      while(rsConnection.next()) {
        long id = rsConnection.getLong(1);
        String name = rsConnection.getString(2);
        long connectorId = rsConnection.getLong(3);
        boolean enabled = rsConnection.getBoolean(4);
        String creationUser = rsConnection.getString(5);
        Date creationDate = rsConnection.getTimestamp(6);
        String updateUser = rsConnection.getString(7);
        Date lastUpdateDate = rsConnection.getTimestamp(8);

        connectorConfigFetchStatement.setLong(1, connectorId);
        connectorConfigInputStatement.setLong(1, id);
        connectorConfigInputStatement.setLong(3, id);

        List<MConfig> connectorLinkConfig = new ArrayList<MConfig>();
        List<MConfig> fromConfig = new ArrayList<MConfig>();
        List<MConfig> toConfig = new ArrayList<MConfig>();

        loadConnectorConfigTypes(connectorLinkConfig, fromConfig, toConfig, connectorConfigFetchStatement,
            connectorConfigInputStatement, 2, conn);
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
      closeStatements(connectorConfigFetchStatement, connectorConfigInputStatement);
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
      fromConfigFetchStmt  = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_CONFIG_FOR_CONFIGURABLE);
      toConfigFetchStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_CONFIG_FOR_CONFIGURABLE);
      driverConfigfetchStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_CONFIG_FOR_CONFIGURABLE);
      jobInputFetchStmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_FETCH_JOB_INPUT);

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
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_INSERT_SQ_CONFIG_DIRECTIONS);
      stmt.setLong(1, configId);
      stmt.setLong(2, getDirection(direction, conn));
      if (stmt.executeUpdate() != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0042);
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
   * @throws java.sql.SQLException
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
        throw new SqoopException(CommonRepositoryError.COMMON_0012,
            Integer.toString(baseConfigCount));
      }
      ResultSet rsetConfigId = baseConfigStmt.getGeneratedKeys();
      if (!rsetConfigId.next()) {
        throw new SqoopException(CommonRepositoryError.COMMON_0013);
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
   * @throws java.sql.SQLException In case of any failure on Derby side
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
        MStringInput strInput = (MStringInput) input;
        baseInputStmt.setShort(6, strInput.getMaxLength());
      } else {
        baseInputStmt.setNull(6, Types.INTEGER);
      }
      // Enum specific column(s)
      if(input.getType() == MInputType.ENUM) {
        baseInputStmt.setString(7, StringUtils.join(((MEnumInput) input).getValues(), ","));
      } else {
        baseInputStmt.setNull(7, Types.VARCHAR);
      }

      int baseInputCount = baseInputStmt.executeUpdate();
      if (baseInputCount != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0014,
            Integer.toString(baseInputCount));
      }

      ResultSet rsetInputId = baseInputStmt.getGeneratedKeys();
      if (!rsetInputId.next()) {
        throw new SqoopException(CommonRepositoryError.COMMON_0015);
      }

      long inputId = rsetInputId.getLong(1);
      input.setPersistenceId(inputId);
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
   * @throws java.sql.SQLException In case of any failure on Derby side
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
            throw new SqoopException(CommonRepositoryError.COMMON_0003,
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
          throw new SqoopException(CommonRepositoryError.COMMON_0006,
              "config: " + mDriverConfig
                  + "; input: " + input
                  + "; index: " + inputIndex
                  + "; expected: " + mDriverConfig.getInputs().size()
          );
        }

        mDriverConfig.getInputs().add(input);
      }

      if (mDriverConfig.getInputs().size() == 0) {
        throw new SqoopException(CommonRepositoryError.COMMON_0005,
            "owner-" + fromConnectorId
                + "; config: " + mDriverConfig
        );
      }

      MConfigType configType = MConfigType.valueOf(configTYpe);
      switch (configType) {
        case JOB:
          if (driverConfig.size() != configIndex) {
            throw new SqoopException(CommonRepositoryError.COMMON_0007,
                "owner-" + fromConnectorId
                    + "; config: " + configType
                    + "; index: " + configIndex
                    + "; expected: " + driverConfig.size()
            );
          }
          driverConfig.add(mDriverConfig);
          break;
        default:
          throw new SqoopException(CommonRepositoryError.COMMON_0004,
              "connector-" + fromConnectorId + ":" + configType);
      }
    }
  }

  private Direction findConfigDirection(long configId, Connection conn) throws SQLException {
    PreparedStatement stmt = null;
    ResultSet rs = null;

    try {
      stmt = conn.prepareStatement(CommonRepositoryInsertUpdateDeleteSelectQuery.STMT_SELECT_SQ_CONFIG_DIRECTIONS);
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
   * Load configs and corresponding inputs related to a connector.
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
   * @throws java.sql.SQLException In case of any failure on Derby side
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
            throw new SqoopException(CommonRepositoryError.COMMON_0003,
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
          throw new SqoopException(CommonRepositoryError.COMMON_0006,
              "config: " + config
                  + "; input: " + input
                  + "; index: " + inputIndex
                  + "; expected: " + config.getInputs().size()
          );
        }

        config.getInputs().add(input);
      }

      if (config.getInputs().size() == 0) {
        throw new SqoopException(CommonRepositoryError.COMMON_0005,
            "connector-" + configConnectorId
                + "; config: " + config
        );
      }

      MConfigType mConfigType = MConfigType.valueOf(configType);
      switch (mConfigType) {
        case LINK:
          if (linkConfig.size() != configIndex) {
            throw new SqoopException(CommonRepositoryError.COMMON_0007,
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
            throw new SqoopException(CommonRepositoryError.COMMON_0007,
                "connector-" + configConnectorId
                    + "; config: " + config
                    + "; index: " + configIndex
                    + "; expected: " + jobConfigs.size()
            );
          }

          jobConfigs.add(config);
          break;
        default:
          throw new SqoopException(CommonRepositoryError.COMMON_0004,
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
            throw new SqoopException(CommonRepositoryError.COMMON_0017,
                Integer.toString(result));
          }
        }
      }
    } finally {
      closeStatements(stmt);
    }
  }

  /**
   * Execute given query on database.
   *
   * @param query Query that should be executed
   */
  protected void runQuery(String query, Connection conn, Object... args) {
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
      throw new SqoopException(CommonRepositoryError.COMMON_0000, query, ex);
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
  protected void closeResultSets(ResultSet ... resultSets) {
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
  protected void closeStatements(Statement... stmts) {
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
  protected void logException(Throwable throwable, Object ...objects) {
    LOG.error("Exception in repository operation", throwable);
    LOG.error("Using database " + name());
    LOG.error("Associated objects: "+ objects.length);
    for(Object object : objects) {
      LOG.error("\t" + object.getClass().getSimpleName() + ": " + object.toString());
    }
  }
}
