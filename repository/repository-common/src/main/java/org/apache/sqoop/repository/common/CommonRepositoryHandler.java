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

import java.sql.Connection;
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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.DirectionError;
import org.apache.sqoop.common.ImmutableContext;
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.SupportedDirections;
import org.apache.sqoop.driver.Driver;
import org.apache.sqoop.error.code.CommonRepositoryError;
import org.apache.sqoop.model.InputEditable;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConfigType;
import org.apache.sqoop.model.MConfigUpdateEntityType;
import org.apache.sqoop.model.MConfigurableType;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDateTimeInput;
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
import org.apache.sqoop.model.MListInput;
import org.apache.sqoop.model.MLongInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.model.SubmissionError;
import org.apache.sqoop.repository.JdbcRepositoryHandler;
import org.apache.sqoop.repository.RepositoryError;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;

/**
 * Set of methods required from each JDBC based repository.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
public abstract class CommonRepositoryHandler extends JdbcRepositoryHandler {

  private static final Logger LOG =
      Logger.getLogger(CommonRepositoryHandler.class);

  protected CommonRepositoryInsertUpdateDeleteSelectQuery crudQueries;

  public CommonRepositoryHandler() {
    crudQueries = new CommonRepositoryInsertUpdateDeleteSelectQuery();
  }

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
  public MConnector findConnector(long connectorId, Connection conn) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking up connector: " + connectorId);
    }
    try (PreparedStatement connectorFetchStmt = conn
        .prepareStatement(crudQueries.getStmtSelectFromConfigurableById())) {
      connectorFetchStmt.setLong(1, connectorId);

      return findConnectorInternal(connectorFetchStmt, conn,
          String.valueOf(connectorId));
    } catch (SQLException ex) {
      logException(ex, connectorId);
      throw new SqoopException(CommonRepositoryError.COMMON_0001,
          String.valueOf(connectorId), ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MConnector findConnector(String shortName, Connection conn) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Looking up connector: " + shortName);
    }

    try (PreparedStatement connectorFetchStmt = conn
        .prepareStatement(crudQueries.getStmtSelectFromConfigurableByName())) {
      connectorFetchStmt.setString(1, shortName);

      return findConnectorInternal(connectorFetchStmt, conn, shortName);
    } catch (SQLException ex) {
      logException(ex, shortName);
      throw new SqoopException(CommonRepositoryError.COMMON_0001, shortName, ex);
    }
  }

  private MConnector findConnectorInternal(PreparedStatement stmt,
      Connection conn, String connectorIdentifier) throws SQLException {
    List<MConnector> connectors = loadConnectors(stmt, conn);

    if (connectors.size() == 0) {
      LOG.debug("Looking up connector: " + connectorIdentifier + ", no connector found");
      return null;
    } else if (connectors.size() == 1) {
      MConnector mc = connectors.get(0);
      LOG.debug("Looking up connector: " + connectorIdentifier + ", found: " + mc);
      return mc;
    } else {
      throw new SqoopException(CommonRepositoryError.COMMON_0002, connectorIdentifier);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MConnector> findConnectors(Connection conn) {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectConfigurableAllForType())) {
      stmt.setString(1, MConfigurableType.CONNECTOR.name());

      return loadConnectors(stmt, conn);
    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(CommonRepositoryError.COMMON_0041, ex);
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
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectAllJobsForConnectorConfigurable())) {

      stmt.setLong(1, connectorId);
      stmt.setLong(2, connectorId);
      return loadJobs(stmt, conn);

    } catch (SQLException ex) {
      logException(ex, connectorId);
      throw new SqoopException(CommonRepositoryError.COMMON_0028, ex);
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
    try (PreparedStatement updateConnectorStatement = conn.prepareStatement(crudQueries.getStmtUpdateConfigurable());
         PreparedStatement deleteInputRelation = conn.prepareStatement(crudQueries
            .getStmtDeleteInputRelationsForInput());
         PreparedStatement deleteInput = conn.prepareStatement(crudQueries.getStmtDeleteInputsForConfigurable());
         PreparedStatement deleteConfigDirection = conn.prepareStatement(crudQueries.getStmtDeleteDirectionsForConfigurable());
         PreparedStatement deleteConfig = conn.prepareStatement(crudQueries.getStmtDeleteConfigsForConfigurable());) {

      updateConnectorStatement.setString(1, mConnector.getUniqueName());
      updateConnectorStatement.setString(2, mConnector.getClassName());
      updateConnectorStatement.setString(3, mConnector.getVersion());
      updateConnectorStatement.setString(4, mConnector.getType().name());
      updateConnectorStatement.setLong(5, mConnector.getPersistenceId());

      if (updateConnectorStatement.executeUpdate() != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0035);
      }
      deleteInputRelation.setLong(1, mConnector.getPersistenceId());
      deleteInput.setLong(1, mConnector.getPersistenceId());
      deleteConfigDirection.setLong(1, mConnector.getPersistenceId());
      deleteConfig.setLong(1, mConnector.getPersistenceId());
      deleteInputRelation.executeUpdate();
      deleteInput.executeUpdate();
      deleteConfigDirection.executeUpdate();
      deleteConfig.executeUpdate();

    } catch (SQLException e) {
      logException(e, mConnector);
      throw new SqoopException(CommonRepositoryError.COMMON_0035, e);
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
    try (PreparedStatement updateDriverStatement = conn.prepareStatement(crudQueries.getStmtUpdateConfigurable());
         PreparedStatement deleteInputRelation = conn.prepareStatement(crudQueries
            .getStmtDeleteInputRelationsForInput());
         PreparedStatement deleteInput = conn.prepareStatement(crudQueries.getStmtDeleteInputsForConfigurable());
         PreparedStatement deleteConfig = conn.prepareStatement(crudQueries.getStmtDeleteConfigsForConfigurable());) {

      updateDriverStatement.setString(1, mDriver.getUniqueName());
      updateDriverStatement.setString(2, Driver.getClassName());
      updateDriverStatement.setString(3, mDriver.getVersion());
      updateDriverStatement.setString(4, mDriver.getType().name());
      updateDriverStatement.setLong(5, mDriver.getPersistenceId());

      if (updateDriverStatement.executeUpdate() != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0035);
      }
      deleteInputRelation.setLong(1, mDriver.getPersistenceId());
      deleteInput.setLong(1, mDriver.getPersistenceId());
      deleteConfig.setLong(1, mDriver.getPersistenceId());
      deleteInputRelation.executeUpdate();
      deleteInput.executeUpdate();
      deleteConfig.executeUpdate();

    } catch (SQLException e) {
      logException(e, mDriver);
      throw new SqoopException(CommonRepositoryError.COMMON_0040, e);
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
    long driverId = mDriver.getPersistenceId();
    try (PreparedStatement baseConfigStmt = conn.prepareStatement(crudQueries.getStmtInsertIntoConfig(),
            Statement.RETURN_GENERATED_KEYS);
         PreparedStatement baseInputStmt = conn.prepareStatement(crudQueries.getStmtInsertIntoInput(),
            Statement.RETURN_GENERATED_KEYS);) {

      // Register a driver config as a job type with no direction
      registerConfigs(driverId, null, mDriver.getDriverConfig().getConfigs(),
          MConfigType.JOB.name(), baseConfigStmt, baseInputStmt, conn);

    } catch (SQLException ex) {
      throw new SqoopException(CommonRepositoryError.COMMON_0011, mDriver.toString(), ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MDriver findDriver(String shortName, Connection conn) {
    LOG.debug("Looking up Driver and config ");
    MDriver mDriver;
    try (PreparedStatement driverFetchStmt = conn.prepareStatement(crudQueries.getStmtSelectFromConfigurableByName());
         PreparedStatement driverConfigFetchStmt = conn.prepareStatement(crudQueries.getStmtSelectConfigForConfigurable());
         PreparedStatement driverConfigInputFetchStmt = conn.prepareStatement(crudQueries.getStmtSelectInput());) {

      driverFetchStmt.setString(1, shortName);

      try (ResultSet rsDriverSet = driverFetchStmt.executeQuery()) {
        if (!rsDriverSet.next()) {
          return null;
        }
        Long driverId = rsDriverSet.getLong(1);
        String driverVersion = rsDriverSet.getString(4);

        driverConfigFetchStmt.setLong(1, driverId);
        List<MConfig> driverConfigs = new ArrayList<MConfig>();
        loadDriverConfigs(driverConfigs, driverConfigFetchStmt, driverConfigInputFetchStmt, 1, conn);

        mDriver = new MDriver(new MDriverConfig(driverConfigs), driverVersion);
        mDriver.setPersistenceId(driverId);
      }
    } catch (SQLException ex) {
      throw new SqoopException(CommonRepositoryError.COMMON_0001, "Driver", ex);
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
    insertConfigsForDriver(mDriver, conn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createLink(MLink link, Connection conn) {
    int result;
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtInsertLink(),
          Statement.RETURN_GENERATED_KEYS)) {

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

      try (ResultSet rsetConnectionId = stmt.getGeneratedKeys()) {

        if (!rsetConnectionId.next()) {
          throw new SqoopException(CommonRepositoryError.COMMON_0010);
        }

        long connectionId = rsetConnectionId.getLong(1);

        createInputValues(
                crudQueries.getStmtInsertLinkInput(),
                connectionId,
                link.getConnectorLinkConfig().getConfigs(),
                conn);
        link.setPersistenceId(connectionId);
      }
    } catch (SQLException ex) {
      logException(ex, link);
      throw new SqoopException(CommonRepositoryError.COMMON_0016, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateLink(MLink link, Connection conn) {
    try (PreparedStatement deleteStmt = conn.prepareStatement(crudQueries.getStmtDeleteLinkInput());
         PreparedStatement updateStmt = conn.prepareStatement(crudQueries.getStmtUpdateLink());) {
      // Firstly remove old values
      deleteStmt.setLong(1, link.getPersistenceId());
      deleteStmt.executeUpdate();

      // Update LINK_CONFIG table
      updateStmt.setString(1, link.getName());
      updateStmt.setString(2, link.getLastUpdateUser());
      updateStmt.setTimestamp(3, new Timestamp(new Date().getTime()));

      updateStmt.setLong(4, link.getPersistenceId());
      updateStmt.executeUpdate();

      // And reinsert new values
      createInputValues(crudQueries.getStmtInsertLinkInput(),
          link.getPersistenceId(),
          link.getConnectorLinkConfig().getConfigs(),
          conn);

    } catch (SQLException ex) {
      logException(ex, link);
      throw new SqoopException(CommonRepositoryError.COMMON_0018, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean existsLink(long linkId, Connection conn) {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectLinkCheckById())) {
      stmt.setLong(1, linkId);
      try (ResultSet rs = stmt.executeQuery()) {

        // Should be always valid in query with count
        rs.next();

        return rs.getLong(1) == 1;
      }
    } catch (SQLException ex) {
      logException(ex, linkId);
      throw new SqoopException(CommonRepositoryError.COMMON_0022, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean inUseLink(long linkId, Connection conn) {

    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectJobsForLinkCheck())) {
      stmt.setLong(1, linkId);
      try (ResultSet rs = stmt.executeQuery()) {

        // Should be always valid in case of count(*) query
        rs.next();

        return rs.getLong(1) != 0;
      }
    } catch (SQLException e) {
      logException(e, linkId);
      throw new SqoopException(CommonRepositoryError.COMMON_0029, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void enableLink(long linkId, boolean enabled, Connection conn) {

    try (PreparedStatement enableConn = conn.prepareStatement(crudQueries.getStmtEnableLink())) {
      enableConn.setBoolean(1, enabled);
      enableConn.setLong(2, linkId);
      enableConn.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, linkId);
      throw new SqoopException(CommonRepositoryError.COMMON_0038, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteLink(long linkId, Connection conn) {
    try (PreparedStatement dltConn = conn.prepareStatement(crudQueries.getStmtDeleteLink())) {
      deleteLinkInputs(linkId, conn);
      dltConn.setLong(1, linkId);
      dltConn.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, linkId);
      throw new SqoopException(CommonRepositoryError.COMMON_0019, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteLinkInputs(long id, Connection conn) {
    try (PreparedStatement dltConnInput = conn.prepareStatement(crudQueries.getStmtDeleteLinkInput())) {
      dltConnInput.setLong(1, id);
      dltConnInput.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, id);
      throw new SqoopException(CommonRepositoryError.COMMON_0019, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MLink findLink(long linkId, Connection conn) {
    try (PreparedStatement linkFetchStmt = conn.prepareStatement(crudQueries
        .getStmtSelectLinkSingleById())) {
      linkFetchStmt.setLong(1, linkId);

      return findLinkInternal(linkFetchStmt, conn, String.valueOf(linkId));
    } catch (SQLException ex) {
      logException(ex, linkId);
      throw new SqoopException(CommonRepositoryError.COMMON_0020, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MLink findLink(String linkName, Connection conn) {
    try (PreparedStatement linkFetchStmt = conn.prepareStatement(crudQueries
        .getStmtSelectLinkSingleByName())) {
      linkFetchStmt.setString(1, linkName);

      return findLinkInternal(linkFetchStmt, conn, linkName);
    } catch (SQLException ex) {
      logException(ex, linkName);
      throw new SqoopException(CommonRepositoryError.COMMON_0020, ex);
    }
  }

  private MLink findLinkInternal(PreparedStatement stmt, Connection conn,
      String linkIdentifier) throws SQLException {
    List<MLink> links = loadLinks(stmt, conn);

    if (links.size() == 0) {
      LOG.debug("Looking up link: " + linkIdentifier + ", no link found");
      return null;
    } else if (links.size() == 1) {
      // Return the first and only one link object with the given name or id
      MLink link = links.get(0);
      LOG.debug("Looking up link: " + linkIdentifier + ", found: " + link);
      return link;
    } else {
      throw new SqoopException(CommonRepositoryError.COMMON_0021,
          linkIdentifier);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MLink> findLinks(Connection conn) {
    try (PreparedStatement linksFetchStmt = conn.prepareStatement(crudQueries.getStmtSelectLinkAll())) {

      return loadLinks(linksFetchStmt, conn);
    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(CommonRepositoryError.COMMON_0020, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MLink> findLinksForConnector(long connectorId, Connection conn) {
    try (PreparedStatement linkByConnectorFetchStmt = conn.prepareStatement(crudQueries.getStmtSelectLinkForConnectorConfigurable())) {

      linkByConnectorFetchStmt.setLong(1, connectorId);
      return loadLinks(linkByConnectorFetchStmt, conn);
    } catch (SQLException ex) {
      logException(ex, connectorId);
      throw new SqoopException(CommonRepositoryError.COMMON_0020, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void createJob(MJob job, Connection conn) {
    int result;
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtInsertJob(), Statement.RETURN_GENERATED_KEYS)) {
      stmt.setString(1, job.getName());
      stmt.setLong(2, job.getFromLinkId());
      stmt.setLong(3, job.getToLinkId());
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

      try (ResultSet rsetJobId = stmt.getGeneratedKeys()) {

        if (!rsetJobId.next()) {
          throw new SqoopException(CommonRepositoryError.COMMON_0010);
        }

        long jobId = rsetJobId.getLong(1);

        // from config for the job
        createInputValues(crudQueries.getStmtInsertJobInput(),
                jobId,
                job.getFromJobConfig().getConfigs(),
                conn);
        // to config for the job
        createInputValues(crudQueries.getStmtInsertJobInput(),
                jobId,
                job.getToJobConfig().getConfigs(),
                conn);
        // driver config per job
        createInputValues(crudQueries.getStmtInsertJobInput(),
                jobId,
                job.getDriverConfig().getConfigs(),
                conn);

        job.setPersistenceId(jobId);
      }
    } catch (SQLException ex) {
      logException(ex, job);
      throw new SqoopException(CommonRepositoryError.COMMON_0023, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateJob(MJob job, Connection conn) {
    try (PreparedStatement updateStmt = conn.prepareStatement(crudQueries.getStmtUpdateJob());
         PreparedStatement deleteStmt = conn.prepareStatement(crudQueries.getStmtDeleteJobInput());) {
      // Firstly update job table
      updateStmt.setString(1, job.getName());
      updateStmt.setString(2, job.getLastUpdateUser());
      updateStmt.setTimestamp(3, new Timestamp(new Date().getTime()));

      updateStmt.setLong(4, job.getPersistenceId());
      updateStmt.executeUpdate();

      // Secondly remove old values
      deleteStmt.setLong(1, job.getPersistenceId());
      deleteStmt.executeUpdate();

      // And reinsert new values
      createInputValues(crudQueries.getStmtInsertJobInput(),
          job.getPersistenceId(),
          job.getFromJobConfig().getConfigs(),
          conn);
      createInputValues(crudQueries.getStmtInsertJobInput(),
          job.getPersistenceId(),
          job.getToJobConfig().getConfigs(),
          conn);
      createInputValues(crudQueries.getStmtInsertJobInput(),
          job.getPersistenceId(),
          job.getDriverConfig().getConfigs(),
          conn);

    } catch (SQLException ex) {
      logException(ex, job);
      throw new SqoopException(CommonRepositoryError.COMMON_0024, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean existsJob(long jobId, Connection conn) {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectJobCheckById())) {
      stmt.setLong(1, jobId);
      try (ResultSet rs = stmt.executeQuery()) {

        // Should be always valid in query with count
        rs.next();

        return rs.getLong(1) == 1;
      }
    } catch (SQLException ex) {
      logException(ex, jobId);
      throw new SqoopException(CommonRepositoryError.COMMON_0026, ex);
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
    try (PreparedStatement enableConn = conn.prepareStatement(crudQueries.getStmtEnableJob())) {
      enableConn.setBoolean(1, enabled);
      enableConn.setLong(2, jobId);
      enableConn.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, jobId);
      throw new SqoopException(CommonRepositoryError.COMMON_0039, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteJobInputs(long id, Connection conn) {
    try (PreparedStatement dltInput = conn.prepareStatement(crudQueries.getStmtDeleteJobInput())) {
      dltInput.setLong(1, id);
      dltInput.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, id);
      throw new SqoopException(CommonRepositoryError.COMMON_0025, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteJob(long jobId, Connection conn) {
    try (PreparedStatement dlt = conn.prepareStatement(crudQueries.getStmtDeleteJob())) {
      deleteJobInputs(jobId, conn);
      dlt.setLong(1, jobId);
      dlt.executeUpdate();
    } catch (SQLException ex) {
      logException(ex, jobId);
      throw new SqoopException(CommonRepositoryError.COMMON_0025, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MJob findJob(long jobId, Connection conn) {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries
        .getStmtSelectJobSingleById())) {
      stmt.setLong(1, jobId);

      return findJobInternal(stmt, conn, String.valueOf(jobId));
    } catch (SQLException ex) {
      logException(ex, jobId);
      throw new SqoopException(CommonRepositoryError.COMMON_0028, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MJob findJob(String name, Connection conn) {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries
        .getStmtSelectJobSingleByName())) {
      stmt.setString(1, name);

      return findJobInternal(stmt, conn, name);
    } catch (SQLException ex) {
      logException(ex, name);
      throw new SqoopException(CommonRepositoryError.COMMON_0028, ex);
    }
  }

  private MJob findJobInternal(PreparedStatement stmt, Connection conn,
      String jobIdentifier) throws SQLException {
    List<MJob> jobs = loadJobs(stmt, conn);

    if (jobs.size() == 0) {
      LOG.debug("Looking up job: " + jobIdentifier + ", no job found");
      return null;
    } else if (jobs.size() == 1) {
      // Return the first and only one job object with the given name or id
      MJob job = jobs.get(0);
      LOG.debug("Looking up job: " + jobIdentifier + ", found: " + job);
      return job;
    } else {
      throw new SqoopException(CommonRepositoryError.COMMON_0027, jobIdentifier);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MJob> findJobs(Connection conn) {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectJobAllWithOrder())) {

      return loadJobs(stmt, conn);
    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(CommonRepositoryError.COMMON_0028, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createSubmission(MSubmission submission, Connection conn) {
    int result;
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtInsertSubmission(),
          Statement.RETURN_GENERATED_KEYS)) {
      stmt.setLong(1, submission.getJobId());
      stmt.setString(2, submission.getStatus().name());
      stmt.setString(3, submission.getCreationUser());
      stmt.setTimestamp(4, new Timestamp(submission.getCreationDate().getTime()));
      stmt.setString(5, submission.getLastUpdateUser());
      stmt.setTimestamp(6, new Timestamp(submission.getLastUpdateDate().getTime()));
      stmt.setString(7, submission.getExternalJobId());
      stmt.setString(8, submission.getExternalLink());
      stmt.setString(9, submission.getError().getErrorSummary());
      stmt.setString(10, submission.getError().getErrorDetails());

      result = stmt.executeUpdate();
      if (result != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0009,
            Integer.toString(result));
      }

      try (ResultSet rsetSubmissionId = stmt.getGeneratedKeys()) {

        if (!rsetSubmissionId.next()) {
          throw new SqoopException(CommonRepositoryError.COMMON_0010);
        }

        long submissionId = rsetSubmissionId.getLong(1);

        if (submission.getCounters() != null) {
          createSubmissionCounters(submissionId, submission.getCounters(), conn);
        }

        createSubmissionContext(submissionId, submission.getFromConnectorContext(), ContextType.FROM, conn);
        createSubmissionContext(submissionId, submission.getToConnectorContext(), ContextType.TO, conn);
        createSubmissionContext(submissionId, submission.getDriverContext(), ContextType.DRIVER, conn);

        // Save created persistence id
        submission.setPersistenceId(submissionId);
      }
    } catch (SQLException ex) {
      logException(ex, submission);
      throw new SqoopException(CommonRepositoryError.COMMON_0031, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean existsSubmission(long submissionId, Connection conn) {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectSubmissionCheck())) {
      stmt.setLong(1, submissionId);
      try (ResultSet rs = stmt.executeQuery()) {

        // Should be always valid in query with count
        rs.next();

        return rs.getLong(1) == 1;
      }
    } catch (SQLException ex) {
      logException(ex, submissionId);
      throw new SqoopException(CommonRepositoryError.COMMON_0030, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateSubmission(MSubmission submission, Connection conn) {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtUpdateSubmission());
         PreparedStatement deleteStmt = conn.prepareStatement(crudQueries.getStmtDeleteCounterSubmission());) {
      //  Update properties in main table
      stmt.setString(1, submission.getStatus().name());
      stmt.setString(2, submission.getLastUpdateUser());
      stmt.setTimestamp(3, new Timestamp(submission.getLastUpdateDate().getTime()));
      stmt.setString(4, submission.getError().getErrorSummary());
      stmt.setString(5, submission.getError().getErrorDetails());

      stmt.setLong(6, submission.getPersistenceId());
      stmt.executeUpdate();

      // Delete previous counters
      deleteStmt.setLong(1, submission.getPersistenceId());
      deleteStmt.executeUpdate();

      // Reinsert new counters if needed
      if(submission.getCounters() != null) {
        createSubmissionCounters(submission.getPersistenceId(), submission.getCounters(), conn);
      }

      // We are not updating contexts as they are immutable once the submission is created

    } catch (SQLException ex) {
      logException(ex, submission);
      throw new SqoopException(CommonRepositoryError.COMMON_0032, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void purgeSubmissions(Date threshold, Connection conn) {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtPurgeSubmissions())) {
      stmt.setTimestamp(1, new Timestamp(threshold.getTime()));
      stmt.executeUpdate();

    } catch (SQLException ex) {
      logException(ex, threshold);
      throw new SqoopException(CommonRepositoryError.COMMON_0033, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MSubmission> findUnfinishedSubmissions(Connection conn) {
    List<MSubmission> submissions = new LinkedList<MSubmission>();
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectSubmissionUnfinished())) {

      for(SubmissionStatus status : SubmissionStatus.unfinished()) {
        stmt.setString(1, status.name());
        try (ResultSet rs = stmt.executeQuery()) {

          while (rs.next()) {
            submissions.add(loadSubmission(rs, conn));
          }
        }
      }
    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(CommonRepositoryError.COMMON_0034, ex);
    }

    return submissions;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MSubmission> findSubmissions(Connection conn) {
    List<MSubmission> submissions = new LinkedList<MSubmission>();
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectSubmissions());
         ResultSet rs = stmt.executeQuery();) {
      while(rs.next()) {
        submissions.add(loadSubmission(rs, conn));
      }
    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(CommonRepositoryError.COMMON_0036, ex);
    }

    return submissions;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MSubmission> findSubmissionsForJob(long jobId, Connection conn) {
    List<MSubmission> submissions = new LinkedList<MSubmission>();
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectSubmissionsForJob())) {
      stmt.setLong(1, jobId);
      try (ResultSet rs = stmt.executeQuery()) {

        while (rs.next()) {
          submissions.add(loadSubmission(rs, conn));
        }
      }
    } catch (SQLException ex) {
      logException(ex);
      throw new SqoopException(CommonRepositoryError.COMMON_0037, ex);
    }

    return submissions;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MSubmission findLastSubmissionForJob(long jobId, Connection conn) {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectSubmissionsForJob())) {

      stmt.setLong(1, jobId);
      stmt.setMaxRows(1);
      try (ResultSet rs = stmt.executeQuery()) {

        if (!rs.next()) {
          return null;
        }

        return loadSubmission(rs, conn);
      }
    } catch (SQLException ex) {
      logException(ex, jobId);
      throw new SqoopException(CommonRepositoryError.COMMON_0037, ex);
    }
  }

  private void insertConnectorDirection(Long connectorId, Direction direction, Connection conn)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtInsertSqConnectorDirections())) {
      stmt.setLong(1, connectorId);
      stmt.setLong(2, getDirection(direction, conn));

      if (stmt.executeUpdate() != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0043);
      }
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
    try (PreparedStatement baseConnectorStmt = conn.prepareStatement(crudQueries.getStmtInsertIntoConfigurable(),
          Statement.RETURN_GENERATED_KEYS)) {
      baseConnectorStmt.setString(1, mc.getUniqueName());
      baseConnectorStmt.setString(2, mc.getClassName());
      baseConnectorStmt.setString(3, mc.getVersion());
      baseConnectorStmt.setString(4, mc.getType().name());

      int baseConnectorCount = baseConnectorStmt.executeUpdate();
      if (baseConnectorCount != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0009,
            Integer.toString(baseConnectorCount));
      }

      try (ResultSet rsetConnectorId = baseConnectorStmt.getGeneratedKeys()) {

        if (!rsetConnectorId.next()) {
          throw new SqoopException(CommonRepositoryError.COMMON_0010);
        }
        // connector configurable also have directions
        insertConnectorDirections(rsetConnectorId.getLong(1), mc.getSupportedDirections(), conn);
        return rsetConnectorId.getLong(1);
      }
    } catch (SQLException ex) {
      throw new SqoopException(CommonRepositoryError.COMMON_0011, mc.toString(), ex);
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
    try (PreparedStatement baseConfigStmt = conn.prepareStatement(crudQueries.getStmtInsertIntoConfig(),
          Statement.RETURN_GENERATED_KEYS);
         PreparedStatement baseInputStmt = conn.prepareStatement(crudQueries.getStmtInsertIntoInput(),
          Statement.RETURN_GENERATED_KEYS);) {
      // Register link type config
      registerConfigs(connectorId, null /* No direction for LINK type config */, mc.getLinkConfig()
          .getConfigs(), MConfigType.LINK.name(), baseConfigStmt, baseInputStmt, conn);

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
    }
  }

  private long insertAndGetDriverId(MDriver mDriver, Connection conn) {
    try (PreparedStatement baseDriverStmt = conn.prepareStatement(crudQueries.getStmtInsertIntoConfigurable(),
          Statement.RETURN_GENERATED_KEYS)) {
      baseDriverStmt.setString(1, mDriver.getUniqueName());
      baseDriverStmt.setString(2, Driver.getClassName());
      baseDriverStmt.setString(3, mDriver.getVersion());
      baseDriverStmt.setString(4, mDriver.getType().name());

      int baseDriverCount = baseDriverStmt.executeUpdate();
      if (baseDriverCount != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0009, Integer.toString(baseDriverCount));
      }

      try (ResultSet rsetDriverId = baseDriverStmt.getGeneratedKeys()) {

        if (!rsetDriverId.next()) {
          throw new SqoopException(CommonRepositoryError.COMMON_0010);
        }
        return rsetDriverId.getLong(1);
      }
    } catch (SQLException ex) {
      throw new SqoopException(CommonRepositoryError.COMMON_0044, mDriver.toString(), ex);
    }
  }

  private void createSubmissionContext(long submissionId, ImmutableContext context, ContextType contextType, Connection conn) throws SQLException {
    if(context == null) {
      return;
    }

    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtInsertContext())) {
      long contextTypeId = getContextType(contextType, conn);

      for(Map.Entry<String, String> entry: context) {
        long propertyId = getContextProperty(entry.getKey(), conn);

        stmt.setLong(1, submissionId);
        stmt.setLong(2, contextTypeId);
        stmt.setLong(3, propertyId);
        stmt.setString(4, entry.getValue());

        stmt.executeUpdate();
      }
    }
  }

  private long getContextType(ContextType type, Connection conn) throws SQLException {
    try (PreparedStatement select = conn.prepareStatement(crudQueries.getStmtSelectContextType());
         PreparedStatement insert = conn.prepareStatement(crudQueries.getStmtInsertContextType(), Statement.RETURN_GENERATED_KEYS);) {

      select.setString(1, type.toString());

      try (ResultSet rsSelect = select.executeQuery()) {
        if (rsSelect.next()) {
          return rsSelect.getLong(1);
        }
      }

      insert.setString(1, type.toString());
      insert.executeUpdate();

      try (ResultSet rsInsert = insert.getGeneratedKeys()) {
        if (!rsInsert.next()) {
          throw new SqoopException(CommonRepositoryError.COMMON_0010);
        }
        return rsInsert.getLong(1);
      }
    }
  }

  private long getContextProperty(String property, Connection conn) throws SQLException {
    try (PreparedStatement select = conn.prepareStatement(crudQueries.getStmtSelectContextProperty());
         PreparedStatement insert = conn.prepareStatement(crudQueries.getStmtInsertContextProperty(), Statement.RETURN_GENERATED_KEYS);) {

      select.setString(1, property);
      try (ResultSet rsSelect = select.executeQuery()) {
        if (rsSelect.next()) {
          return rsSelect.getLong(1);
        }
      }

      insert.setString(1, property);
      insert.executeUpdate();

      try (ResultSet rsInsert = insert.getGeneratedKeys()) {
        if (!rsInsert.next()) {
          throw new SqoopException(CommonRepositoryError.COMMON_0010);
        }
        return rsInsert.getLong(1);
      }
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
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtInsertCounterSubmission())) {
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
    try (PreparedStatement select = conn.prepareStatement(crudQueries.getStmtSelectCounterGroup());
         PreparedStatement insert = conn.prepareStatement(crudQueries.getStmtInsertCounterGroup(), Statement.RETURN_GENERATED_KEYS);) {
      select.setString(1, group.getName());

      try (ResultSet rsSelect = select.executeQuery()) {
        if (rsSelect.next()) {
          return rsSelect.getLong(1);
        }
      }
      insert.setString(1, group.getName());
      insert.executeUpdate();

      try (ResultSet rsInsert = insert.getGeneratedKeys()) {
        if (!rsInsert.next()) {
          throw new SqoopException(CommonRepositoryError.COMMON_0010);
        }
        return rsInsert.getLong(1);
      }
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
    try (PreparedStatement select = conn.prepareStatement(crudQueries.getStmtSelectCounter());
         PreparedStatement insert = conn.prepareStatement(crudQueries.getStmtInsertCounter(), Statement.RETURN_GENERATED_KEYS);) {

      select.setString(1, counter.getName());
      try (ResultSet rsSelect = select.executeQuery()) {
        if (rsSelect.next()) {
          return rsSelect.getLong(1);
        }
      }

      insert.setString(1, counter.getName());
      insert.executeUpdate();

      try (ResultSet rsInsert = insert.getGeneratedKeys()) {
        if (!rsInsert.next()) {
          throw new SqoopException(CommonRepositoryError.COMMON_0010);
        }

        return rsInsert.getLong(1);
      }
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
    submission.setExternalJobId(rs.getString(8));
    submission.setExternalLink(rs.getString(9));
    SubmissionError error = new SubmissionError();
    error.setErrorSummary(rs.getString(10));
    error.setErrorDetails(rs.getString(11));
    submission.setError(error);
    Counters counters = loadCountersSubmission(rs.getLong(1), conn);
    submission.setCounters(counters);

    submission.setFromConnectorContext(loadContextSubmission(rs.getLong(1), ContextType.FROM, conn));
    submission.setToConnectorContext(loadContextSubmission(rs.getLong(1), ContextType.TO, conn));
    submission.setDriverContext(loadContextSubmission(rs.getLong(1), ContextType.DRIVER, conn));

    return submission;
  }

  private MutableContext loadContextSubmission(long submissionId, ContextType type, Connection conn) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectContext())) {
      stmt.setLong(1, submissionId);
      stmt.setLong(2, getContextType(type, conn));
      try (ResultSet rs = stmt.executeQuery()) {

        MutableContext context = new MutableMapContext();

        while (rs.next()) {
          String key = rs.getString(1);
          String value = rs.getString(2);

          context.setString(key, value);
        }

        return context;
      }
    }
  }

  private Counters loadCountersSubmission(long submissionId, Connection conn) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectCounterSubmission())) {
      stmt.setLong(1, submissionId);
      try (ResultSet rs = stmt.executeQuery()) {

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
      }
    }
  }

  private Long getDirection(Direction direction, Connection conn) throws SQLException {
    try (PreparedStatement directionStmt = conn.prepareStatement(crudQueries.getStmtSelectSqdIdBySqdName())) {
      directionStmt.setString(1, direction.toString());
      try (ResultSet rs = directionStmt.executeQuery()) {

        rs.next();
        return rs.getLong(1);
      }
    }
  }

  private Direction getDirection(long directionId, Connection conn) throws SQLException {
    try (PreparedStatement directionStmt = conn.prepareStatement(crudQueries.getStmtSelectSqdNameBySqdId())) {
      directionStmt.setLong(1, directionId);
      try (ResultSet rs = directionStmt.executeQuery()) {

        rs.next();
        return Direction.valueOf(rs.getString(1));
      }
    }
  }

  private SupportedDirections findConnectorSupportedDirections(long connectorId, Connection conn) throws SQLException {
    boolean from = false, to = false;

    try (PreparedStatement connectorDirectionsStmt = conn.prepareStatement(crudQueries.getStmtSelectSqConnectorDirections())) {

      connectorDirectionsStmt.setLong(1, connectorId);
      try (ResultSet rs = connectorDirectionsStmt.executeQuery()) {

        while (rs.next()) {
          switch (getDirection(rs.getLong(2), conn)) {
            case FROM:
              from = true;
              break;

            case TO:
              to = true;
              break;
          }
        }
      }
    }

    return new SupportedDirections(from, to);
  }

  private List<MConnector> loadConnectors(PreparedStatement stmt, Connection conn) throws SQLException {
    List<MConnector> connectors = new ArrayList<MConnector>();

    try (ResultSet rsConnectors = stmt.executeQuery();
         PreparedStatement connectorConfigFetchStmt = conn.prepareStatement(crudQueries.getStmtSelectConfigForConfigurable());
         PreparedStatement connectorConfigInputFetchStmt = conn.prepareStatement(crudQueries.getStmtSelectInput());) {

      while(rsConnectors.next()) {
        long connectorId = rsConnectors.getLong(1);
        String connectorName = rsConnectors.getString(2);
        String connectorClassName = rsConnectors.getString(3);
        String connectorVersion = rsConnectors.getString(4);

        connectorConfigFetchStmt.setLong(1, connectorId);

        List<MConfig> linkConfig = new ArrayList<MConfig>();
        List<MConfig> fromConfig = new ArrayList<MConfig>();
        List<MConfig> toConfig = new ArrayList<MConfig>();

        loadConnectorConfigs(linkConfig, fromConfig, toConfig, connectorConfigFetchStmt,
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
    }
    return connectors;
  }

  private List<MLink> loadLinks(PreparedStatement stmt,
                                Connection conn)
      throws SQLException {
    List<MLink> links = new ArrayList<MLink>();

    try (ResultSet rsConnection = stmt.executeQuery();
         PreparedStatement connectorConfigFetchStatement = conn.prepareStatement(crudQueries.getStmtSelectConfigForConfigurable());
         PreparedStatement connectorConfigInputStatement = conn.prepareStatement(crudQueries.getStmtFetchLinkInput());) {

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

        List<MConfig> connectorLinkConfig = new ArrayList<MConfig>();
        List<MConfig> fromConfig = new ArrayList<MConfig>();
        List<MConfig> toConfig = new ArrayList<MConfig>();

        loadConnectorConfigs(connectorLinkConfig, fromConfig, toConfig, connectorConfigFetchStatement,
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
    }

    return links;
  }

  private List<MJob> loadJobs(PreparedStatement stmt,
                              Connection conn)
      throws SQLException {
    List<MJob> jobs = new ArrayList<MJob>();

    try (ResultSet rsJob = stmt.executeQuery();
         PreparedStatement fromConfigFetchStmt  = conn.prepareStatement(crudQueries.getStmtSelectConfigForConfigurable());
         PreparedStatement toConfigFetchStmt = conn.prepareStatement(crudQueries.getStmtSelectConfigForConfigurable());
         PreparedStatement driverConfigfetchStmt = conn.prepareStatement(crudQueries.getStmtSelectConfigForConfigurable());
         PreparedStatement jobInputFetchStmt = conn.prepareStatement(crudQueries.getStmtFetchJobInput());) {

      // Note: Job does not hold a explicit reference to the driver since every
      // job has the same driver
      long driverId = this.findDriver(MDriver.DRIVER_NAME, conn).getPersistenceId();

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

        // FROM entity configs
        List<MConfig> fromConnectorLinkConfig = new ArrayList<MConfig>();
        List<MConfig> fromConnectorFromJobConfig = new ArrayList<MConfig>();
        List<MConfig> fromConnectorToJobConfig = new ArrayList<MConfig>();

        loadConnectorConfigs(fromConnectorLinkConfig, fromConnectorFromJobConfig, fromConnectorToJobConfig,
            fromConfigFetchStmt, jobInputFetchStmt, 2, conn);

        // TO entity configs
        List<MConfig> toConnectorLinkConfig = new ArrayList<MConfig>();
        List<MConfig> toConnectorFromJobConfig = new ArrayList<MConfig>();
        List<MConfig> toConnectorToJobConfig = new ArrayList<MConfig>();

        List<MConfig> driverConfig = new ArrayList<MConfig>();

        loadConnectorConfigs(toConnectorLinkConfig, toConnectorFromJobConfig, toConnectorToJobConfig,
            toConfigFetchStmt, jobInputFetchStmt, 2, conn);

        loadDriverConfigs(driverConfig, driverConfigfetchStmt, jobInputFetchStmt, 2, conn);

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
    }

    return jobs;
  }

  private void registerConfigDirection(Long configId, Direction direction, Connection conn)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtInsertSqConfigDirections())) {
      stmt.setLong(1, configId);
      stmt.setLong(2, getDirection(direction, conn));
      if (stmt.executeUpdate() != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0042);
      }
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
  private short registerConfigs(Long configurableId, Direction direction, List<MConfig> configs,
      String type, PreparedStatement baseConfigStmt, PreparedStatement baseInputStmt,
      Connection conn) throws SQLException {
    short configIndex = 0;

    for (MConfig config : configs) {
      baseConfigStmt.setLong(1, configurableId);

      baseConfigStmt.setString(2, config.getName());
      baseConfigStmt.setString(3, type);
      baseConfigStmt.setShort(4, configIndex++);

      int baseConfigCount = baseConfigStmt.executeUpdate();
      if (baseConfigCount != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0012,
            Integer.toString(baseConfigCount));
      }
      try (ResultSet rsetConfigId = baseConfigStmt.getGeneratedKeys()) {
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
        registerConfigInputs(config, inputs, baseInputStmt);
        // validate all the input relations
        Map<Long, List<String>> inputRelationships = new HashMap<Long, List<String>>();
        for (MInput<?> input : inputs) {
          List<String> inputOverrides = validateAndGetOverridesAttribute(input, config);
          if (inputOverrides != null && inputOverrides.size() > 0) {
            inputRelationships.put(input.getPersistenceId(), inputOverrides);
          }
        }

        // Insert all input relations
        if (inputRelationships.size() > 0) {
          for (Map.Entry<Long, List<String>> entry : inputRelationships.entrySet()) {
            List<String> children = entry.getValue();
            for (String child : children) {
              Long childId = config.getInput(child).getPersistenceId();
              insertConfigInputRelationship(entry.getKey(), childId, conn);
            }
          }
        }
      }
    }
    return configIndex;
  }

  /**
   * Save given inputs to the database.
   *
   * Use given prepare statement to save all inputs into repository.
   *
   * @param config
   *          corresponding config
   * @param inputs
   *          List of inputs that needs to be saved
   * @param baseInputStmt
   *          Statement that we can utilize
   * @throws java.sql.SQLException
   *           In case of any failure on Derby side
   */
  private void registerConfigInputs(MConfig config, List<MInput<?>> inputs,
      PreparedStatement baseInputStmt) throws SQLException {

    short inputIndex = 0;
    for (MInput<?> input : inputs) {
      baseInputStmt.setString(1, input.getName());
      baseInputStmt.setLong(2, config.getPersistenceId());
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

      baseInputStmt.setString(7, input.getEditable().name());

      // Enum specific column(s)
      if (input.getType() == MInputType.ENUM) {
        baseInputStmt.setString(8, StringUtils.join(((MEnumInput) input).getValues(), ","));
      } else {
        baseInputStmt.setNull(8, Types.VARCHAR);
      }

      int baseInputCount = baseInputStmt.executeUpdate();
      if (baseInputCount != 1) {
        throw new SqoopException(CommonRepositoryError.COMMON_0014,
            Integer.toString(baseInputCount));
      }

      try (ResultSet rsetInputId = baseInputStmt.getGeneratedKeys()) {
        if (!rsetInputId.next()) {
          throw new SqoopException(CommonRepositoryError.COMMON_0015);
        }

        long inputId = rsetInputId.getLong(1);
        input.setPersistenceId(inputId);
      }
    }
  }

  private void insertConfigInputRelationship(Long parent, Long child, Connection conn) {
    try (PreparedStatement baseInputRelationStmt = conn.prepareStatement(crudQueries
            .getStmtInsertIntoInputRelation())) {

      baseInputRelationStmt.setLong(1, parent);
      baseInputRelationStmt.setLong(2, child);
      baseInputRelationStmt.executeUpdate();

    } catch (SQLException ex) {
      throw new SqoopException(CommonRepositoryError.COMMON_0047, ex);
    }
  }

  /**
   * Validate that the input override attribute adheres to the rules imposed
   * NOTE: all input names in a config class will and must be unique
   * Rule #1.
   * If editable == USER_ONLY ( cannot override itself ) can override other  CONNECTOR_ONLY and ANY inputs,
   * but cannot overriding other USER_ONLY attributes
   * Rule #2.
   * If editable == CONNECTOR_ONLY or ANY ( cannot override itself ) can override any other attribute in the config object
   * @param currentInput
   *
   */
  private List<String> validateAndGetOverridesAttribute(MInput<?> currentInput, MConfig config) {

    // split the overrides string into comma separated list
    String overrides = currentInput.getOverrides();
    if (StringUtils.isEmpty(overrides)) {
      return null;
    }
    String[] overrideInputs = overrides.split("\\,");
    List<String> children = new ArrayList<String>();

    for (String override : overrideInputs) {
      if (override.equals(currentInput.getName())) {
        throw new SqoopException(CommonRepositoryError.COMMON_0046, "for input :"
            + currentInput.toString());
      }
      if (currentInput.getEditable().equals(InputEditable.USER_ONLY)) {
        if (config.getUserOnlyEditableInputNames().contains(override)) {
          throw new SqoopException(CommonRepositoryError.COMMON_0045, "for input :"
              + currentInput.toString());
        }
      }
      children.add(override);
    }
    return children;
  }

  @Override
  public MConfig findFromJobConfig(long jobId, String configName, Connection conn) {
    MJob job = findJob(jobId, conn);
    if (job == null) {
      throw new SqoopException(RepositoryError.JDBCREPO_0020, "Invalid id: " + jobId);
    }
    MFromConfig fromConfigs = job.getFromJobConfig();
    if (fromConfigs != null) {
      MConfig config = fromConfigs.getConfig(configName);
      if (config == null) {
        throw new SqoopException(CommonRepositoryError.COMMON_0049, "for configName :" + configName);
      }
      return config;
    }
    throw new SqoopException(CommonRepositoryError.COMMON_0049, "for configName :" + configName);
  }

  @Override
  public MConfig findToJobConfig(long jobId, String configName, Connection conn) {
    MJob job = findJob(jobId, conn);
    if (job == null) {
      throw new SqoopException(RepositoryError.JDBCREPO_0020, "Invalid id: " + jobId);
    }
    MToConfig toConfigs = job.getToJobConfig();
    if (toConfigs != null) {
      MConfig config = toConfigs.getConfig(configName);
      if (config == null) {
        throw new SqoopException(CommonRepositoryError.COMMON_0050, "for configName :" + configName);
      }
      return config;
    }
    throw new SqoopException(CommonRepositoryError.COMMON_0050, "for configName :" + configName);
  }

  @Override
  public MConfig findDriverJobConfig(long jobId, String configName, Connection conn) {
    MJob job = findJob(jobId, conn);
    if (job == null) {
      throw new SqoopException(RepositoryError.JDBCREPO_0020, "Invalid id: " + jobId);
    }
    MDriverConfig driverConfigs = job.getDriverConfig();
    if (driverConfigs != null) {
      MConfig config = driverConfigs.getConfig(configName);
      if (config == null) {
        throw new SqoopException(CommonRepositoryError.COMMON_0051, "for configName :" + configName);
      }
      return config;
    }
    throw new SqoopException(CommonRepositoryError.COMMON_0051, "for configName :" + configName);
  }

  @Override
  public MConfig findLinkConfig(long linkId, String configName, Connection conn) {
    MLink link = findLink(linkId, conn);
    if (link == null) {
      throw new SqoopException(RepositoryError.JDBCREPO_0017, "Invalid id: " + linkId);
    }
    MConfig driverConfig = link.getConnectorLinkConfig(configName);
    if (driverConfig == null) {
      throw new SqoopException(CommonRepositoryError.COMMON_0052, "for configName :" + configName);
    }
    return driverConfig;
  }

  @SuppressWarnings("resource")
  @Override
  public void updateJobConfig(long jobId, MConfig config, MConfigUpdateEntityType type,
      Connection conn) {
    List<MInput<?>> inputs = config.getInputs();

    try (PreparedStatement updateStmt = conn.prepareStatement(crudQueries.getStmtUpdateJobInput())) {
      for (MInput<?> input : inputs) {
        if (input.isEmpty()) {
          continue;
        }
        validateEditableConstraints(type, input);
        updateStmt.setString(1, input.getUrlSafeValueString());
        updateStmt.setLong(2, input.getPersistenceId());
        updateStmt.setLong(3, jobId);
        updateStmt.executeUpdate();
      }
    } catch (SQLException ex) {
      logException(ex, jobId);
      throw new SqoopException(CommonRepositoryError.COMMON_0053, ex);
    }
  }

  private void validateEditableConstraints(MConfigUpdateEntityType type, MInput<?> input) {
    if (input.getEditable().equals(InputEditable.CONNECTOR_ONLY)
        && type.equals(MConfigUpdateEntityType.USER)) {
      throw new SqoopException(CommonRepositoryError.COMMON_0055);
    }
    if (input.getEditable().equals(InputEditable.USER_ONLY)
        && type.equals(MConfigUpdateEntityType.CONNECTOR)) {
      throw new SqoopException(CommonRepositoryError.COMMON_0056);
    }
  }

  @Override
  public void updateLinkConfig(long linkId, MConfig config, MConfigUpdateEntityType type,
      Connection conn) {
    List<MInput<?>> inputs = config.getInputs();
    try (PreparedStatement updateStmt = conn.prepareStatement(crudQueries.getStmtUpdateLinkInput());) {
      for (MInput<?> input : inputs) {
        if (input.isEmpty()) {
          continue;
        }
        validateEditableConstraints(type, input);
        updateStmt.setString(1, input.getUrlSafeValueString());
        updateStmt.setLong(2, input.getPersistenceId());
        updateStmt.setLong(3, linkId);
        updateStmt.executeUpdate();
      }
    } catch (SQLException ex) {
      logException(ex, linkId);
      throw new SqoopException(CommonRepositoryError.COMMON_0054, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String validationQuery() {
    return "values(1)"; // Yes, this is valid PostgreSQL SQL
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
  private void loadDriverConfigs(List<MConfig> driverConfig,
                                PreparedStatement configFetchStatement,
                                PreparedStatement inputFetchStmt,
                                int configPosition, Connection conn) throws SQLException {

    // Get list of structures from database
    try (ResultSet rsetConfig = configFetchStatement.executeQuery()) {
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

        try (ResultSet rsetInput = inputFetchStmt.executeQuery()) {
          while (rsetInput.next()) {
            long inputId = rsetInput.getLong(1);
            String inputName = rsetInput.getString(2);
            long inputConfig = rsetInput.getLong(3);
            short inputIndex = rsetInput.getShort(4);
            String inputType = rsetInput.getString(5);
            boolean inputSensitivity = rsetInput.getBoolean(6);
            short inputStrLength = rsetInput.getShort(7);
            String editable = rsetInput.getString(8);
            InputEditable editableEnum = editable != null ? InputEditable.valueOf(editable)
                    : InputEditable.ANY;
            // get the overrides value from the SQ_INPUT_RELATION table
            String overrides = getOverrides(inputId, conn);
            String inputEnumValues = rsetInput.getString(9);
            String value = rsetInput.getString(10);

            MInputType mit = MInputType.valueOf(inputType);
            MInput input = null;
            switch (mit) {
              case STRING:
                input = new MStringInput(inputName, inputSensitivity, editableEnum, overrides, inputStrLength);
                break;
              case MAP:
                input = new MMapInput(inputName, inputSensitivity, editableEnum, overrides);
                break;
              case BOOLEAN:
                input = new MBooleanInput(inputName, inputSensitivity, editableEnum, overrides);
                break;
              case INTEGER:
                input = new MIntegerInput(inputName, inputSensitivity, editableEnum, overrides);
                break;
              case LONG:
                input = new MLongInput(inputName, inputSensitivity, editableEnum, overrides);
                break;
              case ENUM:
                input = new MEnumInput(inputName, inputSensitivity, editableEnum, overrides, inputEnumValues.split(","));
                break;
              case LIST:
                input = new MListInput(inputName, inputSensitivity, editableEnum, overrides);
                break;
              case DATETIME:
                input = new MDateTimeInput(inputName, inputSensitivity, editableEnum, overrides);
                break;
              default:
                throw new SqoopException(CommonRepositoryError.COMMON_0003,
                        "input-" + inputName + ":" + inputId + ":"
                                + "config-" + inputConfig + ":" + mit.name());
            }

            // Set persistent ID
            input.setPersistenceId(inputId);

            // Set value
            if (value == null) {
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
          case CONNECTION:
            // do nothing
            break;
          default:
            throw new SqoopException(CommonRepositoryError.COMMON_0004,
                    "connector-" + fromConnectorId + ":" + configType);
        }
      }
    }
  }

  private Direction findConfigDirection(long configId, Connection conn) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(crudQueries.getStmtSelectSqConfigDirections())) {
      stmt.setLong(1, configId);
      try (ResultSet rs = stmt.executeQuery()) {
        rs.next();
        return getDirection(rs.getLong(2), conn);
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
  public void loadConnectorConfigs(List<MConfig> linkConfig, List<MConfig> fromConfig, List<MConfig> toConfig,
                                       PreparedStatement configFetchStmt, PreparedStatement inputFetchStmt,
                                       int configPosition, Connection conn) throws SQLException {

    // Get list of structures from database
    try (ResultSet rsetConfig = configFetchStmt.executeQuery()) {
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

        try (ResultSet rsetInput = inputFetchStmt.executeQuery()) {
          while (rsetInput.next()) {
            long inputId = rsetInput.getLong(1);
            String inputName = rsetInput.getString(2);
            long inputConfig = rsetInput.getLong(3);
            short inputIndex = rsetInput.getShort(4);
            String inputType = rsetInput.getString(5);
            boolean inputSensitivity = rsetInput.getBoolean(6);
            short inputStrLength = rsetInput.getShort(7);
            String editable = rsetInput.getString(8);
            InputEditable editableEnum = editable != null ? InputEditable.valueOf(editable)
                    : InputEditable.ANY;
            // get the overrides value from the SQ_INPUT_RELATION table
            String overrides = getOverrides(inputId, conn);
            String inputEnumValues = rsetInput.getString(9);
            String value = rsetInput.getString(10);

            MInputType mit = MInputType.valueOf(inputType);

            MInput<?> input = null;
            switch (mit) {
              case STRING:
                input = new MStringInput(inputName, inputSensitivity, editableEnum, overrides,
                        inputStrLength);
                break;
              case MAP:
                input = new MMapInput(inputName, inputSensitivity, editableEnum, overrides);
                break;
              case BOOLEAN:
                input = new MBooleanInput(inputName, inputSensitivity, editableEnum, overrides);
                break;
              case INTEGER:
                input = new MIntegerInput(inputName, inputSensitivity, editableEnum, overrides);
                break;
              case LONG:
                input = new MLongInput(inputName, inputSensitivity, editableEnum, overrides);
                break;
              case ENUM:
                input = new MEnumInput(inputName, inputSensitivity, editableEnum, overrides,
                        inputEnumValues.split(","));
                break;
              case LIST:
                input = new MListInput(inputName, inputSensitivity, editableEnum, overrides);
                break;
              case DATETIME:
                input = new MDateTimeInput(inputName, inputSensitivity, editableEnum, overrides);
                break;
              default:
                throw new SqoopException(CommonRepositoryError.COMMON_0003, "input-" + inputName + ":"
                        + inputId + ":" + "config-" + inputConfig + ":" + mit.name());
            }

            // Set persistent ID
            input.setPersistenceId(inputId);

            // Set value
            if (value == null) {
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
        }

        if (config.getInputs().size() == 0) {
          throw new SqoopException(CommonRepositoryError.COMMON_0005,
                  "connector-" + configConnectorId
                          + "; config: " + config
          );
        }

        MConfigType mConfigType = MConfigType.valueOf(configType);
        switch (mConfigType) {
          case CONNECTION:
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
            switch (type) {
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
  }

  /**
   * @param inputId
   * @param conn
   * @return
   */
  private String getOverrides(long inputId, Connection conn) {

    List<String> overrides = new ArrayList<String>();
    try (PreparedStatement overridesStmt = conn.prepareStatement(crudQueries.getStmtSelectInputOverrides());
         PreparedStatement inputStmt = conn.prepareStatement(crudQueries.getStmtSelectInputById());) {
      overridesStmt.setLong(1, inputId);
      try (ResultSet rsOverride = overridesStmt.executeQuery()) {

        while (rsOverride.next()) {
          long overrideId = rsOverride.getLong(1);
          inputStmt.setLong(1, overrideId);
          try (ResultSet rsInput = inputStmt.executeQuery()) {
            if (rsInput.next()) {
              overrides.add(rsInput.getString(2));
            }
          }
        }
        if (overrides.size() > 0) {
          return StringUtils.join(overrides, ",");
        } else {
          return StringUtils.EMPTY;
        }
      }
    } catch (SQLException ex) {
      logException(ex, inputId);
      throw new SqoopException(CommonRepositoryError.COMMON_0048, ex);
    }
  }

  private void createInputValues(String query, long id, List<MConfig> configs, Connection conn)
      throws SQLException {
    int result;

    try (PreparedStatement stmt = conn.prepareStatement(query)) {
      for (MConfig config : configs) {
        for (MInput<?> input : config.getInputs()) {
          // Skip empty values as we're not interested in storing those in db
          if (input.isEmpty()) {
            continue;
          }
          stmt.setLong(1, id);
          stmt.setLong(2, input.getPersistenceId());
          stmt.setString(3, input.getUrlSafeValueString());

          result = stmt.executeUpdate();
          if (result != 1) {
            throw new SqoopException(CommonRepositoryError.COMMON_0017, Integer.toString(result));
          }
        }
      }
    }
  }

  /**
   * Execute given query via a PreparedStatement.
   * A list of args can be passed to the query.
   *
   * Example: runQuery("SELECT * FROM example WHERE test = ?", "test");
   *
   * @param query Query that should be executed
   * @param args Long, String, or Object arguments
   */
  protected void runQuery(String query, Connection conn, Object... args) {
    try (PreparedStatement stmt = conn.prepareStatement(query);) {
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
        try (ResultSet rset = stmt.getResultSet()) {
          int count = 0;
          while (rset.next()) {
            count++;
          }
          LOG.info("QUERY(" + query + ") produced unused resultset with " + count + " rows");
        }
      } else {
        int updateCount = stmt.getUpdateCount();
        LOG.info("QUERY(" + query + ") Update count: " + updateCount);
      }
    } catch (SQLException ex) {
      LOG.error("Can't execute query: " + query, ex);
      throw new SqoopException(CommonRepositoryError.COMMON_0000, query, ex);
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
  public void closeStatements(Statement... stmts) {
    CommonRepoUtils.closeStatements(stmts);
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
