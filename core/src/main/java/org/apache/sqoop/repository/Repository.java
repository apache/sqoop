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
package org.apache.sqoop.repository;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.driver.Driver;
import org.apache.sqoop.driver.DriverUpgrader;
import org.apache.sqoop.json.DriverBean;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConfigUpdateEntityType;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.validation.ConfigValidationResult;
import org.apache.sqoop.validation.Message;


/**
 * Defines the contract for repository used by Sqoop. A Repository allows
 * Sqoop to store entities such as connectors, links, jobs, submissions and its related configs,
 * statistics and other state relevant the entities in the store
 */
public abstract class Repository {

  private static final Logger LOG = Logger.getLogger(Repository.class);

  public abstract RepositoryTransaction getTransaction();

  /**
   * Create or update the repository schema structures.
   *
   * This method will be called from the Sqoop server if enabled via a config
   * {@link RepoConfigurationConstants#SYSCFG_REPO_SCHEMA_IMMUTABLE} to enforce
   * changing the repository schema structure or explicitly via the
   * {@link UpgradeTool} Repository should not change its schema structure
   * outside of this method. This method must be no-op in case that the schema
   * structure do not need any upgrade.
   */
  public abstract void createOrUpgradeRepository();

  /**
   * Return true if internal repository structures exists and are suitable for use.
   * This method should return false in case that the structures do exists, but
   * are not suitable to use i.e corrupted as part of the upgrade
   *
   * @return Boolean values if internal structures are suitable for use
   */
  public abstract boolean isRepositorySuitableForUse();

  /**
   * Registers given connector in the repository and return registered
   * variant.This method might return an exception in case that
   * given connector are already registered with different structure
   *
   * @param mConnector the connector to be registered
   * @param autoUpgrade whether to upgrade driver config automatically
   * @return Registered connector structure
   */
  public abstract MConnector registerConnector(MConnector mConnector, boolean autoUpgrade);

   /**
   * Registers given driver and its config in the repository and return registered
   * variant. This method might return an exception in case that the
   * given driverConfig are already registered with different structure
   *
   * @param mDriverConfig driverConfig to be registered
   * @param autoUpgrade whether to upgrade driverConfig automatically
   * @return Registered connector structure
   */
  public abstract MDriver registerDriver(MDriver mDriverConfig, boolean autoUpgrade);

  /**
   * Search for connector with given name in repository.
   *
   * And return corresponding entity structure.
   *
   * @param shortName Connector unique name
   * @return null if connector is not yet registered in repository or
   *   loaded representation.
   */
  public abstract MConnector findConnector(String shortName);

  /**
   * Get all connectors in repository
   *
   * @return List with all connectors in repository
   */
  public abstract List<MConnector> findConnectors();

  /**
   * Search for driver in the repository.
   * @param shortName Driver unique name
   * @return null if driver are not yet present in repository or
   *  loaded representation.
   */
  public abstract MDriver findDriver(String shortName);

  /**
   * Save given link to repository. This link must not be already
   * present in the repository otherwise exception will be thrown.
   *
   * @param link link object to serialize into repository.
   */
  public abstract void createLink(MLink link);

  /**
   * Update given link representation in repository. This link
   * object must already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param link link object that should be updated in repository.
   */
  public abstract void updateLink(MLink link);

  /**
   * Update given link representation in repository. This link
   * object must already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param link Link object that should be updated in repository.
   * @param tx The repository transaction to use to push the data to the
   *           repository. If this is null, a new transaction will be created.
   *           method will not call begin, commit,
   *           rollback or close on this transaction.
   */
  public abstract void updateLink(final MLink link, RepositoryTransaction tx);

  /**
   * Enable or disable Link with given id from the repository
   *
   * @param id Link object that is going to be enabled or disabled
   * @param enabled enable or disable
   */
  public abstract void enableLink(long id, boolean enabled);

  /**
   * Delete Link with given id from the repository.
   *
   * @param id Link object that should be removed from repository
   */
  public abstract void deleteLink(long id);

  /**
   * Find link with given id in repository.
   *
   * @param id Link id
   * @return link that is saved in repository
   */
  public abstract MLink findLink(long id);

  /**
   * Find link with given id in repository.
   *
   * @param name unique link name
   * @return link that is saved in repository or null if it does not exists
   */
  public abstract MLink findLink(String name);

  /**
   * Retrieve links which use the given connector.
   * @param connectorId Connector id whose links should be fetched
   * @return List of MLink that use <code>connectorId</code>.
   */
  public abstract List<MLink> findLinksForConnector(long connectorId);

  /**
   * Get all Link objects.
   *
   * @return List will all saved link objects
   */
  public abstract List<MLink> findLinks();

  /**
   * Save given job to repository. This job object must not be already present
   * in repository otherwise exception will be thrown.
   *
   * @param job Job object that should be saved to repository
   */
  public abstract void createJob(MJob job);

  /**
   * Update given job entity in repository. This object must already be saved
   * in repository otherwise exception will be thrown.
   *
   * @param job Job object that should be updated in the repository
   */
  public abstract void updateJob(MJob job);

  /**
   * Update given job entity in repository. This object must already be saved
   * in repository otherwise exception will be thrown.
   *
   * @param job Job object that should be updated in the repository
   * @param tx The repository transaction to use to push the data to the
   *           repository. If this is null, a new transaction will be created.
   *           method will not call begin, commit,
   *           rollback or close on this transaction.
   */
  public abstract void updateJob(MJob job, RepositoryTransaction tx);

  /**
   * Enable or disable job with given id from entity repository
   *
   * @param id Job object that is going to be enabled or disabled
   * @param enabled Enable or disable
   */
  public abstract void enableJob(long id, boolean enabled);

  /**
   * Delete job with given id from entity repository.
   *
   * @param id Job id that should be removed
   */
  public abstract void deleteJob(long id);

  /**
   * Find job object with given id.
   *
   * @param id Job id
   * @return job with given id loaded from repository
   */
  public abstract MJob findJob(long id);

  /**
   * Find job object with given name.
   *
   * @param name unique name for the job
   * @return job with given name loaded from repository or null if not present
   */
  public abstract MJob findJob(String name);

  /**
   * Get all job objects.
   *
   * @return List of all jobs in the repository
   */
  public abstract List<MJob> findJobs();

  /**
   * Retrieve jobs which use the given link.
   *
   * @param connectorId Connector ID whose jobs should be fetched
   * @return List of MJobs that use <code>linkID</code>.
   */
  public abstract List<MJob> findJobsForConnector(long connectorId);

  /**
   * Create new submission record in repository.
   *
   * @param submission Submission object that should be serialized to repository
   */
  public abstract void createSubmission(MSubmission submission);

  /**
   * Update already existing submission record in repository.
   *
   * @param submission Submission object that should be updated
   */
  public abstract void updateSubmission(MSubmission submission);

  /**
   * Remove submissions older then given date from repository.
   *
   * @param threshold Threshold date
   */
  public abstract void purgeSubmissions(Date threshold);

  /**
   * Return all unfinished submissions as far as repository is concerned.
   *
   * @return List of unfinished submissions
   */
  public abstract List<MSubmission> findUnfinishedSubmissions();

  /**
   * Return all submissions from repository
   *
   * @return List of all submissions
   */
  public abstract List<MSubmission> findSubmissions();

  /**
   * Return all submissions for given jobId.
   *
   * @return List of of submissions
   */
  public abstract List<MSubmission> findSubmissionsForJob(long jobId);

  /**
   * Find last submission for given jobId.
   *
   * @param jobId Job id
   * @return Most recent submission
   */
  public abstract MSubmission findLastSubmissionForJob(long jobId);

 /**
  * fetch the job config for the FROM type for the given name
  * @param jobId id of the job
  * @param configName name of the config unique to this job and type
  * @return config object
  */
  public abstract MConfig findFromJobConfig(long jobId, String configName);


  /**
   * fetch the job config for the TO type for the given name
   * @param jobId id of the job
   * @param configName name of the config unique to this job and type
   * @return config object
   */
  public abstract MConfig findToJobConfig(long jobId, String configName);


  /**
   * fetch the job config for the DRIVER type for the given name
   * @param jobId id of the job
   * @param configName name of the config unique to this job and type
   * @return config object
   */
  public abstract MConfig findDriverJobConfig(long jobId, String configName);


  /**
   * fetch the link config for the link type for the given name
   * @param linkId id of the link
   * @param configName name of the config unique to this link and type
   * @return config object
   */
  public abstract MConfig findLinkConfig(long linkId, String configName);


  /**
   * Update the config object for the job
   * @param jobId id of the job
   * @param config name of the config
   * @param updateEntityType entity type updating the link config
   */
  public abstract void updateJobConfig(long jobId, MConfig config, MConfigUpdateEntityType updateEntityType);

  /**
   * Update the config object for the job
   * @param jobId id of the job
   * @param config name of the config
   * @param updateEntityType entity type updating the link config
   * @param tx database transaction
   */
  public abstract void updateJobConfig(long jobId, MConfig config, MConfigUpdateEntityType updateEntityType,  RepositoryTransaction tx);


  /**
   * Update the config object for the link
   * @param linkId id of the link
   * @param config name of the config
   * @param updateEntityType entity type updating the link config
   */
  public abstract void updateLinkConfig(long linkId, MConfig config, MConfigUpdateEntityType updateEntityType);

  /**
   * Update the config object for the link
   * @param linkId id of the link
   * @param config name of the config
   * @param updateEntityType entity type updating the link config
   * @param tx database transaction
   */
  public abstract void updateLinkConfig(long linkId, MConfig config, MConfigUpdateEntityType updateEntityType, RepositoryTransaction tx);


  /*********************Configurable Upgrade APIs ******************************/

  /**
   * Update the connector with the new data supplied in the
   * <tt>newConnector</tt>. Also Update all configs associated with this
   * connector in the repository with the configs specified in
   * <tt>mConnector</tt>. <tt>mConnector </tt> must
   * minimally have the configurableID and all required configs (including ones
   * which may not have changed). After this operation the repository is
   * guaranteed to only have the new configs specified in this object.
   *
   * @param newConnector The new data to be inserted into the repository for
   *                     this connector.
   * @param tx The repository transaction to use to push the data to the
   *           repository. If this is null, a new transaction will be created.
   *           method will not call begin, commit,
   *           rollback or close on this transaction.
   */
  protected abstract void upgradeConnectorAndConfigs(MConnector newConnector, RepositoryTransaction tx);

  /**
   * Upgrade the driver with the new data supplied in the
   * <tt>mDriver</tt>. Also Update all configs associated with the driver
   * in the repository with the configs specified in
   * <tt>mDriver</tt>. <tt>mDriver </tt> must
   * minimally have the configurableID and all required configs (including ones
   * which may not have changed). After this operation the repository is
   * guaranteed to only have the new configs specified in this object.
   *
   * @param newDriver The new data to be inserted into the repository for
   *                     the driverConfig.
   * @param tx The repository transaction to use to push the data to the
   *           repository. If this is null, a new transaction will be created.
   *           method will not call begin, commit,
   *           rollback or close on this transaction.
   */
  protected abstract void upgradeDriverAndConfigs(MDriver newDriver, RepositoryTransaction tx);

  /**
   * Delete all inputs for a job
   * @param jobId The id of the job whose inputs are to be deleted.
   * @param tx A transaction on the repository. This
   *           method will not call <code>begin, commit,
   *           rollback or close on this transaction.</code>
   */
  protected abstract void deleteJobInputs(long jobId, RepositoryTransaction tx);

  /**
   * Delete all inputs for a link
   * @param linkId The id of the link whose inputs are to be
   *                     deleted.
   * @param tx The repository transaction to use to push the data to the
   *           repository. If this is null, a new transaction will be created.
   *           method will not call begin, commit,
   *           rollback or close on this transaction.
   */
  protected abstract void deleteLinkInputs(long linkId, RepositoryTransaction tx);

  private void deletelinksAndJobInputs(List<MLink> links, List<MJob> jobs, RepositoryTransaction tx) {
    for (MJob job : jobs) {
      deleteJobInputs(job.getPersistenceId(), tx);
    }
    for (MLink link : links) {
      deleteLinkInputs(link.getPersistenceId(), tx);
    }
  }

  private void deleteJobInputsOnly(List<MJob> jobs, RepositoryTransaction tx) {
    for (MJob job : jobs) {
      deleteJobInputs(job.getPersistenceId(), tx);
    }
  }


  /**
   * Upgrade the connector with the same {@linkplain MConnector#uniqueName}
   * in the repository with values from <code>newConnector</code>.
   * <p/>
   * All links and jobs associated with this connector will be upgraded
   * automatically.
   *
   * @param oldConnector The old connector that should be upgraded.
   * @param newConnector New properties for the Connector that should be
   *                     upgraded.
   */
  public final void upgradeConnector(MConnector oldConnector, MConnector newConnector) {
    LOG.info("Upgrading connector: " + oldConnector.getUniqueName());
    long connectorId = oldConnector.getPersistenceId();
    String connectorName = oldConnector.getUniqueName();
    newConnector.setPersistenceId(connectorId);

    RepositoryTransaction tx = null;
    try {
      SqoopConnector connector = ConnectorManager.getInstance().getSqoopConnector(
          newConnector.getUniqueName());

      boolean upgradeSuccessful = true;
      // 1. Get an upgrader for the connector
      ConnectorConfigurableUpgrader upgrader = connector.getConfigurableUpgrader();
      // 2. Get all links associated with the connector.
      List<MLink> existingLinksByConnector = findLinksForConnector(connectorId);
      // 3. Get all jobs associated with the connector.
      List<MJob> existingJobsByConnector = findJobsForConnector(connectorId);
      // -- BEGIN TXN --
      tx = getTransaction();
      tx.begin();
      // 4. Delete the inputs for all of the jobs and links (in that order) for
      // this connector
      deletelinksAndJobInputs(existingLinksByConnector, existingJobsByConnector, tx);
      // 5. Delete all inputs and configs associated with the connector, and
      // insert the new configs and inputs for this connector
      upgradeConnectorAndConfigs(newConnector, tx);
      // 6. Run upgrade logic for the configs related to the link objects
      // dont always rely on the repository implementation to return empty list for links
      if (existingLinksByConnector != null) {
        for (MLink link : existingLinksByConnector) {
          LOG.info(" Link upgrade for link:" + link.getName() + " for connector:" + connectorName);
          // Make a new copy of the configs
          List<MConfig> linkConfig = newConnector.getLinkConfig().clone(false).getConfigs();
          MLinkConfig newLinkConfig = new MLinkConfig(linkConfig);
          MLinkConfig oldLinkConfig = link.getConnectorLinkConfig();
          upgrader.upgradeLinkConfig(oldLinkConfig, newLinkConfig);
          MLink newlink = new MLink(link, newLinkConfig);

          // 7. Run link config validation
          ConfigValidationResult validationResult = ConfigUtils.validateConfigs(
            newlink.getConnectorLinkConfig().getConfigs(),
            connector.getLinkConfigurationClass()
          );
          if (validationResult.getStatus().canProceed()) {
            updateLink(newlink, tx);
          } else {
            // If any invalid links or jobs detected, throw an exception
            // and stop the bootup of Sqoop server
            logInvalidModelObject("link", newlink, validationResult);
            upgradeSuccessful = false;
            LOG.info(" LINK config upgrade FAILED for link: " + link.getName() + " for connector:" + connectorName);
          }
        }
      }
      LOG.info(" All Link and configs for this connector processed");
      // 8. Run upgrade logic for the configs related to the job objects
      if (existingJobsByConnector != null) {
        for (MJob job : existingJobsByConnector) {
          // every job has 2 parts, the FROM and the TO links and their
          // corresponding connectors.
          LOG.info(" Job upgrade for job:" + job.getName()+ " for connector:" + connectorName);

          if (newConnector.getSupportedDirections().isDirectionSupported(Direction.FROM)) {
            List<MConfig> fromConfig = newConnector.getFromConfig().clone(false).getConfigs();
            if (job.getFromConnectorId() == newConnector.getPersistenceId()) {
              MFromConfig newFromConfig = new MFromConfig(fromConfig);
              MFromConfig oldFromConfig = job.getFromJobConfig();
              upgrader.upgradeFromJobConfig(oldFromConfig, newFromConfig);
              MToConfig oldToConfig = job.getToJobConfig();
              // create a job with new FROM direction configs but old TO direction
              // configs
              MJob newJob = new MJob(job, newFromConfig, oldToConfig, job.getDriverConfig());

              ConfigValidationResult validationResult = ConfigUtils.validateConfigs(
                  newJob.getFromJobConfig().getConfigs(),
                  connector.getJobConfigurationClass(Direction.FROM)
              );

              if (validationResult.getStatus().canProceed()) {
                updateJob(newJob, tx);
              } else {
                logInvalidModelObject("fromJob", newJob, validationResult);
                upgradeSuccessful = false;
                LOG.error(" From JOB config upgrade FAILED for job: " + job.getName() + " for connector:" + connectorName);
              }
            }
          }

          if (newConnector.getSupportedDirections().isDirectionSupported(Direction.TO)) {
            List<MConfig> toConfig = newConnector.getToConfig().clone(false).getConfigs();
            if (job.getToConnectorId() == newConnector.getPersistenceId()) {
              MToConfig oldToConfig = job.getToJobConfig();
              MToConfig newToConfig = new MToConfig(toConfig);
              upgrader.upgradeToJobConfig(oldToConfig, newToConfig);
              MFromConfig oldFromConfig = job.getFromJobConfig();
              // create a job with old FROM direction configs but new TO direction
              // configs
              MJob newJob = new MJob(job, oldFromConfig, newToConfig, job.getDriverConfig());

              ConfigValidationResult validationResult = ConfigUtils.validateConfigs(
                  newJob.getToJobConfig().getConfigs(),
                  connector.getJobConfigurationClass(Direction.TO)
              );

              if (validationResult.getStatus().canProceed()) {
                updateJob(newJob, tx);
              } else {
                logInvalidModelObject("toJob", newJob, validationResult);
                upgradeSuccessful = false;
                LOG.error(" TO JOB config upgrade FAILED for job: " + job.getName() + " for connector:" + connectorName);
              }
            }
          }
        }
      }
      LOG.info(" All Job and configs for this connector processed");
      if (upgradeSuccessful) {
        tx.commit();
      } else {
        throw new SqoopException(RepositoryError.JDBCREPO_0027, " for connector:" + connectorName);
      }
    } catch (SqoopException ex) {
      if (tx != null) {
        tx.rollback();
      }
      throw ex;
    } catch (Exception ex) {
      if (tx != null) {
        tx.rollback();
      }
      throw new SqoopException(RepositoryError.JDBCREPO_0000, ex);
    } finally {
      if (tx != null) {
        tx.close();
      }
      LOG.info("Connector upgrade finished for: " + connectorName);
    }
  }

  public final void upgradeDriver(MDriver driver) {
    LOG.info("Upgrading driver");
    RepositoryTransaction tx = null;
    try {
      //1. find upgrader
      DriverUpgrader upgrader = Driver.getInstance().getConfigurableUpgrader();
      //2. find all jobs in the system
      List<MJob> existingJobs = findJobs();
      boolean upgradeSuccessful = true;

      // -- BEGIN TXN --
      tx = getTransaction();
      tx.begin();
      //3. delete all jobs in the system
      deleteJobInputsOnly(existingJobs, tx);
      // 4. Delete all inputs and configs associated with the driver, and
      // insert the new configs and inputs for this driver
      upgradeDriverAndConfigs(driver, tx);

      for (MJob job : existingJobs) {
        // Make a new copy of the configs
        MDriverConfig driverConfig = driver.getDriverConfig().clone(false);
        MDriver newDriver = new MDriver(driverConfig, DriverBean.CURRENT_DRIVER_VERSION);
        // At this point, the driver only supports JOB config type
        upgrader.upgradeJobConfig(job.getDriverConfig(), newDriver.getDriverConfig());
        // create a new job with old FROM and TO configs but new driver configs
        MJob newJob = new MJob(job, job.getFromJobConfig(), job.getToJobConfig(), newDriver.getDriverConfig());

        // 5. validate configs
        ConfigValidationResult validationResult = ConfigUtils.validateConfigs(
          newJob.getDriverConfig().getConfigs(),
          Driver.getInstance().getDriverJobConfigurationClass()
        );
        if (validationResult.getStatus().canProceed()) {
          // 6. update job
          updateJob(newJob, tx);
        } else {
          logInvalidModelObject("job", newJob, validationResult);
          upgradeSuccessful = false;
        }
      }

      if (upgradeSuccessful) {
        tx.commit();
      } else {
        throw new SqoopException(RepositoryError.JDBCREPO_0027, " Driver");
      }
    } catch (SqoopException ex) {
      if(tx != null) {
        tx.rollback();
      }
      throw ex;
    } catch (Exception ex) {
      if(tx != null) {
        tx.rollback();
      }
      throw new SqoopException(RepositoryError.JDBCREPO_0000, ex);
    } finally {
      if(tx != null) {
        tx.close();
      }
      LOG.info("Driver upgrade finished");
    }
  }

  private void logInvalidModelObject(String objectType, MPersistableEntity entity, ConfigValidationResult validation) {
    LOG.error("Upgrader created invalid " + objectType + " with id " + entity.getPersistenceId());
    LOG.error("Validation errors:");

    for(Map.Entry<String, List<Message>> entry : validation.getMessages().entrySet()) {
      LOG.error("\t" + entry.getKey() + ": " + StringUtils.join(entry.getValue(), ","));
    }
  }
}
