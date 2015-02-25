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

import java.sql.Connection;
import java.util.Date;
import java.util.List;

import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConfigUpdateEntityType;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MSubmission;

/**
 * Set of methods required from each JDBC based repository.
 */
public abstract class JdbcRepositoryHandler {

  /**
   * Initialize JDBC based repository.
   *
   * @param repoContext Context for this instance
   */
  public abstract void initialize(JdbcRepositoryContext repoContext);

  /**
   * Search for connector with given name in repository.
   * And return corresponding connector entity.
   *
   * @param shortName Connector unique name
   * @param conn JDBC link for querying repository.
   * @return null if connector is not yet registered in repository or
   *   loaded representation.
   */
  public abstract MConnector findConnector(String shortName, Connection conn);

  /**
   * Get all connectors in repository
   *
   * @return List will all connectors in repository
   */
  public abstract List<MConnector> findConnectors(Connection conn);

  /**
   * Register given connector in repository.
   *
   * Save given connector data to the repository. Given connector should not be
   * already registered or present in the repository.
   *
   * @param mc Connector that should be registered.
   * @param conn JDBC link for querying repository.
   */
  public abstract void registerConnector(MConnector mc, Connection conn);


  /**
   * Retrieve links which use the given connector.
   * @param connectorId Connector ID whose links should be fetched
   * @param conn JDBC link for querying repository
   * @return List of MLinks that use <code>connectorID</code>.
   */
  public abstract List<MLink> findLinksForConnector(long connectorId, Connection conn);

  /**
   * Retrieve jobs which use the given link.
   *
   * @param connectorId Connector ID whose jobs should be fetched
   * @param conn JDBC link for querying repository
   * @return List of MJobs that use <code>linkID</code>.
   */
  public abstract List<MJob> findJobsForConnector(long c, Connection conn);

  /**
   * Upgrade the connector with the new data supplied in the <tt>newConnector</tt>.
   * Also Update all configs associated with this connector in the repository
   * with the configs specified in <tt>mConnector</tt>. <tt>mConnector </tt> must
   * minimally have the configurableID and all required configs (including ones
   * which may not have changed). After this operation the repository is
   * guaranteed to only have the new configs specified in this object.
   *
   * @param mConnector The new data to be inserted into the repository for
   *                     this connector.
   * @param conn JDBC link for querying repository
   */

  protected abstract void upgradeConnectorAndConfigs(MConnector mConnector, Connection conn);

  /**
   * Upgrade the driver with the new data supplied in the
   * <tt>mDriver</tt>.
   * Also Update all configs in the repository
   * with the configs specified in <tt>mDriverConfig</tt>. <tt>mDriver </tt> must
   * minimally have the configurableID and all required configs (including ones
   * which may not have changed). After this operation the repository is
   * guaranteed to only have the new configs specified in this object.
   *
   * @param mDriver The new data to be inserted into the repository for
   *                     the driverConfig.
   * @param conn JDBC link for querying repository
   */
  protected abstract void upgradeDriverAndConfigs(MDriver mDriver, Connection conn);

  /**
   * Search for driver in the repository.
   * @params shortName the name for the driver
   * @param conn JDBC link for querying repository.
   * @return null if driver are not yet present in repository or
   *  loaded representation.
   */
  public abstract MDriver findDriver(String shortName, Connection conn);

  /**
   * Register driver config in repository.
   *
   * Save driver config into repository. Driver config  should not be already
   * registered or present in the repository.
   *
   * @param mDriver Driver config that should be registered.
   * @param conn JDBC link for querying repository.
   */
  public abstract void registerDriver(MDriver mDriver, Connection conn);

  /**
   * Create or update the repository schema structures.
   *
   * This method will be called from the Sqoop server if enabled via a config
   * {@link RepoConfigurationConstants#SYSCFG_REPO_SCHEMA_IMMUTABLE} to enforce
   * changing the repository schema structure or explicitly via the
   * {@link UpgradeTool} Repository should not change its schema structure
   * outside of this method. This method must be no-op in case that the schema
   * structure do not need any upgrade.
   * @param conn JDBC link for querying repository
   */
  public abstract void createOrUpgradeRepository(Connection conn);

  /**
   * Return true if internal repository structures exists and are suitable for use.
   * This method should return false in case that the structures do exists, but
   * are not suitable to use i.e corrupted as part of the upgrade
   * @param conn JDBC link for querying repository
   * @return Boolean values if internal structures are suitable for use
   */
  public abstract boolean isRepositorySuitableForUse(Connection conn);

  /**
   * Termination callback for repository.
   *
   * Should clean up all resources and commit all uncommitted data.
   */
  public abstract void shutdown();

  /**
   * Specify query that Sqoop can use to validate link to
   * repository. This query should return at least one row.
   *
   * @return Query or NULL in case that this repository do not support or do not
   *   want to validate live links.
   */
  public abstract String validationQuery();

  /**
   * Save given link to repository. This link must not be already
   * present in the repository otherwise exception will be thrown.
   *
   * @param link Link object to serialize into repository.
   * @param conn Connection to the repository
   */
  public abstract void createLink(MLink link, Connection conn);

  /**
   * Update given link representation in repository. This link
   * object must already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param link Link object that should be updated in repository.
   * @param conn Connection to the repository
   */
  public abstract void updateLink(MLink link, Connection conn);

  /**
   * Check if given link exists in repository.
   *
   * @param linkId Link id
   * @param conn Connection to the repository
   * @return True if the link exists
   */
  public abstract boolean existsLink(long linkId, Connection conn);

  /**
   * Check if given Connection id is referenced somewhere and thus can't
   * be removed.
   *
   * @param linkId Link id
   * @param conn Connection to the repository
   * @return
   */
  public abstract boolean inUseLink(long linkId, Connection conn);

  /**
   * Enable or disable link with given id from the repository
   *
   * @param linkId Link object that is going to be enabled or disabled
   * @param enabled Enable or disable
   * @param conn Connection to the repository
   */
  public abstract void enableLink(long linkId, boolean enabled, Connection conn);

  /**
   * Delete link with given id from the repository.
   *
   * @param linkId Link object that should be removed from repository
   * @param conn Connection to the repository
   */
  public abstract void deleteLink(long linkId, Connection conn);

  /**
   * Delete the input values for the link with given id from the
   * repository.
   * @param linkId Link object whose inputs should be removed from repository
   * @param conn Connection to the repository
   */
  protected abstract void deleteLinkInputs(long linkId, Connection conn);

  /**
   * Find link with given id in repository.
   *
   * @param linkId Link id
   * @param conn Connection to the repository
   * @return  the link that is saved in repository
   */
  public abstract MLink findLink(long linkId, Connection conn);

  /**
   * Find link with given name in repository.
   *
   * @param linkName unique link name
   * @param conn Connection to the repository
   * @return the link that is saved in repository or returns null if not found
   */
  public abstract MLink findLink(String linkName, Connection conn);

  /**
   * Get all link objects.
   *
   * @param conn Connection to the repository
   * @return List will all saved link objects
   */
  public abstract List<MLink> findLinks(Connection conn);


  /**
   * Save given job to repository. This job object must not be already
   * present in the repository otherwise exception will be thrown.
   *
   * @param job Job object to serialize into repository.
   * @param conn Connection to the repository
   */
  public abstract void createJob(MJob job, Connection conn);

  /**
   * Update given job representation in repository. This job object must
   * already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param job Job object that should be updated in repository.
   * @param conn Connection to the repository
   */
  public abstract void updateJob(MJob job, Connection conn);

  /**
   * Check if given job exists in the repository.
   *
   * @param jobId Job id
   * @param conn Connection to the repository
   * @return True if the job exists
   */
  public abstract boolean existsJob(long jobId, Connection conn);

  /**
   * Check if given job id is referenced somewhere and thus can't
   * be removed.
   *
   * @param jobId Job id
   * @param conn Connection to the repository
   * @return
   */
  public abstract boolean inUseJob(long jobId, Connection conn);

  /**
   * Enable or disable job with given id from the repository
   *
   * @param jobId Job id
   * @param enabled Enable or disable
   * @param conn Connection to the repository
   */
  public abstract void enableJob(long jobId, boolean enabled, Connection conn);

  /**
   * Delete the input values for the job with given id from the repository.
   * @param id Job object whose inputs should be removed from repository
   * @param conn Connection to the repository
   */
  protected abstract void deleteJobInputs(long id, Connection conn);
  /**
   * Delete job with given id from the repository. This method will
   * delete all inputs for this job also.
   *
   * @param jobId Job object that should be removed from repository
   * @param conn Connection to the repository
   */
  public abstract void deleteJob(long jobId, Connection conn);

  /**
   * Find job with given id in repository.
   *
   * @param jobId Job id
   * @param conn Connection to the repository
   * @return job for a given id that is present in the repository
   */
  public abstract MJob findJob(long jobId, Connection conn);

  /**
   * Find job with given name in repository.
   *
   * @param name unique name for the job
   * @param conn Connection to the repository
   * @return job for a given name that is present in the repository or null if not present
   */
  public abstract MJob findJob(String name, Connection conn);

  /**
   * Get all job objects.
   *
   * @param conn Connection to the repository
   * @return List will all saved job objects
   */
  public abstract List<MJob> findJobs(Connection conn);

  /**
   * Save given submission associates with a job in repository.
   *
   * @param submission Submission object
   * @param conn Connection to the repository
   */
  public abstract void createSubmission(MSubmission submission, Connection conn);

  /**
   * Check if submission with given id already exists in repository.
   *
   * @param submissionId Submission internal id
   * @param conn Connection to the repository
   */
  public abstract boolean existsSubmission(long submissionId, Connection conn);

  /**
   * Update given submission in repository.
   *
   * @param submission Submission object
   * @param conn Connection to the repository
   */
  public abstract void updateSubmission(MSubmission submission, Connection conn);

  /**
   * Remove submissions older then threshold from repository.
   *
   * @param threshold Threshold date
   * @param conn Connection to the repository
   */
  public abstract void purgeSubmissions(Date threshold, Connection conn);

  /**
   * Return list of unfinished submissions (as far as repository is concerned).
   *
   * @param conn Connection to the repository
   * @return List of unfinished submissions.
   */
  public abstract List<MSubmission> findUnfinishedSubmissions(Connection conn);

  /**
   * Return list of all submissions from the repository.
   *
   * @param conn Connection to the repository
   * @return List of all submissions.
   */
  public abstract List<MSubmission> findSubmissions(Connection conn);

  /**
   * Return list of submissions from the repository for given jobId.
   * @param jobId Job id
   * @param conn Connection to the repository
   * @return List of submissions
   */
  public abstract List<MSubmission> findSubmissionsForJob(long jobId, Connection conn);

  /**
   * Find last submission for given jobId.
   *
   * @param jobId Job id
   * @param conn Connection to the repository
   * @return Most recent submission
   */
  public abstract MSubmission findLastSubmissionForJob(long jobId, Connection conn);

  /**
   * fetch the job config for the FROM type for the given name
   * @param jobId id of the job
   * @param configName name of the config unique to this job and type
   * @param conn Connection to the repository
   * @return config object
   */
   public abstract MConfig findFromJobConfig(long jobId, String configName, Connection con);


   /**
    * fetch the job config for the TO type for the given name
    * @param jobId id of the job
    * @param configName name of the config unique to this job and type
    * @param conn Connection to the repository
    * @return config object
    */
   public abstract MConfig findToJobConfig(long jobId, String configName, Connection con);


   /**
    * fetch the job config for the DRIVER type for the given name
    * @param jobId id of the job
    * @param configName name of the config unique to this job and type
    * @param conn Connection to the repository
    * @return config object
    */
   public abstract MConfig findDriverJobConfig(long jobId, String configName, Connection con);


   /**
    * fetch the link config for the link type for the given name
    * @param linkId id of the link
    * @param configName name of the config unique to this link and type
    * @param conn Connection to the repository
    * @return config object
    */
   public abstract MConfig findLinkConfig(long linkId, String configName, Connection con);

   /**
    * Update the config object for the job
    * @param jobId id of the job
    * @param config name of the config
    * @param type entity type updating the link config
    * @param conn Connection to the repository
    */
   public abstract void updateJobConfig(long jobId, MConfig config, MConfigUpdateEntityType type,  Connection con);

   /**
    * Update the config object for the link
    * @param linkId id of the link
    * @param config name of the config
    * @param type entity type updating the link config
    * @param conn Connection to the repository
    */
   public abstract void updateLinkConfig(long linkId, MConfig config, MConfigUpdateEntityType type, Connection con);

}
