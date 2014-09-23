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

import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
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
   * And return corresponding connector structure.
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
   * @param connectorID Connector ID whose links should be fetched
   * @param conn JDBC link for querying repository
   * @return List of MLinks that use <code>connectorID</code>.
   */
  public abstract List<MLink> findLinksForConnector(long
    connectorID, Connection conn);

  /**
   * Retrieve jobs which use the given link.
   *
   * @param connectorID Connector ID whose jobs should be fetched
   * @param conn JDBC link for querying repository
   * @return List of MJobs that use <code>linkID</code>.
   */
  public abstract List<MJob> findJobsForConnector(long connectorID,
    Connection conn);

  /**
   * Update the connector with the new data supplied in the <tt>newConnector</tt>.
   * Also Update all forms associated with this connector in the repository
   * with the forms specified in <tt>mConnector</tt>. <tt>mConnector </tt> must
   * minimally have the connectorID and all required forms (including ones
   * which may not have changed). After this operation the repository is
   * guaranteed to only have the new forms specified in this object.
   *
   * @param mConnector The new data to be inserted into the repository for
   *                     this connector.
   * @param conn JDBC link for querying repository
   */

  public abstract void updateConnector(MConnector mConnector, Connection conn);


  /**
   * Update the driverConfig with the new data supplied in the
   * <tt>mDriverConfig</tt>.
   * Also Update all forms in the repository
   * with the forms specified in <tt>mDriverConfig</tt>. <tt>mDriverConfig </tt> must
   * minimally have the connectorID and all required forms (including ones
   * which may not have changed). After this operation the repository is
   * guaranteed to only have the new forms specified in this object.
   *
   * @param mDriverConfig The new data to be inserted into the repository for
   *                     the driverConfig.
   * @param conn JDBC link for querying repository
   */
  public abstract void updateDriverConfig(MDriverConfig mDriverConfig, Connection conn);


  /**
   * Search for driverConfigin the repository.
   *
   * @param conn JDBC link for querying repository.
   * @return null if driverConfig are not yet present in repository or
   *  loaded representation.
   */
  public abstract MDriverConfig findDriverConfig(Connection conn);

  /**
   * Register driver config in repository.
   *
   * Save driver config into repository. Driver config  should not be already
   * registered or present in the repository.
   *
   * @param driverConfig Driver config that should be registered.
   * @param conn JDBC link for querying repository.
   */
  public abstract void registerDriverConfig(MDriverConfig driverConfig, Connection conn);

  /**
   * Return true if repository tables exists and are suitable for use.
   *
   * This method should return false in case that the tables do exists, but
   * are not suitable for use or if they requires upgrade.
   *
   * @return Boolean values if internal structures are suitable for use
   */
  public abstract boolean haveSuitableInternals(Connection conn);

  /**
   * Create or update tables in the repository.
   *
   * This method will be called only if Sqoop server is enabled with changing
   * repository on disk structures. Repository should not change its disk structures
   * outside of this method. This method must be no-op in case that the structures
   * do not need any maintenance.
   */
  public abstract void createOrUpdateInternals(Connection conn);

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
   * @param id Link object whose inputs should be removed from repository
   * @param conn Connection to the repository
   */
  public abstract void deleteLinkInputs(long id, Connection conn);
  /**
   * Find link with given id in repository.
   *
   * @param linkId Link id
   * @param conn Connection to the repository
   * @return Deserialized form of the link that is saved in repository
   */
  public abstract MLink findLink(long linkId, Connection conn);

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
  public abstract void deleteJobInputs(long id, Connection conn);
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
   * @return Deserialized form of the job that is present in the repository
   */
  public abstract MJob findJob(long jobId, Connection conn);

  /**
   * Get all job objects.
   *
   * @param conn Connection to the repository
   * @return List will all saved job objects
   */
  public abstract List<MJob> findJobs(Connection conn);

  /**
   * Save given submission in repository.
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
  public abstract List<MSubmission> findSubmissionsUnfinished(Connection conn);

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
  public abstract MSubmission findSubmissionLastForJob(long jobId,
    Connection conn);
}
