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

import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFramework;
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
   *
   * And return corresponding metadata structure.
   *
   * @param shortName Connector unique name
   * @param conn JDBC connection for querying repository.
   * @return null if connector is not yet registered in repository or
   *   loaded representation.
   */
  public abstract MConnector findConnector(String shortName, Connection conn);

  /**
   * Register given connector in repository.
   *
   * Save given connector data to the repository. Given connector should not be
   * already registered or present in the repository.
   *
   * @param mc Connector that should be registered.
   * @param conn JDBC connection for querying repository.
   */
  public abstract void registerConnector(MConnector mc, Connection conn);


  /**
   * Retrieve connections which use the given connector.
   * @param connectorID Connector ID whose connections should be fetched
   * @param conn JDBC connection for querying repository
   * @return List of MConnections that use <code>connectorID</code>.
   */
  public abstract List<MConnection> findConnectionsForConnector(long
    connectorID, Connection conn);

  /**
   * Retrieve jobs which use the given connection.
   *
   * @param connectorID Connector ID whose jobs should be fetched
   * @param conn JDBC connection for querying repository
   * @return List of MJobs that use <code>connectionID</code>.
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
   * @param conn JDBC connection for querying repository
   */

  public abstract void updateConnector(MConnector mConnector, Connection conn);


  /**
   * Update the framework with the new data supplied in the
   * <tt>mFramework</tt>.
   * Also Update all forms in the repository
   * with the forms specified in <tt>mFramework</tt>. <tt>mFramework </tt> must
   * minimally have the connectorID and all required forms (including ones
   * which may not have changed). After this operation the repository is
   * guaranteed to only have the new forms specified in this object.
   *
   * @param mFramework The new data to be inserted into the repository for
   *                     the framework.
   * @param conn JDBC connection for querying repository
   */
  public abstract void updateFramework(MFramework mFramework, Connection conn);


  /**
   * Search for framework metadata in the repository.
   *
   * @param conn JDBC connection for querying repository.
   * @return null if framework metadata are not yet present in repository or
   *  loaded representation.
   */
  public abstract MFramework findFramework(Connection conn);

  /**
   * Register framework metadata in repository.
   *
   * Save framework metadata into repository. Metadata should not be already
   * registered or present in the repository.
   *
   * @param mf Framework metadata that should be registered.
   * @param conn JDBC connection for querying repository.
   */
  public abstract void registerFramework(MFramework mf, Connection conn);

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
   * Specify query that Sqoop framework can use to validate connection to
   * repository. This query should return at least one row.
   *
   * @return Query or NULL in case that this repository do not support or do not
   *   want to validate live connections.
   */
  public abstract String validationQuery();

  /**
   * Save given connection to repository. This connection must not be already
   * present in the repository otherwise exception will be thrown.
   *
   * @param connection Connection object to serialize into repository.
   * @param conn Connection to metadata repository
   */
  public abstract void createConnection(MConnection connection,
    Connection conn);

  /**
   * Update given connection representation in repository. This connection
   * object must already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param connection Connection object that should be updated in repository.
   * @param conn Connection to metadata repository
   */
  public abstract void updateConnection(MConnection connection,
    Connection conn);

  /**
   * Check if given connection exists in metastore.
   *
   * @param connetionId Connection id
   * @param conn Connection to metadata repository
   * @return True if the connection exists
   */
  public abstract boolean existsConnection(long connetionId, Connection conn);

  /**
   * Check if given Connection id is referenced somewhere and thus can't
   * be removed.
   *
   * @param connectionId Connection id
   * @param conn Connection to metadata repository
   * @return
   */
  public abstract boolean inUseConnection(long connectionId, Connection conn);

  /**
   * Enable or disable connection with given id from metadata repository
   *
   * @param connectionId Connection object that is going to be enabled or disabled
   * @param enabled Enable or disable
   * @param conn Connection to metadata repository
   */
  public abstract void enableConnection(long connectionId, boolean enabled, Connection conn);

  /**
   * Delete connection with given id from metadata repository.
   *
   * @param connectionId Connection object that should be removed from repository
   * @param conn Connection to metadata repository
   */
  public abstract void deleteConnection(long connectionId, Connection conn);

  /**
   * Delete the input values for the connection with given id from the
   * repository.
   * @param id Connection object whose inputs should be removed from repository
   * @param conn Connection to metadata repository
   */
  public abstract void deleteConnectionInputs(long id, Connection conn);
  /**
   * Find connection with given id in repository.
   *
   * @param connectionId Connection id
   * @param conn Connection to metadata repository
   * @return Deserialized form of the connection that is saved in repository
   */
  public abstract MConnection findConnection(long connectionId,
    Connection conn);

  /**
   * Get all connection objects.
   *
   * @param conn Connection to metadata repository
   * @return List will all saved connection objects
   */
  public abstract List<MConnection> findConnections(Connection conn);


  /**
   * Save given job to repository. This job object must not be already
   * present in the repository otherwise exception will be thrown.
   *
   * @param job Job object to serialize into repository.
   * @param conn Connection to metadata repository
   */
  public abstract void createJob(MJob job, Connection conn);

  /**
   * Update given job representation in repository. This job object must
   * already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param job Job object that should be updated in repository.
   * @param conn Connection to metadata repository
   */
  public abstract void updateJob(MJob job, Connection conn);

  /**
   * Check if given job exists in metastore.
   *
   * @param jobId Job id
   * @param conn Connection to metadata repository
   * @return True if the job exists
   */
  public abstract boolean existsJob(long jobId, Connection conn);

  /**
   * Check if given job id is referenced somewhere and thus can't
   * be removed.
   *
   * @param jobId Job id
   * @param conn Connection to metadata repository
   * @return
   */
  public abstract boolean inUseJob(long jobId, Connection conn);

  /**
   * Enable or disable job with given id from the repository
   *
   * @param jobId Job id
   * @param enabled Enable or disable
   * @param conn Connection to metadata repository
   */
  public abstract void enableJob(long jobId, boolean enabled, Connection conn);

  /**
   * Delete the input values for the job with given id from the repository.
   * @param id Job object whose inputs should be removed from repository
   * @param conn Connection to metadata repository
   */
  public abstract void deleteJobInputs(long id, Connection conn);
  /**
   * Delete job with given id from metadata repository. This method will
   * delete all inputs for this job also.
   *
   * @param jobId Job object that should be removed from repository
   * @param conn Connection to metadata repository
   */
  public abstract void deleteJob(long jobId, Connection conn);

  /**
   * Find job with given id in repository.
   *
   * @param jobId Job id
   * @param conn Connection to metadata repository
   * @return Deserialized form of the job that is present in the repository
   */
  public abstract MJob findJob(long jobId, Connection conn);

  /**
   * Get all job objects.
   *
   * @param conn Connection to metadata repository
   * @return List will all saved job objects
   */
  public abstract List<MJob> findJobs(Connection conn);

  /**
   * Save given submission in repository.
   *
   * @param submission Submission object
   * @param conn Connection to metadata repository
   */
  public abstract void createSubmission(MSubmission submission,
    Connection conn);

  /**
   * Check if submission with given id already exists in repository.
   *
   * @param submissionId Submission internal id
   * @param conn Connection to metadata repository
   */
  public abstract boolean existsSubmission(long submissionId, Connection conn);

  /**
   * Update given submission in repository.
   *
   * @param submission Submission object
   * @param conn Connection to metadata repository
   */
  public abstract void updateSubmission(MSubmission submission,
    Connection conn);

  /**
   * Remove submissions older then threshold from repository.
   *
   * @param threshold Threshold date
   * @param conn Connection to metadata repository
   */
  public abstract void purgeSubmissions(Date threshold, Connection conn);

  /**
   * Return list of unfinished submissions (as far as repository is concerned).
   *
   * @param conn Connection to metadata repository
   * @return List of unfinished submissions.
   */
  public abstract List<MSubmission> findSubmissionsUnfinished(Connection conn);

  /**
   * Return list of all submissions from metadata repository.
   *
   * @param conn Connection to metadata repository
   * @return List of all submissions.
   */
  public abstract List<MSubmission> findSubmissions(Connection conn);

  /**
   * Return list of submissions from metadata repository for given jobId.
   * @param jobId Job id
   * @param conn Connection to metadata repository
   * @return List of submissions
   */
  public abstract List<MSubmission> findSubmissionsForJob(long jobId, Connection conn);

  /**
   * Find last submission for given jobId.
   *
   * @param jobId Job id
   * @param conn Connection to metadata repository
   * @return Most recent submission
   */
  public abstract MSubmission findSubmissionLastForJob(long jobId,
    Connection conn);
}
