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

import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MSubmission;

import java.util.Date;
import java.util.List;


/**
 * Defines the contract of a Repository used by Sqoop. A Repository allows
 * Sqoop to store metadata, statistics and other state relevant to Sqoop
 * Jobs in the system.
 */
public interface Repository {

  RepositoryTransaction getTransaction();

  /**
   * Registers given connector in the repository and return registered
   * variant. This method might return an exception in case that metadata for
   * given connector are already registered with different structure.
   *
   * @param mConnector the connector metadata to be registered
   * @return Registered connector structure
   */
  MConnector registerConnector(MConnector mConnector);


  /**
   * Registers given framework in the repository and return registered
   * variant. This method might return an exception in case that metadata for
   * given framework are already registered with different structure.
   *
   * @param mFramework framework metadata to be registered
   * @return Registered connector structure
   */
  MFramework registerFramework(MFramework mFramework);

  /**
   * Save given connection to repository. This connection must not be already
   * present in the repository otherwise exception will be thrown.
   *
   * @param connection Connection object to serialize into repository.
   */
  void createConnection(MConnection connection);

  /**
   * Update given connection representation in repository. This connection
   * object must already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param connection Connection object that should be updated in repository.
   */
  void updateConnection(MConnection connection);

  /**
   * Delete connection with given id from metadata repository.
   *
   * @param id Connection object that should be removed from repository
   */
  void deleteConnection(long id);

  /**
   * Find connection with given id in repository.
   *
   * @param id Connection id
   * @return Deserialized form of the connection that is saved in repository
   */
  MConnection findConnection(long id);

  /**
   * Get all connection objects.
   *
   * @return List will all saved connection objects
   */
  List<MConnection> findConnections();

  /**
   * Save given job to repository. This job object must not be already present
   * in repository otherwise exception will be thrown.
   *
   * @param job Job object that should be saved to repository
   */
  void createJob(MJob job);

  /**
   * Update given job metadata in repository. This object must already be saved
   * in repository otherwise exception will be thrown.
   *
   * @param job Job object that should be updated in the repository
   */
  void updateJob(MJob job);

  /**
   * Delete job with given id from metadata repository.
   *
   * @param id Job id that should be removed
   */
  void deleteJob(long id);

  /**
   * Find job object with given id.
   *
   * @param id Job id
   * @return Deserialized form of job loaded from repository
   */
  MJob findJob(long id);

  /**
   * Get all job objects.
   *
   * @return List of all jobs in the repository
   */
  List<MJob> findJobs();

  /**
   * Create new submission record in repository.
   *
   * @param submission Submission object that should be serialized to repository
   */
  void createSubmission(MSubmission submission);

  /**
   * Update already existing submission record in repository.
   *
   * @param submission Submission object that should be updated
   */
  void updateSubmission(MSubmission submission);

  /**
   * Remove submissions older then given date from repository.
   *
   * @param threshold Threshold date
   */
  void purgeSubmissions(Date threshold);

  /**
   * Return all unfinished submissions as far as repository is concerned.
   *
   * @return List of unfinished submissions
   */
  List<MSubmission> findSubmissionsUnfinished();

  /**
   * Find last submission for given jobId.
   *
   * @param jobId Job id
   * @return Most recent submission
   */
  MSubmission findSubmissionLastForJob(long jobId);
}
