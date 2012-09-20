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
import java.util.List;

import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJob;

/**
 * Set of methods required from each JDBC based repository.
 */
public interface JdbcRepositoryHandler {

  /**
   * Initialize JDBC based repository.
   *
   * @param repoContext Context for this instance
   */
  public void initialize(JdbcRepositoryContext repoContext);

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
  public MConnector findConnector(String shortName, Connection conn);

  /**
   * Register given connector in repository.
   *
   * Save given connector data to the repository. Given connector should not be
   * already registered or present in the repository.
   *
   * @param mc Connector that should be registered.
   * @param conn JDBC connection for querying repository.
   */
  public void registerConnector(MConnector mc, Connection conn);

  /**
   * Search for framework metadata in the repository.
   *
   * @param conn JDBC connection for querying repository.
   * @return null if framework metadata are not yet present in repository or
   *  loaded representation.
   */
  public MFramework findFramework(Connection conn);

  /**
   * Register framework metadata in repository.
   *
   * Save framework metadata into repository. Metadata should not be already
   * registered or present in the repository.
   *
   * @param mf Framework metadata that should be registered.
   * @param conn JDBC connection for querying repository.
   */
  public void registerFramework(MFramework mf, Connection conn);

  /**
   * Check if schema is already present in the repository.
   *
   * @return true if schema is already present or false if it's not
   */
  public boolean schemaExists();

  /**
   * Create required schema in repository.
   */
  public void createSchema();

  /**
   * Termination callback for repository.
   *
   * Should clean up all resources and commit all uncommitted data.
   */
  public void shutdown();

  /**
   * Specify query that Sqoop framework can use to validate connection to
   * repository. This query should return at least one row.
   *
   * @return Query or NULL in case that this repository do not support or do not
   *   want to validate live connections.
   */
  public String validationQuery();

  /**
   * Save given connection to repository. This connection must not be already
   * present in the repository otherwise exception will be thrown.
   *
   * @param connection Connection object to serialize into repository.
   * @param conn Connection to metadata repository
   */
  public void createConnection(MConnection connection, Connection conn);

  /**
   * Update given connection representation in repository. This connection
   * object must already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param connection Connection object that should be updated in repository.
   * @param conn Connection to metadata repository
   */
  public void updateConnection(MConnection connection, Connection conn);

  /**
   * Check if given connection exists in metastore.
   *
   * @param id Connection id
   * @param conn Connection to metadata repository
   * @return True if the connection exists
   */
  public boolean existsConnection(long id, Connection conn);

  /**
   * Delete connection with given id from metadata repository.
   *
   * @param id Connection object that should be removed from repository
   * @param conn Connection to metadata repository
   */
  public void deleteConnection(long id, Connection conn);

  /**
   * Find connection with given id in repository.
   *
   * @param id Connection id
   * @param conn Connection to metadata repository
   * @return Deserialized form of the connection that is saved in repository
   */
  public MConnection findConnection(long id, Connection conn);

  /**
   * Get all connection objects.
   *
   * @param conn Connection to metadata repository
   * @return List will all saved connection objects
   */
  public List<MConnection> findConnections(Connection conn);


  /**
   * Save given job to repository. This job object must not be already
   * present in the repository otherwise exception will be thrown.
   *
   * @param job Job object to serialize into repository.
   * @param conn Connection to metadata repository
   */
  public void createJob(MJob job, Connection conn);

  /**
   * Update given job representation in repository. This job object must
   * already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param job Job object that should be updated in repository.
   * @param conn Connection to metadata repository
   */
  public void updateJob(MJob job, Connection conn);

  /**
   * Check if given job exists in metastore.
   *
   * @param id Job id
   * @param conn Connection to metadata repository
   * @return True if the job exists
   */
  public boolean existsJob(long id, Connection conn);

  /**
   * Delete job with given id from metadata repository.
   *
   * @param id Job object that should be removed from repository
   * @param conn Connection to metadata repository
   */
  public void deleteJob(long id, Connection conn);

  /**
   * Find job with given id in repository.
   *
   * @param id Job id
   * @param conn Connection to metadata repository
   * @return Deserialized form of the job that is present in the repository
   */
  public MJob findJob(long id, Connection conn);

  /**
   * Get all job objects.
   *
   * @param conn Connection to metadata repository
   * @return List will all saved job objects
   */
  public List<MJob> findJobs(Connection conn);
}
