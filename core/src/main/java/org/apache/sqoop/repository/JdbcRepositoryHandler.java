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

import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFramework;

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
}
