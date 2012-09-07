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

import java.util.List;


/**
 * Defines the contract of a Repository used by Sqoop. A Repository allows
 * Sqoop to store metadata, statistics and other state relevant to Sqoop
 * Jobs in the system.
 */
public interface Repository {

  public RepositoryTransaction getTransaction();

  /**
   * Registers the given connector in the repository. If the connector was
   * already registered, its associated metadata is returned from the
   * repository.
   *
   * Method will set persistent ID of given MConnector instance in case of a
   * success.
   *
   * @param mConnector the connector metadata to be registered
   * @return <tt>null</tt> if the connector was successfully registered or
   * a instance of previously registered metadata with the same connector
   * unique name.
   */
  public MConnector registerConnector(MConnector mConnector);

  /**
   * Registers framework metadata in the repository. No more than one set of
   * framework metadata structure is allowed.
   *
   * Method will set persistent ID of given MFramework instance in case of a
   * success.
   *
   * @param mFramework Framework data that should be registered.
   */
  public void registerFramework(MFramework mFramework);

  /**
   * Save given connection to repository. This connection must not be already
   * present in the repository otherwise exception will be thrown.
   *
   * @param connection Connection object to serialize into repository.
   */
  public void createConnection(MConnection connection);

  /**
   * Update given connection representation in repository. This connection
   * object must already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param connection Connection object that should be updated in repository.
   */
  public void updateConnection(MConnection connection);

  /**
   * Delete connection with given id from metadata repository.
   *
   * @param id Connection object that should be removed from repository
   */
  public void deleteConnection(long id);

  /**
   * Find connection with given id in repository.
   *
   * @param id Connection id
   * @return Deserialized form of the connection that is saved in repository
   */
  public MConnection findConnection(long id);

  /**
   * Get all connection objects.
   *
   * @return Array will all saved connection objects
   */
  public List<MConnection> findConnections();
}
