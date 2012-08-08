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

import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFramework;


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
}
