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
package org.apache.sqoop.repository.derby;

public final class DerbyRepoConstants {

  public static final String CONF_PREFIX_DERBY = "derby.";

  @Deprecated
  // use only for the upgrade code
  public static final String SYSKEY_VERSION = "version";

  /**
   * Expected version of the repository structures.
   *
   * History:
   * 0 - empty/unknown state
   * 1 - First two releases (1.99.1, 1.99.2)
   * 2 - Version 1.99.3
   *     added SQ_SYSTEM, SQ_CONNECTION add column ENABLED,
   *     SQ_CONNECTION add column CREATION_USER, SQ_CONNECTION add column UPDATE_USER,
   *     SQ_JOB add column ENABLED, SQ_JOB add column CREATION_USER,
   *     SQ_JOB add column UPDATE_USER, SQ_SUBMISSION add column CREATION_USER,
   *     SQ_SUBMISSION add column UPDATE_USER
   * 3 - Version 1.99.4
   *     SQ_SUBMISSION modified SQS_EXTERNAL_ID varchar(50)
   *     Increased size of SQ_CONNECTOR.SQC_VERSION to 64
   * 4 - Version 1.99.4
   *     Changed to FROM/TO design.
   * 5 - Version 1.99.5
   * 6 - Version 1.99.6
   */
  public static final int LATEST_DERBY_REPOSITORY_VERSION = 6;

  private DerbyRepoConstants() {
    // Disable explicit object creation
  }
}