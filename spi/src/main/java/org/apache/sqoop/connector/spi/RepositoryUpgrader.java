/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sqoop.connector.spi;

import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MLinkConfig;

/**
 * Repository represents the sqoop entity store. Sqoop entities include
 * the connectors, links, jobs and submissions and corresponding configs.
 *
 */
public abstract class RepositoryUpgrader {

  /**
   * Upgrade the original link config and fill into the upgradeTarget. Note
   * that any data already in {@code upgradeTarget} maybe overwritten.
   * @param original - original link config as in the repository
   * @param upgradeTarget - the instance that will be filled in with the
   *                      upgraded link config.
   */
  public abstract void upgrade(MLinkConfig original, MLinkConfig upgradeTarget);
  /**
   * Upgrade the original job config and fill into the upgradeTarget. Note
   * that any config data already in {@code upgradeTarget} maybe overwritten.
   * This method must be called only after the link config has
   * already been upgraded.
   * @param original - original job config as in the repository
   * @param upgradeTarget - the instance that will be filled in with the
   *                      upgraded job config.
   *  NOTE(VB): This api will be revisited to accomodate from and to job config update
   */
  public abstract void upgrade(MConfigList original, MConfigList upgradeTarget);
}

