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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.ConfigurableError;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MToConfig;

/**
 * Configurable represents an entity that can provide configurations for the
 * support config types {@linkplain ConfigType}
 * This api represents the interface that configurable such as the connector/driver
 * will implement to upgrade both the config and its corresponding data across different
 * versions
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class ConnectorConfigurableUpgrader {

  /**
   * Upgrade the original link config for the given config type and fill into the upgradeTarget. Note
   * that any data already in {@code upgradeTarget} maybe overwritten.
   * @param original - original config as in the repository
   * @param upgradeTarget - the instance that will be filled in with the
   *                      upgraded config
   */
  public void upgradeLinkConfig(MLinkConfig original, MLinkConfig upgradeTarget) {
    // The reasoning for throwing an exception by default is as follows.
    // Sqoop calls the upgrade apis for every connector if and only if the
    // corresponding link object that the config is associated with exists in the sqoop
    // repository. In unexpected scenarios, if a link object is created in the
    // sqoop repository without a corresponding upgrade routine for
    // the link config, then this exception will be thrown to indicate a
    // unexpected code path. In normal circumstances this
    // scenario of having a link object for a connector without link config is
    // very unlikely to happen. A likely scenario is that a connector will not have a link config and hence
    // no link object will be created and thus this method will not be invoked.
    throw new SqoopException(ConfigurableError.CONFIGURABLE_0001);

  }

  /**
   * Upgrade the original FROM job config for the given config type and fill into the upgradeTarget. Note
   * that any data already in {@code upgradeTarget} maybe overwritten.
   * @param original - original config as in the repository
   * @param upgradeTarget - the instance that will be filled in with the
   *                      upgraded config
   */
  public void upgradeFromJobConfig(MFromConfig original, MFromConfig upgradeTarget) {
    // see above for the reasoning behind the exception
    throw new SqoopException(ConfigurableError.CONFIGURABLE_0002);
  }

  /**
   * Upgrade the original TO job config for the given config type and fill into the upgradeTarget. Note
   * that any data already in {@code upgradeTarget} maybe overwritten.
   * @param original - original config as in the repository
   * @param upgradeTarget - the instance that will be filled in with the
   *                      upgraded config
   */
  public void upgradeToJobConfig(MToConfig original, MToConfig upgradeTarget) {
    // see above for the reasoning behind the exception
    throw new SqoopException(ConfigurableError.CONFIGURABLE_0003);

  }

}
