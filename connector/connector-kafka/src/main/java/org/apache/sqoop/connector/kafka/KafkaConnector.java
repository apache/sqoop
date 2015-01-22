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

package org.apache.sqoop.connector.kafka;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.connector.kafka.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.kafka.configuration.LinkConfiguration;
import org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.job.etl.From;
import org.apache.sqoop.job.etl.To;


import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

public class KafkaConnector extends SqoopConnector {

  private static final To TO = new To(
          KafkaToInitializer.class,
          KafkaLoader.class,
          KafkaToDestroyer.class);

  /**
   * Retrieve connector version.
   *
   * @return Version encoded as a string
   */
  @Override
  public String getVersion() {
    return VersionInfo.getBuildVersion();
  }

  /**
   * @param locale
   * @return the resource bundle associated with the given locale.
   */
  @Override
  public ResourceBundle getBundle(Locale locale) {
    return ResourceBundle.getBundle(KafkaConstants.RESOURCE_BUNDLE_NAME, locale);
  }

  /**
   * @return Get link configuration group class
   */
  @Override
  public Class getLinkConfigurationClass() {
    return LinkConfiguration.class;
  }

  /**
   * @param direction
   * @return Get job configuration group class per direction type or null if
   * not supported
   */
  @Override
  public Class getJobConfigurationClass(Direction direction) {
    return ToJobConfiguration.class;
  }

  @Override
  public List<Direction> getSupportedDirections() {
    // TODO: Remove when we add the FROM part of the connector (SQOOP-1583)
    return Arrays.asList(Direction.TO);
  }

  /**
   * @return an <tt>From</tt> that provides classes for performing import.
   */
  @Override
  public From getFrom() {
    //TODO: SQOOP-1583
    return null;
  }

  /**
   * @return an <tt>To</tt> that provides classes for performing export.n
   */
  @Override
  public To getTo() {
    return TO;
  }

  /**
   * Returns an {@linkplain org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader} object that can upgrade the
   * configs related to the link and job
   *
   * @return ConnectorConfigurableUpgrader object
   */
  @Override
  public ConnectorConfigurableUpgrader getConfigurableUpgrader() {
    // Nothing to upgrade at this point
    return null;
  }

}
