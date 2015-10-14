/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.connector.sftp;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.connector.sftp.configuration.LinkConfiguration;
import org.apache.sqoop.connector.sftp.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.job.etl.From;
import org.apache.sqoop.job.etl.To;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Implementation of a Sqoop 2 connector to support data movement to/from an
 * SFTP server.
 */
public class SftpConnector extends SqoopConnector {

  /**
   * Define the TO instance.
   */
  private static final To TO = new To(SftpToInitializer.class,
                                      SftpLoader.class,
                                      SftpToDestroyer.class);

  /**
   * {@inheritDoc}
   *
   * Since this is a built-in connector it will return the same version as the
   * rest of the Sqoop code.
   */
  @Override
  public String getVersion() {
     return VersionInfo.getBuildVersion();
  }

  /**
   * Return the configuration resource bundle for this connector.
   *
   * @param locale The Locale object.
   *
   * @return The resource bundle associated with the input locale.
   */
  @Override
  public ResourceBundle getBundle(Locale locale) {
    return ResourceBundle.getBundle(SftpConstants.RESOURCE_BUNDLE_NAME, locale);
  }

  /**
   * Get the class encapsulating link configuration for this connector.
   *
   * @return The link configuration class for this connector.
   */
  @Override
  public Class getLinkConfigurationClass() {
    return LinkConfiguration.class;
  }

  /**
   * Get the appropriate job configuration class for the input direction.
   *
   * @param direction Whether to return TO or FROM configuration class.
   *
   * @return Job configuration class for given direction.
   */
  @Override
  public Class getJobConfigurationClass(Direction direction) {
    switch (direction) {
      case TO:
        return ToJobConfiguration.class;
      default:
        return null;
    }
  }

  /**
   * Get the object which defines classes for performing import jobs.
   *
   * @return the From object defining classes for performing import.
   */
  @Override
  public From getFrom() {
    return null;
  }

  /**
   * Get the object which defines classes for performing export jobs.
   *
   * @return the To object defining classes for performing export.
   */
  @Override
  public To getTo() {
    return TO;
  }

  /**
   * Returns an {@linkplain org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader}
   * object that can upgrade the connection and job configs.
   *
   * @return configurable upgrader object
   */
  @Override
  public ConnectorConfigurableUpgrader getConfigurableUpgrader(String oldConnectorVersion) {
    return new SftpConnectorUpgrader();
  }

  /**
   * Return a List of directions supported by this connector.
   *
   * @return list of enums representing supported directions.
   */
  public List<Direction> getSupportedDirections() {
    return Arrays.asList(Direction.TO);
  }
}
