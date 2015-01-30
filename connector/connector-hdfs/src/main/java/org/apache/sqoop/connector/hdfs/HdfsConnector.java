/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.connector.hdfs;

import java.util.Locale;
import java.util.ResourceBundle;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.connector.hdfs.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.error.code.HdfsConnectorError;
import org.apache.sqoop.job.etl.From;
import org.apache.sqoop.job.etl.To;

public class HdfsConnector extends SqoopConnector {

  private static final From FROM = new From(
          HdfsFromInitializer.class,
          HdfsPartitioner.class,
          HdfsExtractor.class,
          HdfsFromDestroyer.class);

  private static final To TO = new To(
          HdfsToInitializer.class,
          HdfsLoader.class,
          HdfsToDestroyer.class);

  /**
   * {@inheritDoc}
   *
   * As this is built-in connector it will return same version as rest of the
   * Sqoop code.
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
    return ResourceBundle.getBundle(
            HdfsConstants.RESOURCE_BUNDLE_NAME, locale);
  }

  /**
   * @return Get connection configuration class
   */
  @SuppressWarnings("rawtypes")
  @Override
  public Class getLinkConfigurationClass() {
    return LinkConfiguration.class;
  }

  /**
   * @param jobType
   * @return Get job configuration class for given type or null if not supported
   */
  @SuppressWarnings("rawtypes")
  @Override
  public Class getJobConfigurationClass(Direction jobType) {
    switch (jobType) {
      case FROM:
        return FromJobConfiguration.class;
      case TO:
        return ToJobConfiguration.class;
      default:
        throw new SqoopException(
                HdfsConnectorError.GENERIC_HDFS_CONNECTOR_0006,
                String.valueOf(jobType));
    }
  }

  /**
   * @return an <tt>From</tt> that provides classes for performing import.
   */
  @Override
  public From getFrom() {
    return FROM;
  }

  /**
   * @return an <tt>To</tt> that provides classes for performing export.
   */
  @Override
  public To getTo() {
    return TO;
  }

  /**
   * Returns an {@linkplain org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader} object that can upgrade the
   * connection and job configs.
   *
   * @return configurable upgrader object
   */
  @Override
  public ConnectorConfigurableUpgrader getConfigurableUpgrader() {
    return new HdfsConnectorUpgrader();
  }
}
