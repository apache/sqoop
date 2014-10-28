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
package org.apache.sqoop.connector.jdbc;

import java.util.Locale;
import java.util.ResourceBundle;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader;
import org.apache.sqoop.job.etl.From;
import org.apache.sqoop.job.etl.To;
import org.apache.sqoop.connector.spi.SqoopConnector;

public class GenericJdbcConnector extends SqoopConnector {

  private static final From FROM = new From(
      GenericJdbcFromInitializer.class,
      GenericJdbcPartitioner.class,
      GenericJdbcExtractor.class,
      GenericJdbcFromDestroyer.class);

  private static final To TO = new To(
      GenericJdbcToInitializer.class,
      GenericJdbcLoader.class,
      GenericJdbcToDestroyer.class);


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

  @Override
  public ResourceBundle getBundle(Locale locale) {
    return ResourceBundle.getBundle(
      GenericJdbcConnectorConstants.RESOURCE_BUNDLE_NAME, locale);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Class getLinkConfigurationClass() {
    return LinkConfiguration.class;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Class getJobConfigurationClass(Direction jobType) {
    switch (jobType) {
      case FROM:
        return FromJobConfiguration.class;
      case TO:
        return ToJobConfiguration.class;
      default:
        return null;
    }
  }

  @Override
  public From getFrom() {
    return FROM;
  }

  @Override
  public To getTo() {
    return TO;
  }

  @Override
  public ConnectorConfigurableUpgrader getConfigurableUpgrader() {
    return new GenericJdbcConnectorUpgrader();
  }

}
