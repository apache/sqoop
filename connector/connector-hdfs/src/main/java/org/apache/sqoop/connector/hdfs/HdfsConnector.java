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

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.connector.hdfs.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.spi.MetadataUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.job.etl.From;
import org.apache.sqoop.job.etl.To;
import org.apache.sqoop.validation.Validator;

import java.util.Locale;
import java.util.ResourceBundle;

public class HdfsConnector extends SqoopConnector {

  private static final From FROM = new From(
          HdfsInitializer.class,
          HdfsPartitioner.class,
          HdfsExtractor.class,
          HdfsDestroyer.class);

  private static final To TO = new To(
          HdfsInitializer.class,
          HdfsLoader.class,
          HdfsDestroyer.class);

  private static final HdfsValidator hdfsValidator = new HdfsValidator();

  /**
   * Retrieve connector version.
   *
   * @return Version encoded as a string
   */
  @Override
  public String getVersion() {
    return VersionInfo.getVersion();
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
  @Override
  public Class getConnectionConfigurationClass() {
    return ConnectionConfiguration.class;
  }

  /**
   * @param jobType
   * @return Get job configuration class for given type or null if not supported
   */
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
   * Returns validation object that Sqoop framework can use to validate user
   * supplied forms before accepting them. This object will be used both for
   * connection and job forms.
   *
   * @return Validator object
   */
  @Override
  public Validator getValidator() {
    return hdfsValidator;
  }

  /**
   * Returns an {@linkplain org.apache.sqoop.connector.spi.MetadataUpgrader} object that can upgrade the
   * connection and job metadata.
   *
   * @return MetadataUpgrader object
   */
  @Override
  public MetadataUpgrader getMetadataUpgrader() {
    return new HdfsMetadataUpgrader();
  }
}
