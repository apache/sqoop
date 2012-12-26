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

import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.connector.jdbc.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ExportJobConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ImportJobConfiguration;
import org.apache.sqoop.job.etl.Exporter;
import org.apache.sqoop.job.etl.Importer;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.validation.Validator;

public class GenericJdbcConnector extends SqoopConnector {

  private static final Importer IMPORTER = new Importer(
      GenericJdbcImportInitializer.class,
      GenericJdbcImportPartitioner.class,
      GenericJdbcImportExtractor.class,
      GenericJdbcImportDestroyer.class);

  private static final Exporter EXPORTER = new Exporter(
      GenericJdbcExportInitializer.class,
      GenericJdbcExportLoader.class,
      GenericJdbcExportDestroyer.class);


  /**
   * {@inheritDoc}
   *
   * As this is build-in connector it will return same version as rest of the
   * Sqoop infrastructure.
   */
  @Override
  public String getVersion() {
    return VersionInfo.getVersion();
  }

  @Override
  public ResourceBundle getBundle(Locale locale) {
    return ResourceBundle.getBundle(
        GenericJdbcConnectorConstants.RESOURCE_BUNDLE_NAME, locale);
  }

  @Override
  public Class getConnectionConfigurationClass() {
    return ConnectionConfiguration.class;
  }

  @Override
  public Class getJobConfigurationClass(MJob.Type jobType) {
    switch (jobType) {
      case IMPORT:
        return ImportJobConfiguration.class;
      case EXPORT:
        return ExportJobConfiguration.class;
      default:
        return null;
    }
  }

  @Override
  public Importer getImporter() {
    return IMPORTER;
  }

  @Override
  public Exporter getExporter() {
    return EXPORTER;
  }

  @Override
  public Validator getValidator() {
    // TODO(jarcec): Cache this object eventually
    return new GenericJdbcValidator();
  }

}
