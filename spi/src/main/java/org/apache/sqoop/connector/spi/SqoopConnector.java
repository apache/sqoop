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
package org.apache.sqoop.connector.spi;

import java.util.Locale;
import java.util.ResourceBundle;

import org.apache.sqoop.job.etl.Exporter;
import org.apache.sqoop.job.etl.Importer;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.validation.Validator;

/**
 * Service provider interface for Sqoop Connectors.
 */
public abstract class SqoopConnector {

  /**
   * Retrieve connector version.
   *
   * @return Version encoded as a string
   */
  public abstract String getVersion();

  /**
   * @param locale
   * @return the resource bundle associated with the given locale.
   */
  public abstract ResourceBundle getBundle(Locale locale);

  /**
   * @return Get connection configuration class
   */
  public abstract Class getConnectionConfigurationClass();

  /**
   * @return Get job configuration class for given type or null if not supported
   */
  public abstract Class getJobConfigurationClass(MJob.Type jobType);

  /**
   * @return an <tt>Importer</tt> that provides classes for performing import.
   */
  public abstract Importer getImporter();

  /**
   * @return an <tt>Exporter</tt> that provides classes for performing export.
   */
  public abstract Exporter getExporter();

  /**
   * Returns validation object that Sqoop framework can use to validate user
   * supplied forms before accepting them. This object will be used both for
   * connection and job forms.
   *
   * @return Validator object
   */
  public abstract Validator getValidator();

}
