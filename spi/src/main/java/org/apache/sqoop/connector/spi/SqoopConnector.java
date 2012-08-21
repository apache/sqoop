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

import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

import org.apache.sqoop.job.etl.Exporter;
import org.apache.sqoop.job.etl.Importer;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MJob;

/**
 * Service provider interface for Sqoop Connectors.
 */
public interface SqoopConnector {

  /**
   * @param locale
   * @return the resource bundle associated with the given locale.
   */
  public ResourceBundle getBundle(Locale locale);

  /**
   * @return Get connection structure
   */
  public MConnection getConnection();

  /**
   * @return Get supported jobs and their associated data structures
   */
  public List<MJob> getJobs();

  /**
   * @return an <tt>Importer</tt> that provides classes for performing import.
   */
  public Importer getImporter();

  /**
   * @return an <tt>Exporter</tt> that provides classes for performing export.
   */
  public Exporter getExporter();

}
