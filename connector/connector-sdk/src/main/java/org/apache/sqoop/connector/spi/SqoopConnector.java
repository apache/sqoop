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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.connector.idf.CSVIntermediateDataFormat;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.job.etl.From;
import org.apache.sqoop.job.etl.To;

/**
 * Service provider interface for Sqoop Connectors.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
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
   * @return The supported directions
   */
  public List<Direction> getSupportedDirections() {
    return Arrays.asList(new Direction[]{
        Direction.FROM,
        Direction.TO
    });
  }

  /**
   * @return Get link configuration group class
   */
  @SuppressWarnings("rawtypes")
  public abstract Class getLinkConfigurationClass();

  /**
   * @return Get job configuration group class per direction type or null if not supported
   */
  @SuppressWarnings("rawtypes")
  public abstract Class getJobConfigurationClass(Direction direction);

  /**
   * @return an <tt>From</tt> that provides classes for performing import.
   */
  public abstract From getFrom();

  /**
   * @return an <tt>To</tt> that provides classes for performing export.n
   */
  public abstract To getTo();

  /**
   * Returns an {@linkplain ConnectorConfigurableUpgrader} object that can upgrade the
   * configs related to the link and job
   * @return ConnectorConfigurableUpgrader object
   */
  public abstract ConnectorConfigurableUpgrader getConfigurableUpgrader();

  /**
   * Returns the {@linkplain IntermediateDataFormat} this connector
   * can return natively in. This will support retrieving the data as text
   * and an array of objects. This should never return null.
   *
   * @return {@linkplain IntermediateDataFormat} object
   */
  public Class<? extends IntermediateDataFormat<?>> getIntermediateDataFormat() {
    return CSVIntermediateDataFormat.class;
  }
}
