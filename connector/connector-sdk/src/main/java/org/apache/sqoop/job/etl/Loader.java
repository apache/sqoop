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
package org.apache.sqoop.job.etl;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * This allows connector to load data into a target system.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Loader<LinkConfiguration, ToJobConfiguration> {

  /**
   * Load data to target.
   *
   * @param context Loader context object
   * @param linkConfiguration link configuration object
   * @param jobConfiguration TO job configuration object
   * @throws Exception
   */
  public abstract void load(LoaderContext context, LinkConfiguration linkConfiguration,
      ToJobConfiguration jobConfiguration) throws Exception;

  /**
   * Return the number of rows witten by the last call to
   * {@linkplain Loader#load(org.apache.sqoop.job.etl.LoaderContext, java.lang.Object) }
   * method. This method returns only the number of rows written in the last call,
   * and not a cumulative total of the number of rows written by this Loader
   * since its creation.
   *
   * @return the number of rows written by the last call to
   * {@linkplain Loader#load(org.apache.sqoop.job.etl.LoaderContext, java.lang.Object) }
   */
  public abstract long getRowsWritten();

}
