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

import java.util.HashSet;
import java.util.Set;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.schema.NullSchema;
import org.apache.sqoop.schema.Schema;

/**
 * This allows connector to define initialization work for execution,
 * for example, context configuration.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Initializer<LinkConfiguration, JobConfiguration> {

  /**
   * Initialize new submission based on given configuration properties. Any
   * needed temporary values might be saved to context object and they will be
   * promoted to all other part of the workflow automatically.
   *
   * @param context Initializer context object
   * @param linkConfiguration link configuration object
   * @param jobConfiguration job configuration object for the FROM and TO
   *        In case of the FROM initializer this will represent the FROM job configuration
   *        In case of the TO initializer this will represent the TO job configuration
   */
  public abstract void initialize(InitializerContext context, LinkConfiguration linkConfiguration,
      JobConfiguration jobConfiguration);

  /**
   * Return list of all jars that this particular connector needs to operate on
   * following job. This method will be called after running initialize method.
   * @param context Initializer context object
   * @param linkConfiguration link configuration object
   * @param jobConfiguration job configuration object for the FROM and TO
   *        In case of the FROM initializer this will represent the FROM job configuration
   *        In case of the TO initializer this will represent the TO job configuration
   * @return
   */
  public Set<String> getJars(InitializerContext context, LinkConfiguration linkConfiguration,
      JobConfiguration jobConfiguration) {
    return new HashSet<String>();
  }

  /**
   * Return schema associated with the connector for FROM and TO
   * By default we assume a null schema. Override the method if there a custom schema to provide either for FROM or TO
   * @param context Initializer context object
   * @param linkConfiguration link configuration object
   * @param jobConfiguration job configuration object for the FROM and TO
   *        In case of the FROM initializer this will represent the FROM job configuration
   *        In case of the TO initializer this will represent the TO job configuration
   * @return
   */

  public Schema getSchema(InitializerContext context, LinkConfiguration linkConfiguration,
      JobConfiguration jobConfiguration) {
    return NullSchema.getInstance();
  }

}
