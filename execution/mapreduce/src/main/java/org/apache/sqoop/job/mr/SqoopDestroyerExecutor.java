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
package org.apache.sqoop.job.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.PrefixContext;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.DestroyerContext;
import org.apache.sqoop.utils.ClassUtils;

/**
 *  Helper class to execute destroyers on mapreduce job side.
 */
public class SqoopDestroyerExecutor {

  public static final Log LOG =
    LogFactory.getLog(SqoopNullOutputFormat.class.getName());

  /**
   * Execute destroyer.
   *
   * @param success True if the job execution was successfull
   * @param configuration Configuration object to get destroyer class with context
   *                      and configuration objects.
   * @param propertyName Name of property that holds destroyer class.
   */
  public static void executeDestroyer(boolean success, Configuration configuration, String propertyName) {
    Destroyer destroyer = (Destroyer) ClassUtils.instantiate(configuration.get(propertyName));

    if(destroyer == null) {
      LOG.info("Skipping running destroyer as non was defined.");
      return;
    }

    // Objects that should be pass to the Destroyer execution
    PrefixContext subContext = new PrefixContext(configuration, JobConstants.PREFIX_CONNECTOR_CONTEXT);
    Object configConnection = ConfigurationUtils.getConnectorConnection(configuration);
    Object configJob = ConfigurationUtils.getConnectorJob(configuration);

    DestroyerContext destroyerContext = new DestroyerContext(subContext, success);

    LOG.info("Executing destroyer class " + destroyer.getClass());
    destroyer.destroy(destroyerContext, configConnection, configJob);
  }

  private SqoopDestroyerExecutor() {
    // Instantiation is prohibited
  }

}
