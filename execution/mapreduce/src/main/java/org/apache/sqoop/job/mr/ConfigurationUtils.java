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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.utils.ClassUtils;

/**
 * Helper class to load configuration specific objects from job configuration
 */
public final class ConfigurationUtils {

  public static MJob.Type getJobType(Configuration configuration) {
    return MJob.Type.valueOf(configuration.get(JobConstants.JOB_TYPE));
  }

  public static Object getConnectorConnection(Configuration configuration) {
    return loadConfiguration((JobConf) configuration,
      JobConstants.JOB_CONFIG_CLASS_CONNECTOR_CONNECTION,
      JobConstants.JOB_CONFIG_CONNECTOR_CONNECTION_KEY);
  }

  public static Object getConnectorJob(Configuration configuration) {
    return loadConfiguration((JobConf) configuration,
      JobConstants.JOB_CONFIG_CLASS_CONNECTOR_JOB,
      JobConstants.JOB_CONFIG_CONNECTOR_JOB_KEY);
  }

  public static Object getFrameworkConnection(Configuration configuration) {
    return loadConfiguration((JobConf) configuration,
      JobConstants.JOB_CONFIG_CLASS_FRAMEWORK_CONNECTION,
      JobConstants.JOB_CONFIG_FRAMEWORK_CONNECTION_KEY);
  }

  public static Object getFrameworkJob(Configuration configuration) {
    return loadConfiguration((JobConf) configuration,
      JobConstants.JOB_CONFIG_CLASS_FRAMEWORK_JOB,
      JobConstants.JOB_CONFIG_FRAMEWORK_JOB_KEY);
  }

  /**
   * Load configuration instance serialized in Hadoop credentials cache.
   *
   * @param configuration JobConf object associated with the job
   * @param classProperty Property with stored configuration class name
   * @param valueProperty Property with stored JSON representation of the
   *                      configuration object
   * @return New instance with loaded data
   */
  private static Object loadConfiguration(JobConf configuration, String classProperty, Text valueProperty) {
    // Create new instance of configuration class
    Object object = ClassUtils.instantiate(configuration.get(classProperty));
    if(object == null) {
      return null;
    }

    String json = new String(configuration.getCredentials().getSecretKey(valueProperty));

    // Fill it with JSON data
    FormUtils.fillValues(json, object);

    // And give it back
    return object;
  }

  private ConfigurationUtils() {
    // Instantiation is prohibited
  }
}
