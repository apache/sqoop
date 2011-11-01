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
package com.cloudera.sqoop.config;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public final class ConfigurationConstants {

  public static final String PROP_MAPRED_TASK_ID =
    org.apache.sqoop.config.ConfigurationConstants.PROP_MAPRED_TASK_ID;
  public static final String PROP_JOB_LOCAL_DIRECTORY =
    org.apache.sqoop.config.ConfigurationConstants.PROP_JOB_LOCAL_DIRECTORY;
  public static final String PROP_MAPRED_MAP_TASKS =
    org.apache.sqoop.config.ConfigurationConstants.PROP_MAPRED_MAP_TASKS;
  public static final String PROP_MAPRED_MAP_TASKS_SPECULATIVE_EXEC =
    org.apache.sqoop.config.
        ConfigurationConstants.PROP_MAPRED_MAP_TASKS_SPECULATIVE_EXEC;
  public static final String PROP_MAPRED_REDUCE_TASKS_SPECULATIVE_EXEC =
    org.apache.sqoop.config.
        ConfigurationConstants.PROP_MAPRED_REDUCE_TASKS_SPECULATIVE_EXEC;
  public static final String PROP_MAPRED_JOB_TRACKER_ADDRESS =
    org.apache.sqoop.config.
        ConfigurationConstants.PROP_MAPRED_JOB_TRACKER_ADDRESS;
  public static final String COUNTER_GROUP_MAPRED_TASK_COUNTERS =
    org.apache.sqoop.config.
        ConfigurationConstants.COUNTER_GROUP_MAPRED_TASK_COUNTERS;
  public static final String COUNTER_MAP_OUTPUT_RECORDS =
    org.apache.sqoop.config.ConfigurationConstants.COUNTER_MAP_OUTPUT_RECORDS;
  public static final String COUNTER_MAP_INPUT_RECORDS =
    org.apache.sqoop.config.ConfigurationConstants.COUNTER_MAP_INPUT_RECORDS;

  private ConfigurationConstants() {
  }

}
