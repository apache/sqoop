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


package org.apache.sqoop.config;

/**
 * Static constants that identify configuration keys, counter group names, and
 * counter names.
 */
public final class ConfigurationConstants {

  /**
   * The Configuration property identifying the current task id.
   */
  public static final String PROP_MAPRED_TASK_ID = "mapred.task.id";

  /**
   * The Configuration property identifying the job's local directory.
   */
  public static final String PROP_JOB_LOCAL_DIRECTORY = "job.local.dir";

  /**
   * The Configuration property identifying the number of map tasks to be used.
   */
  public static final String PROP_MAPRED_MAP_TASKS = "mapred.map.tasks";

  /**
   * The Configuration property identifying the speculative execution flag for
   * map tasks.
   */
  public static final String PROP_MAPRED_MAP_TASKS_SPECULATIVE_EXEC =
                                "mapred.map.tasks.speculative.execution";

  /**
   * The Configuration property identifying the speculative execution flag for
   * reduce tasks.
   */
  public static final String PROP_MAPRED_REDUCE_TASKS_SPECULATIVE_EXEC =
                                "mapred.reduce.tasks.speculative.execution";

  /**
   * The Configuration property identifying the framework name. If set to YARN
   * then we will not be in local mode.
   */
  public static final String PROP_MAPREDUCE_FRAMEWORK_NAME =
    "mapreduce.framework.name";

  public static final String MAPREDUCE_FRAMEWORK_LOCAL = "local";

  /**
   * The group name of task counters.
   */
  public static final String COUNTER_GROUP_MAPRED_TASK_COUNTERS =
                                "org.apache.hadoop.mapred.Task$Counter";

  /**
   * The name of the counter that tracks output records from Map phase.
   */
  public static final String COUNTER_MAP_OUTPUT_RECORDS =
                                "MAP_OUTPUT_RECORDS";

  /**
   * The name of the counter that tracks input records to the Map phase.
   */
  public static final String COUNTER_MAP_INPUT_RECORDS =
                                "MAP_INPUT_RECORDS";

  /**
   * The name of the parameter for ToolRunner to set jars to add to distcache.
   */
  public static final String MAPRED_DISTCACHE_CONF_PARAM = "tmpjars";

  /**
   * The Configuration property identifying the split size.
   */
  public static final String PROP_SPLIT_LIMIT = "split.limit";

  /**
   * Enable avro logical types (decimal support only).
   */
  public static final String PROP_ENABLE_AVRO_LOGICAL_TYPE_DECIMAL = "sqoop.avro.logical_types.decimal.enable";

  /**
   * Enable parquet logical types (decimal support only).
   */
  public static final String PROP_ENABLE_PARQUET_LOGICAL_TYPE_DECIMAL = "sqoop.parquet.logical_types.decimal.enable";

  /**
   * Default precision for avro schema
   */
  public static final String PROP_AVRO_DECIMAL_PRECISION = "sqoop.avro.logical_types.decimal.default.precision";

  /**
   * Default scale for avro schema
   */
  public static final String PROP_AVRO_DECIMAL_SCALE = "sqoop.avro.logical_types.decimal.default.scale";

  /**
   * Enable padding for avro logical types (decimal support only).
   */
  public static final String PROP_ENABLE_AVRO_DECIMAL_PADDING = "sqoop.avro.decimal_padding.enable";

  /**
   * The Configuration property identifying data publisher class.
   */
  public static final String DATA_PUBLISH_CLASS = "sqoop.job.data.publish.class";

  private ConfigurationConstants() {
    // Disable Explicit Object Creation
  }
}
