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
package org.apache.sqoop.job;

import org.apache.hadoop.io.Text;
import org.apache.sqoop.core.ConfigurationConstants;

public final class JobConstants extends Constants {
  /**
   * All job related configuration is prefixed with this:
   * <tt>org.apache.sqoop.job.</tt>
   */
  public static final String PREFIX_JOB_CONFIG =
      ConfigurationConstants.PREFIX_GLOBAL_CONFIG + "job.";

  public static final String JOB_TYPE = PREFIX_JOB_CONFIG + "type";

  public static final String JOB_ETL_PARTITIONER = PREFIX_JOB_CONFIG
      + "etl.partitioner";

  public static final String JOB_ETL_EXTRACTOR = PREFIX_JOB_CONFIG
      + "etl.extractor";

  public static final String JOB_ETL_LOADER = PREFIX_JOB_CONFIG
      + "etl.loader";

  public static final String JOB_ETL_DESTROYER = PREFIX_JOB_CONFIG
      + "etl.destroyer";


  public static final String JOB_MR_OUTPUT_FILE = PREFIX_JOB_CONFIG
      + "mr.output.file";

  public static final String JOB_MR_OUTPUT_CODEC = PREFIX_JOB_CONFIG
      + "mr.output.codec";


  public static final String JOB_ETL_EXTRACTOR_NUM = PREFIX_JOB_CONFIG
    + "etl.extractor.count";

  public static final String JOB_CONFIG_CLASS_CONNECTOR_CONNECTION =
    PREFIX_JOB_CONFIG + "config.class.connector.connection";

  public static final String JOB_CONFIG_CLASS_CONNECTOR_JOB =
    PREFIX_JOB_CONFIG + "config.class.connector.job";

  public static final String JOB_CONFIG_CLASS_FRAMEWORK_CONNECTION =
    PREFIX_JOB_CONFIG + "config.class.framework.connection";

  public static final String JOB_CONFIG_CLASS_FRAMEWORK_JOB =
    PREFIX_JOB_CONFIG + "config.class.framework.job";

  public static final String JOB_CONFIG_CONNECTOR_CONNECTION =
    PREFIX_JOB_CONFIG + "config.connector.connection";

  public static final Text JOB_CONFIG_CONNECTOR_CONNECTION_KEY =
    new Text(JOB_CONFIG_CONNECTOR_CONNECTION);

  public static final String JOB_CONFIG_CONNECTOR_JOB =
    PREFIX_JOB_CONFIG + "config.connector.job";

  public static final Text JOB_CONFIG_CONNECTOR_JOB_KEY =
    new Text(JOB_CONFIG_CONNECTOR_JOB);

  public static final String JOB_CONFIG_FRAMEWORK_CONNECTION =
    PREFIX_JOB_CONFIG + "config.framework.connection";

  public static final Text JOB_CONFIG_FRAMEWORK_CONNECTION_KEY =
    new Text(JOB_CONFIG_FRAMEWORK_CONNECTION);

  public static final String JOB_CONFIG_FRAMEWORK_JOB =
    PREFIX_JOB_CONFIG + "config.framework.job";

  public static final Text JOB_CONFIG_FRAMEWORK_JOB_KEY =
    new Text(JOB_CONFIG_FRAMEWORK_JOB);

  public static final String PREFIX_CONNECTOR_CONTEXT =
    PREFIX_JOB_CONFIG + "connector.context.";

  // Hadoop specific constants
  // We're using constants from Hadoop 1. Hadoop 2 has different names, but
  // provides backward compatibility layer for those names as well.

  public static final String HADOOP_INPUTDIR = "mapred.input.dir";

  public static final String HADOOP_OUTDIR = "mapred.output.dir";

  public static final String HADOOP_COMPRESS = "mapred.output.compress";

  public static final String HADOOP_COMPRESS_CODEC =
    "mapred.output.compression.codec";

  private JobConstants() {
    // Disable explicit object creation
  }
}
