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
package com.cloudera.sqoop.tool;

import com.cloudera.sqoop.SqoopOptions;
import org.apache.sqoop.manager.SupportedManagers;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public abstract class BaseSqoopTool
    extends org.apache.sqoop.tool.BaseSqoopTool {

  public static final String HELP_STR =
          org.apache.sqoop.tool.BaseSqoopTool.HELP_STR;
  public static final String CONNECT_STRING_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.CONNECT_STRING_ARG;
  public static final String CONN_MANAGER_CLASS_NAME =
          org.apache.sqoop.tool.BaseSqoopTool.CONN_MANAGER_CLASS_NAME;
  public static final String CONNECT_PARAM_FILE =
          org.apache.sqoop.tool.BaseSqoopTool.CONNECT_PARAM_FILE;
  public static final String DRIVER_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.DRIVER_ARG;
  public static final String USERNAME_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.USERNAME_ARG;
  public static final String PASSWORD_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.PASSWORD_ARG;
  public static final String PASSWORD_PROMPT_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.PASSWORD_PROMPT_ARG;
  public static final String DIRECT_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.DIRECT_ARG;
  public static final String BATCH_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.BATCH_ARG;
  public static final String TABLE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.TABLE_ARG;
  public static final String STAGING_TABLE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.STAGING_TABLE_ARG;
  public static final String CLEAR_STAGING_TABLE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.CLEAR_STAGING_TABLE_ARG;
  public static final String COLUMNS_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.COLUMNS_ARG;
  public static final String SPLIT_BY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.SPLIT_BY_ARG;
  public static final String WHERE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.WHERE_ARG;
  public static final String HADOOP_HOME_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HADOOP_HOME_ARG;
  public static final String HIVE_HOME_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HIVE_HOME_ARG;
  public static final String WAREHOUSE_DIR_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.WAREHOUSE_DIR_ARG;
  public static final String TARGET_DIR_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.TARGET_DIR_ARG;
  public static final String APPEND_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.APPEND_ARG;
  public static final String NULL_STRING =
          org.apache.sqoop.tool.BaseSqoopTool.NULL_STRING;
  public static final String INPUT_NULL_STRING =
          org.apache.sqoop.tool.BaseSqoopTool.INPUT_NULL_STRING;
  public static final String NULL_NON_STRING =
          org.apache.sqoop.tool.BaseSqoopTool.NULL_NON_STRING;
  public static final String INPUT_NULL_NON_STRING =
          org.apache.sqoop.tool.BaseSqoopTool.INPUT_NULL_NON_STRING;
  public static final String MAP_COLUMN_JAVA =
          org.apache.sqoop.tool.BaseSqoopTool.MAP_COLUMN_JAVA;
  public static final String MAP_COLUMN_HIVE =
          org.apache.sqoop.tool.BaseSqoopTool.MAP_COLUMN_HIVE;
  public static final String FMT_SEQUENCEFILE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.FMT_SEQUENCEFILE_ARG;
  public static final String FMT_TEXTFILE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.FMT_TEXTFILE_ARG;
  public static final String FMT_AVRODATAFILE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.FMT_AVRODATAFILE_ARG;
  public static final String HIVE_IMPORT_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HIVE_IMPORT_ARG;
  public static final String HIVE_TABLE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HIVE_TABLE_ARG;
  public static final String HIVE_OVERWRITE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HIVE_OVERWRITE_ARG;
  public static final String HIVE_DROP_DELIMS_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HIVE_DROP_DELIMS_ARG;
  public static final String HIVE_DELIMS_REPLACEMENT_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HIVE_DELIMS_REPLACEMENT_ARG;
  public static final String HIVE_PARTITION_KEY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HIVE_PARTITION_KEY_ARG;
  public static final String HIVE_PARTITION_VALUE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HIVE_PARTITION_VALUE_ARG;
  public static final String CREATE_HIVE_TABLE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.CREATE_HIVE_TABLE_ARG;
  public static final String NUM_MAPPERS_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.NUM_MAPPERS_ARG;
  public static final String NUM_MAPPERS_SHORT_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.NUM_MAPPERS_SHORT_ARG;
  public static final String COMPRESS_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.COMPRESS_ARG;
  public static final String COMPRESSION_CODEC_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.COMPRESSION_CODEC_ARG;
  public static final String COMPRESS_SHORT_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.COMPRESS_SHORT_ARG;
  public static final String DIRECT_SPLIT_SIZE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.DIRECT_SPLIT_SIZE_ARG;
  public static final String INLINE_LOB_LIMIT_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.INLINE_LOB_LIMIT_ARG;
  public static final String FETCH_SIZE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.FETCH_SIZE_ARG;
  public static final String EXPORT_PATH_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.EXPORT_PATH_ARG;
  public static final String FIELDS_TERMINATED_BY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.FIELDS_TERMINATED_BY_ARG;
  public static final String LINES_TERMINATED_BY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.LINES_TERMINATED_BY_ARG;
  public static final String OPTIONALLY_ENCLOSED_BY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.OPTIONALLY_ENCLOSED_BY_ARG;
  public static final String ENCLOSED_BY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.ENCLOSED_BY_ARG;
  public static final String ESCAPED_BY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.ESCAPED_BY_ARG;
  public static final String MYSQL_DELIMITERS_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.MYSQL_DELIMITERS_ARG;
  public static final String INPUT_FIELDS_TERMINATED_BY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.INPUT_FIELDS_TERMINATED_BY_ARG;
  public static final String INPUT_LINES_TERMINATED_BY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.INPUT_LINES_TERMINATED_BY_ARG;
  public static final String INPUT_OPTIONALLY_ENCLOSED_BY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.INPUT_OPTIONALLY_ENCLOSED_BY_ARG;
  public static final String INPUT_ENCLOSED_BY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.INPUT_ENCLOSED_BY_ARG;
  public static final String INPUT_ESCAPED_BY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.INPUT_ESCAPED_BY_ARG;
  public static final String CODE_OUT_DIR_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.CODE_OUT_DIR_ARG;
  public static final String BIN_OUT_DIR_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.BIN_OUT_DIR_ARG;
  public static final String PACKAGE_NAME_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.PACKAGE_NAME_ARG;
  public static final String CLASS_NAME_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.CLASS_NAME_ARG;
  public static final String JAR_FILE_NAME_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.JAR_FILE_NAME_ARG;
  public static final String SQL_QUERY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.SQL_QUERY_ARG;
  public static final String SQL_QUERY_BOUNDARY =
          org.apache.sqoop.tool.BaseSqoopTool.SQL_QUERY_BOUNDARY;
  public static final String SQL_QUERY_SHORT_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.SQL_QUERY_SHORT_ARG;
  public static final String VERBOSE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.VERBOSE_ARG;
  public static final String HELP_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HELP_ARG;
  public static final String UPDATE_KEY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.UPDATE_KEY_ARG;
  public static final String UPDATE_MODE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.UPDATE_MODE_ARG;
  public static final String INCREMENT_TYPE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.INCREMENT_TYPE_ARG;
  public static final String INCREMENT_COL_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.INCREMENT_COL_ARG;
  public static final String INCREMENT_LAST_VAL_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.INCREMENT_LAST_VAL_ARG;
  public static final String HBASE_TABLE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HBASE_TABLE_ARG;
  public static final String HBASE_COL_FAM_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HBASE_COL_FAM_ARG;
  public static final String HBASE_ROW_KEY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HBASE_ROW_KEY_ARG;
  public static final String HBASE_CREATE_TABLE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.HBASE_CREATE_TABLE_ARG;
  public static final String STORAGE_METASTORE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.STORAGE_METASTORE_ARG;
  public static final String JOB_CMD_CREATE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.JOB_CMD_CREATE_ARG;
  public static final String JOB_CMD_DELETE_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.JOB_CMD_DELETE_ARG;
  public static final String JOB_CMD_EXEC_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.JOB_CMD_EXEC_ARG;
  public static final String JOB_CMD_LIST_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.JOB_CMD_LIST_ARG;
  public static final String JOB_CMD_SHOW_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.JOB_CMD_SHOW_ARG;
  public static final String METASTORE_SHUTDOWN_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.METASTORE_SHUTDOWN_ARG;
  public static final String NEW_DATASET_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.NEW_DATASET_ARG;
  public static final String OLD_DATASET_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.OLD_DATASET_ARG;
  public static final String MERGE_KEY_ARG =
          org.apache.sqoop.tool.BaseSqoopTool.MERGE_KEY_ARG;

  public BaseSqoopTool() {
  }

  public BaseSqoopTool(String toolName) {
    super(toolName);
  }

  protected void validateHasDirectConnectorOption(SqoopOptions options) throws SqoopOptions.InvalidOptionsException {
    SupportedManagers m = SupportedManagers.createFrom(options);
    if (m != null && options.isDirect() && !m.hasDirectConnector()) {
      throw new SqoopOptions.InvalidOptionsException(
          "Was called with the --direct option, but no direct connector available.");
    }
  }
}
