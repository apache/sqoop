/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.manager;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.mapreduce.SQLServerResilientExportOutputFormat;
import org.apache.sqoop.mapreduce.SQLServerResilientUpdateOutputFormat;
import org.apache.sqoop.mapreduce.db.SQLServerConnectionFailureHandler;
import org.apache.sqoop.mapreduce.db.SQLServerDBInputFormat;
import org.apache.sqoop.mapreduce.sqlserver.SqlServerExportBatchOutputFormat;

public class SqlServerManagerContextConfigurator {

  public static final String RESILIENT_OPTION = "resilient";

  /**
   * Check if the user has requested the operation to be resilient.
   */
  private boolean isResilientOperation(SqoopOptions options) {
    String [] extraArgs = options.getExtraArgs();
    if (extraArgs != null) {
      // Traverse the extra options
      for (int iArg = 0; iArg < extraArgs.length; ++iArg) {
        String currentArg = extraArgs[iArg];
        if (currentArg.startsWith("--")
          && currentArg.substring(2).equalsIgnoreCase(RESILIENT_OPTION)) {
          // User has explicitly requested the operation to be resilient
          return true;
        }
      }
    }
    return false;
  }

  public void configureContextForExport(ExportJobContext context) {
    if (isResilientOperation(context.getOptions())) {
      context.setOutputFormatClass(SQLServerResilientExportOutputFormat.class);
      configureConnectionRecoveryForExport(context);
    } else {
      context.setOutputFormatClass(SqlServerExportBatchOutputFormat.class);
    }
  }

  /**
   * Configure SQLServer Sqoop export Jobs to recover failed connections by
   * using {@link SQLServerConnectionFailureHandler}. This can be overridden by setting the
   * {@link SQLServerResilientExportOutputFormat#EXPORT_FAILURE_HANDLER_CLASS} in the configuration.
   */
  private void configureConnectionRecoveryForExport(
      ExportJobContext context) {

    Configuration conf = context.getOptions().getConf();

    // Set connection failure handler and recovery settings
    // Can be overridden if provided as Configuration
    // properties by the user
    String clsFailureHandler = conf.get(
      SQLServerResilientExportOutputFormat.EXPORT_FAILURE_HANDLER_CLASS);
    if (clsFailureHandler == null) {
      conf.set(
        SQLServerResilientExportOutputFormat.EXPORT_FAILURE_HANDLER_CLASS,
        SQLServerConnectionFailureHandler.class.getName());
    }
  }

  /**
   * Configure SQLServer Sqoop Jobs to recover failed connections by using
   * {@link SQLServerConnectionFailureHandler}. This can be overridden by setting the
   * {@link SQLServerDBInputFormat#IMPORT_FAILURE_HANDLER_CLASS} in the configuration.
   */
  private void configureConnectionRecoveryForImport(
      ImportJobContext context) {

    Configuration conf = context.getOptions().getConf();

    // Configure input format class
    context.setInputFormat(SQLServerDBInputFormat.class);

    // Set connection failure handler and recovery settings
    // Can be overridden if provided as Configuration
    // properties by the user
    if (conf.get(SQLServerDBInputFormat.IMPORT_FAILURE_HANDLER_CLASS) == null) {
      conf.set(SQLServerDBInputFormat.IMPORT_FAILURE_HANDLER_CLASS,
        SQLServerConnectionFailureHandler.class.getName());
    }
  }

  public void configureContextForImport(ImportJobContext context, String splitCol) {
    if (isResilientOperation(context.getOptions())) {
      // Enable connection recovery only if split column is provided
      if (splitCol != null) {
        // Configure SQLServer table import jobs for connection recovery
        configureConnectionRecoveryForImport(context);
      }
    }
  }

  /**
   *
   * @param context
   * @param manager
   * @return whether the job should be executed as an exportjob
   */
  public boolean configureContextForUpdate(ExportJobContext context, SQLServerManager manager) {
    boolean runAsExportJob = isResilientOperation(context.getOptions());
    if (runAsExportJob) {
      context.setConnManager(manager);
      context.setOutputFormatClass(SQLServerResilientUpdateOutputFormat.class);
      configureConnectionRecoveryForExport(context);
    }
    return runAsExportJob;
  }
}
