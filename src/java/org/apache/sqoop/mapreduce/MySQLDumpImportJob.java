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

package org.apache.sqoop.mapreduce;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.manager.MySQLUtils;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;

/**
 * Class that runs an import job using mysqldump in the mapper.
 */
public class MySQLDumpImportJob extends ImportJobBase {

  public static final Log LOG =
      LogFactory.getLog(MySQLDumpImportJob.class.getName());

  public MySQLDumpImportJob(final SqoopOptions opts, ImportJobContext context)
      throws ClassNotFoundException {
    super(opts, MySQLDumpMapper.class, MySQLDumpInputFormat.class,
        RawKeyTextOutputFormat.class, context);
  }

  /**
   * Configure the inputformat to use for the job.
   */
  protected void configureInputFormat(Job job, String tableName,
      String tableClassName, String splitByCol)
      throws ClassNotFoundException, IOException {

    if (null == tableName) {
        LOG.error(
            "mysqldump-based import cannot support free-form query imports.");
        LOG.error("Do not use --direct and --query together for MySQL.");
        throw new IOException("null tableName for MySQLDumpImportJob.");
    }

    ConnManager mgr = getContext().getConnManager();
    String username = options.getUsername();
    if (null == username || username.length() == 0) {
      DBConfiguration.configureDB(job.getConfiguration(),
          mgr.getDriverClass(), options.getConnectString(),
          options.getConnectionParams());
    } else {
      DBConfiguration.configureDB(job.getConfiguration(),
          mgr.getDriverClass(), options.getConnectString(), username,
          options.getPassword(), options.getConnectionParams());
    }

    String [] colNames = options.getColumns();
    if (null == colNames) {
      colNames = mgr.getColumnNames(tableName);
    }

    String [] sqlColNames = null;
    if (null != colNames) {
      sqlColNames = new String[colNames.length];
      for (int i = 0; i < colNames.length; i++) {
        sqlColNames[i] = mgr.escapeColName(colNames[i]);
      }
    }

    // It's ok if the where clause is null in DBInputFormat.setInput.
    String whereClause = options.getWhereClause();

    // We can't set the class properly in here, because we may not have the
    // jar loaded in this JVM. So we start by calling setInput() with
    // DBWritable and then overriding the string manually.

    // Note that mysqldump also does *not* want a quoted table name.
    DataDrivenDBInputFormat.setInput(job, DBWritable.class,
        tableName, whereClause,
        mgr.escapeColName(splitByCol), sqlColNames);

    Configuration conf = job.getConfiguration();
    conf.setInt(MySQLUtils.OUTPUT_FIELD_DELIM_KEY,
        options.getOutputFieldDelim());
    conf.setInt(MySQLUtils.OUTPUT_RECORD_DELIM_KEY,
        options.getOutputRecordDelim());
    conf.setInt(MySQLUtils.OUTPUT_ENCLOSED_BY_KEY,
        options.getOutputEnclosedBy());
    conf.setInt(MySQLUtils.OUTPUT_ESCAPED_BY_KEY,
        options.getOutputEscapedBy());
    conf.setBoolean(MySQLUtils.OUTPUT_ENCLOSE_REQUIRED_KEY,
        options.isOutputEncloseRequired());
    String [] extraArgs = options.getExtraArgs();
    if (null != extraArgs) {
      conf.setStrings(MySQLUtils.EXTRA_ARGS_KEY, extraArgs);
    }

    LOG.debug("Using InputFormat: " + inputFormatClass);
    job.setInputFormatClass(getInputFormatClass());
  }

  /**
   * Set the mapper class implementation to use in the job,
   * as well as any related configuration (e.g., map output types).
   */
  protected void configureMapper(Job job, String tableName,
      String tableClassName) throws ClassNotFoundException, IOException {
    job.setMapperClass(getMapperClass());
    job.setOutputKeyClass(String.class);
    job.setOutputValueClass(NullWritable.class);
  }

}
