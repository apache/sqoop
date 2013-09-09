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

package org.apache.sqoop.mapreduce.netezza;

import java.io.IOException;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.manager.DirectNetezzaManager;
import
  org.apache.sqoop.mapreduce.db.netezza.NetezzaExternalTableHCatExportMapper;
import
  org.apache.sqoop.mapreduce.db.netezza.NetezzaExternalTableRecordExportMapper;
import
  org.apache.sqoop.mapreduce.db.netezza.NetezzaExternalTableTextExportMapper;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatUtilities;

import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.mapreduce.ExportJobBase;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DBOutputFormat;

/**
 * Class that runs an export job using netezza external tables in the mapper.
 */
public class NetezzaExternalTableExportJob extends ExportJobBase {

  public static final Log LOG = LogFactory
      .getLog(NetezzaExternalTableExportJob.class.getName());

  public NetezzaExternalTableExportJob(final ExportJobContext context) {
    super(context, null, null, NullOutputFormat.class);
  }

  @Override
  protected void propagateOptionsToJob(Job job) {
    Configuration conf = job.getConfiguration();
    String nullValue = options.getInNullStringValue();
    if (nullValue != null) {
      conf.set(DirectNetezzaManager.NETEZZA_NULL_VALUE,
          StringEscapeUtils.unescapeJava(nullValue));
    }
    conf.setInt(DelimiterSet.INPUT_FIELD_DELIM_KEY,
        options.getInputFieldDelim());
    conf.setInt(DelimiterSet.INPUT_RECORD_DELIM_KEY,
        options.getInputRecordDelim());
    conf.setInt(DelimiterSet.INPUT_ENCLOSED_BY_KEY,
        options.getInputEnclosedBy());
    // Netezza uses \ as the escape character. Force the use of it
    int escapeChar = options.getInputEscapedBy();
    if (escapeChar > 0) {
      if (escapeChar != '\\') {
        LOG.info(
            "Setting escaped char to \\ for Netezza external table export");
      }
      conf.setInt(DelimiterSet.INPUT_ESCAPED_BY_KEY, '\\');
    }
    conf.setBoolean(DelimiterSet.INPUT_ENCLOSE_REQUIRED_KEY,
        options.isOutputEncloseRequired());
  }

  @Override
  protected Class<? extends InputFormat> getInputFormatClass()
      throws ClassNotFoundException {
    if (isHCatJob) {
      return SqoopHCatUtilities.getInputFormatClass();
    }
    return super.getInputFormatClass();
  }

  @Override
  protected void configureInputFormat(Job job, String tableName,
     String tableClassName, String splitCol)
     throws ClassNotFoundException, IOException {
    super.configureInputFormat(job, tableName, tableClassName, splitCol);
    if (isHCatJob) {
      SqoopHCatUtilities.configureExportInputFormat(options, job,
          context.getConnManager(), tableName, job.getConfiguration());
      return;
    }
  }
  @Override
  protected void configureOutputFormat(Job job, String tableName,
                                       String tableClassName)
      throws ClassNotFoundException, IOException {
    ConnManager mgr = context.getConnManager();
    try {
      String username = options.getUsername();
      if (null == username || username.length() == 0) {
        DBConfiguration.configureDB(job.getConfiguration(),
            mgr.getDriverClass(),
            options.getConnectString(),
            options.getConnectionParams());
      } else {
        DBConfiguration.configureDB(job.getConfiguration(),
            mgr.getDriverClass(),
            options.getConnectString(),
            username, options.getPassword(),
            options.getConnectionParams());
      }

      String [] colNames = options.getColumns();
      if (null == colNames) {
        colNames = mgr.getColumnNames(tableName);
      }

      if (mgr.escapeTableNameOnExport()) {
        DBOutputFormat.setOutput(job, mgr.escapeTableName(tableName), colNames);
      } else {
        DBOutputFormat.setOutput(job, tableName, colNames);
      }

      job.setOutputFormatClass(getOutputFormatClass());
      if (isHCatJob) {
        job.getConfiguration().set(SQOOP_EXPORT_TABLE_CLASS_KEY,
          tableClassName);
      }
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Could not load OutputFormat", cnfe);
    }
  }

  @Override
  protected Class<? extends Mapper> getMapperClass() {
    if (isHCatJob) {
      return NetezzaExternalTableHCatExportMapper.class;
    }
    if (inputIsSequenceFiles()) {
      return NetezzaExternalTableRecordExportMapper.class;
    } else {
      return NetezzaExternalTableTextExportMapper.class;
    }
  }
}
