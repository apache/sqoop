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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.DirectNetezzaManager;
import org.apache.sqoop.mapreduce.DBWritable;
import
  org.apache.sqoop.mapreduce.db.netezza.NetezzaExternalTableRecordExportMapper;
import
  org.apache.sqoop.mapreduce.db.netezza.NetezzaExternalTableTextExportMapper;

import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.mapreduce.ExportJobBase;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;

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
  /**
   * Configure the inputformat to use for the job.
   */
  @Override
  protected void configureInputFormat(Job job, String tableName,
      String tableClassName, String splitByCol) throws ClassNotFoundException,
      IOException {

    // Configure the delimiters, etc.
    Configuration conf = job.getConfiguration();

    ConnManager mgr = context.getConnManager();
    String username = options.getUsername();
    if (null == username || username.length() == 0) {
      DBConfiguration.configureDB(job.getConfiguration(), mgr.getDriverClass(),
          options.getConnectString());
    } else {
      DBConfiguration.configureDB(job.getConfiguration(), mgr.getDriverClass(),
          options.getConnectString(), username, options.getPassword());
    }

    String[] colNames = options.getColumns();
    if (null == colNames) {
      colNames = mgr.getColumnNames(tableName);
    }

    String[] sqlColNames = null;
    if (null != colNames) {
      sqlColNames = new String[colNames.length];
      for (int i = 0; i < colNames.length; i++) {
        sqlColNames[i] = mgr.escapeColName(colNames[i]);
      }
    }

    DataDrivenDBInputFormat.setInput(job, DBWritable.class, tableName, null,
        null, sqlColNames);

    // Configure the actual InputFormat to use.
    super.configureInputFormat(job, tableName, tableClassName, splitByCol);
  }

  @Override
  protected Class<? extends Mapper> getMapperClass() {
    if (inputIsSequenceFiles()) {
      return NetezzaExternalTableRecordExportMapper.class;
    } else {
      return NetezzaExternalTableTextExportMapper.class;
    }
  }
}
