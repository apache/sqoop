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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.config.ConfigurationHelper;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.manager.DirectNetezzaManager;
import org.apache.sqoop.mapreduce.DBWritable;
import org.apache.sqoop.mapreduce.ImportJobBase;
import org.apache.sqoop.mapreduce.RawKeyTextOutputFormat;
import
  org.apache.sqoop.mapreduce.db.netezza.NetezzaExternalTableHCatImportMapper;
import
  org.apache.sqoop.mapreduce.db.netezza.NetezzaExternalTableTextImportMapper;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatUtilities;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;

/**
 * Class that runs an import job using netezza external tables in the mapper.
 */
public class NetezzaExternalTableImportJob extends ImportJobBase {

  public NetezzaExternalTableImportJob(final SqoopOptions opts,
      ImportJobContext context) {
    super(opts, NetezzaExternalTableTextImportMapper.class,
        NetezzaExternalTableInputFormat.class, RawKeyTextOutputFormat.class,
        context);
  }

  @Override
  protected void propagateOptionsToJob(Job job) {
    Configuration conf = job.getConfiguration();
    String nullValue = options.getNullStringValue();
    if (nullValue != null) {
      conf.set(DirectNetezzaManager.NETEZZA_NULL_VALUE,
          StringEscapeUtils.unescapeJava(nullValue));
    }
    conf.setInt(DelimiterSet.OUTPUT_FIELD_DELIM_KEY,
        options.getOutputFieldDelim());
    conf.setInt(DelimiterSet.OUTPUT_RECORD_DELIM_KEY,
        options.getOutputRecordDelim());
    conf.setInt(DelimiterSet.OUTPUT_ENCLOSED_BY_KEY,
        options.getOutputEnclosedBy());
    // Netezza uses \ as the escape character. Force the use of it
    int escapeChar = options.getOutputEscapedBy();
    if (escapeChar > 0) {
      if (escapeChar != '\\') {
        LOG.info(
            "Setting escaped char to \\ for Netezza external table import");
      }
      conf.setInt(DelimiterSet.OUTPUT_ESCAPED_BY_KEY, '\\');
    }
    conf.setBoolean(DelimiterSet.OUTPUT_ENCLOSE_REQUIRED_KEY,
        options.isOutputEncloseRequired());

  }
  /**
   * Configure the inputformat to use for the job.
   */
  @Override
  protected void configureInputFormat(Job job, String tableName,
      String tableClassName, String splitByCol) throws ClassNotFoundException,
      IOException {

    ConnManager mgr = getContext().getConnManager();
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

    // It's ok if the where clause is null in DBInputFormat.setInput.
    String whereClause = options.getWhereClause();

    // We can't set the class properly in here, because we may not have the
    // jar loaded in this JVM. So we start by calling setInput() with
    // DBWritable and then overriding the string manually.

    // Note that mysqldump also does *not* want a quoted table name.
    DataDrivenDBInputFormat.setInput(job, DBWritable.class, tableName,
        whereClause, mgr.escapeColName(splitByCol), sqlColNames);

    LOG.debug("Using InputFormat: " + inputFormatClass);
    job.setInputFormatClass(getInputFormatClass());

    if (isHCatJob) {
      LOG.debug("Using table class: " + tableClassName);
      job.getConfiguration().set(ConfigurationHelper.getDbInputClassProperty(),
        tableClassName);
    }
  }

  /**
   * Set the mapper class implementation to use in the job, as well as any
   * related configuration (e.g., map output types).
   */
  protected void configureMapper(Job job, String tableName,
      String tableClassName) throws ClassNotFoundException, IOException {
    super.configureMapper(job, tableName, tableClassName);
    job.setMapperClass(getMapperClass());
    if (isHCatJob) {
      LOG.info("Configuring mapper for HCatalog import job");
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(SqoopHCatUtilities.getImportValueClass());
      return;
    }
    job.setOutputKeyClass(String.class);
    job.setOutputValueClass(NullWritable.class);
  }

  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass()
      throws ClassNotFoundException {
    if (isHCatJob) {
      return SqoopHCatUtilities.getOutputFormatClass();
    } else {
      return RawKeyTextOutputFormat.class;
    }
  }

  @Override
  protected Class<? extends Mapper> getMapperClass() {
    if (isHCatJob) {
      return NetezzaExternalTableHCatImportMapper.class;
    } else {
      return NetezzaExternalTableTextImportMapper.class;
    }
  }
}
