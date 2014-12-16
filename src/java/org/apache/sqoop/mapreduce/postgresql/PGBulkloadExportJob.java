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

package org.apache.sqoop.mapreduce.postgresql;

import java.io.IOException;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.SqoopOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.mapreduce.ExportJobBase;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.orm.TableClassName;


/**
 * Class that runs an export job using pg_bulkload in the mapper.
 */
public class PGBulkloadExportJob extends ExportJobBase {

  public static final Log LOG =
      LogFactory.getLog(PGBulkloadExportJob.class.getName());


  public PGBulkloadExportJob(final ExportJobContext context) {
    super(context);
  }


  public PGBulkloadExportJob(final ExportJobContext ctxt,
      final Class<? extends Mapper> mapperClass,
      final Class<? extends InputFormat> inputFormatClass,
      final Class<? extends OutputFormat> outputFormatClass) {
    super(ctxt, mapperClass, inputFormatClass, outputFormatClass);
  }


  @Override
  protected void configureInputFormat(Job job, String tableName,
                                      String tableClassName, String splitByCol)
    throws ClassNotFoundException, IOException {
    super.configureInputFormat(job, tableName, tableClassName, splitByCol);
    ConnManager mgr = context.getConnManager();
    String username = options.getUsername();
    if (null == username || username.length() == 0) {
      DBConfiguration.configureDB(job.getConfiguration(),
                                  mgr.getDriverClass(),
                                  options.getConnectString(),
                                  options.getFetchSize(),
                                  options.getConnectionParams());
    } else {
      DBConfiguration.configureDB(job.getConfiguration(),
                                  mgr.getDriverClass(),
                                  options.getConnectString(),
                                  username, options.getPassword(),
                                  options.getFetchSize(),
                                  options.getConnectionParams());
    }
  }


  @Override
  protected Class<? extends Mapper> getMapperClass() {
    return PGBulkloadExportMapper.class;
  }


  protected Class<? extends Reducer> getReducerClass() {
    return PGBulkloadExportReducer.class;
  }


  private void setDelimiter(String prop, char val, Configuration conf) {
    switch (val) {
    case DelimiterSet.NULL_CHAR:
      break;
    case '\t':
    default:
      conf.set(prop, String.valueOf(val));
    }
  }


  @Override
  protected void propagateOptionsToJob(Job job) {
    super.propagateOptionsToJob(job);
    SqoopOptions opts = context.getOptions();
    Configuration conf = job.getConfiguration();
    conf.setIfUnset("pgbulkload.bin", "pg_bulkload");
    if (opts.getNullStringValue() != null) {
      conf.set("pgbulkload.null.string", opts.getNullStringValue());
    }
    setDelimiter("pgbulkload.input.field.delim",
                 opts.getInputFieldDelim(),
                 conf);
    setDelimiter("pgbulkload.input.record.delim",
                 opts.getInputRecordDelim(),
                 conf);
    setDelimiter("pgbulkload.input.enclosedby",
                 opts.getInputEnclosedBy(),
                 conf);
    setDelimiter("pgbulkload.input.escapedby",
                 opts.getInputEscapedBy(),
                 conf);
    conf.setBoolean("pgbulkload.input.encloserequired",
                    opts.isInputEncloseRequired());
    conf.setIfUnset("pgbulkload.check.constraints", "YES");
    conf.setIfUnset("pgbulkload.parse.errors", "INFINITE");
    conf.setIfUnset("pgbulkload.duplicate.errors", "INFINITE");
    conf.set("mapred.jar", context.getJarFile());
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
    conf.setInt("mapred.map.max.attempts", 1);
    conf.setInt("mapred.reduce.max.attempts", 1);
  }


  @Override
  public void runExport() throws ExportException, IOException {
    SqoopOptions options = context.getOptions();
    Configuration conf = options.getConf();
    DBConfiguration dbConf = null;
    String outputTableName = context.getTableName();
    String tableName = outputTableName;
    String tableClassName =
        new TableClassName(options).getClassForTable(outputTableName);

    LOG.info("Beginning export of " + outputTableName);
    loadJars(conf, context.getJarFile(), tableClassName);

    try {
      Job job = new Job(conf);
      dbConf = new DBConfiguration(job.getConfiguration());
      dbConf.setOutputTableName(tableName);
      configureInputFormat(job, tableName, tableClassName, null);
      configureOutputFormat(job, tableName, tableClassName);
      configureNumTasks(job);
      propagateOptionsToJob(job);
      job.setMapperClass(getMapperClass());
      job.setMapOutputKeyClass(LongWritable.class);
      job.setMapOutputValueClass(Text.class);
      job.setReducerClass(getReducerClass());
      cacheJars(job, context.getConnManager());
      setJob(job);

      boolean success = runJob(job);
      if (!success) {
        throw new ExportException("Export job failed!");
      }
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    } finally {
      unloadJars();
    }
  }


  @Override
  protected int configureNumReduceTasks(Job job) throws IOException {
    if (job.getNumReduceTasks() < 1) {
      job.setNumReduceTasks(1);
    }
    return job.getNumReduceTasks();
  }


  private void clearStagingTable(DBConfiguration dbConf, String tableName)
    throws IOException {
    // clearing stagingtable is done each mapper tasks
  }
}
