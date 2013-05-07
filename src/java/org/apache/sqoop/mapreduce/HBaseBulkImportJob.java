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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.hbase.HBasePutProcessor;
import com.cloudera.sqoop.lib.FieldMapProcessor;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.mapreduce.DataDrivenImportJob;
import com.cloudera.sqoop.orm.TableClassName;
import com.cloudera.sqoop.util.ImportException;
import com.google.common.base.Preconditions;

/**
 * Runs an HBase import via DataDrivenDBInputFormat to the HBasePutProcessor
 * in the DelegatingOutputFormat.
 */
public class HBaseBulkImportJob extends HBaseImportJob {

  public static final Log LOG = LogFactory.getLog(
      HBaseBulkImportJob.class.getName());

  public HBaseBulkImportJob(final SqoopOptions opts,
      final ImportJobContext importContext) {
    super(opts, importContext);
  }

  @Override
  protected void configureMapper(Job job, String tableName,
      String tableClassName) throws IOException {
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    job.setMapperClass(getMapperClass());
  }

  @Override
  protected Class<? extends Mapper> getMapperClass() {
    return HBaseBulkImportMapper.class;
  }

  @Override
  protected void configureOutputFormat(Job job, String tableName,
      String tableClassName) throws ClassNotFoundException, IOException {
    super.configureOutputFormat(job, tableName, tableClassName);
    if (null != options.getHBaseBulkLoadDir()) {
      job.getConfiguration().set(HBasePutProcessor.BULK_LOAD_DIR_KEY,
          options.getHBaseBulkLoadDir());
    }
  }

  /**
   * Run an HBase bulk import job to read a table in to HDFS.
   *
   * @param tableName  the database table to read; may be null if a free-form
   * query is specified in the SqoopOptions, and the ImportJobBase subclass
   * supports free-form queries.
   * @param ormJarFile the Jar file to insert into the dcache classpath.
   * (may be null)
   * @param splitByCol the column of the database table to use to split
   * the import
   * @param conf A fresh Hadoop Configuration to use to build an MR job.
   * @throws IOException if the job encountered an IO problem
   * @throws ImportException if the job failed unexpectedly or was
   * misconfigured.
   */
  @Override
  public void runImport(String tableName, String ormJarFile, String splitByCol,
      Configuration conf) throws IOException, ImportException {
    String bulkLoadDir = options.getHBaseBulkLoadDir();
    // if it's not bulk load, use the common method
    if(StringUtils.isBlank(bulkLoadDir)){
      super.runImport(tableName, ormJarFile, splitByCol, conf);
      return;
    } 
    if (null != tableName) {
      LOG.info("Beginning bulk hbase load import of " + tableName);
    } else {
      LOG.info("Beginning query bulk hbase load import.");
    }
    String tableClassName =
        new TableClassName(options).getClassForTable(tableName);
    loadJars(conf, ormJarFile, tableClassName);

    Job job = new Job(conf);
    conf = job.getConfiguration();
    try {
      // Set the external jar to use for the job.
      conf.set("mapred.jar", ormJarFile);

      propagateOptionsToJob(job);
      configureInputFormat(job, tableName, tableClassName, splitByCol);
      configureOutputFormat(job, tableName, tableClassName);
      configureMapper(job, tableName, tableClassName);
      configureNumTasks(job);
      cacheJars(job, getContext().getConnManager());
      jobSetup(job);
      TableMapReduceUtil.addDependencyJars(conf, Preconditions.class);
      FileOutputFormat.setOutputPath(job, new Path(bulkLoadDir));
      HTable hTable = new HTable(conf, options.getHBaseTable());
      HFileOutputFormat.configureIncrementalLoad(job, hTable);
      boolean success = runJob(job);
      setJob(job);
      if (!success) {
        throw new ImportException("Import job failed!");
      }
   // Load generated HFiles into table
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
      loader.doBulkLoad(new Path(bulkLoadDir), hTable);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      unloadJars();
      jobTeardown(job);
    }
  }
}
