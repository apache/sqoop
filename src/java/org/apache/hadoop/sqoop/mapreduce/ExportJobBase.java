/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package org.apache.hadoop.sqoop.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.sqoop.ConnFactory;
import org.apache.hadoop.sqoop.SqoopOptions;
import org.apache.hadoop.sqoop.lib.SqoopRecord;
import org.apache.hadoop.sqoop.manager.ConnManager;
import org.apache.hadoop.sqoop.manager.ExportJobContext;
import org.apache.hadoop.sqoop.orm.TableClassName;
import org.apache.hadoop.sqoop.shims.HadoopShim;
import org.apache.hadoop.sqoop.util.ClassLoaderStack;
import org.apache.hadoop.sqoop.util.ExportException;
import org.apache.hadoop.sqoop.util.PerfCounters;

/**
 * Base class for running an export MapReduce job.
 */
public class ExportJobBase extends JobBase {

  public static final Log LOG = LogFactory.getLog(
      ExportJobBase.class.getName());

  public static final String SQOOP_EXPORT_TABLE_CLASS_KEY =
      "sqoop.export.table.class";

  protected ExportJobContext context;

  public ExportJobBase(final ExportJobContext ctxt) {
    this(ctxt, null, null, null);
  }

  public ExportJobBase(final ExportJobContext ctxt,
      final Class<? extends Mapper> mapperClass,
      final Class<? extends InputFormat> inputFormatClass,
      final Class<? extends OutputFormat> outputFormatClass) {
    super(ctxt.getOptions(), mapperClass, inputFormatClass, outputFormatClass);
    this.context = ctxt;
  }

  /**
   * @return the Path to the files we are going to export to the db.
   */
  protected Path getInputPath() throws IOException {
    Path inputPath = new Path(context.getOptions().getExportDir());
    Configuration conf = options.getConf();
    inputPath = inputPath.makeQualified(FileSystem.get(conf));
    return inputPath;
  }

  @Override
  protected void configureInputFormat(Job job, String tableName,
      String tableClassName, String splitByCol) throws IOException {

    super.configureInputFormat(job, tableName, tableClassName, splitByCol);
    FileInputFormat.addInputPath(job, getInputPath());
  }

  @Override
  protected Class<? extends InputFormat> getInputFormatClass() {
    Class<? extends InputFormat> configuredIF = super.getInputFormatClass();
    if (null == configuredIF) {
      return ExportInputFormat.class;
    } else {
      return configuredIF;
    }
  }

  @Override
  protected void configureMapper(Job job, String tableName,
      String tableClassName) throws IOException {

    job.setMapperClass(getMapperClass());

    // Concurrent writes of the same records would be problematic.
    HadoopShim.get().setJobMapSpeculativeExecution(job, false);

    job.setMapOutputKeyClass(SqoopRecord.class);
    job.setMapOutputValueClass(NullWritable.class);
  }

  @Override
  protected int configureNumTasks(Job job) throws IOException {
    int numMaps = super.configureNumTasks(job);
    ExportInputFormat.setNumMapTasks(job, numMaps);
    return numMaps;
  }

  @Override
  protected boolean runJob(Job job) throws ClassNotFoundException, IOException,
      InterruptedException {

    PerfCounters counters = new PerfCounters();
    counters.startClock();

    boolean success = job.waitForCompletion(false);
    counters.stopClock();
    counters.addBytes(job.getCounters().getGroup("FileSystemCounters")
      .findCounter("HDFS_BYTES_READ").getValue());
    LOG.info("Transferred " + counters.toString());
    long numRecords = HadoopShim.get().getNumMapInputRecords(job);
    LOG.info("Exported " + numRecords + " records.");

    return success;
  }
      

  /**
   * Run an export job to dump a table from HDFS to a database
   * @throws IOException if the export job encounters an IO error
   * @throws ExportException if the job fails unexpectedly or is misconfigured.
   */
  public void runExport() throws ExportException, IOException {

    SqoopOptions options = context.getOptions();
    Configuration conf = options.getConf();
    String tableName = context.getTableName();
    String tableClassName = new TableClassName(options).getClassForTable(tableName);
    String ormJarFile = context.getJarFile();

    LOG.info("Beginning export of " + tableName);
    loadJars(conf, ormJarFile, tableClassName);

    try {
      Job job = new Job(conf);

      // Set the external jar to use for the job.
      job.getConfiguration().set("mapred.jar", ormJarFile);

      configureInputFormat(job, tableName, tableClassName, null);
      configureOutputFormat(job, tableName, tableClassName);
      configureMapper(job, tableName, tableClassName);
      configureNumTasks(job);

      try {
        boolean success = runJob(job);
        if (!success) {
          throw new ExportException("Export job failed!");
        }
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }
    } finally {
      unloadJars();
    }
  }

  /**
   * @return true if the input directory contains SequenceFiles.
   */
  protected boolean inputIsSequenceFiles() {
    try {
      return ExportInputFormat.isSequenceFiles(
          context.getOptions().getConf(), getInputPath());
    } catch (IOException ioe) {
      LOG.warn("Could not check file format for export; assuming text");
      return false;
    }
  }
}
