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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import org.apache.hadoop.sqoop.ConnFactory;
import org.apache.hadoop.sqoop.SqoopOptions;
import org.apache.hadoop.sqoop.manager.ConnManager;
import org.apache.hadoop.sqoop.orm.TableClassName;
import org.apache.hadoop.sqoop.util.ClassLoaderStack;
import org.apache.hadoop.sqoop.util.ImportException;
import org.apache.hadoop.sqoop.util.PerfCounters;

/**
 * Base class for running an import MapReduce job.
 * Allows dependency injection, etc, for easy customization of import job types.
 */
public class ImportJobBase {

  public static final Log LOG = LogFactory.getLog(ImportJobBase.class.getName());

  protected SqoopOptions options;
  protected Class<? extends Mapper> mapperClass;
  protected Class<? extends InputFormat> inputFormatClass;
  protected Class<? extends OutputFormat> outputFormatClass;

  private ClassLoader prevClassLoader = null;

  public ImportJobBase() {
    this(null);
  }

  public ImportJobBase(final SqoopOptions opts) {
    this(opts, null, null, null);
  }

  public ImportJobBase(final SqoopOptions opts,
      final Class<? extends Mapper> mapperClass,
      final Class<? extends InputFormat> inputFormatClass,
      final Class<? extends OutputFormat> outputFormatClass) {

    this.options = opts;
    this.mapperClass = mapperClass;
    this.inputFormatClass = inputFormatClass;
    this.outputFormatClass = outputFormatClass;
  }

  /**
   * @return the mapper class to use for the job.
   */
  protected Class<? extends Mapper> getMapperClass() {
    return this.mapperClass;
  }

  /**
   * @return the inputformat class to use for the job.
   */
  protected Class<? extends InputFormat> getInputFormatClass() {
    return this.inputFormatClass;
  }

  /**
   * @return the outputformat class to use for the job.
   */
  protected Class<? extends OutputFormat> getOutputFormatClass() {
    return this.outputFormatClass;
  }

  /** Set the OutputFormat class to use for this job */
  public void setOutputFormatClass(Class<? extends OutputFormat> cls) {
    this.outputFormatClass = cls;
  }

  /** Set the InputFormat class to use for this job */
  public void setInputFormatClass(Class<? extends InputFormat> cls) {
    this.inputFormatClass = cls;
  }

  /** Set the Mapper class to use for this job */
  public void setMapperClass(Class<? extends Mapper> cls) {
    this.mapperClass = cls;
  }

  /**
   * Set the SqoopOptions configuring this job
   */
  public void setOptions(SqoopOptions opts) {
    this.options = opts;
  }

  /**
   * If jars must be loaded into the local environment, do so here.
   */
  protected void loadJars(Configuration conf, String ormJarFile,
      String tableClassName) throws IOException {
    boolean isLocal = "local".equals(conf.get("mapreduce.jobtracker.address"))
        || "local".equals(conf.get("mapred.job.tracker"));
    if (isLocal) {
      // If we're using the LocalJobRunner, then instead of using the compiled jar file
      // as the job source, we're running in the current thread. Push on another classloader
      // that loads from that jar in addition to everything currently on the classpath.
      this.prevClassLoader = ClassLoaderStack.addJarFile(ormJarFile,
          tableClassName);
    }
  }

  /**
   * If any classloader was invoked by loadJars, free it here.
   */
  protected void unloadJars() {
    if (null != this.prevClassLoader) {
      // unload the special classloader for this jar.
      ClassLoaderStack.setCurrentClassLoader(this.prevClassLoader);
    }
  }

  /**
   * Configure the inputformat to use for the job.
   */
  protected void configureInputFormat(Job job, String tableName,
      String tableClassName, String splitByCol) throws IOException {
    LOG.debug("Using InputFormat: " + inputFormatClass);
    job.setInputFormatClass(getInputFormatClass());
  }

  /**
   * Configure the output format to use for the job.
   */
  protected void configureOutputFormat(Job job, String tableName,
      String tableClassName) throws IOException {
    String hdfsWarehouseDir = options.getWarehouseDir();
    Path outputPath;

    if (null != hdfsWarehouseDir) {
      Path hdfsWarehousePath = new Path(hdfsWarehouseDir);
      hdfsWarehousePath.makeQualified(FileSystem.get(job.getConfiguration()));
      outputPath = new Path(hdfsWarehousePath, tableName);
    } else {
      outputPath = new Path(tableName);
    }

    job.setOutputFormatClass(getOutputFormatClass());

    if (options.getFileLayout() == SqoopOptions.FileLayout.SequenceFile) {
      job.getConfiguration().set("mapred.output.value.class", tableClassName);
    }

    if (options.shouldUseCompression()) {
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
      SequenceFileOutputFormat.setOutputCompressionType(job,
          CompressionType.BLOCK);
    }

    FileOutputFormat.setOutputPath(job, outputPath);
  }

  /**
   * Set the mapper class implementation to use in the job,
   * as well as any related configuration (e.g., map output types).
   */
  protected void configureMapper(Job job, String tableName,
      String tableClassName) throws IOException {
    job.setMapperClass(getMapperClass());
  }

  /**
   * Configure the number of map/reduce tasks to use in the job.
   */
  protected void configureNumTasks(Job job) throws IOException {
    int numMapTasks = options.getNumMappers();
    if (numMapTasks < 1) {
      numMapTasks = SqoopOptions.DEFAULT_NUM_MAPPERS;
      LOG.warn("Invalid mapper count; using " + numMapTasks + " mappers.");
    }
    job.getConfiguration().setInt(JobContext.NUM_MAPS, numMapTasks);
    job.setNumReduceTasks(0);
  }

  /**
   * Actually run the MapReduce job.
   */
  protected void runJob(Job job) throws ClassNotFoundException, IOException,
      ImportException, InterruptedException {

    PerfCounters counters = new PerfCounters();
    counters.startClock();

    boolean success = job.waitForCompletion(true);
    counters.stopClock();
    counters.addBytes(job.getCounters().getGroup("FileSystemCounters")
      .findCounter("HDFS_BYTES_WRITTEN").getValue());
    LOG.info("Transferred " + counters.toString());
    long numRecords = job.getCounters()
      .findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
    LOG.info("Retrieved " + numRecords + " records.");

    if (!success) {
      throw new ImportException("Import job failed!");
    }
  }


  /**
   * Run an import job to read a table in to HDFS
   *
   * @param tableName  the database table to read
   * @param ormJarFile the Jar file to insert into the dcache classpath. (may be null)
   * @param splitByCol the column of the database table to use to split the import
   * @param conf A fresh Hadoop Configuration to use to build an MR job.
   * @throws IOException if the job encountered an IO problem
   * @throws ImportException if the job failed unexpectedly or was misconfigured.
   */
  public void runImport(String tableName, String ormJarFile, String splitByCol,
      Configuration conf) throws IOException, ImportException {

    LOG.info("Beginning import of " + tableName);
    String tableClassName = new TableClassName(options).getClassForTable(tableName);
    loadJars(conf, ormJarFile, tableClassName);

    try {
      Job job = new Job(conf);

      // Set the external jar to use for the job.
      job.getConfiguration().set("mapred.jar", ormJarFile);

      configureInputFormat(job, tableName, tableClassName, splitByCol);
      configureOutputFormat(job, tableName, tableClassName);
      configureMapper(job, tableName, tableClassName);
      configureNumTasks(job);

      try {
        runJob(job);
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }
    } finally {
      unloadJars();
    }
  }
}
