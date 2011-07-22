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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;

import org.apache.hadoop.sqoop.SqoopOptions;
import org.apache.hadoop.sqoop.shims.HadoopShim;
import org.apache.hadoop.sqoop.util.ClassLoaderStack;

/**
 * Base class for configuring and running a MapReduce job.
 * Allows dependency injection, etc, for easy customization of import job types.
 */
public class JobBase {

  public static final Log LOG = LogFactory.getLog(JobBase.class.getName());

  protected SqoopOptions options;
  protected Class<? extends Mapper> mapperClass;
  protected Class<? extends InputFormat> inputFormatClass;
  protected Class<? extends OutputFormat> outputFormatClass;

  private ClassLoader prevClassLoader = null;

  public JobBase() {
    this(null);
  }

  public JobBase(final SqoopOptions opts) {
    this(opts, null, null, null);
  }

  public JobBase(final SqoopOptions opts,
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
  protected Class<? extends Mapper> getMapperClass()
      throws ClassNotFoundException {
    return this.mapperClass;
  }

  /**
   * @return the inputformat class to use for the job.
   */
  protected Class<? extends InputFormat> getInputFormatClass()
      throws ClassNotFoundException {
    return this.inputFormatClass;
  }

  /**
   * @return the outputformat class to use for the job.
   */
  protected Class<? extends OutputFormat> getOutputFormatClass()
      throws ClassNotFoundException {
    return this.outputFormatClass;
  }

  /** Set the OutputFormat class to use for this job. */
  public void setOutputFormatClass(Class<? extends OutputFormat> cls) {
    this.outputFormatClass = cls;
  }

  /** Set the InputFormat class to use for this job. */
  public void setInputFormatClass(Class<? extends InputFormat> cls) {
    this.inputFormatClass = cls;
  }

  /** Set the Mapper class to use for this job. */
  public void setMapperClass(Class<? extends Mapper> cls) {
    this.mapperClass = cls;
  }

  /**
   * Set the SqoopOptions configuring this job.
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
      // If we're using the LocalJobRunner, then instead of using the compiled
      // jar file as the job source, we're running in the current thread. Push
      // on another classloader that loads from that jar in addition to
      // everything currently on the classpath.
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
      String tableClassName, String splitByCol)
      throws ClassNotFoundException, IOException {
    //TODO: 'splitByCol' is import-job specific; lift it out of this API.
    Class<? extends InputFormat> ifClass = getInputFormatClass();
    LOG.debug("Using InputFormat: " + ifClass);
    job.setInputFormatClass(ifClass);
  }

  /**
   * Configure the output format to use for the job.
   */
  protected void configureOutputFormat(Job job, String tableName,
      String tableClassName) throws ClassNotFoundException, IOException {
    Class<? extends OutputFormat> ofClass = getOutputFormatClass();
    LOG.debug("Using OutputFormat: " + ofClass);
    job.setOutputFormatClass(ofClass);
  }

  /**
   * Set the mapper class implementation to use in the job,
   * as well as any related configuration (e.g., map output types).
   */
  protected void configureMapper(Job job, String tableName,
      String tableClassName) throws ClassNotFoundException, IOException {
    job.setMapperClass(getMapperClass());
  }

  /**
   * Configure the number of map/reduce tasks to use in the job.
   */
  protected int configureNumTasks(Job job) throws IOException {
    int numMapTasks = options.getNumMappers();
    if (numMapTasks < 1) {
      numMapTasks = SqoopOptions.DEFAULT_NUM_MAPPERS;
      LOG.warn("Invalid mapper count; using " + numMapTasks + " mappers.");
    }

    HadoopShim.get().setJobNumMaps(job, numMapTasks);
    job.setNumReduceTasks(0);
    return numMapTasks;
  }

  /**
   * Actually run the MapReduce job.
   */
  protected boolean runJob(Job job) throws ClassNotFoundException, IOException,
      InterruptedException {
    return job.waitForCompletion(true);
  }
}
