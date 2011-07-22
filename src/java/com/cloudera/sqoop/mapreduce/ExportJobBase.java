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

package com.cloudera.sqoop.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.config.ConfigurationHelper;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.orm.TableClassName;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.PerfCounters;

/**
 * Base class for running an export MapReduce job.
 */
public class ExportJobBase extends JobBase {

  public static final Log LOG = LogFactory.getLog(
      ExportJobBase.class.getName());

  /** What SqoopRecord class to use to read a record for export. */
  public static final String SQOOP_EXPORT_TABLE_CLASS_KEY =
      "sqoop.mapreduce.export.table.class";

  /**
   * What column of the table to use for the WHERE clause of
   * an updating export.
   */
  public static final String SQOOP_EXPORT_UPDATE_COL_KEY =
      "sqoop.mapreduce.export.update.col";

  /** Number of map tasks to use for an export. */
  public static final String EXPORT_MAP_TASKS_KEY =
      "sqoop.mapreduce.export.map.tasks";

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
   * @return true if p is a SequenceFile, or a directory containing
   * SequenceFiles.
   */
  public static boolean isSequenceFiles(Configuration conf, Path p)
      throws IOException {
    FileSystem fs = p.getFileSystem(conf);

    try {
      FileStatus stat = fs.getFileStatus(p);

      if (null == stat) {
        // Couldn't get the item.
        LOG.warn("Input path " + p + " does not exist");
        return false;
      }

      if (stat.isDir()) {
        FileStatus [] subitems = fs.listStatus(p);
        if (subitems == null || subitems.length == 0) {
          LOG.warn("Input path " + p + " contains no files");
          return false; // empty dir.
        }

        // Pick a child entry to examine instead.
        boolean foundChild = false;
        for (int i = 0; i < subitems.length; i++) {
          stat = subitems[i];
          if (!stat.isDir() && !stat.getPath().getName().startsWith("_")) {
            foundChild = true;
            break; // This item is a visible file. Check it.
          }
        }

        if (!foundChild) {
          stat = null; // Couldn't find a reasonable candidate.
        }
      }

      if (null == stat) {
        LOG.warn("null FileStatus object in isSequenceFiles(); "
            + "assuming false.");
        return false;
      }

      Path target = stat.getPath();
      return hasSequenceFileHeader(target, conf);
    } catch (FileNotFoundException fnfe) {
      LOG.warn("Input path " + p + " does not exist");
      return false; // doesn't exist!
    }
  }

  /**
   * @param file a file to test.
   * @return true if 'file' refers to a SequenceFile.
   */
  private static boolean hasSequenceFileHeader(Path file, Configuration conf) {
    // Test target's header to see if it contains magic numbers indicating it's
    // a SequenceFile.
    byte [] header = new byte[3];
    FSDataInputStream is = null;
    try {
      FileSystem fs = file.getFileSystem(conf);
      is = fs.open(file);
      is.readFully(header);
    } catch (IOException ioe) {
      // Error reading header or EOF; assume not a SequenceFile.
      LOG.warn("IOException checking SequenceFile header: " + ioe);
      return false;
    } finally {
      try {
        if (null != is) {
          is.close();
        }
      } catch (IOException ioe) {
        // ignore; closing.
        LOG.warn("IOException closing input stream: " + ioe + "; ignoring.");
      }
    }

    // Return true (isSequenceFile) iff the magic number sticks.
    return header[0] == 'S' && header[1] == 'E' && header[2] == 'Q';
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
      String tableClassName, String splitByCol)
      throws ClassNotFoundException, IOException {

    super.configureInputFormat(job, tableName, tableClassName, splitByCol);
    FileInputFormat.addInputPath(job, getInputPath());
  }

  @Override
  protected Class<? extends InputFormat> getInputFormatClass()
      throws ClassNotFoundException {
    Class<? extends InputFormat> configuredIF = super.getInputFormatClass();
    if (null == configuredIF) {
      return ExportInputFormat.class;
    } else {
      return configuredIF;
    }
  }

  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass()
      throws ClassNotFoundException {
    Class<? extends OutputFormat> configuredOF = super.getOutputFormatClass();
    if (null == configuredOF) {
      return ExportOutputFormat.class;
    } else {
      return configuredOF;
    }
  }

  @Override
  protected void configureMapper(Job job, String tableName,
      String tableClassName) throws ClassNotFoundException, IOException {

    job.setMapperClass(getMapperClass());

    // Concurrent writes of the same records would be problematic.
    ConfigurationHelper.setJobMapSpeculativeExecution(job, false);

    job.setMapOutputKeyClass(SqoopRecord.class);
    job.setMapOutputValueClass(NullWritable.class);
  }

  @Override
  protected int configureNumTasks(Job job) throws IOException {
    int numMaps = super.configureNumTasks(job);
    job.getConfiguration().setInt(EXPORT_MAP_TASKS_KEY, numMaps);
    return numMaps;
  }

  @Override
  protected boolean runJob(Job job) throws ClassNotFoundException, IOException,
      InterruptedException {

    PerfCounters perfCounters = new PerfCounters();
    perfCounters.startClock();

    boolean success = job.waitForCompletion(true);
    perfCounters.stopClock();

    Counters jobCounters = job.getCounters();
    // If the job has been retired, these may be unavailable.
    if (null == jobCounters) {
      displayRetiredJobNotice(LOG);
    } else {
      perfCounters.addBytes(jobCounters.getGroup("FileSystemCounters")
        .findCounter("HDFS_BYTES_READ").getValue());
      LOG.info("Transferred " + perfCounters.toString());
      long numRecords =  ConfigurationHelper.getNumMapInputRecords(job);
      LOG.info("Exported " + numRecords + " records.");
    }

    return success;
  }

  /**
   * Run an export job to dump a table from HDFS to a database. If a staging
   * table is specified and the connection manager supports staging of data,
   * the export will first populate the staging table and then migrate the
   * data to the target table.
   * @throws IOException if the export job encounters an IO error
   * @throws ExportException if the job fails unexpectedly or is misconfigured.
   */
  public void runExport() throws ExportException, IOException {

    ConnManager cmgr = context.getConnManager();
    SqoopOptions options = context.getOptions();
    Configuration conf = options.getConf();

    String outputTableName = context.getTableName();
    String stagingTableName = context.getOptions().getStagingTableName();

    String tableName = outputTableName;
    boolean stagingEnabled = false;
    if (stagingTableName != null) { // user has specified the staging table
      if (cmgr.supportsStagingForExport()) {
        LOG.info("Data will be staged in the table: " + stagingTableName);
        tableName = stagingTableName;
        stagingEnabled = true;
      } else {
        throw new ExportException("The active connection manager ("
            + cmgr.getClass().getCanonicalName()
            + ") does not support staging of data for export. "
            + "Please retry without specifying the --staging-table option.");
      }
    }

    String tableClassName =
        new TableClassName(options).getClassForTable(outputTableName);
    String ormJarFile = context.getJarFile();

    LOG.info("Beginning export of " + outputTableName);
    loadJars(conf, ormJarFile, tableClassName);

    if (stagingEnabled) {
      // Prepare the staging table
      if (options.doClearStagingTable()) {
        try {
          // Delete all records from staging table
          cmgr.deleteAllRecords(stagingTableName);
        } catch (SQLException ex) {
          throw new ExportException(
              "Failed to empty staging table before export run", ex);
        }
      } else {
        // User has not explicitly specified the clear staging table option.
        // Assert that the staging table is empty.
        try {
          long rowCount = cmgr.getTableRowCount(stagingTableName);
          if (rowCount != 0L) {
            throw new ExportException("The specified staging table ("
                + stagingTableName + ") is not empty. To force deletion of "
                + "its data, please retry with --clear-staging-table option.");
          }
        } catch (SQLException ex) {
          throw new ExportException(
              "Failed to count data rows in staging table: "
                  + stagingTableName, ex);
        }
      }
    }

    try {
      Job job = new Job(conf);

      // Set the external jar to use for the job.
      job.getConfiguration().set("mapred.jar", ormJarFile);

      configureInputFormat(job, tableName, tableClassName, null);
      configureOutputFormat(job, tableName, tableClassName);
      configureMapper(job, tableName, tableClassName);
      configureNumTasks(job);
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

    // Unstage the data if needed
    if (stagingEnabled) {
      // Migrate data from staging table to the output table
      try {
        LOG.info("Starting to migrate data from staging table to destination.");
        cmgr.migrateData(stagingTableName, outputTableName);
      } catch (SQLException ex) {
        LOG.error("Failed to move data from staging table ("
            + stagingTableName + ") to target table ("
            + outputTableName + ")", ex);
        throw new ExportException(
            "Failed to move data from staging table", ex);
      }
    }
  }

  /**
   * @return true if the input directory contains SequenceFiles.
   */
  protected boolean inputIsSequenceFiles() {
    try {
      return isSequenceFiles(
          context.getOptions().getConf(), getInputPath());
    } catch (IOException ioe) {
      LOG.warn("Could not check file format for export; assuming text");
      return false;
    }
  }
}
