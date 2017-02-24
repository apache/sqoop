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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.sqoop.avro.AvroUtil;
import org.apache.sqoop.mapreduce.ExportJobBase.FileType;
import org.apache.sqoop.util.Jars;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.mapreduce.JobBase;
import org.apache.sqoop.util.FileSystemUtil;

/**
 * Run a MapReduce job that merges two datasets.
 */
public class MergeJob extends JobBase {

  /** Configuration key specifying the path to the "old" dataset. */
  public static final String MERGE_OLD_PATH_KEY = "sqoop.merge.old.path";

  /** Configuration key specifying the path to the "new" dataset. */
  public static final String MERGE_NEW_PATH_KEY = "sqoop.merge.new.path";

  /** Configuration key specifying the name of the key column for joins. */
  public static final String MERGE_KEY_COL_KEY = "sqoop.merge.key.col";

  /** Configuration key specifying the SqoopRecord class name for
   * the records we are merging.
   */
  public static final String MERGE_SQOOP_RECORD_KEY = "sqoop.merge.class";

  public MergeJob(final SqoopOptions opts) {
    super(opts, null, null, null);
  }

  public boolean runMergeJob() throws IOException {
    Configuration conf = options.getConf();
    Job job = createJob(conf);

    String userClassName = options.getClassName();
    if (null == userClassName) {
      // Shouldn't get here.
      throw new IOException("Record class name not specified with "
          + "--class-name.");
    }

    // Set the external jar to use for the job.
    String existingJar = options.getExistingJarName();
    if (existingJar != null) {
      // User explicitly identified a jar path.
      LOG.debug("Setting job jar to user-specified jar: " + existingJar);
      job.getConfiguration().set("mapred.jar", existingJar);
    } else {
      // Infer it from the location of the specified class, if it's on the
      // classpath.
      try {
        Class<? extends Object> userClass = conf.getClassByName(userClassName);
        if (null != userClass) {
          String userJar = Jars.getJarPathForClass(userClass);
          LOG.debug("Setting job jar based on user class " + userClassName
              + ": " + userJar);
          job.getConfiguration().set("mapred.jar", userJar);
        } else {
          LOG.warn("Specified class " + userClassName + " is not in a jar. "
              + "MapReduce may not find the class");
        }
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }
    }

    try {
      Path oldPath = new Path(options.getMergeOldPath());
      Path newPath = new Path(options.getMergeNewPath());

      Configuration jobConf = job.getConfiguration();

      oldPath = FileSystemUtil.makeQualified(oldPath, jobConf);
      newPath = FileSystemUtil.makeQualified(newPath, jobConf);

      propagateOptionsToJob(job);

      FileInputFormat.addInputPath(job, oldPath);
      FileInputFormat.addInputPath(job, newPath);

      jobConf.set(MERGE_OLD_PATH_KEY, oldPath.toString());
      jobConf.set(MERGE_NEW_PATH_KEY, newPath.toString());
      jobConf.set(MERGE_KEY_COL_KEY, options.getMergeKeyCol());
      jobConf.set(MERGE_SQOOP_RECORD_KEY, userClassName);

      FileOutputFormat.setOutputPath(job, new Path(options.getTargetDir()));

      FileType fileType = ExportJobBase.getFileType(jobConf, oldPath);
      switch (fileType) {
        case AVRO_DATA_FILE:
          configueAvroMergeJob(conf, job, oldPath, newPath);
          break;
        case SEQUENCE_FILE:
          job.setInputFormatClass(SequenceFileInputFormat.class);
          job.setOutputFormatClass(SequenceFileOutputFormat.class);
          job.setMapperClass(MergeRecordMapper.class);
          job.setReducerClass(MergeReducer.class);
          break;
        default:
          job.setMapperClass(MergeTextMapper.class);
          job.setOutputFormatClass(RawKeyTextOutputFormat.class);
          job.setReducerClass(MergeReducer.class);
      }

      jobConf.set("mapred.output.key.class", userClassName);
      job.setOutputValueClass(NullWritable.class);

      // Set the intermediate data types.
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(MergeRecord.class);

      // Make sure Sqoop and anything else we need is on the classpath.
      cacheJars(job, null);
      setJob(job);
      return this.runJob(job);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    }
  }

  private void configueAvroMergeJob(Configuration conf, Job job, Path oldPath, Path newPath)
      throws IOException {
    LOG.info("Trying to merge avro files");
    final Schema oldPathSchema = AvroUtil.getAvroSchema(oldPath, conf);
    final Schema newPathSchema = AvroUtil.getAvroSchema(newPath, conf);
    if (oldPathSchema == null || newPathSchema == null || !oldPathSchema.equals(newPathSchema)) {
      throw new IOException("Invalid schema for input directories. Schema for old data: ["
          + oldPathSchema + "]. Schema for new data: [" + newPathSchema + "]");
    }
    LOG.debug("Avro Schema:" + oldPathSchema);
    job.setInputFormatClass(AvroInputFormat.class);
    job.setOutputFormatClass(AvroOutputFormat.class);
    job.setMapperClass(MergeAvroMapper.class);
    job.setReducerClass(MergeAvroReducer.class);
    AvroJob.setOutputSchema(job.getConfiguration(), oldPathSchema);
  }
}


