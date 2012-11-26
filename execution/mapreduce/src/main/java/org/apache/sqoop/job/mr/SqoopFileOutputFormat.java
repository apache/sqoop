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

package org.apache.sqoop.job.mr;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.io.Data;

/**
 * An output format for MapReduce job.
 */
public class SqoopFileOutputFormat
    extends FileOutputFormat<Data, NullWritable> {

  public static final Log LOG =
      LogFactory.getLog(SqoopFileOutputFormat.class.getName());

  public static final Class<? extends CompressionCodec> DEFAULT_CODEC =
      DefaultCodec.class;

  @Override
  public RecordWriter<Data, NullWritable> getRecordWriter(
      TaskAttemptContext context) throws IOException {
    Configuration conf = context.getConfiguration();

    Path filepath = getDefaultWorkFile(context, "");
    String filename = filepath.toString();
    conf.set(JobConstants.JOB_MR_OUTPUT_FILE, filename);

    boolean isCompressed = getCompressOutput(context);
    if (isCompressed) {
      String codecname =
          conf.get(JobConstants.HADOOP_COMPRESS_CODEC, DEFAULT_CODEC.getName());
      conf.set(JobConstants.JOB_MR_OUTPUT_CODEC, codecname);
    }

    SqoopOutputFormatLoadExecutor executor =
        new SqoopOutputFormatLoadExecutor(context);
    return executor.getRecordWriter();
  }

  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    Path output = getOutputPath(context);
    return new DestroyerFileOutputCommitter(output, context);
  }

  public class DestroyerFileOutputCommitter extends FileOutputCommitter {

    public DestroyerFileOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
      super(outputPath, context);
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);

      Configuration config = context.getConfiguration();
      SqoopDestroyerExecutor.executeDestroyer(true, config, JobConstants.JOB_ETL_DESTROYER);
    }

    @Override
    public void abortJob(JobContext context, JobStatus.State state) throws IOException {
      super.abortJob(context, state);

      Configuration config = context.getConfiguration();
      SqoopDestroyerExecutor.executeDestroyer(false, config, JobConstants.JOB_ETL_DESTROYER);
    }
  }
}
