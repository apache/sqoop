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
package org.apache.sqoop.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.job.mr.SqoopSplit;
import org.apache.sqoop.utils.ClassUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobUtils {

  public static boolean runJob(Configuration conf,
    Class<? extends InputFormat<SqoopSplit, NullWritable>> input,
    Class<? extends Mapper<SqoopSplit, NullWritable, SqoopWritable, NullWritable>> mapper,
    Class<? extends OutputFormat<SqoopWritable, NullWritable>> output)
    throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(conf);
    job.setInputFormatClass(input);
    job.setMapperClass(mapper);
    job.setMapOutputKeyClass(SqoopWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(output);
    job.setOutputKeyClass(SqoopWritable.class);
    job.setOutputValueClass(NullWritable.class);

    boolean ret = job.waitForCompletion(true);

    // Hadoop 1.0 (and 0.20) have nasty bug when job committer is not called in LocalJobRuner
    if (isHadoop1()) {
      callOutputCommitter(job, output);
    }

    return ret;
  }

  /**
   * Call output format on given job manually.
   */
  private static void callOutputCommitter(Job job, Class<? extends OutputFormat<SqoopWritable, NullWritable>> outputFormat) throws IOException, InterruptedException {
    OutputCommitter committer = ((OutputFormat)ClassUtils.instantiate(outputFormat)).getOutputCommitter(null);

    JobContext jobContext = mock(JobContext.class);
    when(jobContext.getConfiguration()).thenReturn(job.getConfiguration());

    committer.commitJob(jobContext);
  }

  /**
   * Detect Hadoop 1.0 installation
   *
   * @return True if and only if this is Hadoop 1 and below
   */
  public static boolean isHadoop1() {
    String version = org.apache.hadoop.util.VersionInfo.getVersion();
    if (version.matches("\\b0\\.20\\..+\\b")
      || version.matches("\\b1\\.\\d\\.\\d")) {
      return true;
    }
    return false;
  }

  private JobUtils() {
    // Disable explicit object creation
  }

}
