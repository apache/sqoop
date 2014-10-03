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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.job.mr.SqoopFileOutputFormat;
import org.apache.sqoop.job.mr.SqoopInputFormat;
import org.apache.sqoop.job.mr.SqoopMapper;
import org.apache.sqoop.job.mr.SqoopNullOutputFormat;
import org.apache.sqoop.job.mr.SqoopSplit;
import org.junit.Assert;

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

    return job.waitForCompletion(true);
  }

  private JobUtils() {
    // Disable explicit object creation
  }

}
