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
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.job.PrefixContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.MRExecutionError;
import org.apache.sqoop.job.MRJobConstants;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;

/**
 * An InputFormat for MapReduce job.
 */
public class SqoopInputFormat extends InputFormat<SqoopSplit, NullWritable> {

  public static final Logger LOG =
    Logger.getLogger(SqoopInputFormat.class);

  @Override
  public RecordReader<SqoopSplit, NullWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    return new SqoopRecordReader();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public List<InputSplit> getSplits(JobContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    String partitionerName = conf.get(MRJobConstants.JOB_ETL_PARTITIONER);
    Partitioner partitioner = (Partitioner) ClassUtils.instantiate(partitionerName);

    PrefixContext connectorContext = new PrefixContext(conf, MRJobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);
    Object connectorLinkConfig = MRConfigurationUtils.getConnectorLinkConfig(Direction.FROM, conf);
    Object connectorFromJobConfig = MRConfigurationUtils.getConnectorJobConfig(Direction.FROM, conf);
    Schema fromSchema = MRConfigurationUtils.getConnectorSchema(Direction.FROM, conf);

    long maxPartitions = conf.getLong(MRJobConstants.JOB_ETL_EXTRACTOR_NUM, 10);
    PartitionerContext partitionerContext = new PartitionerContext(connectorContext, maxPartitions, fromSchema);

    List<Partition> partitions = partitioner.getPartitions(partitionerContext, connectorLinkConfig, connectorFromJobConfig);
    List<InputSplit> splits = new LinkedList<InputSplit>();
    for (Partition partition : partitions) {
      LOG.debug("Partition: " + partition);
      SqoopSplit split = new SqoopSplit();
      split.setPartition(partition);
      splits.add(split);
    }

    if(splits.size() > maxPartitions) {
      throw new SqoopException(MRExecutionError.MAPRED_EXEC_0025,
        String.format("Got %d, max was %d", splits.size(), maxPartitions));
    }

    return splits;
  }

  public static class SqoopRecordReader
      extends RecordReader<SqoopSplit, NullWritable> {

    private boolean delivered = false;
    private SqoopSplit split = null;

    @Override
    public boolean nextKeyValue() {
      if (delivered) {
        return false;
      } else {
        delivered = true;
        return true;
      }
    }

    @Override
    public SqoopSplit getCurrentKey() {
      return split;
    }

    @Override
    public NullWritable getCurrentValue() {
      return NullWritable.get();
    }

    @Override
    public void close() {
    }

    @Override
    public float getProgress() {
      return delivered ? 1.0f : 0.0f;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
      this.split = (SqoopSplit)split;
    }
  }

}
