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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;
import org.apache.sqoop.job.io.Data;
import org.apache.sqoop.job.mr.SqoopInputFormat;
import org.apache.sqoop.job.mr.SqoopMapper;
import org.apache.sqoop.job.mr.SqoopNullOutputFormat;
import org.apache.sqoop.job.mr.SqoopSplit;

public class TestMapReduce extends TestCase {

  private static final int START_PARTITION = 1;
  private static final int NUMBER_OF_PARTITIONS = 9;
  private static final int NUMBER_OF_ROWS_PER_PARTITION = 10;

  public void testInputFormat() throws Exception {
    Configuration conf = new Configuration();
    conf.set(JobConstants.JOB_TYPE, "IMPORT");
    conf.set(JobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
    Job job = new Job(conf);

    SqoopInputFormat inputformat = new SqoopInputFormat();
    List<InputSplit> splits = inputformat.getSplits(job);
    assertEquals(9, splits.size());

    for (int id = START_PARTITION; id <= NUMBER_OF_PARTITIONS; id++) {
      SqoopSplit split = (SqoopSplit)splits.get(id-1);
      DummyPartition partition = (DummyPartition)split.getPartition();
      assertEquals(id, partition.getId());
    }
  }

  public void testMapper() throws Exception {
    Configuration conf = new Configuration();
    conf.set(JobConstants.JOB_TYPE, "IMPORT");
    conf.set(JobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
    conf.set(JobConstants.JOB_ETL_EXTRACTOR, DummyExtractor.class.getName());

    JobUtils.runJob(conf, SqoopInputFormat.class, SqoopMapper.class,
        DummyOutputFormat.class);
  }

  public void testOutputFormat() throws Exception {
    Configuration conf = new Configuration();
    conf.set(JobConstants.JOB_TYPE, "IMPORT");
    conf.set(JobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
    conf.set(JobConstants.JOB_ETL_EXTRACTOR, DummyExtractor.class.getName());
    conf.set(JobConstants.JOB_ETL_LOADER, DummyLoader.class.getName());

    JobUtils.runJob(conf, SqoopInputFormat.class, SqoopMapper.class,
        SqoopNullOutputFormat.class);
  }

  public static class DummyPartition extends Partition {
    private int id;

    public void setId(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      id = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(id);
    }

    @Override
    public String toString() {
      return Integer.toString(id);
    }
  }

  public static class DummyPartitioner extends Partitioner {
    @Override
    public List<Partition> getPartitions(PartitionerContext context, Object oc, Object oj) {
      List<Partition> partitions = new LinkedList<Partition>();
      for (int id = START_PARTITION; id <= NUMBER_OF_PARTITIONS; id++) {
        DummyPartition partition = new DummyPartition();
        partition.setId(id);
        partitions.add(partition);
      }
      return partitions;
    }
  }

  public static class DummyExtractor extends Extractor {
    @Override
    public void extract(ExtractorContext context, Object oc, Object oj, Object partition) {
      int id = ((DummyPartition)partition).getId();
      for (int row = 0; row < NUMBER_OF_ROWS_PER_PARTITION; row++) {
        context.getDataWriter().writeArrayRecord(new Object[] {
            id * NUMBER_OF_ROWS_PER_PARTITION + row,
            (double) (id * NUMBER_OF_ROWS_PER_PARTITION + row),
            String.valueOf(id*NUMBER_OF_ROWS_PER_PARTITION+row)});
      }
    }

    @Override
    public long getRowsRead() {
      return NUMBER_OF_ROWS_PER_PARTITION;
    }
  }

  public static class DummyOutputFormat
      extends OutputFormat<Data, NullWritable> {
    @Override
    public void checkOutputSpecs(JobContext context) {
      // do nothing
    }

    @Override
    public RecordWriter<Data, NullWritable> getRecordWriter(
        TaskAttemptContext context) {
      return new DummyRecordWriter();
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
      return new DummyOutputCommitter();
    }

    public static class DummyRecordWriter
        extends RecordWriter<Data, NullWritable> {
      private int index = START_PARTITION*NUMBER_OF_ROWS_PER_PARTITION;
      private Data data = new Data();

      @Override
      public void write(Data key, NullWritable value) {
        data.setContent(new Object[] {
          index,
          (double) index,
          String.valueOf(index)},
          Data.ARRAY_RECORD);
        index++;

        assertEquals(data.toString(), key.toString());
      }

      @Override
      public void close(TaskAttemptContext context) {
        // do nothing
      }
    }

    public static class DummyOutputCommitter extends OutputCommitter {
      @Override
      public void setupJob(JobContext jobContext) { }

      @Override
      public void setupTask(TaskAttemptContext taskContext) { }

      @Override
      public void commitTask(TaskAttemptContext taskContext) { }

      @Override
      public void abortTask(TaskAttemptContext taskContext) { }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext) {
        return false;
      }
    }
  }

  public static class DummyLoader extends Loader {
    private int index = START_PARTITION*NUMBER_OF_ROWS_PER_PARTITION;
    private Data expected = new Data();
    private Data actual = new Data();

    @Override
    public void load(LoaderContext context, Object oc, Object oj) throws Exception{
      Object[] array;
      while ((array = context.getDataReader().readArrayRecord()) != null) {
        actual.setContent(array, Data.ARRAY_RECORD);

        expected.setContent(new Object[] {
          index,
          (double) index,
          String.valueOf(index)},
          Data.ARRAY_RECORD);
        index++;

        assertEquals(expected.toString(), actual.toString());
      }
    }
  }
}
