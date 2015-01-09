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

import static org.testng.AssertJUnit.assertEquals;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.connector.common.EmptyConfiguration;
import org.apache.sqoop.connector.idf.CSVIntermediateDataFormat;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.DestroyerContext;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.job.mr.MRConfigurationUtils;
import org.apache.sqoop.job.mr.SqoopInputFormat;
import org.apache.sqoop.job.mr.SqoopMapper;
import org.apache.sqoop.job.mr.SqoopNullOutputFormat;
import org.apache.sqoop.job.mr.SqoopSplit;
import org.apache.sqoop.job.util.MRJobTestUtil;
import org.testng.annotations.Test;

public class TestMapReduce {

  private static final int START_PARTITION = 1;
  private static final int NUMBER_OF_PARTITIONS = 9;
  private static final int NUMBER_OF_ROWS_PER_PARTITION = 10;

  @Test
  public void testSqoopInputFormat() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
    conf.set(MRJobConstants.FROM_INTERMEDIATE_DATA_FORMAT, CSVIntermediateDataFormat.class.getName());
    conf.set(MRJobConstants.TO_INTERMEDIATE_DATA_FORMAT, CSVIntermediateDataFormat.class.getName());

    Job job = new Job(conf);

    SqoopInputFormat inputformat = new SqoopInputFormat();
    List<InputSplit> splits = inputformat.getSplits(job);
    assertEquals(9, splits.size());

    for (int id = START_PARTITION; id <= NUMBER_OF_PARTITIONS; id++) {
      SqoopSplit split = (SqoopSplit) splits.get(id - 1);
      DummyPartition partition = (DummyPartition) split.getPartition();
      assertEquals(id, partition.getId());
    }
  }

  @Test
  public void testSqoopMapper() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
    conf.set(MRJobConstants.JOB_ETL_EXTRACTOR, DummyExtractor.class.getName());
    conf.set(MRJobConstants.FROM_INTERMEDIATE_DATA_FORMAT, CSVIntermediateDataFormat.class.getName());
    conf.set(MRJobConstants.TO_INTERMEDIATE_DATA_FORMAT, CSVIntermediateDataFormat.class.getName());

    Job job = new Job(conf);
    // from and to have the same schema in this test case
    MRConfigurationUtils.setConnectorSchema(Direction.FROM, job, MRJobTestUtil.getTestSchema());
    MRConfigurationUtils.setConnectorSchema(Direction.TO, job, MRJobTestUtil.getTestSchema());
    boolean success = MRJobTestUtil.runJob(job.getConfiguration(),
                                      SqoopInputFormat.class,
                                      SqoopMapper.class,
                                      DummyOutputFormat.class);
    assertEquals("Job failed!", true, success);
  }

  @Test
  public void testNullOutputFormat() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MRJobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
    conf.set(MRJobConstants.JOB_ETL_EXTRACTOR, DummyExtractor.class.getName());
    conf.set(MRJobConstants.JOB_ETL_LOADER, DummyLoader.class.getName());
    conf.set(MRJobConstants.JOB_ETL_FROM_DESTROYER, DummyFromDestroyer.class.getName());
    conf.set(MRJobConstants.JOB_ETL_TO_DESTROYER, DummyToDestroyer.class.getName());
    conf.set(MRJobConstants.FROM_INTERMEDIATE_DATA_FORMAT, CSVIntermediateDataFormat.class.getName());
    conf.set(MRJobConstants.TO_INTERMEDIATE_DATA_FORMAT, CSVIntermediateDataFormat.class.getName());

    Job job = new Job(conf);
    // from and to have the same schema in this test case
    MRConfigurationUtils.setConnectorSchema(Direction.FROM, job, MRJobTestUtil.getTestSchema());
    MRConfigurationUtils.setConnectorSchema(Direction.TO, job, MRJobTestUtil.getTestSchema());
    boolean success = MRJobTestUtil.runJob(job.getConfiguration(),
                                     SqoopInputFormat.class,
                                     SqoopMapper.class,
                                     SqoopNullOutputFormat.class);
    assertEquals("Job failed!", true, success);

    // Make sure both destroyers get called.
    assertEquals(1, DummyFromDestroyer.count);
    assertEquals(1, DummyToDestroyer.count);
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

  public static class DummyExtractor extends
      Extractor<EmptyConfiguration, EmptyConfiguration, DummyPartition> {
    @Override
    public void extract(ExtractorContext context, EmptyConfiguration oc, EmptyConfiguration oj,
        DummyPartition partition) {
      int id = ((DummyPartition) partition).getId();
      for (int row = 0; row < NUMBER_OF_ROWS_PER_PARTITION; row++) {
        context.getDataWriter().writeArrayRecord(
            new Object[] { id * NUMBER_OF_ROWS_PER_PARTITION + row,
                (double) (id * NUMBER_OF_ROWS_PER_PARTITION + row),
                String.valueOf(id * NUMBER_OF_ROWS_PER_PARTITION + row) });
      }
    }

    @Override
    public long getRowsRead() {
      return NUMBER_OF_ROWS_PER_PARTITION;
    }
  }

  public static class DummyOutputFormat extends OutputFormat<SqoopWritable, NullWritable> {
    @Override
    public void checkOutputSpecs(JobContext context) {
      // do nothing
    }

    @Override
    public RecordWriter<SqoopWritable, NullWritable> getRecordWriter(TaskAttemptContext context) {
      return new DummyRecordWriter();
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
      return new DummyOutputCommitter();
    }

    public static class DummyRecordWriter extends RecordWriter<SqoopWritable, NullWritable> {
      private int index = START_PARTITION * NUMBER_OF_ROWS_PER_PARTITION;
      // should I use a dummy IDF for testing?
      private IntermediateDataFormat<?> dataFormat = MRJobTestUtil.getTestIDF();

      @Override
      public void write(SqoopWritable key, NullWritable value) {
        String testData = "" + index + "," +  (double) index + ",'" + String.valueOf(index) + "'";
        dataFormat.setCSVTextData(testData);
        index++;
        assertEquals(dataFormat.getCSVTextData().toString(), key.toString());
      }

      @Override
      public void close(TaskAttemptContext context) {
        // do nothing
      }
    }

    public static class DummyOutputCommitter extends OutputCommitter {
      @Override
      public void setupJob(JobContext jobContext) {
      }

      @Override
      public void setupTask(TaskAttemptContext taskContext) {
      }

      @Override
      public void commitTask(TaskAttemptContext taskContext) {
      }

      @Override
      public void abortTask(TaskAttemptContext taskContext) {
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext) {
        return false;
      }
    }
  }

  // it is writing to the target.
  public static class DummyLoader extends Loader<EmptyConfiguration, EmptyConfiguration> {
    private int index = START_PARTITION * NUMBER_OF_ROWS_PER_PARTITION;
    private IntermediateDataFormat<?> dataFormat = MRJobTestUtil.getTestIDF();
    private long rowsWritten = 0;

    @Override
    public void load(LoaderContext context, EmptyConfiguration oc, EmptyConfiguration oj)
        throws Exception {
      String data;
      while ((data = context.getDataReader().readTextRecord()) != null) {
        String testData = "" + index + "," +  (double) index + ",'" + String.valueOf(index) + "'";
        dataFormat.setCSVTextData(testData);
        index++;
        rowsWritten ++;
        assertEquals(dataFormat.getCSVTextData().toString(), data);
      }
    }

    /* (non-Javadoc)
     * @see org.apache.sqoop.job.etl.Loader#getRowsWritten()
     */
    @Override
    public long getRowsWritten() {
      return rowsWritten;
    }
  }

  public static class DummyFromDestroyer extends Destroyer<EmptyConfiguration, EmptyConfiguration> {
    public static int count = 0;
    @Override
    public void destroy(DestroyerContext context, EmptyConfiguration o, EmptyConfiguration o2) {
      count++;
    }
  }

  public static class DummyToDestroyer extends Destroyer<EmptyConfiguration, EmptyConfiguration> {
    public static int count = 0;
    @Override
    public void destroy(DestroyerContext context, EmptyConfiguration o, EmptyConfiguration o2) {
      count++;
    }
  }
}
