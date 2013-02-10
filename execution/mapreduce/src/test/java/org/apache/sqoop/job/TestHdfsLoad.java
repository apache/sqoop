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

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.job.etl.HdfsSequenceImportLoader;
import org.apache.sqoop.job.etl.HdfsTextImportLoader;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;
import org.apache.sqoop.job.io.Data;
import org.apache.sqoop.job.mr.SqoopFileOutputFormat;

public class TestHdfsLoad extends TestCase {

  private static final String OUTPUT_ROOT = "/tmp/sqoop/warehouse/";
  private static final String OUTPUT_FILE = "part-r-00000";
  private static final int START_ID = 1;
  private static final int NUMBER_OF_IDS = 9;
  private static final int NUMBER_OF_ROWS_PER_ID = 10;

  private String outdir;

  public TestHdfsLoad() {
    outdir = OUTPUT_ROOT + "/" + getClass().getSimpleName();
  }

  public void testUncompressedText() throws Exception {
    FileUtils.delete(outdir);

    Configuration conf = new Configuration();
    conf.set(JobConstants.JOB_TYPE, "IMPORT");
    conf.set(JobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
    conf.set(JobConstants.JOB_ETL_EXTRACTOR, DummyExtractor.class.getName());
    conf.set(JobConstants.JOB_ETL_LOADER, HdfsTextImportLoader.class.getName());
    conf.set(JobConstants.HADOOP_OUTDIR, outdir);
    JobUtils.runJob(conf);

    String fileName = outdir + "/" +  OUTPUT_FILE;
    InputStream filestream = FileUtils.open(fileName);
    BufferedReader filereader = new BufferedReader(new InputStreamReader(
        filestream, Data.CHARSET_NAME));
    verifyOutputText(filereader);
  }

  public void testCompressedText() throws Exception {
    FileUtils.delete(outdir);

    Configuration conf = new Configuration();
    conf.set(JobConstants.JOB_TYPE, "IMPORT");
    conf.set(JobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
    conf.set(JobConstants.JOB_ETL_EXTRACTOR, DummyExtractor.class.getName());
    conf.set(JobConstants.JOB_ETL_LOADER, HdfsTextImportLoader.class.getName());
    conf.set(JobConstants.HADOOP_OUTDIR, outdir);
    conf.setBoolean(JobConstants.HADOOP_COMPRESS, true);
    JobUtils.runJob(conf);

    Class<? extends CompressionCodec> codecClass = conf.getClass(
        JobConstants.HADOOP_COMPRESS_CODEC, SqoopFileOutputFormat.DEFAULT_CODEC)
        .asSubclass(CompressionCodec.class);
    CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
    String fileName = outdir + "/" +  OUTPUT_FILE + codec.getDefaultExtension();
    InputStream filestream = codec.createInputStream(FileUtils.open(fileName));
    BufferedReader filereader = new BufferedReader(new InputStreamReader(
        filestream, Data.CHARSET_NAME));
    verifyOutputText(filereader);
  }

  private void verifyOutputText(BufferedReader reader) throws IOException {
    String actual = null;
    String expected;
    Data data = new Data();
    int index = START_ID*NUMBER_OF_ROWS_PER_ID;
    while ((actual = reader.readLine()) != null){
      data.setContent(new Object[] {
        index, (double) index, String.valueOf(index) },
          Data.ARRAY_RECORD);
      expected = data.toString();
      index++;

      assertEquals(expected, actual);
    }
    reader.close();

    assertEquals(NUMBER_OF_IDS*NUMBER_OF_ROWS_PER_ID,
        index-START_ID*NUMBER_OF_ROWS_PER_ID);
  }

  public void testUncompressedSequence() throws Exception {
    FileUtils.delete(outdir);

    Configuration conf = new Configuration();
    conf.set(JobConstants.JOB_TYPE, "IMPORT");
    conf.set(JobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
    conf.set(JobConstants.JOB_ETL_EXTRACTOR, DummyExtractor.class.getName());
    conf.set(JobConstants.JOB_ETL_LOADER, HdfsSequenceImportLoader.class.getName());
    conf.set(JobConstants.HADOOP_OUTDIR, outdir);
    JobUtils.runJob(conf);

    Path filepath = new Path(outdir,
        OUTPUT_FILE + HdfsSequenceImportLoader.EXTENSION);
    SequenceFile.Reader filereader = new SequenceFile.Reader(
      filepath.getFileSystem(conf), filepath, conf);
    verifyOutputSequence(filereader);
  }

  public void testCompressedSequence() throws Exception {
    FileUtils.delete(outdir);

    Configuration conf = new Configuration();
    conf.set(JobConstants.JOB_TYPE, "IMPORT");
    conf.set(JobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
    conf.set(JobConstants.JOB_ETL_EXTRACTOR, DummyExtractor.class.getName());
    conf.set(JobConstants.JOB_ETL_LOADER, HdfsSequenceImportLoader.class.getName());
    conf.set(JobConstants.HADOOP_OUTDIR, outdir);
    conf.setBoolean(JobConstants.HADOOP_COMPRESS, true);
    JobUtils.runJob(conf);

    Path filepath = new Path(outdir,
        OUTPUT_FILE + HdfsSequenceImportLoader.EXTENSION);
    SequenceFile.Reader filereader = new SequenceFile.Reader(filepath.getFileSystem(conf), filepath, conf);
    verifyOutputSequence(filereader);
  }

  private void verifyOutputSequence(SequenceFile.Reader reader) throws IOException {
    int index = START_ID*NUMBER_OF_ROWS_PER_ID;
    Text actual = new Text();
    Text expected = new Text();
    Data data = new Data();
    while (reader.next(actual)){
      data.setContent(new Object[] {
          index, (double) index, String.valueOf(index) },
          Data.ARRAY_RECORD);
      expected.set(data.toString());
      index++;

      assertEquals(expected.toString(), actual.toString());
    }
    reader.close();

    assertEquals(NUMBER_OF_IDS*NUMBER_OF_ROWS_PER_ID,
        index-START_ID*NUMBER_OF_ROWS_PER_ID);
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
      for (int id = START_ID; id <= NUMBER_OF_IDS; id++) {
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
      for (int row = 0; row < NUMBER_OF_ROWS_PER_ID; row++) {
        Object[] array = new Object[] {
          id * NUMBER_OF_ROWS_PER_ID + row,
          (double) (id * NUMBER_OF_ROWS_PER_ID + row),
          String.valueOf(id*NUMBER_OF_ROWS_PER_ID+row)
        };
        context.getDataWriter().writeArrayRecord(array);
      }
    }

    @Override
    public long getRowsRead() {
      return NUMBER_OF_ROWS_PER_ID;
    }
  }
}
