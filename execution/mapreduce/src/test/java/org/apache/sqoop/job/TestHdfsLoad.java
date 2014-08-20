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

import junit.framework.TestCase;

//import org.apache.sqoop.connector.idf.CSVIntermediateDataFormat;
//import org.apache.sqoop.job.etl.HdfsSequenceImportLoader;
//import org.apache.sqoop.job.etl.HdfsTextImportLoader;

public class TestHdfsLoad extends TestCase {

//  private static final String OUTPUT_ROOT = System.getProperty("maven.build.directory", "/tmp") + "/sqoop/warehouse/";
//  private static final String OUTPUT_FILE = "part-r-00000";
//  private static final int START_ID = 1;
//  private static final int NUMBER_OF_IDS = 9;
//  private static final int NUMBER_OF_ROWS_PER_ID = 10;
//
//  private String outdir;
//
//  public TestHdfsLoad() {
//    outdir = OUTPUT_ROOT + "/" + getClass().getSimpleName();
//  }
//
//  public void testUncompressedText() throws Exception {
//    FileUtils.delete(outdir);
//
//    Configuration conf = new Configuration();
//    ConfigurationUtils.setJobType(conf, MJob.Type.IMPORT);
//    conf.set(JobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
//    conf.set(JobConstants.JOB_ETL_EXTRACTOR, DummyExtractor.class.getName());
//    conf.set(JobConstants.JOB_ETL_LOADER, HdfsTextImportLoader.class.getName());
//    conf.set(JobConstants.INTERMEDIATE_DATA_FORMAT,
//      CSVIntermediateDataFormat.class.getName());
//    conf.set(JobConstants.HADOOP_OUTDIR, outdir);
//    Schema schema = new Schema("Test");
//    schema.addColumn(new FixedPoint("1")).addColumn(new FloatingPoint("2"))
//      .addColumn(new org.apache.sqoop.schema.type.Text("3"));
//
//    Job job = new Job(conf);
//    ConfigurationUtils.setConnectorSchema(job, schema);
//    JobUtils.runJob(job.getConfiguration());
//
//    String fileName = outdir + "/" +  OUTPUT_FILE;
//    InputStream filestream = FileUtils.open(fileName);
//    BufferedReader filereader = new BufferedReader(new InputStreamReader(
//        filestream, Charsets.UTF_8));
//    verifyOutputText(filereader);
//  }
//
//  public void testCompressedText() throws Exception {
//    FileUtils.delete(outdir);
//
//    Configuration conf = new Configuration();
//    ConfigurationUtils.setJobType(conf, MJob.Type.IMPORT);
//    conf.set(JobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
//    conf.set(JobConstants.JOB_ETL_EXTRACTOR, DummyExtractor.class.getName());
//    conf.set(JobConstants.JOB_ETL_LOADER, HdfsTextImportLoader.class.getName());
//    conf.set(JobConstants.INTERMEDIATE_DATA_FORMAT,
//      CSVIntermediateDataFormat.class.getName());
//    conf.set(JobConstants.HADOOP_OUTDIR, outdir);
//    conf.setBoolean(JobConstants.HADOOP_COMPRESS, true);
//
//    Schema schema = new Schema("Test");
//    schema.addColumn(new FixedPoint("1")).addColumn(new FloatingPoint("2"))
//      .addColumn(new org.apache.sqoop.schema.type.Text("3"));
//
//    Job job = new Job(conf);
//    ConfigurationUtils.setConnectorSchema(job, schema);
//    JobUtils.runJob(job.getConfiguration());
//
//    Class<? extends CompressionCodec> codecClass = conf.getClass(
//        JobConstants.HADOOP_COMPRESS_CODEC, SqoopFileOutputFormat.DEFAULT_CODEC)
//        .asSubclass(CompressionCodec.class);
//    CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
//    String fileName = outdir + "/" +  OUTPUT_FILE + codec.getDefaultExtension();
//    InputStream filestream = codec.createInputStream(FileUtils.open(fileName));
//    BufferedReader filereader = new BufferedReader(new InputStreamReader(
//        filestream, Charsets.UTF_8));
//    verifyOutputText(filereader);
//  }
//
//  private void verifyOutputText(BufferedReader reader) throws IOException {
//    String actual = null;
//    String expected;
//    Data data = new Data();
//    int index = START_ID*NUMBER_OF_ROWS_PER_ID;
//    while ((actual = reader.readLine()) != null){
//      data.setContent(new Object[] {
//        index, (double) index, new String(new byte[] {(byte)(index + 127)}, Charsets.ISO_8859_1) },
//          Data.ARRAY_RECORD);
//      expected = data.toString();
//      index++;
//
//      assertEquals(expected, actual);
//    }
//    reader.close();
//
//    assertEquals(NUMBER_OF_IDS*NUMBER_OF_ROWS_PER_ID,
//        index-START_ID*NUMBER_OF_ROWS_PER_ID);
//  }
//
//  public void testUncompressedSequence() throws Exception {
//    FileUtils.delete(outdir);
//
//    Configuration conf = new Configuration();
//    ConfigurationUtils.setJobType(conf, MJob.Type.IMPORT);
//    conf.set(JobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
//    conf.set(JobConstants.JOB_ETL_EXTRACTOR, DummyExtractor.class.getName());
//    conf.set(JobConstants.JOB_ETL_LOADER, HdfsSequenceImportLoader.class.getName());
//    conf.set(JobConstants.INTERMEDIATE_DATA_FORMAT,
//      CSVIntermediateDataFormat.class.getName());
//    conf.set(JobConstants.HADOOP_OUTDIR, outdir);
//
//    Schema schema = new Schema("Test");
//    schema.addColumn(new FixedPoint("1")).addColumn(new FloatingPoint("2"))
//      .addColumn(new org.apache.sqoop.schema.type.Text("3"));
//
//    Job job = new Job(conf);
//    ConfigurationUtils.setConnectorSchema(job, schema);
//    JobUtils.runJob(job.getConfiguration());
//
//    Path filepath = new Path(outdir,
//        OUTPUT_FILE + HdfsSequenceImportLoader.EXTENSION);
//    SequenceFile.Reader filereader = new SequenceFile.Reader(
//      filepath.getFileSystem(conf), filepath, conf);
//    verifyOutputSequence(filereader);
//  }
//
//  public void testCompressedSequence() throws Exception {
//    FileUtils.delete(outdir);
//
//    Configuration conf = new Configuration();
//    ConfigurationUtils.setJobType(conf, MJob.Type.IMPORT);
//    conf.set(JobConstants.JOB_ETL_PARTITIONER, DummyPartitioner.class.getName());
//    conf.set(JobConstants.JOB_ETL_EXTRACTOR, DummyExtractor.class.getName());
//    conf.set(JobConstants.JOB_ETL_LOADER, HdfsSequenceImportLoader.class.getName());
//    conf.set(JobConstants.INTERMEDIATE_DATA_FORMAT,
//      CSVIntermediateDataFormat.class.getName());
//    conf.set(JobConstants.HADOOP_OUTDIR, outdir);
//    conf.setBoolean(JobConstants.HADOOP_COMPRESS, true);
//
//    Schema schema = new Schema("Test");
//    schema.addColumn(new FixedPoint("1")).addColumn(new FloatingPoint("2"))
//      .addColumn(new org.apache.sqoop.schema.type.Text("3"));
//
//    Job job = new Job(conf);
//    ConfigurationUtils.setConnectorSchema(job, schema);
//    JobUtils.runJob(job.getConfiguration());
//    Path filepath = new Path(outdir,
//        OUTPUT_FILE + HdfsSequenceImportLoader.EXTENSION);
//    SequenceFile.Reader filereader = new SequenceFile.Reader(filepath.getFileSystem(conf), filepath, conf);
//    verifyOutputSequence(filereader);
//  }
//
//  private void verifyOutputSequence(SequenceFile.Reader reader) throws IOException {
//    int index = START_ID*NUMBER_OF_ROWS_PER_ID;
//    Text actual = new Text();
//    Text expected = new Text();
//    Data data = new Data();
//    while (reader.next(actual)){
//      data.setContent(new Object[] {
//          index, (double) index, new String(new byte[] {(byte)(index + 127)}, Charsets.ISO_8859_1) },
//          Data.ARRAY_RECORD);
//      expected.set(data.toString());
//      index++;
//
//      assertEquals(expected.toString(), actual.toString());
//    }
//    reader.close();
//
//    assertEquals(NUMBER_OF_IDS*NUMBER_OF_ROWS_PER_ID,
//        index-START_ID*NUMBER_OF_ROWS_PER_ID);
//  }
//
//  public static class DummyPartition extends Partition {
//    private int id;
//
//    public void setId(int id) {
//      this.id = id;
//    }
//
//    public int getId() {
//      return id;
//    }
//
//    @Override
//    public void readFields(DataInput in) throws IOException {
//      id = in.readInt();
//    }
//
//    @Override
//    public void write(DataOutput out) throws IOException {
//      out.writeInt(id);
//    }
//
//    @Override
//    public String toString() {
//      return Integer.toString(id);
//    }
//  }
//
//  public static class DummyPartitioner extends Partitioner {
//    @Override
//    public List<Partition> getPartitions(PartitionerContext context, Object oc, Object oj) {
//      List<Partition> partitions = new LinkedList<Partition>();
//      for (int id = START_ID; id <= NUMBER_OF_IDS; id++) {
//        DummyPartition partition = new DummyPartition();
//        partition.setId(id);
//        partitions.add(partition);
//      }
//      return partitions;
//    }
//  }
//
//  public static class DummyExtractor extends Extractor {
//    @Override
//    public void extract(ExtractorContext context, Object oc, Object oj, Object partition) {
//      int id = ((DummyPartition)partition).getId();
//      for (int row = 0; row < NUMBER_OF_ROWS_PER_ID; row++) {
//        Object[] array = new Object[] {
//          id * NUMBER_OF_ROWS_PER_ID + row,
//          (double) (id * NUMBER_OF_ROWS_PER_ID + row),
//          new String(new byte[]{(byte)(id * NUMBER_OF_ROWS_PER_ID + row + 127)}, Charsets.ISO_8859_1)
//        };
//        context.getDataWriter().writeArrayRecord(array);
//      }
//    }
//
//    @Override
//    public long getRowsRead() {
//      return NUMBER_OF_ROWS_PER_ID;
//    }
//  }
}
