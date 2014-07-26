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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.sqoop.connector.idf.CSVIntermediateDataFormat;
import org.apache.sqoop.job.etl.HdfsExportExtractor;
import org.apache.sqoop.job.etl.HdfsExportPartitioner;
import org.apache.sqoop.job.etl.HdfsSequenceImportLoader;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.PartitionerContext;
import org.apache.sqoop.job.io.Data;
import org.apache.sqoop.job.mr.ConfigurationUtils;
import org.apache.sqoop.job.mr.SqoopFileOutputFormat;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.junit.Test;

public class TestHdfsExtract extends TestCase {

  private static final String INPUT_ROOT = System.getProperty("maven.build.directory", "/tmp") + "/sqoop/warehouse/";
  private static final int NUMBER_OF_FILES = 5;
  private static final int NUMBER_OF_ROWS_PER_FILE = 1000;

  private final String indir;

  public TestHdfsExtract() {
    indir = INPUT_ROOT + getClass().getSimpleName();
  }

  @Override
  public void setUp() throws IOException {
    FileUtils.mkdirs(indir);
  }

  @Override
  public void tearDown() throws IOException {
    FileUtils.delete(indir);
  }

  /**
   * Test case for validating the number of partitions creation
   * based on input.
   * Success if the partitions list size is less or equal to
   * given max partition.
   * @throws Exception
   */
  @Test
  public void testHdfsExportPartitioner() throws Exception {
    createTextInput(null);
    Configuration conf = new Configuration();
    conf.set(JobConstants.HADOOP_INPUTDIR, indir);

    conf.set(JobConstants.INTERMEDIATE_DATA_FORMAT,
      CSVIntermediateDataFormat.class.getName());
    HdfsExportPartitioner partitioner = new HdfsExportPartitioner();
    PrefixContext prefixContext = new PrefixContext(conf, "");
    int[] partitionValues = {2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 17};

    for(int maxPartitions : partitionValues) {
      PartitionerContext partCont = new PartitionerContext(prefixContext, maxPartitions, null);
      List<Partition> partitionList = partitioner.getPartitions(partCont, null, null);
      assertTrue(partitionList.size()<=maxPartitions);
    }
  }

  @Test
  public void testUncompressedText() throws Exception {
    createTextInput(null);

    JobUtils.runJob(createJob(createConf(), createSchema()).getConfiguration());
  }

  @Test
  public void testDefaultCompressedText() throws Exception {
    createTextInput(SqoopFileOutputFormat.DEFAULT_CODEC);

    JobUtils.runJob(createJob(createConf(), createSchema()).getConfiguration());
  }

  @Test
  public void testBZip2CompressedText() throws Exception {
    createTextInput(BZip2Codec.class);

    JobUtils.runJob(createJob(createConf(), createSchema()).getConfiguration());
  }

  @Test
  public void testDefaultCompressedSequence() throws Exception {
    createSequenceInput(SqoopFileOutputFormat.DEFAULT_CODEC);

    JobUtils.runJob(createJob(createConf(), createSchema()).getConfiguration());
  }

  @Test
  public void testUncompressedSequence() throws Exception {
    createSequenceInput(null);

    JobUtils.runJob(createJob(createConf(), createSchema()).getConfiguration());
  }

  private Schema createSchema() {
    Schema schema = new Schema("Test");
    schema.addColumn(new FixedPoint("1")).addColumn(new FloatingPoint("2"))
    .addColumn(new org.apache.sqoop.schema.type.Text("3"));
    return schema;
  }

  private Configuration createConf() {
    Configuration conf = new Configuration();
    ConfigurationUtils.setJobType(conf, MJob.Type.EXPORT);
    conf.setIfUnset(JobConstants.JOB_ETL_PARTITIONER,
        HdfsExportPartitioner.class.getName());
    conf.setIfUnset(JobConstants.JOB_ETL_EXTRACTOR,
        HdfsExportExtractor.class.getName());
    conf.setIfUnset(JobConstants.JOB_ETL_LOADER, DummyLoader.class.getName());
    conf.setIfUnset(Constants.JOB_ETL_NUMBER_PARTITIONS, "4");
    conf.setIfUnset(JobConstants.INTERMEDIATE_DATA_FORMAT,
        CSVIntermediateDataFormat.class.getName());
    conf.setIfUnset(JobConstants.HADOOP_INPUTDIR, indir);
    return conf;
  }

  private Job createJob(Configuration conf, Schema schema) throws Exception {
    Job job = new Job(conf);
    ConfigurationUtils.setConnectorSchema(job, schema);
    job.getConfiguration().set(JobConstants.INTERMEDIATE_DATA_FORMAT,
        CSVIntermediateDataFormat.class.getName());
    return job;
  }

  private void createTextInput(Class<? extends CompressionCodec> clz)
      throws IOException, InstantiationException, IllegalAccessException {
    Configuration conf = new Configuration();

    CompressionCodec codec = null;
    String extension = "";
    if (clz != null) {
      codec = clz.newInstance();
      if (codec instanceof Configurable) {
        ((Configurable) codec).setConf(conf);
      }
      extension = codec.getDefaultExtension();
    }

    int index = 1;
    for (int fi=0; fi<NUMBER_OF_FILES; fi++) {
      String fileName = indir + "/" + "part-r-" + padZeros(fi, 5) + extension;
      OutputStream filestream = FileUtils.create(fileName);
      BufferedWriter filewriter;
      if (codec != null) {
        filewriter = new BufferedWriter(new OutputStreamWriter(
            codec.createOutputStream(filestream, codec.createCompressor()),
            Data.CHARSET_NAME));
      } else {
        filewriter = new BufferedWriter(new OutputStreamWriter(
            filestream, Data.CHARSET_NAME));
      }

      for (int ri=0; ri<NUMBER_OF_ROWS_PER_FILE; ri++) {
        String row = index + "," + (double)index + ",'" + index + "'";
        filewriter.write(row + Data.DEFAULT_RECORD_DELIMITER);
        index++;
      }

      filewriter.close();
    }
  }

  private void createSequenceInput(Class<? extends CompressionCodec> clz)
      throws IOException, InstantiationException, IllegalAccessException {
    Configuration conf = new Configuration();

    CompressionCodec codec = null;
    if (clz != null) {
      codec = clz.newInstance();
      if (codec instanceof Configurable) {
        ((Configurable) codec).setConf(conf);
      }
    }

    int index = 1;
    for (int fi=0; fi<NUMBER_OF_FILES; fi++) {
      Path filepath = new Path(indir,
          "part-r-" + padZeros(fi, 5) + HdfsSequenceImportLoader.EXTENSION);
      SequenceFile.Writer filewriter;
      if (codec != null) {
        filewriter = SequenceFile.createWriter(filepath.getFileSystem(conf),
            conf, filepath, Text.class, NullWritable.class,
            CompressionType.BLOCK, codec);
      } else {
        filewriter = SequenceFile.createWriter(filepath.getFileSystem(conf),
            conf, filepath, Text.class, NullWritable.class, CompressionType.NONE);
      }

      Text text = new Text();
      for (int ri=0; ri<NUMBER_OF_ROWS_PER_FILE; ri++) {
        String row = index + "," + (double)index + ",'" + index + "'";
        text.set(row);
        filewriter.append(text, NullWritable.get());
        index++;
      }

      filewriter.close();
    }
  }

  private String padZeros(int number, int digits) {
    String string = String.valueOf(number);
    for (int i=(digits-string.length()); i>0; i--) {
      string = "0" + string;
    }
    return string;
  }

  public static class DummyLoader extends Loader {
    @Override
    public void load(LoaderContext context, Object oc, Object oj) throws Exception {
      int index = 1;
      int sum = 0;
      Object[] array;
      while ((array = context.getDataReader().readArrayRecord()) != null) {
        sum += Integer.valueOf(array[0].toString());
        index++;
      };

      int numbers = NUMBER_OF_FILES*NUMBER_OF_ROWS_PER_FILE;
      assertEquals((1+numbers)*numbers/2, sum);

      assertEquals(NUMBER_OF_FILES*NUMBER_OF_ROWS_PER_FILE, index-1);
    }
  }

}
