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
package org.apache.sqoop.connector.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.sqoop.common.PrefixContext;
import org.apache.sqoop.connector.hdfs.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.OutputCompression;
import org.apache.sqoop.connector.hdfs.configuration.OutputFormat;
import org.apache.sqoop.connector.hdfs.configuration.ToJobConfiguration;
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.sqoop.connector.hdfs.configuration.OutputFormat.SEQUENCE_FILE;
import static org.apache.sqoop.connector.hdfs.configuration.OutputFormat.TEXT_FILE;

@RunWith(Parameterized.class)
public class TestLoader extends TestHdfsBase {
  private static final String INPUT_ROOT = System.getProperty("maven.build.directory", "/tmp") + "/sqoop/warehouse/";
  private static final int NUMBER_OF_ROWS_PER_FILE = 1000;

  private OutputFormat outputFormat;
  private OutputCompression compression;
  private final String outputDirectory;
  private Loader loader;

  public TestLoader(OutputFormat outputFormat,
                    OutputCompression compression)
      throws Exception {
    this.outputDirectory = INPUT_ROOT + getClass().getSimpleName();
    this.outputFormat = outputFormat;
    this.compression = compression;
    this.loader = new HdfsLoader();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> parameters = new ArrayList<Object[]>();
    for (OutputCompression compression : new OutputCompression[]{
        OutputCompression.DEFAULT,
        OutputCompression.BZIP2,
        OutputCompression.NONE
    }) {
      for (Object outputFileType : new Object[]{TEXT_FILE, SEQUENCE_FILE}) {
        parameters.add(new Object[]{outputFileType, compression});
      }
    }
    return parameters;
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws IOException {
    FileUtils.delete(outputDirectory);
  }

  @Test
  public void testLoader() throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());

    Configuration conf = new Configuration();
    PrefixContext prefixContext = new PrefixContext(conf, "org.apache.sqoop.job.connector.from.context.");
    LoaderContext context = new LoaderContext(prefixContext, new DataReader() {
      private long index = 0L;

      @Override
      public Object[] readArrayRecord() {
        return null;
      }

      @Override
      public String readTextRecord() {
        if (index++ < NUMBER_OF_ROWS_PER_FILE) {
          return index + "," + (double)index + ",'" + index + "'";
        } else {
          return null;
        }
      }

      @Override
      public Object readContent() {
        return null;
      }
    }, null);
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();
    jobConf.output.outputDirectory = outputDirectory;
    jobConf.output.compression = compression;
    jobConf.output.outputFormat = outputFormat;
    Path outputPath = new Path(outputDirectory);

    loader.load(context, connConf, jobConf);
    Assert.assertEquals(1, fs.listStatus(outputPath).length);

    for (FileStatus status : fs.listStatus(outputPath)) {
      verifyOutput(fs, status.getPath());
    }

    loader.load(context, connConf, jobConf);
    Assert.assertEquals(2, fs.listStatus(outputPath).length);
    loader.load(context, connConf, jobConf);
    loader.load(context, connConf, jobConf);
    loader.load(context, connConf, jobConf);
    Assert.assertEquals(5, fs.listStatus(outputPath).length);
  }

  private void verifyOutput(FileSystem fs, Path file) throws IOException {
    Configuration conf = new Configuration();
    FSDataInputStream fsin = fs.open(file);
    CompressionCodec codec;

    switch(outputFormat) {
      case TEXT_FILE:
        codec = (new CompressionCodecFactory(conf)).getCodec(file);

        // Verify compression
        switch(compression) {
          case BZIP2:
            Assert.assertTrue(codec.getClass().getCanonicalName().indexOf("BZip2") != -1);
            break;

          case DEFAULT:
            Assert.assertTrue(codec.getClass().getCanonicalName().indexOf("Deflate") != -1);
            break;

          case NONE:
          default:
            Assert.assertNull(codec);
            break;
        }

        InputStreamReader in;
        if (codec == null) {
          in = new InputStreamReader(fsin);
        } else {
          in = new InputStreamReader(codec.createInputStream(fsin, codec.createDecompressor()));
        }
        BufferedReader textReader = new BufferedReader(in);

        for (int i = 1; i <= NUMBER_OF_ROWS_PER_FILE; ++i) {
          Assert.assertEquals(i + "," + (double)i + ",'" + i + "'", textReader.readLine());
        }
        break;

      case SEQUENCE_FILE:
        SequenceFile.Reader sequenceReader = new SequenceFile.Reader(fs, file, conf);
        codec = sequenceReader.getCompressionCodec();

        // Verify compression
        switch(compression) {
          case BZIP2:
            Assert.assertTrue(codec.getClass().getCanonicalName().indexOf("BZip2") != -1);
            break;

          case DEFAULT:
            Assert.assertTrue(codec.getClass().getCanonicalName().indexOf("Default") != -1);
            break;

          case NONE:
          default:
            Assert.assertNull(codec);
            break;
        }

        Text line = new Text();
        int index = 1;
        while (sequenceReader.next(line)) {
          Assert.assertEquals(index + "," + (double)index + ",'" + index++ + "'", line.toString());
          line = new Text();
        }
        break;
    }
  }
}
