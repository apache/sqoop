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

import static org.apache.sqoop.connector.hdfs.configuration.ToFormat.SEQUENCE_FILE;
import static org.apache.sqoop.connector.hdfs.configuration.ToFormat.TEXT_FILE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.ToCompression;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.connector.hdfs.configuration.ToJobConfiguration;
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.apache.sqoop.schema.type.Text;
import org.testng.annotations.AfterMethod;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class TestLoader extends TestHdfsBase {
  private static final String INPUT_ROOT = System.getProperty("maven.build.directory", "/tmp") + "/sqoop/warehouse/";
  private static final int NUMBER_OF_ROWS_PER_FILE = 1000;

  private ToFormat outputFormat;
  private ToCompression compression;
  private final String outputDirectory;
  private Loader loader;

  @Factory(dataProvider="test-hdfs-loader")
  public TestLoader(ToFormat outputFormat,
                    ToCompression compression)
      throws Exception {
    this.outputDirectory = INPUT_ROOT + getClass().getSimpleName();
    this.outputFormat = outputFormat;
    this.compression = compression;
    this.loader = new HdfsLoader();
  }

  @DataProvider(name="test-hdfs-loader")
  public static Object[][] data() {
    List<Object[]> parameters = new ArrayList<Object[]>();
    for (ToCompression compression : new ToCompression[]{
        ToCompression.DEFAULT,
        ToCompression.BZIP2,
        ToCompression.NONE
    }) {
      for (Object outputFileType : new Object[]{TEXT_FILE, SEQUENCE_FILE}) {
        parameters.add(new Object[]{outputFileType, compression});
      }
    }
    return parameters.toArray(new Object[0][]);
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {}

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws IOException {
    FileUtils.delete(outputDirectory);
  }

  @Test
  public void testLoader() throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());
    Schema schema = new Schema("schema").addColumn(new FixedPoint("col1", 8L, true))
        .addColumn(new FloatingPoint("col2", 4L))
        .addColumn(new Text("col3"));

    MutableMapContext mutableContext = new MutableMapContext(new HashMap<String, String>());
    mutableContext.setString(HdfsConstants.WORK_DIRECTORY, outputDirectory);
    LoaderContext context = new LoaderContext(mutableContext, new DataReader() {
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
    LinkConfiguration linkConf = new LinkConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();
    jobConf.toJobConfig.compression = compression;
    jobConf.toJobConfig.outputFormat = outputFormat;
    Path outputPath = new Path(outputDirectory);

    loader.load(context, linkConf, jobConf);
    Assert.assertEquals(1, fs.listStatus(outputPath).length);

    for (FileStatus status : fs.listStatus(outputPath)) {
      verifyOutput(fs, status.getPath());
    }

    loader.load(context, linkConf, jobConf);
    Assert.assertEquals(2, fs.listStatus(outputPath).length);
    loader.load(context, linkConf, jobConf);
    loader.load(context, linkConf, jobConf);
    loader.load(context, linkConf, jobConf);
    Assert.assertEquals(5, fs.listStatus(outputPath).length);
  }

  @Test
  public void testOverrideNull() throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());
    Schema schema = new Schema("schema").addColumn(new FixedPoint("col1", 8L, true))
        .addColumn(new FloatingPoint("col2", 8L))
        .addColumn(new Text("col3"))
        .addColumn(new Text("col4"));

    MutableMapContext mutableContext = new MutableMapContext(new HashMap<String, String>());
    mutableContext.setString(HdfsConstants.WORK_DIRECTORY, outputDirectory);
    LoaderContext context = new LoaderContext(mutableContext, new DataReader() {
      private long index = 0L;

      @Override
      public Object[] readArrayRecord() {
        if (index++ < NUMBER_OF_ROWS_PER_FILE) {
          return new Object[]{
              index,
              (double)index,
              null,
              String.valueOf(index)
          };
        } else {
          return null;
        }
      }

      @Override
      public String readTextRecord() {
        throw new AssertionError("should not be at readTextRecord");
      }

      @Override
      public Object readContent() {
        throw new AssertionError("should not be at readContent");
      }
    }, schema);
    LinkConfiguration linkConf = new LinkConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();
    jobConf.toJobConfig.compression = compression;
    jobConf.toJobConfig.outputFormat = outputFormat;
    jobConf.toJobConfig.overrideNullValue = true;
    jobConf.toJobConfig.nullValue = "\\N";
    Path outputPath = new Path(outputDirectory);

    loader.load(context, linkConf, jobConf);
    Assert.assertEquals(1, fs.listStatus(outputPath).length);

    for (FileStatus status : fs.listStatus(outputPath)) {
      verifyOutput(fs, status.getPath(), "%d,%f,'\\\\N',%s");
    }

    loader.load(context, linkConf, jobConf);
    Assert.assertEquals(2, fs.listStatus(outputPath).length);
    loader.load(context, linkConf, jobConf);
    loader.load(context, linkConf, jobConf);
    loader.load(context, linkConf, jobConf);
    Assert.assertEquals(5, fs.listStatus(outputPath).length);
  }

  private void verifyOutput(FileSystem fs, Path file, String format) throws IOException {
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
            if(org.apache.hadoop.util.VersionInfo.getVersion().matches("\\b1\\.\\d\\.\\d")) {
              Assert.assertTrue(codec.getClass().getCanonicalName().indexOf("Default") != -1);
            } else {
              Assert.assertTrue(codec.getClass().getCanonicalName().indexOf("Deflate") != -1);
            }
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
          Assert.assertEquals(textReader.readLine(), formatRow(format, i));
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

        org.apache.hadoop.io.Text line = new org.apache.hadoop.io.Text();
        int index = 1;
        while (sequenceReader.next(line)) {
          Assert.assertEquals(line.toString(), formatRow(format, index++));
          line = new org.apache.hadoop.io.Text();
        }
        break;
    }
  }

  private void verifyOutput(FileSystem fs, Path file) throws IOException {
    verifyOutput(fs, file, "%d,%f,%s");
  }
}
