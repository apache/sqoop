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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.connector.common.SqoopIDFUtils;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.ToCompression;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.connector.hdfs.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.idf.AVROIntermediateDataFormat;
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.apache.sqoop.schema.type.Text;
import org.apache.sqoop.utils.ClassUtils;
import org.testng.Assert;
import org.testng.ITest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static org.apache.sqoop.connector.hdfs.configuration.ToFormat.PARQUET_FILE;
import static org.apache.sqoop.connector.hdfs.configuration.ToFormat.SEQUENCE_FILE;
import static org.apache.sqoop.connector.hdfs.configuration.ToFormat.TEXT_FILE;

public class TestLoader extends TestHdfsBase implements ITest {
  private static final String INPUT_ROOT = System.getProperty("maven.build.directory", "/tmp") + "/sqoop/warehouse/";
  private static final int NUMBER_OF_ROWS_PER_FILE = 1000;

  private ToFormat outputFormat;
  private ToCompression compression;
  private final String outputDirectory;
  private Loader loader;
  private String user = "test_user";
  private Schema schema;

  private String methodName;

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
        ToCompression.GZIP,
        ToCompression.NONE
    }) {
      for (Object outputFileType : new Object[]{TEXT_FILE, SEQUENCE_FILE, PARQUET_FILE}) {
        parameters.add(new Object[]{outputFileType, compression});
      }
    }
    return parameters.toArray(new Object[0][]);
  }

  @BeforeMethod
  public void findMethodName(Method method) {
    methodName = method.getName();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws IOException {
    FileUtils.delete(outputDirectory);
  }

  @Override
  public String getTestName() {
    return methodName + "[" + outputFormat.name() + ", " + compression + "]";
  }

  @Test
  public void testLoader() throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());
    schema = new Schema("schema").addColumn(new FixedPoint("col1", 8L, true))
        .addColumn(new FloatingPoint("col2", 4L))
        .addColumn(new Text("col3"));

    MutableMapContext mutableContext = new MutableMapContext(new HashMap<String, String>());
    mutableContext.setString(HdfsConstants.WORK_DIRECTORY, outputDirectory);
    LoaderContext context = new LoaderContext(mutableContext, new DataReader() {
      private long index = 0L;

      @Override
      public Object[] readArrayRecord() {
        assertTestUser(user);
        if (index++ < NUMBER_OF_ROWS_PER_FILE) {
          return new Object[] {index, (float)index, String.valueOf(index)};
        } else {
          return null;
        }
      }

      @Override
      public String readTextRecord() {
        assertTestUser(user);
        if (index++ < NUMBER_OF_ROWS_PER_FILE) {
          return index + "," + (double)index + ",'" + index + "'";
        } else {
          return null;
        }
      }

      @Override
      public Object readContent() {
        assertTestUser(user);
        return null;
      }
    }, schema, user);
    LinkConfiguration linkConf = new LinkConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();
    jobConf.toJobConfig.compression = compression;
    jobConf.toJobConfig.outputFormat = outputFormat;
    Path outputPath = new Path(outputDirectory);

    try {
      loader.load(context, linkConf, jobConf);
    } catch (Exception e) {
      // we may wait to fail if the compression format selected is not supported by the
      // output format
      Assert.assertTrue(compressionNotSupported());
      return;
    }

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

  private boolean compressionNotSupported() {
    switch (outputFormat) {
      case SEQUENCE_FILE:
        return compression == ToCompression.GZIP;
      case PARQUET_FILE:
        return compression == ToCompression.BZIP2 || compression == ToCompression.DEFAULT;
    }
    return false;
  }

  @Test
  public void testOverrideNull() throws Exception {
    // Parquet supports an actual "null" value so overriding null would not make
    // sense here
    if (outputFormat == PARQUET_FILE) {
      return;
    }

    FileSystem fs = FileSystem.get(new Configuration());
    schema = new Schema("schema").addColumn(new FixedPoint("col1", 8L, true))
        .addColumn(new FloatingPoint("col2", 8L))
        .addColumn(new Text("col3"))
        .addColumn(new Text("col4"));

    MutableMapContext mutableContext = new MutableMapContext(new HashMap<String, String>());
    mutableContext.setString(HdfsConstants.WORK_DIRECTORY, outputDirectory);
    LoaderContext context = new LoaderContext(mutableContext, new DataReader() {
      private long index = 0L;

      @Override
      public Object[] readArrayRecord() {
        assertTestUser(user);

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
    }, schema, "test_user");
    LinkConfiguration linkConf = new LinkConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();
    jobConf.toJobConfig.compression = compression;
    jobConf.toJobConfig.outputFormat = outputFormat;
    jobConf.toJobConfig.overrideNullValue = true;
    jobConf.toJobConfig.nullValue = "\\N";
    Path outputPath = new Path(outputDirectory);

    try {
      loader.load(context, linkConf, jobConf);
    } catch (Exception e) {
      // we may wait to fail if the compression format selected is not supported by the
      // output format
      assert(compressionNotSupported());
      return;
    }

    Assert.assertEquals(1, fs.listStatus(outputPath).length);

    for (FileStatus status : fs.listStatus(outputPath)) {
      verifyOutput(fs, status.getPath(), "%d,%f,\\N,%s");
    }

    loader.load(context, linkConf, jobConf);
    Assert.assertEquals(2, fs.listStatus(outputPath).length);
    loader.load(context, linkConf, jobConf);
    loader.load(context, linkConf, jobConf);
    loader.load(context, linkConf, jobConf);
    Assert.assertEquals(5, fs.listStatus(outputPath).length);
  }

  private void verifyOutput(FileSystem fs, Path file, String format) throws Exception {
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
          case GZIP:
            Assert.assertTrue(codec.getClass().getCanonicalName().indexOf("Gzip") != -1);
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
      case PARQUET_FILE:
        String compressionCodecClassName = ParquetFileReader.readFooter(conf, file,  ParquetMetadataConverter.NO_FILTER).getBlocks().get(0).getColumns().get(0).getCodec().getHadoopCompressionCodecClassName();

        if (compressionCodecClassName == null) {
          codec = null;
        } else {
          codec = (CompressionCodec) ClassUtils.loadClass(compressionCodecClassName).newInstance();
        }

        // Verify compression
        switch(compression) {
          case GZIP:
            Assert.assertTrue(codec.getClass().getCanonicalName().indexOf("Gzip") != -1);
            break;

          case NONE:
          default:
            Assert.assertNull(codec);
            break;
        }


        ParquetReader<GenericRecord> avroParquetReader = AvroParquetReader.builder(file).build();
        AVROIntermediateDataFormat avroIntermediateDataFormat = new AVROIntermediateDataFormat();
        avroIntermediateDataFormat.setSchema(schema);
        GenericRecord record;
        index = 1;
        while ((record = avroParquetReader.read()) != null) {
          List<Object> objects = new ArrayList<>();
          for (int i = 0; i < record.getSchema().getFields().size(); i++) {
            objects.add(record.get(i));
          }
          Assert.assertEquals(SqoopIDFUtils.toText(avroIntermediateDataFormat.toCSV(record)), formatRow(format, index++));
        }

        break;
    }
  }

  private void verifyOutput(FileSystem fs, Path file) throws Exception {
    verifyOutput(fs, file, "%d,%f,%s");
  }
}