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
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.sqoop.common.PrefixContext;
import org.apache.sqoop.connector.hdfs.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.testng.annotations.AfterMethod;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class TestExtractor extends TestHdfsBase {
  private static final String INPUT_ROOT = System.getProperty("maven.build.directory", "/tmp") + "/sqoop/warehouse/";
  private static final int NUMBER_OF_FILES = 5;
  private static final int NUMBER_OF_ROWS_PER_FILE = 1000;

  private ToFormat outputFileType;
  private Class<? extends CompressionCodec> compressionClass;
  private final String inputDirectory;
  private Extractor<LinkConfiguration, FromJobConfiguration, HdfsPartition> extractor;

  @Factory(dataProvider="test-hdfs-extractor")
  public TestExtractor(ToFormat outputFileType,
                       Class<? extends CompressionCodec> compressionClass)
      throws Exception {
    this.inputDirectory = INPUT_ROOT + getClass().getSimpleName();
    this.outputFileType = outputFileType;
    this.compressionClass = compressionClass;
    this.extractor = new HdfsExtractor();
  }

  @DataProvider(name="test-hdfs-extractor")
  public static Object[][] data() {
    List<Object[]> parameters = new ArrayList<Object[]>();
    for (Class<?> compressionClass : new Class<?>[]{null, DefaultCodec.class, BZip2Codec.class}) {
      for (Object outputFileType : new Object[]{TEXT_FILE, SEQUENCE_FILE}) {
        parameters.add(new Object[]{outputFileType, compressionClass});
      }
    }
    return parameters.toArray(new Object[0][]);
  }

  @BeforeMethod
  public void setUp() throws Exception {
    FileUtils.mkdirs(inputDirectory);
    switch (this.outputFileType) {
      case TEXT_FILE:
        createTextInput(inputDirectory, this.compressionClass, NUMBER_OF_FILES, NUMBER_OF_ROWS_PER_FILE, "%d,%f,NULL,%s,\\N");
        break;

      case SEQUENCE_FILE:
        createSequenceInput(inputDirectory, this.compressionClass, NUMBER_OF_FILES, NUMBER_OF_ROWS_PER_FILE, "%d,%f,NULL,%s,\\N");
        break;
    }
  }

  @AfterMethod
  public void tearDown() throws IOException {
    FileUtils.delete(inputDirectory);
  }

  @Test
  public void testExtractor() throws Exception {
    Configuration conf = new Configuration();
    PrefixContext prefixContext = new PrefixContext(conf, "org.apache.sqoop.job.connector.from.context.");
    final boolean[] visited = new boolean[NUMBER_OF_FILES * NUMBER_OF_ROWS_PER_FILE];
    ExtractorContext context = new ExtractorContext(prefixContext, new DataWriter() {
      @Override
      public void writeArrayRecord(Object[] array) {
        throw new AssertionError("Should not be writing array.");
      }

      @Override
      public void writeStringRecord(String text) {
        int index;
        String[] components = text.split(",");
        Assert.assertEquals(5, components.length);

        // Value should take on the form <integer>,<float>,NULL,'<integer>'
        // for a single index. IE: 1,1.0,NULL,'1'.
        try {
          index = Integer.parseInt(components[0]);
        } catch (NumberFormatException e) {
          throw new AssertionError("Could not parse int for " + components[0]);
        }

        Assert.assertFalse(visited[index - 1]);
        Assert.assertEquals(String.valueOf((double) index), components[1]);
        Assert.assertEquals("NULL", components[2]);
        Assert.assertEquals("'" + index + "'", components[3]);
        Assert.assertEquals("\\N", components[4]);

        visited[index - 1] = true;
      }

      @Override
      public void writeRecord(Object obj) {
        throw new AssertionError("Should not be writing object.");
      }
    }, null);

    LinkConfiguration emptyLinkConfig = new LinkConfiguration();
    FromJobConfiguration emptyJobConfig = new FromJobConfiguration();
    HdfsPartition partition = createPartition(FileUtils.listDir(inputDirectory));

    extractor.extract(context, emptyLinkConfig, emptyJobConfig, partition);

    for (int index = 0; index < NUMBER_OF_FILES * NUMBER_OF_ROWS_PER_FILE; ++index) {
      assertTrue("Index " + (index + 1) + " was not visited", visited[index]);
    }
  }

  @Test
  public void testOverrideNull() throws Exception {
    Configuration conf = new Configuration();
    PrefixContext prefixContext = new PrefixContext(conf, "org.apache.sqoop.job.connector.from.context.");
    final boolean[] visited = new boolean[NUMBER_OF_FILES * NUMBER_OF_ROWS_PER_FILE];
    ExtractorContext context = new ExtractorContext(prefixContext, new DataWriter() {
      @Override
      public void writeArrayRecord(Object[] array) {
        int index;
        Assert.assertEquals(5, array.length);

        // Value should take on the form <integer>,<float>,NULL,'<integer>'
        // for a single index. IE: 1,1.0,NULL,'1'.
        try {
          index = Integer.parseInt(array[0].toString());
        } catch (NumberFormatException e) {
          throw new AssertionError("Could not parse int for " + array[0]);
        }

        Assert.assertFalse(visited[index - 1]);
        Assert.assertEquals(String.valueOf((double) index), array[1]);
        Assert.assertEquals("NULL", array[2]);
        Assert.assertEquals("'" + index + "'", array[3]);
        Assert.assertNull(array[4]);

        visited[index - 1] = true;
      }

      @Override
      public void writeStringRecord(String text) {
        throw new AssertionError("Should not be writing string.");
      }

      @Override
      public void writeRecord(Object obj) {
        throw new AssertionError("Should not be writing object.");
      }
    }, null);

    LinkConfiguration emptyLinkConfig = new LinkConfiguration();
    FromJobConfiguration fromJobConfiguration = new FromJobConfiguration();
    fromJobConfiguration.fromJobConfig.overrideNullValue = true;
    // Should skip "NULL" values
    fromJobConfiguration.fromJobConfig.nullValue = "\\N";
    HdfsPartition partition = createPartition(FileUtils.listDir(inputDirectory));

    extractor.extract(context, emptyLinkConfig, fromJobConfiguration, partition);

    for (int index = 0; index < NUMBER_OF_FILES * NUMBER_OF_ROWS_PER_FILE; ++index) {
      assertTrue("Index " + (index + 1) + " was not visited", visited[index]);
    }
  }
}
