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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestExtractor extends TestHdfsBase {
  private static final String INPUT_ROOT = System.getProperty("maven.build.directory", "/tmp") + "/sqoop/warehouse/";
  private static final int NUMBER_OF_FILES = 5;
  private static final int NUMBER_OF_ROWS_PER_FILE = 1000;

  private ToFormat outputFileType;
  private Class<? extends CompressionCodec> compressionClass;
  private final String inputDirectory;
  private Extractor<LinkConfiguration, FromJobConfiguration, HdfsPartition> extractor;

  public TestExtractor(ToFormat outputFileType,
                       Class<? extends CompressionCodec> compressionClass)
      throws Exception {
    this.inputDirectory = INPUT_ROOT + getClass().getSimpleName();
    this.outputFileType = outputFileType;
    this.compressionClass = compressionClass;
    this.extractor = new HdfsExtractor();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> parameters = new ArrayList<Object[]>();
    for (Class<?> compressionClass : new Class<?>[]{null, DefaultCodec.class, BZip2Codec.class}) {
      for (Object outputFileType : new Object[]{TEXT_FILE, SEQUENCE_FILE}) {
        parameters.add(new Object[]{outputFileType, compressionClass});
      }
    }
    return parameters;
  }

  @Before
  public void setUp() throws Exception {
    FileUtils.mkdirs(inputDirectory);
    switch (this.outputFileType) {
      case TEXT_FILE:
        createTextInput(inputDirectory, this.compressionClass, NUMBER_OF_FILES, NUMBER_OF_ROWS_PER_FILE);
        break;

      case SEQUENCE_FILE:
        createSequenceInput(inputDirectory, this.compressionClass, NUMBER_OF_FILES, NUMBER_OF_ROWS_PER_FILE);
        break;
    }
  }

  @After
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
        Assert.assertEquals(3, components.length);

        // Value should take on the form <integer>,<float>,'<integer>'
        // for a single index. IE: 1,1.0,'1'.
        try {
          index = Integer.parseInt(components[0]);
        } catch (NumberFormatException e) {
          throw new AssertionError("Could not parse int for " + components[0]);
        }

        Assert.assertFalse(visited[index - 1]);
        Assert.assertEquals(String.valueOf((double)index), components[1]);
        Assert.assertEquals("'" + index + "'", components[2]);

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
      Assert.assertTrue("Index " + (index + 1) + " was not visited", visited[index]);
    }
  }
}