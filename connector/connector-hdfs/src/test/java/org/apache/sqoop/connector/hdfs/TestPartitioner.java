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
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.sqoop.common.PrefixContext;
import org.apache.sqoop.connector.hdfs.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.OutputFormat;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.sqoop.connector.hdfs.configuration.OutputFormat.*;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestPartitioner extends TestHdfsBase {
  private static final String INPUT_ROOT = System.getProperty("maven.build.directory", "/tmp") + "/sqoop/warehouse/";
  private static final int NUMBER_OF_FILES = 5;
  private static final int NUMBER_OF_ROWS_PER_FILE = 1000;

  private OutputFormat outputFileType;
  private Class<? extends CompressionCodec> compressionClass;
  private Partitioner partitioner;

  private final String inputDirectory;

  public TestPartitioner(OutputFormat outputFileType, Class<? extends CompressionCodec> compressionClass) {
    this.inputDirectory = INPUT_ROOT + getClass().getSimpleName();
    this.outputFileType = outputFileType;
    this.compressionClass = compressionClass;
  }

  @Before
  public void setUp() throws Exception {
    partitioner = new HdfsPartitioner();
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

  @Test
  public void testPartitioner() {
    Configuration conf = new Configuration();
    PrefixContext prefixContext = new PrefixContext(conf, "org.apache.sqoop.job.connector.from.context.");
    PartitionerContext context = new PartitionerContext(prefixContext, 5, null);
    ConnectionConfiguration connConf = new ConnectionConfiguration();
    FromJobConfiguration jobConf = new FromJobConfiguration();

    jobConf.input.inputDirectory = inputDirectory;

    List<Partition> partitions = partitioner.getPartitions(context, connConf, jobConf);

    if (this.compressionClass == null) {
      assertEquals(5, partitions.size());
    } else {
      assertEquals(3, partitions.size());
    }
  }
}
