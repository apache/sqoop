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
package org.apache.sqoop.execution.mapreduce;

import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.framework.SubmissionRequest;
import org.apache.sqoop.framework.configuration.ImportJobConfiguration;
import org.apache.sqoop.framework.configuration.OutputCompression;
import org.apache.sqoop.framework.configuration.OutputFormat;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.Importer;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.Partitioner;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class MapreduceExecutionEngineTest {
  @Test
  public void testImportCompression() throws Exception {
    testImportCompressionInner(OutputCompression.NONE,
      null, false);

    testImportCompressionInner(OutputCompression.DEFAULT,
      "org.apache.hadoop.io.compress.DefaultCodec", true);

    testImportCompressionInner(OutputCompression.GZIP,
      "org.apache.hadoop.io.compress.GzipCodec", true);

    testImportCompressionInner(OutputCompression.BZIP2,
      "org.apache.hadoop.io.compress.BZip2Codec", true);

    testImportCompressionInner(OutputCompression.LZO,
      "com.hadoop.compression.lzo.LzoCodec", true);

    testImportCompressionInner(OutputCompression.LZ4,
      "org.apache.hadoop.io.compress.Lz4Codec", true);

    testImportCompressionInner(OutputCompression.SNAPPY,
      "org.apache.hadoop.io.compress.SnappyCodec", true);

    testImportCompressionInner(null,
      null, false);
  }

  private void testImportCompressionInner(OutputCompression comprssionFormat,
    String expectedCodecName, boolean expectedCompressionFlag) {
    MapreduceExecutionEngine executionEngine = new MapreduceExecutionEngine();
    SubmissionRequest request = executionEngine.createSubmissionRequest();
    ImportJobConfiguration jobConf = new ImportJobConfiguration();
    jobConf.output.outputFormat = OutputFormat.TEXT_FILE;
    jobConf.output.compression = comprssionFormat;
    request.setConfigFrameworkJob(jobConf);
    request.setConnectorCallbacks(new Importer(Initializer.class,
      Partitioner.class, Extractor.class, Destroyer.class) {
    });
    executionEngine.prepareImportSubmission(request);

    MutableMapContext context = request.getFrameworkContext();
    final String obtainedCodecName = context.getString(
      JobConstants.HADOOP_COMPRESS_CODEC);
    final boolean obtainedCodecFlag =
      context.getBoolean(JobConstants.HADOOP_COMPRESS, false);
    assertEquals("Unexpected codec name was returned", obtainedCodecName,
      expectedCodecName);
    assertEquals("Unexpected codec flag was returned", obtainedCodecFlag,
      expectedCompressionFlag);
  }
}
