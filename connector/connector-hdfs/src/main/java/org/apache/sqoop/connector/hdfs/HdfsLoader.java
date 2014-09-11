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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.sqoop.common.PrefixContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.hdfs.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.OutputFormat;
import org.apache.sqoop.connector.hdfs.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.hdfs.hdfsWriter.GenericHdfsWriter;
import org.apache.sqoop.connector.hdfs.hdfsWriter.HdfsSequenceWriter;
import org.apache.sqoop.connector.hdfs.hdfsWriter.HdfsTextWriter;
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.utils.ClassUtils;

import java.io.IOException;
import java.util.UUID;

public class HdfsLoader extends Loader<ConnectionConfiguration, ToJobConfiguration> {
  /**
   * Load data to target.
   *
   * @param context Loader context object
   * @param connection       Connection configuration
   * @param job      Job configuration
   * @throws Exception
   */
  @Override
  public void load(LoaderContext context, ConnectionConfiguration connection, ToJobConfiguration job) throws Exception {

    DataReader reader = context.getDataReader();

    Configuration conf = ((PrefixContext)context.getContext()).getConfiguration();

    String directoryName = job.output.outputDirectory;
    String codecname = getCompressionCodecName(job);

    CompressionCodec codec = null;
    if (codecname != null) {
      Class<?> clz = ClassUtils.loadClass(codecname);
      if (clz == null) {
        throw new SqoopException(HdfsConnectorError.GENERIC_HDFS_CONNECTOR_0003, codecname);
      }

      try {
        codec = (CompressionCodec) clz.newInstance();
        if (codec instanceof Configurable) {
          ((Configurable) codec).setConf(conf);
        }
      } catch (Exception e) {
        throw new SqoopException(HdfsConnectorError.GENERIC_HDFS_CONNECTOR_0004, codecname, e);
      }
    }

    String filename = directoryName + "/" + UUID.randomUUID() + getExtension(job,codec);

    try {
      Path filepath = new Path(filename);

      GenericHdfsWriter filewriter = getWriter(job);

      filewriter.initialize(filepath,conf,codec);

      String csv;

      while ((csv = reader.readTextRecord()) != null) {
        filewriter.write(csv);
      }
      filewriter.destroy();

    } catch (IOException e) {
      throw new SqoopException(HdfsConnectorError.GENERIC_HDFS_CONNECTOR_0005, e);
    }

  }

  private GenericHdfsWriter getWriter(ToJobConfiguration job) {
    if (job.output.outputFormat == OutputFormat.SEQUENCE_FILE)
      return new HdfsSequenceWriter();
    else
      return new HdfsTextWriter();
  }


  private String getCompressionCodecName(ToJobConfiguration jobConf) {
    if(jobConf.output.compression == null)
      return null;
    switch(jobConf.output.compression) {
      case NONE:
        return null;
      case DEFAULT:
        return "org.apache.hadoop.io.compress.DefaultCodec";
      case DEFLATE:
        return "org.apache.hadoop.io.compress.DeflateCodec";
      case GZIP:
        return "org.apache.hadoop.io.compress.GzipCodec";
      case BZIP2:
        return "org.apache.hadoop.io.compress.BZip2Codec";
      case LZO:
        return "com.hadoop.compression.lzo.LzoCodec";
      case LZ4:
        return "org.apache.hadoop.io.compress.Lz4Codec";
      case SNAPPY:
        return "org.apache.hadoop.io.compress.SnappyCodec";
      case CUSTOM:
        return jobConf.output.customCompression.trim();
    }
    return null;
  }

  //TODO: We should probably support configurable extensions at some point
  private static String getExtension(ToJobConfiguration job, CompressionCodec codec) {
    if (job.output.outputFormat == OutputFormat.SEQUENCE_FILE)
      return ".seq";
    if (codec == null)
      return ".txt";
    return codec.getDefaultExtension();
  }

}
