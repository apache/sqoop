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

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.common.SqoopIDFUtils;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.connector.hdfs.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.hdfs.hdfsWriter.GenericHdfsWriter;
import org.apache.sqoop.connector.hdfs.hdfsWriter.HdfsSequenceWriter;
import org.apache.sqoop.connector.hdfs.hdfsWriter.HdfsTextWriter;
import org.apache.sqoop.error.code.HdfsConnectorError;
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.utils.ClassUtils;

public class HdfsLoader extends Loader<LinkConfiguration, ToJobConfiguration> {

  private long rowsWritten = 0;

  /**
   * Load data to target.
   *
   * @param context Loader context object
   * @param linkConfiguration Link configuration
   * @param toJobConfig Job configuration
   * @throws Exception
   */
  @Override
  public void load(LoaderContext context, LinkConfiguration linkConfiguration,
                   ToJobConfiguration toJobConfig) throws Exception {
    Configuration conf = new Configuration();
    HdfsUtils.contextToConfiguration(context.getContext(), conf);

    DataReader reader = context.getDataReader();
    String directoryName = context.getString(HdfsConstants.WORK_DIRECTORY);
    String codecname = getCompressionCodecName(toJobConfig);

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

    String filename = directoryName + "/" + UUID.randomUUID() + getExtension(toJobConfig,codec);

    try {
      Path filepath = new Path(filename);

      GenericHdfsWriter filewriter = getWriter(toJobConfig);

      filewriter.initialize(filepath, conf, codec);

      if (HdfsUtils.hasCustomFormat(linkConfiguration, toJobConfig)) {
        Object[] record;

        while ((record = reader.readArrayRecord()) != null) {
          filewriter.write(
              SqoopIDFUtils.toCSV(
                  HdfsUtils.formatRecord(linkConfiguration, toJobConfig, record),
                  context.getSchema()));
          rowsWritten++;
        }
      } else {
        String record;

        while ((record = reader.readTextRecord()) != null) {
          filewriter.write(record);
          rowsWritten++;
        }
      }
      filewriter.destroy();

    } catch (IOException e) {
      throw new SqoopException(HdfsConnectorError.GENERIC_HDFS_CONNECTOR_0005, e);
    }

  }

  private GenericHdfsWriter getWriter(ToJobConfiguration toJobConf) {
    return (toJobConf.toJobConfig.outputFormat == ToFormat.SEQUENCE_FILE) ? new HdfsSequenceWriter()
        : new HdfsTextWriter();
  }

  private String getCompressionCodecName(ToJobConfiguration toJobConf) {
    if(toJobConf.toJobConfig.compression == null)
      return null;
    switch(toJobConf.toJobConfig.compression) {
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
        return toJobConf.toJobConfig.customCompression.trim();
    }
    return null;
  }

  //TODO: We should probably support configurable extensions at some point
  private static String getExtension(ToJobConfiguration toJobConf, CompressionCodec codec) {
    if (toJobConf.toJobConfig.outputFormat == ToFormat.SEQUENCE_FILE)
      return ".seq";
    if (codec == null)
      return ".txt";
    return codec.getDefaultExtension();
  }

  /* (non-Javadoc)
   * @see org.apache.sqoop.job.etl.Loader#getRowsWritten()
   */
  @Override
  public long getRowsWritten() {
    return rowsWritten;
  }

}
